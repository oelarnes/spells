"""
this is where calculations are performed on the 17Lands public data sets and
aggregate calculations are returned.

Aggregate dataframes containing raw counts are cached in the local file system
for performance.
"""

import functools
import hashlib
import re
import logging
from inspect import signature
import os
from typing import Callable, TypeVar, Any

import polars as pl
from polars.exceptions import ColumnNotFoundError

from spells import cache
import spells.filter
import spells.manifest
from spells.columns import ColDef, ColSpec, get_specs
from spells.enums import View, ColName, ColType
from spells.log import make_verbose

DF = TypeVar("DF", pl.LazyFrame, pl.DataFrame)


def _cache_key(args) -> str:
    """
    cache arguments by __str__ (based on the current value of a mutable, so be careful)
    """
    return hashlib.md5(str(args).encode("utf-8")).hexdigest()


@functools.lru_cache(maxsize=None)
def get_names(set_code: str) -> list[str]:
    card_fp = cache.data_file_path(set_code, View.CARD)
    card_view = pl.read_parquet(card_fp)
    card_names_set = frozenset(card_view.get_column("name").to_list())

    draft_fp = cache.data_file_path(set_code, View.DRAFT)
    draft_view = pl.scan_parquet(draft_fp)
    cols = draft_view.collect_schema().names()

    prefix = "pack_card_"
    names = [col[len(prefix) :] for col in cols if col.startswith(prefix)]
    draft_names_set = frozenset(names)

    assert (
        draft_names_set == card_names_set
    ), "names mismatch between card and draft file"
    return names


def _get_card_context(
    set_code: str,
    specs: dict[str, ColSpec],
    card_context: pl.DataFrame | dict[str, dict[str, Any]] | None,
    set_context: pl.DataFrame | dict[str, Any] | None,
    card_only: bool = False,
) -> dict[str, dict[str, Any]]:
    card_attr_specs = {
        col: spec
        for col, spec in specs.items()
        if spec.col_type == ColType.CARD_ATTR or col == ColName.NAME
    }

    if not card_only:
        col_def_map = _hydrate_col_defs(
            set_code,
            card_attr_specs,
            set_context=set_context,
            card_context=card_context,
            card_only=True,
        )

        columns = list(col_def_map.keys())

        fp = cache.data_file_path(set_code, View.CARD)
        card_df = pl.read_parquet(fp)
        select_rows = _view_select(
            card_df, frozenset(columns), col_def_map, is_agg_view=False
        ).to_dicts()

        names = get_names(set_code)
        loaded_context = {row[ColName.NAME]: row for row in select_rows}

        for name in names:
            loaded_context[name] = loaded_context.get(name, {})
    else:
        names = get_names(set_code)
        loaded_context = {name: {} for name in names}

    if card_context is not None:
        if isinstance(card_context, pl.DataFrame):
            try:
                card_context = {
                    row[ColName.NAME]: row for row in card_context.to_dicts()
                }
            except ColumnNotFoundError:
                raise ValueError("card_context DataFrame must have column 'name'")

        names = list(loaded_context.keys())
        for name in names:
            for col, value in card_context.get(name, {}).items():
                loaded_context[name][col] = value

    return loaded_context


def _determine_expression(
    col: str,
    spec: ColSpec,
    names: list[str],
    card_context: dict[str, dict],
    set_context: dict[str, Any],
) -> pl.Expr | tuple[pl.Expr, ...]:
    def seed_params(expr):
        params = {}

        sig_params = signature(expr).parameters
        if "names" in sig_params:
            params["names"] = names
        if "card_context" in sig_params:
            params["card_context"] = card_context
        if "set_context" in sig_params:
            params["set_context"] = set_context
        return params

    if spec.col_type == ColType.NAME_SUM:
        if spec.expr is not None:
            try:
                assert isinstance(
                    spec.expr, Callable
                ), f"NAME_SUM column {col} must have a callable `expr` accepting a `name` argument"
                unnamed_exprs = [
                    spec.expr(**{"name": name, **seed_params(spec.expr)})
                    for name in names
                ]

                expr = tuple(
                    map(
                        lambda ex, name: ex.alias(f"{col}_{name}"),
                        unnamed_exprs,
                        names,
                    )
                )
            except KeyError:
                logging.warning(
                    f"KeyError raised in calculation of {col}, probably caused by "
                    + "missing context. Column will have value `None`"
                )
                expr = tuple(pl.lit(None).alias(f"{col}_{name}") for name in names)
        else:
            expr = tuple(map(lambda name: pl.col(f"{col}_{name}"), names))

    elif spec.expr is not None:
        if isinstance(spec.expr, Callable):
            try:
                assert (
                    not spec.col_type == ColType.AGG
                ), f"AGG column {col} must be a pure spells expression"
                params = seed_params(spec.expr)
                if (
                    spec.col_type in (ColType.PICK_SUM, ColType.CARD_ATTR)
                    and "name" in signature(spec.expr).parameters
                ):
                    condition_col = (
                        ColName.PICK
                        if spec.col_type == ColType.PICK_SUM
                        else ColName.NAME
                    )
                    expr = pl.lit(None)
                    for name in names:
                        name_params = {"name": name, **params}
                        expr = (
                            pl.when(pl.col(condition_col) == name)
                            .then(spec.expr(**name_params))
                            .otherwise(expr)
                        )
                else:
                    expr = spec.expr(**params)
            except KeyError:
                expr = pl.lit(None)
        else:
            expr = spec.expr
        expr = expr.alias(col)

        if spec.col_type == ColType.AGG:
            expr = expr.fill_nan(None)
    else:
        expr = pl.col(col)

    return expr


def _infer_dependencies(
    name: str,
    expr: pl.Expr | tuple[pl.Expr, ...],
    specs: dict[str, ColSpec],
    names: list[str],
) -> set[str]:
    dependencies = set()
    tricky_ones = set()

    if isinstance(expr, pl.Expr):
        dep_cols = [c for c in expr.meta.root_names() if c != name]
        for dep_col in dep_cols:
            if dep_col in specs.keys():
                dependencies.add(dep_col)
            else:
                tricky_ones.add(dep_col)
    else:
        for idx, exp in enumerate(expr):
            pattern = f"_{names[idx]}$"
            dep_cols = [c for c in exp.meta.root_names() if c != name]
            for dep_col in dep_cols:
                if dep_col in specs.keys():
                    dependencies.add(dep_col)
                elif (
                    len(split := re.split(pattern, dep_col)) == 2 and split[0] in specs
                ):
                    dependencies.add(split[0])
                else:
                    tricky_ones.add(dep_col)

    for item in tricky_ones:
        found = False
        for n in names:
            pattern = f"_{n}$"
            if (
                not found
                and len(split := re.split(pattern, item)) == 2
                and split[0] in specs
            ):
                dependencies.add(split[0])
                found = True
        if not found:
            logging.warning(
                f"No column definition found matching dependency {item}! "
                + "`summon` will fail if called with this column"
            )

    return dependencies


def _get_set_context(
    set_code: str, set_context: pl.DataFrame | dict[str, Any] | None
) -> dict[str, Any]:
    context_fp = cache.data_file_path(set_code, "context")

    context = {}
    if os.path.isfile(context_fp):
        context_df = pl.read_parquet(context_fp)
        if len(context_df) == 1:
            context.update(context_df.to_dicts()[0])

    if isinstance(set_context, pl.DataFrame):
        assert len(set_context != 1), "Invalid set context provided"
        context.update(set_context.to_dicts()[0])
    elif isinstance(set_context, dict):
        context.update(set_context)

    return context


def _hydrate_col_defs(
    set_code: str,
    specs: dict[str, ColSpec],
    card_context: pl.DataFrame | dict[str, dict] | None = None,
    set_context: pl.DataFrame | dict[str, Any] | None = None,
    card_only: bool = False,
):
    names = get_names(set_code)

    set_context = _get_set_context(set_code, set_context)

    card_context = _get_card_context(
        set_code, specs, card_context, set_context, card_only=card_only
    )

    assert len(names) > 0, "there should be names"
    hydrated = {}
    for col, spec in specs.items():
        expr = _determine_expression(col, spec, names, card_context, set_context)
        dependencies = _infer_dependencies(col, expr, specs, names)

        sig_expr = expr if isinstance(expr, pl.Expr) else expr[0]
        try:
            expr_sig = sig_expr.meta.serialize(format="json")
        except pl.exceptions.ComputeError:
            if spec.version is not None:
                expr_sig = col + spec.version
            else:
                print(
                    f"Using session-only signature for non-serializable column {col}, please provide a version value"
                )
                expr_sig = str(sig_expr)

        signature = str(
            (
                col,
                spec.col_type.value,
                expr_sig,
                sorted(dependencies),
            )
        )

        cdef = ColDef(
            name=col,
            col_type=spec.col_type,
            views=set(spec.views or set()),
            expr=expr,
            dependencies=dependencies,
            signature=signature,
        )
        hydrated[col] = cdef
    return hydrated


def _view_select(
    df: DF,
    view_cols: frozenset[str],
    col_def_map: dict[str, ColDef],
    is_agg_view: bool,
) -> DF:
    base_cols = frozenset()
    cdefs = [col_def_map[c] for c in sorted(view_cols)]
    select = []
    for cdef in cdefs:
        if is_agg_view:
            if cdef.col_type == ColType.AGG:
                base_cols = base_cols.union(cdef.dependencies)
                select.append(cdef.expr)
            else:
                base_cols = base_cols.union(frozenset({cdef.name}))
                select.append(cdef.name)
        else:
            if cdef.dependencies:
                base_cols = base_cols.union(cdef.dependencies)
            else:
                base_cols = base_cols.union(frozenset({cdef.name}))
            if isinstance(cdef.expr, tuple):
                select.extend(cdef.expr)
            else:
                select.append(cdef.expr)

    if base_cols != view_cols:
        df = _view_select(df, base_cols, col_def_map, is_agg_view)

    return df.select(select)


def _fetch_or_cache(
    calc_fn: Callable,
    set_code: str,
    cache_args,
    read_cache: bool = True,
    write_cache: bool = True,
):
    key = _cache_key(cache_args)

    if read_cache:
        if cache.cache_exists(set_code, key):
            logging.info(f"Cache {key} found")
            return cache.read_cache(set_code, key)

    logging.info("Cache not found, calculating")
    logging.debug(f"Signature:\n{cache_args}")
    df = calc_fn()

    if write_cache:
        cache.write_cache(set_code, key, df)

    return df


def _base_agg_df(
    set_code: str,
    m: spells.manifest.Manifest,
    use_streaming: bool = True,
) -> pl.DataFrame:
    join_dfs = []
    group_by = m.base_view_group_by

    is_name_gb = ColName.NAME in group_by
    nonname_gb = tuple(gb for gb in group_by if gb != ColName.NAME)

    for view, cols_for_view in m.view_cols.items():
        if view == View.CARD:
            continue
        df_path = cache.data_file_path(set_code, view)
        base_view_df = pl.scan_parquet(df_path)
        base_df_prefilter = _view_select(
            base_view_df, cols_for_view, m.col_def_map, is_agg_view=False
        )

        if m.filter is not None:
            base_df = base_df_prefilter.filter(m.filter.expr)
        else:
            base_df = base_df_prefilter

        sum_cols = tuple(
            c
            for c in cols_for_view
            if m.col_def_map[c].col_type in (ColType.PICK_SUM, ColType.GAME_SUM)
        )
        name_sum_cols = tuple(
            c for c in cols_for_view if m.col_def_map[c].col_type == ColType.NAME_SUM
        )

        if sum_cols or not name_sum_cols:
            # manifest will verify that GAME_SUM manifests do not use NAME grouping
            name_col_tuple = (
                (pl.col(ColName.PICK).alias(ColName.NAME),) if is_name_gb else ()
            )

            sum_col_df = base_df.select(nonname_gb + name_col_tuple + sum_cols)

            grouped = sum_col_df.group_by(group_by) if group_by else sum_col_df
            join_dfs.append(grouped.sum().collect(streaming=use_streaming))

        for col in name_sum_cols:
            names = get_names(set_code)
            expr = tuple(pl.col(f"{col}_{name}").alias(name) for name in names)

            pre_agg_df = base_df.select(expr + nonname_gb)

            if nonname_gb:
                agg_df = pre_agg_df.group_by(nonname_gb).sum()
            else:
                agg_df = pre_agg_df.sum()

            index = nonname_gb if nonname_gb else None
            unpivoted = agg_df.unpivot(
                index=index,
                value_name=col,
                variable_name=ColName.NAME,
            )

            if not is_name_gb:
                grouped = (
                    unpivoted.drop("name").group_by(nonname_gb)
                    if nonname_gb
                    else unpivoted.drop("name")
                )
                df = grouped.sum().collect(streaming=use_streaming)
            else:
                df = unpivoted.collect(streaming=use_streaming)

            join_dfs.append(df)

    if group_by:
        joined_df = functools.reduce(
            lambda prev, curr: prev.join(curr, on=group_by, how="outer", coalesce=True),
            join_dfs,
        )
    else:
        joined_df = pl.concat(join_dfs, how="horizontal")

    joined_df = joined_df.select(sorted(joined_df.schema.names()))
    return joined_df


@make_verbose()
def summon(
    set_code: str | list[str],
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: dict[str, ColSpec] | list[dict[str, ColSpec]] | None = None,
    use_streaming: bool = True,
    read_cache: bool = True,
    write_cache: bool = True,
    card_context: pl.DataFrame | dict[str, Any] | None = None,
    set_context: pl.DataFrame | dict[str, Any] | None = None,
) -> pl.DataFrame:
    specs = get_specs()

    if extensions is not None:
        if not isinstance(extensions, list):
            extensions = [extensions]
        for ext in extensions:
            specs.update(ext)

    if isinstance(set_code, str):
        if not (isinstance(card_context, dict) and set_code in card_context):
            card_context = {set_code: card_context}
        if not (isinstance(set_context, dict) and set_code in set_context):
            set_context = {set_code: set_context}
        codes = [set_code]
    else:
        codes = set_code

    assert codes, "Please ask for at least one set"

    m = None

    concat_dfs = []
    for code in codes:
        logging.info(f"Calculating agg df for {code}")
        if isinstance(card_context, pl.DataFrame):
            set_card_context = card_context.filter(pl.col("expansion") == code)
        elif isinstance(card_context, dict):
            set_card_context = card_context[code]
        else:
            set_card_context = None

        if isinstance(set_context, pl.DataFrame):
            this_set_context = set_context.filter(pl.col("expansion") == code)
        elif isinstance(set_context, dict):
            this_set_context = set_context[code]
        else:
            this_set_context = None

        col_def_map = _hydrate_col_defs(code, specs, set_card_context, this_set_context)
        m = spells.manifest.create(col_def_map, columns, group_by, filter_spec)

        calc_fn = functools.partial(_base_agg_df, code, m, use_streaming=use_streaming)
        agg_df = _fetch_or_cache(
            calc_fn,
            code,
            (
                code,
                sorted(m.view_cols.get(View.DRAFT, set())),
                sorted(m.view_cols.get(View.GAME, set())),
                sorted(c.signature or "" for c in m.col_def_map.values()),
                sorted(m.base_view_group_by),
                filter_spec,
            ),
            read_cache=read_cache,
            write_cache=write_cache,
        )

        if View.CARD in m.view_cols:
            card_cols = m.view_cols[View.CARD].union({ColName.NAME})
            fp = cache.data_file_path(code, View.CARD)
            card_df = pl.read_parquet(fp)
            select_df = _view_select(
                card_df, card_cols, m.col_def_map, is_agg_view=False
            )
            agg_df = agg_df.join(select_df, on="name", how="outer", coalesce=True)
        concat_dfs.append(agg_df)

    full_agg_df = pl.concat(concat_dfs, how="vertical")

    assert (
        m is not None
    ), "What happened? We mean to use one of the sets manifest, it shouldn't matter which."

    if m.group_by:
        gb = m.group_by
        # an agg may depend on some card column that hasn't been explicitly requested, but can be safely
        # depended on if we are grouping by name
        if ColName.NAME in m.group_by and View.CARD in m.view_cols:
            for col in m.view_cols[View.CARD]:
                if col not in m.group_by:
                    gb = tuple([*gb, col])
        full_agg_df = full_agg_df.group_by(gb).sum()
    else:
        full_agg_df = full_agg_df.sum()

    ret_cols = m.group_by + m.columns
    ret_df = (
        _view_select(full_agg_df, frozenset(ret_cols), m.col_def_map, is_agg_view=True)
        .select(ret_cols)
        .sort(m.group_by)
    )

    return ret_df


def view_select(
    set_code: str,
    view: View,
    columns: list[str],
    filter_spec: dict | None = None,
    extensions: dict[str, ColSpec] | list[dict[str, ColSpec]] | None = None,
    card_context: dict | pl.DataFrame | None = None,
    set_context: dict | pl.DataFrame | None = None,
) -> pl.LazyFrame:
    specs = get_specs()

    if extensions is not None:
        if not isinstance(extensions, list):
            extensions = [extensions]
        for ext in extensions:
            specs.update(ext)

    col_def_map = _hydrate_col_defs(set_code, specs, card_context, set_context)

    df_path = cache.data_file_path(set_code, view)
    base_view_df = pl.scan_parquet(df_path)

    select_cols = frozenset(columns)

    filter_ = spells.filter.from_spec(filter_spec)
    if filter_ is not None:
        select_cols = select_cols.union(filter_.lhs)

    base_df_prefilter = _view_select(
        base_view_df,
        select_cols,
        col_def_map,
        is_agg_view=False,
    )

    if filter_ is not None:
        base_df = base_df_prefilter.filter(filter_.expr)
    else:
        base_df = base_df_prefilter

    select_defs = [col_def_map[c] for c in columns]
    select_names = []
    for d in select_defs:
        if d.col_type == ColType.NAME_SUM:
            select_names.extend([expr.meta.output_name() for expr in d.expr])
        else:
            select_names.append(d.expr.meta.output_name())

    return base_df.select(select_names)
