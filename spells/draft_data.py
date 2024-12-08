"""
this is where calculations are performed on the 17Lands public data sets and
aggregate calculations are returned.

Aggregate dataframes containing raw counts are cached in the local file system
for performance.
"""

import datetime
import functools
import hashlib
import re
from inspect import signature
from typing import Callable, TypeVar, Any

import polars as pl

from spells.external import data_file_path
import spells.cache
import spells.filter
import spells.manifest
from spells.columns import ColumnDefinition, ColumnSpec
from spells.enums import View, ColName, ColType


DF = TypeVar("DF", pl.LazyFrame, pl.DataFrame)


def _cache_key(args) -> str:
    """
    cache arguments by __str__ (based on the current value of a mutable, so be careful)
    """
    return hashlib.md5(str(args).encode("utf-8")).hexdigest()


@functools.lru_cache(maxsize=None)
def _get_names(set_code: str) -> list[str]:
    card_fp = data_file_path(set_code, View.CARD)
    card_view = pl.read_parquet(card_fp)
    card_names_set = frozenset(card_view.get_column("name").to_list())

    draft_fp = data_file_path(set_code, View.DRAFT)
    draft_view = pl.scan_parquet(draft_fp)
    cols = draft_view.collect_schema().names()

    prefix = "pack_card_"
    names = [col[len(prefix) :] for col in cols if col.startswith(prefix)]
    draft_names_set = frozenset(names)

    assert (
        draft_names_set == card_names_set
    ), "names mismatch between card and draft file"
    return names


def _get_card_context(set_code: str, col_spec_map: dict[str, ColumnSpec]) -> dict[str, dict[str, Any]]:
    card_attr_specs = {col:spec for col, spec in col_spec_map.items() if spec.col_type == ColType.CARD_ATTR or spec.name == ColName.NAME}
    col_def_map = _hydrate_col_defs(set_code, card_attr_specs, card_only=True)

    columns = list(col_def_map.keys())

    fp = data_file_path(set_code, View.CARD)
    card_df = pl.read_parquet(fp)
    select_rows = _view_select(
        card_df, frozenset(columns), col_def_map, is_agg_view=False
    ).to_dicts()

    card_context = {row[ColName.NAME]: row for row in select_rows}

    return card_context
    

def _determine_expression(spec: ColumnSpec, names: list[str], card_context: dict[str, dict]) -> pl.Expr | tuple[pl.Expr, ...]:
    def seed_params(expr):
        params = {}

        sig_params = signature(expr).parameters
        if 'names' in sig_params:
            params['names'] = names
        if 'card_context' in sig_params:
            params['card_context'] = card_context
        return params

    if spec.col_type == ColType.NAME_SUM:
        if spec.expr is not None:
            assert isinstance(spec.expr, Callable), f"NAME_SUM column {spec.name} must have a callable `expr` accepting a `name` argument"
            unnamed_exprs = [spec.expr(**{'name': name, **seed_params(spec.expr)}) for name in names]

            expr = tuple(
                map(
                    lambda ex, name: ex.alias(f"{spec.name}_{name}"),
                    unnamed_exprs,
                    names,
                )
            )
        else:
            expr = tuple(map(lambda name: pl.col(f"{spec.name}_{name}"), names))

    elif spec.expr is not None:
        if isinstance(spec.expr, Callable):
            params = seed_params(spec.expr)
            if spec.col_type == ColType.PICK_SUM and 'name' in signature(spec.expr).parameters:
                expr = pl.lit(None)
                for name in names:
                    name_params = {'name': name, **params}
                    expr = pl.when(pl.col(ColName.PICK) == name).then(spec.expr(**name_params)).otherwise(expr)
            else:
                expr = spec.expr(**params)
        else:
            expr = spec.expr
        expr = expr.alias(spec.name)
    else:
        expr = pl.col(spec.name)

    return expr


def _infer_dependencies(name: str, expr: pl.Expr | tuple[pl.Expr,...], col_spec_map: dict[str, ColumnSpec], names: list[str]) -> set[str]:
    dependencies = set()
    tricky_ones = set()

    if isinstance(expr, pl.Expr):
        dep_cols = [c for c in expr.meta.root_names() if c != name]
        for dep_col in dep_cols:
            if dep_col in col_spec_map.keys():
                dependencies.add(dep_col)
            else: 
                tricky_ones.add(dep_col)
    else:
        for idx, exp in enumerate(expr):
            pattern = f"_{names[idx]}$"
            dep_cols = [c for c in exp.meta.root_names() if c != name]
            for dep_col in dep_cols:
                if dep_col in col_spec_map.keys():
                    dependencies.add(dep_col)
                elif len(split := re.split(pattern, dep_col)) == 2 and split[0] in col_spec_map:
                    dependencies.add(split[0])
                else:
                    tricky_ones.add(dep_col)

    for item in tricky_ones:
        found = False
        for n in names:
            pattern = f"_{n}$"
            if not found and len(split := re.split(pattern, item)) == 2 and split[0] in col_spec_map:
                dependencies.add(split[0])
                found = True
        assert found, f"Could not locate column spec for root col {item}" 

    return dependencies


def _hydrate_col_defs(set_code: str, col_spec_map: dict[str, ColumnSpec], card_only=False):
    names = _get_names(set_code)

    if card_only:
        card_context = {}
    else:
        card_context = _get_card_context(set_code, col_spec_map)

    assert len(names) > 0, "there should be names"
    hydrated = {}
    for key, spec in col_spec_map.items():
        expr = _determine_expression(spec, names, card_context)
        dependencies = _infer_dependencies(key, expr, col_spec_map, names)

        try:
            sig_expr = expr if isinstance(expr, pl.Expr) else expr[0]
            expr_sig = sig_expr.meta.serialize(
                format="json"
            )  # not compatible with renaming
        except pl.exceptions.ComputeError:
            if spec.version is not None:
                expr_sig = spec.name + spec.version
            else:
                expr_sig = str(datetime.datetime.now)

        signature = str(
            (
                spec.name,
                spec.col_type.value,
                expr_sig,
                dependencies,
            )
        )

        cdef = ColumnDefinition(
            name=spec.name,
            col_type=spec.col_type,
            views=set(spec.views or set()),
            expr=expr,
            dependencies=dependencies,
            signature=signature,
        )
        hydrated[key] = cdef
    return hydrated


def _view_select(
    df: DF,
    view_cols: frozenset[str],
    col_def_map: dict[str, ColumnDefinition],
    is_agg_view: bool,
) -> DF:
    base_cols = frozenset()
    cdefs = [col_def_map[c] for c in view_cols]
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
        if spells.cache.cache_exists(set_code, key):
            return spells.cache.read_cache(set_code, key)

    df = calc_fn()

    if write_cache:
        spells.cache.write_cache(set_code, key, df)

    return df


def _base_agg_df(
    set_code: str,
    m: spells.manifest.Manifest,
    use_streaming: bool = False,
) -> pl.DataFrame:
    join_dfs = []
    group_by = m.base_view_group_by

    is_name_gb = ColName.NAME in group_by
    nonname_gb = tuple(gb for gb in group_by if gb != ColName.NAME)

    for view, cols_for_view in m.view_cols.items():
        if view == View.CARD:
            continue
        df_path = data_file_path(set_code, view)
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
        if sum_cols:
            # manifest will verify that GAME_SUM manifests do not use NAME grouping
            name_col_tuple = (
                (pl.col(ColName.PICK).alias(ColName.NAME),) if is_name_gb else ()
            )

            sum_col_df = base_df.select(nonname_gb + name_col_tuple + sum_cols)
            join_dfs.append(
                sum_col_df.group_by(group_by).sum().collect(streaming=use_streaming)
            )

        name_sum_cols = tuple(
            c for c in cols_for_view if m.col_def_map[c].col_type == ColType.NAME_SUM
        )
        for col in name_sum_cols:
            cdef = m.col_def_map[col]
            pattern = f"^{cdef.name}_"
            name_map = functools.partial(
                lambda patt, name: re.split(patt, name)[1], pattern
            )

            expr = pl.col(f"^{cdef.name}_.*$").name.map(name_map)
            pre_agg_df = base_df.select((expr,) + nonname_gb)

            if nonname_gb:
                agg_df = pre_agg_df.group_by(nonname_gb).sum()
            else:
                agg_df = pre_agg_df.sum()

            index = nonname_gb if nonname_gb else None
            unpivoted = agg_df.unpivot(
                index=index,
                value_name=m.col_def_map[col].name,
                variable_name=ColName.NAME,
            )

            if not is_name_gb:
                df = (
                    unpivoted.drop("name")
                    .group_by(nonname_gb)
                    .sum()
                    .collect(streaming=use_streaming)
                )
            else:
                df = unpivoted.collect(streaming=use_streaming)

            join_dfs.append(df)

    return functools.reduce(
        lambda prev, curr: prev.join(curr, on=group_by, how="outer", coalesce=True),
        join_dfs,
    )


def summon(
    set_code: str,
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: list[ColumnSpec] | None = None,
    use_streaming: bool = False,
    read_cache: bool = True,
    write_cache: bool = True,
) -> pl.DataFrame:
    col_spec_map = dict(spells.columns.col_spec_map)
    if extensions is not None:
        for spec in extensions:
            col_spec_map[spec.name] = spec

    col_def_map = _hydrate_col_defs(set_code, col_spec_map)
    m = spells.manifest.create(col_def_map, columns, group_by, filter_spec)

    calc_fn = functools.partial(_base_agg_df, set_code, m, use_streaming=use_streaming)
    agg_df = _fetch_or_cache(
        calc_fn,
        set_code,
        (
            set_code,
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
        fp = data_file_path(set_code, View.CARD)
        card_df = pl.read_parquet(fp)
        select_df = _view_select(card_df, card_cols, m.col_def_map, is_agg_view=False)
        agg_df = agg_df.join(select_df, on="name", how="outer", coalesce=True)

        if ColName.NAME not in m.group_by:
            agg_df = agg_df.group_by(m.group_by).sum()

    ret_cols = m.group_by + m.columns
    ret_df = (
        _view_select(agg_df, frozenset(ret_cols), m.col_def_map, is_agg_view=True)
        .select(ret_cols)
        .sort(m.group_by)
    )

    return ret_df
