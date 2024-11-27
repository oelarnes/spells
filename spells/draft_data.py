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
from typing import Callable, TypeVar

import polars as pl

from spells.external import data_file_path
from spells.schema import schema
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
def _get_names(set_code: str) -> tuple[str, ...]:
    card_fp = data_file_path(set_code, View.CARD)
    card_view = pl.read_csv(card_fp)
    card_names_set = frozenset(card_view.get_column("name").to_list())

    game_fp = data_file_path(set_code, View.GAME)
    game_view = pl.scan_csv(game_fp, schema=schema(game_fp))
    cols = game_view.collect_schema().names()

    names = tuple(col[5:] for col in cols if col.startswith("deck_"))
    game_names_set = frozenset(names)

    assert game_names_set == card_names_set, "names mismatch between card and game file"
    return names


def _hydrate_col_defs(set_code: str, col_spec_map: dict[str, ColumnSpec]):
    names = _get_names(set_code)
    assert len(names) > 0, "there should be names"
    hydrated = {}
    for key, spec in col_spec_map.items():
        if spec.col_type == ColType.NAME_SUM and spec.exprMap is not None:
            unnamed_exprs = map(spec.exprMap, names)
            expr = tuple(
                map(
                    lambda ex, name: ex.alias(f"{spec.name}_{name}"),
                    unnamed_exprs,
                    names,
                )
            )
        elif spec.expr is not None:
            expr = spec.expr.alias(spec.name)

        else:
            if spec.col_type == ColType.NAME_SUM:
                expr = tuple(map(lambda name: pl.col(f"{spec.name}_{name}"), names))
            else:
                expr = pl.col(spec.name)

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

        dependencies = tuple(spec.dependencies or ())
        signature = str(
            (
                spec.name,
                spec.col_type.value,
                expr_sig,
                tuple(view.value for view in spec.views),
                dependencies,
            )
        )
        cdef = ColumnDefinition(
            name=spec.name,
            col_type=spec.col_type,
            views=spec.views,
            expr=expr,
            dependencies=dependencies,
            signature=signature,
        )
        hydrated[key] = cdef
    return hydrated


def _col_df(
    df: DF,
    col: str,
    col_def_map: dict[str, ColumnDefinition],
    is_view: bool,
    anchor_col: str = "",
) -> DF:
    cdef = col_def_map[col]
    if not is_view and cdef.col_type != ColType.AGG:
        return df.select(pl.col(cdef.name))
    if not cdef.dependencies and is_view:
        return df.select(cdef.expr)
    assert cdef.dependencies, f"Column {col} should declare its dependencies"

    col_dfs = []
    for dep in cdef.dependencies:
        dep_def = col_def_map[dep]
        if not is_view and dep_def.col_type != ColType.AGG:
            col_dfs.append(df.select(pl.col(dep_def.name)))
        elif not dep_def.dependencies and is_view:
            col_dfs.append(df.select(dep_def.expr))
        else:
            col_dfs.append(_col_df(df, dep, col_def_map, is_view, anchor_col))

    if anchor_col != "":
        col_dfs.append(df.select(anchor_col))

    dep_df = pl.concat(col_dfs, how="horizontal")

    if anchor_col != "":
        res_df = dep_df.select([anchor_col, cdef.expr]).drop(anchor_col)
    else:
        res_df = dep_df.select(cdef.expr)

    return res_df


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
        base_view_df = pl.scan_csv(df_path, schema=schema(df_path))
        col_dfs = [
            _col_df(base_view_df, col, m.col_def_map, is_view=True)
            for col in cols_for_view
        ]
        base_df_prefilter = pl.concat(col_dfs, how="horizontal")

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
        card_df = pl.read_csv(fp)
        cols_df = pl.concat(
            [_col_df(card_df, col, m.col_def_map, is_view=True) for col in card_cols],
            how="horizontal",
        )

        agg_df = agg_df.join(cols_df, on="name", how="outer", coalesce=True)
        if ColName.NAME not in m.group_by:
            agg_df = agg_df.group_by(m.group_by).sum()

    ret_cols = m.group_by + m.columns
    ret_df = pl.concat(
        [
            _col_df(agg_df, col, m.col_def_map, is_view=False, anchor_col=m.group_by[0])
            for col in ret_cols
        ],
        how="horizontal",
    ).sort(m.group_by)

    return ret_df
