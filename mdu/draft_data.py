"""
this is where calculations are performed on the 17Lands public data sets and
aggregate calculations are returned.

Aggregate dataframes containing raw counts are cached in the local file system
for performance.
"""

import functools
from typing import Callable

import polars as pl
import pandas as pd

from mdu.cache_17l import data_file_path
from mdu.get_schema import schema
import mdu.cache
import mdu.filter
import mdu.manifest
from mdu.columns import ColumnDefinition
from mdu.enums import View, ColName, ColType


def cache_key(*args) -> str:
    return hex(hash(str(args)))[3:]


def col_df(
    df: pl.LazyFrame,
    col: str,
    col_def_map: dict[str, ColumnDefinition],
    is_view: bool,
):
    cdef = col_def_map[col]
    if not cdef.dependencies:
        return df.select(cdef.expr)

    root_col_exprs = []
    col_dfs = []
    for dep in cdef.dependencies:
        dep_def = col_def_map[dep]
        if dep_def.dependencies is None or is_view and len(dep_def.views):
            root_col_exprs.append(dep_def.expr)
        else:
            col_dfs.append(col_df(df, dep, col_def_map, is_view))

    if root_col_exprs:
        col_dfs.append(df.select(root_col_exprs))

    dep_df = pl.concat(col_dfs, how="horizontal")
    return dep_df.select(cdef.expr)


def fetch_or_cache(
    calc_fn: Callable,
    set_code: str,
    args: list,
    read_cache: bool = True,
    write_cache: bool = True,
):
    key = cache_key(args)

    if read_cache:
        if mdu.cache.cache_exists(set_code, key):
            return mdu.cache.read_cache(set_code, key)

    df = calc_fn()

    if write_cache:
        mdu.cache.write_cache(set_code, key, df)

    return df


def base_agg_df(
    set_code: str,
    m: mdu.manifest.Manifest,
    use_streaming: bool = False,
) -> pl.DataFrame:
    base_views = [View.DRAFT, View.GAME]

    agg_dfs = {}
    for view in base_views:
        view_cols = m.view_cols[view]
        df_path = data_file_path(set_code, view)
        base_view_df = pl.scan_csv(df_path, schema=schema(df_path))
        col_dfs = [col_df(base_view_df, col, m.col_def_map, is_view=True) for col in view_cols]
        base_df_prefilter = pl.concat(col_dfs, how="horizontal")

        if m.dd_filter is not None:
            base_df = base_df_prefilter.filter(m.dd_filter.expr)
        else:
            base_df = base_df_prefilter

        groupbys = m.base_view_groupbys
        is_name_gb = ColName.NAME in groupbys
        nonname_gb = tuple(gb for gb in groupbys if gb != ColName.NAME)

        pick_sum_cols = tuple(c for c in view_cols if m.col_def_map[c].col_type == ColType.PICK_SUM)
        if pick_sum_cols:
            name_col_tuple = (pl.col(ColName.PICK).alias(ColName.NAME),) if is_name_gb else ()

            pick_df = base_df.select(nonname_gb + name_col_tuple + pick_sum_cols)
            pick_agg_df = ( # tuple 
                pick_df.group_by(groupbys).sum(),
            ) 
        else:
            pick_agg_df = ()

        

    return pl.DataFrame()


def metrics(
    set_code: str,
    columns: list[str] | None = None,
    groupbys: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: list[ColumnDefinition] | None = None,
    as_pandas: bool = False,
    use_streaming: bool = False,
    read_cache: bool = True,
    write_cache: bool = True,
) -> pl.DataFrame | pd.DataFrame | None:
    m = mdu.manifest.create(columns, groupbys, filter_spec, extensions)

    calc_fn = functools.partial(base_agg_df, set_code, use_streaming=use_streaming)
    agg_df = fetch_or_cache(calc_fn, set_code, [set_code, m], read_cache=read_cache, write_cache=write_cache)
