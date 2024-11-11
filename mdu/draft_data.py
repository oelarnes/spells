"""
this is where calculations are performed on the 17Lands public data sets and
aggregate calculations are returned.

In general, one loads a DraftData object for a certain set with a specified
filter, then asks for pandas DataFrame with certain metrics, grouped by
certain attributes. By default, grouping is by card name.

```
pandas.options.display.max_rows = 1000
ddo = DraftData('BLB', filter={'rank': 'mythic'})
df = ddo.df(['gpwr', 'ata', 'deq_base', 'deq']) # see notebooks/deq.ipynb for info on DEq
df
```

Dask is used to distribute computation among manageable chunks so that 
entire files do not need to be read into memory. 

Aggregate dataframes containing raw counts are cached in the local file system
for performance.
"""

import polars as pl
import pandas as pd

from mdu.cache_17l import data_file_path
from mdu.get_schema import schema
import mdu.cache
import mdu.filter
import mdu.columns as mcol
from mdu.columns import ColumnDefinition
from mdu.enums import View, ColName, ColType


def cache_key(*args, **kwargs) -> str:
    arg_str = str(args) + str(kwargs)
    hash_num = hash(arg_str)
    return hex(hash_num)[3:]


def resolve_base_view_cols(
    col_set: frozenset[str],
    col_def_map: dict[str, ColumnDefinition],
) -> dict[View, frozenset[str]]:
    """
    For each base view ('game' and 'draft'), return the columns
    that must be present at the aggregation step. 'name' need not be
    included, and 'pick' will be added if needed.

    Dependencies within base views will be resolved by `col_df`.
    """
    unresolved_cols = col_set
    view_resolution = {}

    iter = 0
    while unresolved_cols and iter<100:
        iter += 1
        next_cols = frozenset()
        for col in unresolved_cols:
            cdef = col_def_map[col]
            if cdef.col_type == ColType.PICK_SUM:
                view_resolution[View.DRAFT] = view_resolution.get(View.DRAFT, frozenset()).union({ColName.PICK})
            if cdef.base_views:
                for view in cdef.base_views:
                    view_resolution[view] = view_resolution.get(view, frozenset()).union({col})
            else:
                if cdef.dependencies is None:
                    raise ValueError(
                        f"Invalid column def: {col} has neither base views nor dependencies!"
                    )
                for dep in cdef.dependencies:
                    next_cols = next_cols.union({dep})
        unresolved_cols = next_cols

    if iter >= 100:
        raise ValueError("broken dependency chain in column spec, loop probable")
        
    return view_resolution


def col_df(
    df: pl.LazyFrame,
    col: str,
    col_def_map: dict[str, ColumnDefinition],
    is_base_view: bool,
):
    cdef = col_def_map[col]
    if not cdef.dependencies:
        return df.select(cdef.expr)

    root_col_exprs = []
    col_dfs = []
    for dep in cdef.dependencies:
        dep_def = col_def_map[dep]
        if dep_def.dependencies is None or is_base_view and len(dep_def.base_views):
            root_col_exprs.append(dep_def.expr)
        else:
            col_dfs.append(col_df(df, dep, col_def_map, is_base_view))

    if root_col_exprs:
        col_dfs.append(df.select(root_col_exprs))

    dep_df = pl.concat(col_dfs, how="horizontal")
    return dep_df.select(cdef.expr)


def metrics(
    set_code: str,
    columns: list[str] | None = None,
    groupbys: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: list[ColumnDefinition] | None = None,
    as_pandas: bool = False,
    use_streaming: bool = False,
) -> pl.DataFrame | pd.DataFrame | None:
    cols = tuple(mcol.default_columns) if columns is None else tuple(columns)
    gbs = (ColName.NAME,) if groupbys is None else tuple(groupbys)

    col_def_map = dict(mcol.col_def_map)
    if extensions is not None:
        for col in extensions:
            col_def_map[col.name] = col

    dd_filter = mdu.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(gbs)
    if dd_filter is not None:
        col_set = col_set.union(dd_filter.lhs)

    base_view_cols = resolve_base_view_cols(col_set, col_def_map)

    base_views = frozenset()
    for view in [View.DRAFT, View.GAME]:
        for col in base_view_cols[view]:
            if col_def_map[col].base_views == (view,):  # only found in this view
                base_views = base_views.union({view})

    base_dfs = []

    for view in base_views:
        view_cols = base_view_cols[view]
        if dd_filter is not None:
            if len(dd_filter.lhs.difference(view_cols)):
                raise ValueError(
                    "All filter columns must appear in both base views used as dependencies"
                )
        df_path = data_file_path(set_code, view)
        base_view_df = pl.scan_csv(df_path, schema=schema(df_path))
        col_dfs = [col_df(base_view_df, col, col_def_map, is_base_view=True) for col in view_cols]
        base_df = pl.concat(col_dfs, how="horizontal")

        if dd_filter is not None:
            base_df_filtered = base_df.filter(dd_filter.expr)
        else:
            base_df_filtered = base_df
        base_dfs.append(base_df_filtered)

    if not base_dfs:
        return None

    return pl.concat(base_dfs).collect()
    
