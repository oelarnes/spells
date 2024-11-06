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

import functools

import polars as pl
import pandas as pd

from mdu.cache_17l import data_file_path
from mdu.get_schema import schema
import mdu.cache
import mdu.filter
import mdu.columns as mcol
from mdu.columns import DDColumn
from mdu.enums import View, ColName, ColType


def cache_key(ddo, *args, **kwargs) -> str:
    set_code = ddo.set_code
    filter_spec = ddo.filter_str
    arg_str = str(args) + str(kwargs)

    hash_num = hash(set_code + filter_spec + arg_str)
    return hex(hash_num)[3:]


def add_to_manifest(
    columns: frozenset[str],
    manifest: dict[View, frozenset[str]],
    col_def_map: dict[str, DDColumn],
) -> dict[View, frozenset[str]]:
    for col in columns:
        cdef = col_def_map[col]
        view = cdef.view

        if isinstance(view, tuple):
            views = view
        else:
            views = (view,)

        for v in views:
            manifest[v] = manifest.get(v, frozenset()).union({col})

    return manifest


def get_manifest(
    col_set: frozenset[str],
    col_def_map: dict[str, DDColumn],
) -> dict[View, frozenset[str]]:
    added_cols = frozenset()
    expanded_cols = col_set

    manifest = {}
    while expanded_cols != added_cols:
        manifest = add_to_manifest(expanded_cols, {}, col_def_map)
        added_cols = expanded_cols

        for col in expanded_cols:
            col_def = col_def_map[col]
            deps = col_def.dependencies
            if deps is not None:
                expanded_cols = expanded_cols.union(deps)

    return manifest
        

def metrics(
    set_code: str,
    columns: list[str] | None = None, 
    groupbys: list[str] | None = None, 
    filter_spec: dict | None = None,
    extensions: list[DDColumn] | None = None,
    as_pandas: bool = False,
    use_streaming: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    cols = tuple(mcol.default_columns ) if columns is None else tuple(columns)
    gbs = (ColName.NAME,) if groupbys is None else tuple(groupbys)

    col_def_map = dict(mcol.column_def_map)
    if extensions is not None:
        for col in extensions:
            col_def_map[col.name] = col

    dd_filter = mdu.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(gbs)
    if dd_filter is not None:
        col_set = col_set.union(dd_filter.lhs)

    manifest = get_manifest(col_set, col_def_map)

    base_views = frozenset()
    for view in [View.DRAFT, View.GAME]:
        for col in manifest[view]:
            if col_def_map[col].view == view: # only found in this view
                base_views = base_views.union({view})

    for view in base_views:
        df_path = data_file_path(set_code, view)
        df = pl.scan_csv(df_path, schema=schema(df_path))

        view_cols = [col_def_map[c] for c in manifest[view]]

        basic_cols = [v for v in view_cols if v.col_type != ColType.NAME_SUM]
        name_sum_cols = [v for v in view_cols if v.col_type == ColType.NAME_SUM]

        concat_list = [df.select([col.expr for col in basic_cols])]
        for col in name_sum_cols:
            col_df = df.select(col.expr)
            col_df.columns = 
            concat_list.append(df.select(col.expr))

        calc_df = pl.concat(concat_list, how="horizontal")
        

    return pl.DataFrame()


class DraftData:
    def __init__(
        self, 
        set_code: str, 
        filter_spec: dict | None = None, 
        column_specs: list[dict] | None = None
    ):
        self.set_code = set_code.upper()

        if filter_spec:
            self.set_filter(filter_spec)
        draft_path = data_file_path(set_code, "draft")
        game_path = data_file_path(set_code, "game")
        self._base_dfs = {
            View.DRAFT: pl.scan_csv(draft_path, schema=schema(draft_path)),
            View.GAME: pl.scan_csv(game_path, schema=schema(draft_path)),
        }

        self._card_df = pl.read_csv(data_file_path(set_code, "card"))

        self._extension_map = {}
        if column_specs:
            for spec in column_specs:
                self.register_column(spec)

    def register_column(self, spec):
        self._extension_map[spec.view][spec.name] = DDColumn(**spec)

    @property
    def card_names(self):
        """
        The card file is generated from the draft data file, so this is exactly the
        list of card names used in the datasets
        """
        return list(self._card_df["name"])

    def set_filter(self, filter_spec: dict):
        self._filter = mdu.filter.from_spec(filter_spec)
        self.filter_str = str(filter_spec)  # todo: standardize representation for sensible hashing
       
    def extended_view(self, view_name: View, col_names: list[str]):
        """
        extend the base df with the provided columns, which must be defined in columns.py 
        or registered via self.register_column()
        """
        base_df = self._base_dfs[view_name]

        required_columns = [self._column_map[view_name][col] for col in col_names]


    def game_counts(
        self, groupbys: list[str], col_names: list[str], read_cache: bool = True, write_cache: bool = True
    ) -> pd.DataFrame:
        method_name = "game_counts"
        calc_method = self._game_counts

        return self.fetch_or_get(
            method_name,
            calc_method,
            groupbys,
            col_names,
            read_cache=read_cache,
            write_cache=write_cache,
        )

    def fetch_or_get(
        self, method_name: str, calc_method, groupbys: list[str], col_names: list[str], read_cache: bool, write_cache: bool 
    ):
        key = cache_key(self, method_name, groupbys, col_names)
        if read_cache:
            if mdu.cache.cache_exists(self.set_code, key):
                return mdu.cache.read_cache(self.set_code, key)
        result = calc_method(groupbys, col_names)
        if write_cache:
            mdu.cache.write_cache(self.set_code, key, result)
        return result

    def _game_counts(
        self, groupbys = list[str], col_names = list[str]
    ) -> pd.DataFrame:
        """
        A data frame of counts easily aggregated from the 'game' file.
        Card-attribute groupbys can be applied after this stage to be filtered
        through a rates aggregator.
        """
        gv = self._game_df.copy()
        if self._filter is not None:
            gv = gv.loc[self._filter]
        if not groupbys:
            groupbys = ["name"]

        names = self.card_names

        gv = self.apply_extensions(gv, extensions)

        nonname_groupbys = [c for c in groupbys if c != "name"]

        prefixes = [
            "deck",
            "sideboard",
            "opening_hand",
            "drawn",
            "tutored",
        ]

        prefix_names = {prefix: [f"{prefix}_{name}" for name in names] for prefix in prefixes}

        df_components = [game_view]
        for ext in extensions:
            df = ext["calc"](game_view, prefix_names)
            ext_names = [f"{ext['prefix']}_{name}" for name in names]
            prefix_names[ext["prefix"]] = ext_names
            df.columns = ext_names
            df_components.extend(df)

        concat_df = dd.concat(df_components, axis=1)

        all_count_df = concat_df[
            functools.reduce(lambda curr, prev: prev + curr, prefix_names.values())
        ]
        win_count_df = all_count_df.where(df["won"], other=0)

        all_df = dd.concat([all_count_df, concat_df[nonname_groupbys]], axis=1)
        win_df = dd.concat([win_count_df, concat_df[nonname_groupbys]], axis=1)

        if nonname_groupbys:
            games_result = all_df.groupby(nonname_groupbys).sum().compute()
            win_result = win_df.groupby(nonname_groupbys).sum().compute()
        else:
            games_sum = all_df.sum().compute()
            games_result = pd.DataFrame(
                numpy.expand_dims(games_sum.values, 0), columns=games_sum.index
            )
            win_sum = win_df.sum().compute()
            win_result = pd.DataFrame(
                numpy.expand_dims(win_sum.values, 0), columns=win_sum.index
            )

        count_cols = {}
        for prefix in prefixes:
            for outcome, df in {"all": games_result, "win": win_result}.items():
                count_df = df[prefix_names[prefix]]
                count_df.columns = names
                melt_df = pd.melt(count_df, var_name="name", ignore_index=False)
                count_cols[f"{prefix}_{outcome}"] = melt_df["value"].reset_index(drop=True)
        # grab the indexes from the last one, they are all the same
        index_df = melt_df.reset_index().drop("value", axis="columns")

        by_name_df = pd.DataFrame(count_cols, dtype=numpy.int64)
        by_name_df.index = pd.MultiIndex.from_frame(index_df)

        if "name" in groupbys:
            if len(groupbys) == 1:
                by_name_df.index = by_name_df.index.droplevel()
            return by_name_df

        return by_name_df.groupby(groupbys).sum()
