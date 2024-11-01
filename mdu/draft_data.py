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
import pandas

from mdu.cache_17l import data_file_path
from mdu.get_schema import schema
import mdu.cache
import mdu.filter
from mdu.columns import View, ColName, DDColumn, column_def_map, default_columns


def cache_key(ddo, *args, **kwargs):
    set_code = ddo.set_code
    filter_spec = ddo.filter_str
    arg_str = str(args) + str(kwargs)

    hash_num = hash(set_code + filter_spec + arg_str)
    return hex(hash_num)[3:]


def expand_columns(
    view: View,
    columns: set[str],
    col_def_map: dict[View, dict],
):
    cols = set(columns)
    expanded_cols = set(cols)
    for col in columns:
        col_def = col_def_map[view].get(col)
        if col_def is not None:
            for dep in col_def.dependencies:
                expanded_cols.add(dep)

    if cols == expanded_cols:
        return cols
    else:
        return(expand_columns(view, expanded_cols, col_def_map))


def metrics(
    columns: list[str] | None = None, 
    groupbys: list[str] | None = None, 
    filter_spec: dict | None = None,
    extensions: list[DDColumn] | None = None,
):
    if columns is None:
        columns = list(default_columns)

    if groupbys is None:
        groupbys = [ColName.NAME]

    if extensions is not None:
        col_def_map = {
            v: dict(column_def_map[v]) for v in View
        }
        for col in extensions:
            if isinstance(col.view, tuple):
                for i in (0,1):
                    col_def_map[col.view[i]][col.name] = col
            else:
                col_def_map[col.view][col.name] = col
    else:
       col_def_map = column_def_map 

    if filter_spec is None:
        filter_spec = {}

    filter_expr = mdu.filter.from_spec(filter_spec)

    col_set = set(columns)
    col_set.update(groupbys)
    col_set.update(mdu.filter.get_leaf_lhs(filter_expr))

    draft_col_set = expand_columns(View.DRAFT, col_set, col_def_map)
    game_col_set = expand_columns(View.GAME, col_set, col_def_map)


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
    ) -> pandas.DataFrame:
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
    ) -> pandas.DataFrame:
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
            games_result = pandas.DataFrame(
                numpy.expand_dims(games_sum.values, 0), columns=games_sum.index
            )
            win_sum = win_df.sum().compute()
            win_result = pandas.DataFrame(
                numpy.expand_dims(win_sum.values, 0), columns=win_sum.index
            )

        count_cols = {}
        for prefix in prefixes:
            for outcome, df in {"all": games_result, "win": win_result}.items():
                count_df = df[prefix_names[prefix]]
                count_df.columns = names
                melt_df = pandas.melt(count_df, var_name="name", ignore_index=False)
                count_cols[f"{prefix}_{outcome}"] = melt_df["value"].reset_index(drop=True)
        # grab the indexes from the last one, they are all the same
        index_df = melt_df.reset_index().drop("value", axis="columns")

        by_name_df = pandas.DataFrame(count_cols, dtype=numpy.int64)
        by_name_df.index = pandas.MultiIndex.from_frame(index_df)

        if "name" in groupbys:
            if len(groupbys) == 1:
                by_name_df.index = by_name_df.index.droplevel()
            return by_name_df

        return by_name_df.groupby(groupbys).sum()
