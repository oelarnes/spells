# 🪄 spells ✨

**spells** is a python package that tutors up customizable, optimized analysis of the public data sets provided by [17Lands](https://www.17lands.com/) and exiles the annoying, fiddly, and slow parts of your workflow. Spells exposes one first-class function, `summon`, which summons a Polars DataFrame to the battlefield.

```python
>>>import spells
>>>spells.summon('BLB')
shape: (276, 14)
┌─────────────────────────┬───────┬──────────┬──────────┬───┬────────┬──────────┬─────────┬──────────┐
│ name                    ┆ color ┆ rarity   ┆ num_seen ┆ … ┆ num_oh ┆ oh_wr    ┆ num_gih ┆ gih_wr   │
│ ---                     ┆ ---   ┆ ---      ┆ ---      ┆   ┆ ---    ┆ ---      ┆ ---     ┆ ---      │
│ str                     ┆ str   ┆ str      ┆ i64      ┆   ┆ i64    ┆ f64      ┆ i64     ┆ f64      │
╞═════════════════════════╪═══════╪══════════╪══════════╪═══╪════════╪══════════╪═════════╪══════════╡
│ Agate Assault           ┆ R     ┆ common   ┆ 245811   ┆ … ┆ 30659  ┆ 0.529632 ┆ 72959   ┆ 0.541688 │
│ Agate-Blade Assassin    ┆ B     ┆ common   ┆ 289813   ┆ … ┆ 31099  ┆ 0.560886 ┆ 68698   ┆ 0.548939 │
│ Alania's Pathmaker      ┆ R     ┆ common   ┆ 315760   ┆ … ┆ 22774  ┆ 0.52819  ┆ 54702   ┆ 0.537439 │
│ Alania, Divergent Storm ┆ RU    ┆ rare     ┆ 37408    ┆ … ┆ 2133   ┆ 0.473043 ┆ 5843    ┆ 0.491528 │
│ Artist's Talent         ┆ R     ┆ rare     ┆ 44725    ┆ … ┆ 995    ┆ 0.433166 ┆ 2528    ┆ 0.455696 │
│ …                       ┆ …     ┆ …        ┆ …        ┆ … ┆ …      ┆ …        ┆ …       ┆ …        │
│ Wick, the Whorled Mind  ┆ B     ┆ rare     ┆ 27641    ┆ … ┆ 4648   ┆ 0.517857 ┆ 11640   ┆ 0.547938 │
│ Wildfire Howl           ┆ R     ┆ uncommon ┆ 121015   ┆ … ┆ 4370   ┆ 0.514416 ┆ 11029   ┆ 0.529422 │
│ Wishing Well            ┆ U     ┆ rare     ┆ 52946    ┆ … ┆ 584    ┆ 0.491438 ┆ 1622    ┆ 0.563502 │
│ Ygra, Eater of All      ┆ BG    ┆ mythic   ┆ 6513     ┆ … ┆ 3862   ┆ 0.620145 ┆ 9926    ┆ 0.62976  │
│ Zoraline, Cosmos Caller ┆ BW    ┆ rare     ┆ 17689    ┆ … ┆ 6381   ┆ 0.604294 ┆ 15242   ┆ 0.622753 │
└─────────────────────────┴───────┴──────────┴──────────┴───┴────────┴──────────┴─────────┴──────────┘
```

Coverting to pandas DataFrame is as simple as invoking the chained call `summon(...).to_pandas()`.

Spells is not affiliated with 17Lands. Please review the Usage Guidelines for 17lands data before using Spells, and consider supporting their patreon. Spells is free and open-source; please consider contributing and feel free to make use of the source code under the terms of the MIT license.

## spells

- Uses [Polars](https://docs.pola.rs/) for high-performance, multi-threaded, chunked aggregations of large datasets
- Uses Polars to power an expressive query language for specifying custom extensions and optimizing complex queries
- Supports calculating the standard aggregations and measures out of the box with no arguments (ALSA, GIH WR, etc)
- Caches aggregate DataFrames in the local file system automatically for instantaneous reproduction of previous analysis
- Manages grouping and filtering by built-in and custom columns at the row level
- Provides over 50 explicitly specified, enumerated, documented custom column definitions
- Supports "Deck Color Data" aggregations out of the box.
- Provides a CLI tool `spells [add|refresh|remove|clear_local] [SET]` to download and manage external files
- Downloads and manages public datasets from 17Lands
- Downloads and models booster configuration and card data from [MTGJSON](https://mtgjson.com/)
- Is fully typed, linted, and statically analyzed for support of advanced IDE features
- Provides optional enums for all base columns and built-in extensions, as well as for custom extension parameters
- Uses Polars expressions to support second-stage aggregations like z-scores out of the box with one call to summon
- Is lightweight: Polars is the only dependency

## summon

`summon` takes four optional arguments, allowing a fully declarative specification of your desired analysis. Basic functionality not provided by this api can often be managed by simple chained calls using the polars API, e.g. sorting and post-agg filtering.
  - `columns` specifies the desired output columns
    ```python
    >>spells.summon('BLB', columns=["gih_wr"])
    shape: (276, 2)
    ┌─────────────────────────┬──────────┐
    │ name                    ┆ gih_wr   │
    │ ---                     ┆ ---      │
    │ str                     ┆ f64      │
    ╞═════════════════════════╪══════════╡
    │ Agate Assault           ┆ 0.541688 │
    │ Agate-Blade Assassin    ┆ 0.548939 │
    │ Alania's Pathmaker      ┆ 0.537439 │
    │ Alania, Divergent Storm ┆ 0.491528 │
    │ Artist's Talent         ┆ 0.455696 │
    │ …                       ┆ …        │
    │ Wick, the Whorled Mind  ┆ 0.547938 │
    │ Wildfire Howl           ┆ 0.529422 │
    │ Wishing Well            ┆ 0.563502 │
    │ Ygra, Eater of All      ┆ 0.62976  │
    │ Zoraline, Cosmos Caller ┆ 0.622753 │
    └─────────────────────────┴──────────┘
    ```
  - `group_by` specifies the grouping by one or more columns. By default, group by card names, but optionally group by any of a large set of fundamental and derived columns, including card attributes and your own custom extension.
    ```python
    >>>spells.summon('BLB', columns=["trophy_rate"], group_by=["user_game_win_rate_bucket"], filter_spec={'lhs': "user_n_games_bucket", 'op': ">=", 'rhs': 50})
    shape: (26, 2)
    ┌───────────────────────────┬─────────────┐
    │ user_game_win_rate_bucket ┆ trophy_rate │
    │ ---                       ┆ ---         │
    │ f64                       ┆ f64         │
    ╞═══════════════════════════╪═════════════╡
    │ 0.3                       ┆ 0.0         │
    │ 0.32                      ┆ 0.0         │
    │ 0.34                      ┆ 0.002487    │
    │ 0.36                      ┆ 0.009892    │
    │ 0.38                      ┆ 0.014028    │
    │ …                         ┆ …           │
    │ 0.72                      ┆ 0.410297    │
    │ 0.74                      ┆ 0.391304    │
    │ 0.76                      ┆ 0.5         │
    │ 0.78                      ┆ 0.769231    │
    │ 0.8                       ┆ 0.615385    │
    └───────────────────────────┴─────────────┘
    ```
  - `filter_spec` specifies a base filter for the dataset, using an intuitive custom query formulation
    ```python
    >>>from spells.enums import ColName
    >>>spells.summon(
    ...  'BLB',
    ...  columns=[ColName.GP_WR, ColName.NUM_GP],
    ...  group_by=[ColName.MAIN_COLORS],
    ...  filter_spec={'$and': [
    ...     {ColName.PLAYER_COHORT: 'Top'}, # A different definition from 17Lands.com
    ...     {'lhs': ColName.MAIN_COLORS, 'op': 'in', 'rhs': ['WU', 'WB', 'WR', 'WG', 'UB', 'UR', 'UB', 'BR', 'BG', 'RG']}
         ]}
    ...)
    shape: (9, 3)
    ┌─────────────┬──────────┬─────────┐
    │ main_colors ┆ gp_wr    ┆ num_gp  │
    │ ---         ┆ ---      ┆ ---     │
    │ str         ┆ f64      ┆ i64     │
    ╞═════════════╪══════════╪═════════╡
    │ BG          ┆ 0.619011 ┆ 1258468 │
    │ BR          ┆ 0.629755 ┆ 621243  │
    │ RG          ┆ 0.598299 ┆ 417101  │
    │ UB          ┆ 0.609801 ┆ 500252  │
    │ UR          ┆ 0.60948  ┆ 293168  │
    │ WB          ┆ 0.614057 ┆ 798963  │
    │ WG          ┆ 0.620028 ┆ 975636  │
    │ WR          ┆ 0.618621 ┆ 593984  │
    │ WU          ┆ 0.599901 ┆ 227379  │
    └─────────────┴──────────┴─────────┘
    ```
  - `extensions` allows for the specification of arbitrarily complex derived columns and aggregations, including custom columns built on top of custom columns. Note the column 'event_match_wins_sum' is an alias of 'event_match_wins'. Each column must have a defined role, and 'event_match_wins' is defined as a group_by. One could even group by 'event_match_wins' and sum the 'event_match_wins_sum' column within each group.
    ```python
    >>>import polars as pl
    >>>from spells.columns import ColumnDefinition
    >>>from spells.enums import ColType, View, ColName
    >>>ext = ColumnDefinition(
    ...  name="wins_a_lot",
    ...  views=(View.GAME, View.DRAFT),
    ...  col_type=ColType.GROUP_BY,
    ...  expr=pl.col('user_game_win_rate_bucket') > 0.55
    ...)
    >>>spells.summon('BLB', columns=['event_match_wins_sum'], group_by=['wins_a_lot'], extensions=[ext])
    shape: (3, 2)
    ┌────────────┬──────────────────────┐
    │ wins_a_lot ┆ event_match_wins_sum │
    │ ---        ┆ ---                  │
    │ bool       ┆ i64                  │
    ╞════════════╪══════════════════════╡
    │ null       ┆ 5976                 │
    │ false      ┆ 8817828              │
    │ true       ┆ 7997739              │
    └────────────┴──────────────────────┘
    ```
    
## Installation

Spells is available on PyPI as *spells-mtg*, and can be imported using pip or any package manager:

`pip install spells-mtg`

Spells is still in development and could benefit from many new features and improvements. As such, you might rather clone this repository and install locally. It is set up to use pdm, but it's just a regular old python package and you can install with your normal workflow.

If you are new to Python, I recommend using a package manager like poetry, pdm or uv to create a virtual environment and manage your project.

## Performance

Spells provides several features out of the box to optimize performance to the degree possible given its generality.

### Query Optimization

Firstly, it is built on top of Polars, a modern, well-supported DataFrame engine that enables declarative query plans and lazy evaluation, allowing for automatic performance optimization in the execution of the query plan. Spells selects only the necessary columns for your analysis, at the lowest depth possible per column, and uses "concat" rather than "with" throughout to ensure the best performance and flexibility for optimization. 

By default, Polars loads the entire selection into memory before aggregation for optimal time performance. For query plans that are too memory-intensive, Spells exposes the Polars parameter `streaming`, which performs aggregations in chunks based on available system resources. `polars.Config` exposes settings for further tweaking your execution plan. Spells and Polars do not support distributed computation.

### Local Caching

Additionally, by default, Spells caches the results of expensive aggregations in the local file system as parquet files, which by default are found under the `data/local` path from the execution directory, which can be configured using the environment variable `SPELLS_PROJECT_DIR`. Query plans which request the same set of first-stage aggregations (sums over base rows) will attempt to locate the aggregate data in the cache before calculating. This guarantees that a repeated call to `summon` returns instantaneously.

When refreshing a given set's data files from 17Lands using the provided functions, the cache for that set is automatically cleared. Additionally `summon` has named arguments `read_cache` and `write_cache`, and the project exposes `spells.cache.clear_cache(set_code: str)` for further control.

# Documentation
In order to give a valid specification for more complex queries, it's important to understand a bit about what Spells is doing under the hood.

## Basic Concepts
Let's briefly review the structure of the underlying data. Spells supports aggregations on two of the three large data sets provided by 17Lands, which
are identified as "views" within Spells. First there is *draft*, which is the source for information about draft picks. The row model is single draft picks with pack and pool context. Unlike *game*, there are two different paradigms for aggregating over card names. 

First, one can group by the value of the "pick" column and sum numerical column values. This is how ATA is calculated. In Spells, we tag columns to be summed in this way as *pick_sum* columns. For example, "taken_at" is equivalent to "pick_number", but whereas the latter is available for grouping, "taken_at" is summed over groups. 

Second, certain columns are pivoted horizontally within the raw data and suffixed with card names, for example "pack_card_Savor". In Spells we tag such columns as *name_sum*, and group by non-name columns and sum before unpivoting. The columns are identified by their prefix only and Spells handles the mapping. 

A standard way to aggregate information in non-*name_sum* columns over names is to multiply that column over the pivoted column. For example, to calculate the *name_sum* column "last_seen", used in ALSA, we multiply "pack_card" by a modified version of "pick_number".

In the *game* file, the row model represents games, and primarily uses *name_sum* aggregations for the familiar columns, such as "num_gih", from which win rates are derived. For groupings that do not use card names or card attributes (to recreate the "deck color data" page, for example), one can also specify *game_sum* columns which aggregate simply over rows.

## Aggregate View

Once aggregation columns, filters and groupings are determined at the row level for each of the required base views, Spells asks Polars to sum over groups and unpivot as needed to produce the "base aggregate view", which fixes the row model (pre-card attributes) to the provided base groupings. This base aggregate view is cached by default to the local file system, keyed by the *manifest*, which is a function of the specification provided by the user.

Next, card attributes are calculated and joined to the base aggregate view by name, and an additional grouping is performed if requested by the user to produce the *aggregate view*.

A final extension and selection stage is applied to the aggregate view, which is where weighted averages like GIH WR are calculated. Polars expression language enables aggregations to be represented as expressions and broadcast back to the row level, enabling Spells to support arbitrary chains of aggregation and extension at the aggregate view level. For example, one could calculate the mean of a metric over groups by archetype, regress a metric by a function of that mean, then calculate the mean of that regressed metric, all expressed declaratively as column expressions and simply specified by name in the `summon` api call.

So that's it, that's what Spells does from a high level. `summon` will hand off a Polars DataFrame which can be cast to pandas, sorted, filtered, used to be generate plots or whatever you like. If a task can be handled as easily via a chained call or outside library, it should stay that way, but if you have a request for features specific to the structure of limited data that could be handled in a general way, please reach out! In particular I am interested in scientific workflows like maximum likelihood estimation, but I haven't yet considered how to build it into Spells.



