# 🪄 spells ✨

**spells** is a python package that tutors up intuitive, customizable, optimized analysis of the public data sets provided by 17Lands, and exiles the annoying, fiddly, and slow parts of working with those large datasets, so that you can focus on your ideas. Spells exposes one first-class function, `summon`, which summons a Polars DataFrame to the battlefield.

```python
>>>import spells
>>>spells.summon('BLB')
...[output here]
```

Using pandas instead is as simple as invoking the chained call `summon(...).to_pandas()`.

Spells is not affiliated with 17Lands. Please review the Usage Guidelines for 17lands data before using Spells, and consider supporting their patreon. Spells is free and open-source; please consider contributing and feel free to make use of the source code under the terms of the MIT license.

**spells:**
- Uses Polars for high-performance, multi-threaded, chunked aggregations of large datasets
- Uses Polars to power an expressive query language for specifying custom extensions and optimizing complex queries
- Supports calculating the standard aggregations and measures out of the box with no arguments (ALSA, GIH WR, etc)
- Caches aggregate DataFrames in the local file system automatically for instantaneous reproduction of previous analysis
- Provides functions and scripts for downloading and organizing public datasets from 17Lands
- Provides functions and scripts for downloading and modeling necessary card data from Scryfall
- Is fully typed, linted, and statically analyzed for support of advanced IDE features
- Provides enums for all base columns and built-in extensions, as well as for custom extension parameters
  - But enums are entirely optional, and all arguments can be specified as strings if desired
- Uses Polars expressions to support second-stage aggregations like z-scores out of the box with one call to summon

## summon
`summon` takes four optional arguments, allowing a fully declarative specification of your desired analysis
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
  - `group_by` specifies the grouping by one or more columns. By default, group by card names, but optionally group by most any fundamental or derived value, including card attributes
    ```python
    >>>spells.summon('BLB', columns=["trophy_rate"], group_by=["user_game_win_rate_bucket"])
    shape: (46, 2)
    ┌───────────────────────────┬─────────────┐
    │ user_game_win_rate_bucket ┆ trophy_rate │
    │ ---                       ┆ ---         │
    │ f64                       ┆ f64         │
    ╞═══════════════════════════╪═════════════╡
    │ null                      ┆ 0.069536    │
    │ 0.0                       ┆ 0.0         │
    │ 0.06                      ┆ 0.0         │
    │ 0.1                       ┆ 0.0         │
    │ 0.12                      ┆ 0.0         │
    │ …                         ┆ …           │
    │ 0.86                      ┆ 0.590065    │
    │ 0.88                      ┆ 0.25        │
    │ 0.9                       ┆ 0.571429    │
    │ 0.92                      ┆ 0.6         │
    │ 0.94                      ┆ 0.333333    │
    └───────────────────────────┴─────────────┘
    ```
  - `filter` specifies a base filter for the dataset, using an intuitive custom query formulation
    ```python
    >>>from spells.enums import ColName
    >>>spells.summon(
    ...  'BLB',
    ...  columns=[ColName.GP_WR],
    ...  group_by=[ColName.MAIN_COLORS],
    ...  filter= {ColName.PLAYER_COHORT: 'Top'}
    ...)
    shape: (28, 2)
    ┌─────────────┬──────────┐
    │ main_colors ┆ gp_wr    │
    │ ---         ┆ ---      │
    │ str         ┆ f64      │
    ╞═════════════╪══════════╡
    │ B           ┆ 0.61849  │
    │ BG          ┆ 0.619011 │
    │ BR          ┆ 0.629755 │
    │ BRG         ┆ 0.544002 │
    │ G           ┆ 0.621483 │
    │ …           ┆ …        │
    │ WU          ┆ 0.599901 │
    │ WUB         ┆ 0.600773 │
    │ WUBG        ┆ 0.599377 │
    │ WUG         ┆ 0.577653 │
    │ WUR         ┆ 0.510029 │
    └─────────────┴──────────┘
    ```
  - `extensions` allows for the specification of arbitrarily complex derived columns and aggregations, including custom columns built on top of custom columns
    ```python
    >>>import polars as pl
    >>>from spells.columns import ColumnDefinition
    >>>from spells.enums import ColType, View, ColName
    >>>ext = ColumnDefinition(
    ...  name="is_winner",
    ...  views=(View.GAME, View.DRAFT),
    ...  col_type=ColType.GROUPBY,
    ...  expr=pl.col('user_game_win_rate_bucket') > 0.55
    ...)
    >>>spells.summon('BLB', columns=['event_matches_sum'], group_by=['is_winner'], extensions=[ext])
    ...[my output]
    ```

## Performance

Spells provides several features out of the box to optimize performance to the degree possible given its generality.

### Query Optimization

Firstly, it is built on top of *Polars*, a modern, well-supported DataFrame engine that enables declarative query plans and lazy evaluation, allowing for automatic performance optimization in the execution of the query plan. Spells selects only the necessary columns for your analysis, at the lowest depth possible per column, and uses "concat" rather than "with" throughout to ensure the best performance and flexibility for optimization. 

By default, Polars loads the entire selection into memory before aggregation for optimal time performance. For query plans that are too memory-intensive, Spells exposes the Polars parameter `streaming`, which performs aggregations in chunks based on available system resources. `polars.Config` exposes settings for further tweaking your execution plan. Spells and Polars do not support distributed computation.

### Local Caching
Additionally, by default, Spells caches the results of expensive aggregations in the local file system as parquet files, which by default are found under the `data/local` path from the execution directory, which can be configured using the environment variable `SPELLS_PROJECT_DIR`. Query plans which request the same set of first-stage aggregations (sums over base rows) will attempt to locate the aggregate data in the cache before calculating. This guarantees that a repeated call to `summon` returns instantaneously.

When refreshing a given set's data files from 17Lands using the provided functions, the cache for that set is automatically cleared. Additionally `summon` has named arguments `read_cache` and `write_cache`, and the project exposes `spells.cache.clear_cache(set_code: str)` for further control.


