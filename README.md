# 🪄 spells ✨

**spells** is a python package that enables intuitive, optimized analysis of the public data sets provided by 17Lands.com. **spells** is designed to trivialize the annoying, fiddly, and slow parts of working with those large datasets, so that you can focus on your ideas. **spells** exposes one first-class function, `summon`, which summons a polars DataFrame to the battlefield.

```python
>>>import spells
>>>spells.summon('BLB')
...[output here]
```

## spells:
- Uses polars for high-performance, multi-threaded, chunked aggregations of large datasets
- Uses polars to power an expressive query language for specifying custom extensions and optimizing complex queries
- Supports calculating the standard aggregations and measures out of the box with no arguments (ALSA, GIH WR, etc)
- Caches aggregate DataFrames in the local file system automatically for instantaneous reproduction of previous analysis
- Provides functions and scripts for downloading and organizing public datasets from 17Lands.com
- Provides functions and scripts for downloading and modeling necessary card data from Scryfall
- Is fully typed, linted, and statically analyzed for support of advanced IDE features
- Provides enums for all base columns and built-in extensions, as well as for custom extension parameters
  - But enums are entirely optional, and all arguments can be specified as strings if desired
- Uses polars expressions to support second-stage aggregations like z-scores out of the box with one call to summon

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
  - `groupbys` specifies the grouping by one or more columns. By default, group by card names, but optionally group by most any fundamental or derived value, including card attributes
    ```python
    >>>spells.summon('BLB', columns=["trophy_rate"], groupbys=["user_game_win_rate_bucket"])
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
  - `filter_spec` specifies a base filter for the dataset, using an intuitive custom query formulation
    ```python
    >>>from spells.enums import ColName
    >>>spells.summon(
    ...  'BLB',
    ...  columns=[ColName.GP_WR],
    ...  groupbys=[ColName.MAIN_COLORS],
    ...  filter_spec = {ColName.PLAYER_COHORT: 'Top'}
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
    >>>spells.summon('BLB', columns=['event_matches_sum'], groupbys=['is_winner'], extensions=[ext])
    ...[my output]
    ```

## performance and caching
...
    

