# 🪄 spells ✨

**spells** is a python package that enables intuitive, optimized analysis of the public data sets provided by 17Lands.com. **spells** is designed to trivialize the annoying, fiddly, and slow parts of working with those large datasets, so that you can focus on your ideas. **spells** exposes one first-class API component, `summon`, which summons a polars DataFrame to the battlefield.

```python
>>>import spells
>>>spells.summon()
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

## summon
`summon` takes four optional arguments, allowing a fully declarative specification of your desired analysis
  - `columns` specifies the desired output columns
    ```python
    >>spells.summon(columns=["gih_wr"])
    ...[output here]
    ```
  - `groupbys` specifies the grouping by one or more columns. By default, group by card names, but optionally group by most any fundamental or derived value, including card attributes
    ```python
    >>>spells.summon(columns=["trophy_rate"], groupbys=["user_game_win_rate_bucket"])
    ...[output here]
    ```
  - `filter_spec` specifies a base filter for the dataset, using an intuitive custom query formulation
    ```python
    >>>from spells.enums import ColName
    >>>spells.summon(columns=[ColName.GP_WR], groupbys=[ColName.MAIN_COLORS], filter_spec = {ColName.PLAYER_COHORT: 'Top'})
    ...[output here]
    ```
  - `extensions` allows for the specification of arbitrarily complex derived columns and aggregations, including custom columns built on top of custom columns
    ```python
    >>>import polars as pl
    >>>from spells.columns import ColumnDefinition
    >>>from spells.enums import ColType, View, ColName
    >>>ext = ColumnDefinition(
    ...  name="is_winner",
    ...  views=(View.GAME, View.DRAFT),
    ...  expr=pl.col('user_game_win_rate_bucket') > 0.55,
    ...  dependencies=['user_game_win_rate_bucket']
    ...)
    >>>spells.summon(columns=['event_matches_sum'], groupbys=['is_winner'], extensions=[ext])
    ...[my output]
    ```

## performance and caching
...
    

