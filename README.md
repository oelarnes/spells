# 🪄 spells ✨

**spells** is a python package that tutors up blazing-fast and extensible analysis of the public data sets provided by [17Lands](https://www.17lands.com/) and exiles the annoying and slow parts of your workflow. Spells exposes one first-class function, `summon`, which summons a Polars DataFrame to the battlefield.

```
$ spells add DSK
  🪄 spells ✨ [data home]=/home/joel/.local/share/spells/

  🪄 add ✨ Downloading draft dataset from 17Lands.com
100% [......................................................................] 250466473 / 250466473
  🪄 add ✨ Unzipping and transforming to parquet (this might take a few minutes)...
  🪄 add ✨ Wrote file /home/joel/.local/share/spells/external/DSK/DSK_PremierDraft_draft.parquet
  🪄 clean ✨ No local cache found for set DSK
  🪄 add ✨ Fetching card data from mtgjson.com and writing card file
  🪄 add ✨ Wrote file /home/joel/.local/share/spells/external/DSK/DSK_card.parquet
  🪄 add ✨ Calculating set context
  🪄 add ✨ Wrote file /home/joel/.local/share/spells/external/DSK/DSK_PremierDraft_context.parquet
  🪄 add ✨ Downloading game dataset from 17Lands.com
100% [........................................................................] 77145600 / 77145600
  🪄 add ✨ Unzipping and transforming to parquet (this might take a few minutes)...
  🪄 add ✨ Wrote file /home/joel/.local/share/spells/external/DSK/DSK_PremierDraft_game.parquet
  🪄 clean ✨ Removed 1 files from local cache for set DSK
  🪄 clean ✨ Removed local cache dir /home/joel/.local/share/spells/cache/DSK
```

```python
In [1]: from spells import summon

In [2]: %time summon('DSK')
CPU times: user 20.3 s, sys: 7.9 s, total: 28.2 s
Wall time: 7.55 s
Out[2]:
shape: (286, 14)
┌────────────────────────────┬───────┬──────────┬──────────┬───┬────────┬──────────┬─────────┬──────────┐
│ name                       ┆ color ┆ rarity   ┆ num_seen ┆ … ┆ num_oh ┆ oh_wr    ┆ num_gih ┆ gih_wr   │
│ ---                        ┆ ---   ┆ ---      ┆ ---      ┆   ┆ ---    ┆ ---      ┆ ---     ┆ ---      │
│ str                        ┆ str   ┆ str      ┆ i64      ┆   ┆ i64    ┆ f64      ┆ i64     ┆ f64      │
╞════════════════════════════╪═══════╪══════════╪══════════╪═══╪════════╪══════════╪═════════╪══════════╡
│ Abandoned Campground       ┆       ┆ common   ┆ 178750   ┆ … ┆ 21350  ┆ 0.559672 ┆ 49376   ┆ 0.547594 │
│ Abhorrent Oculus           ┆ U     ┆ mythic   ┆ 6676     ┆ … ┆ 4255   ┆ 0.564042 ┆ 11287   ┆ 0.593337 │
│ Acrobatic Cheerleader      ┆ W     ┆ common   ┆ 308475   ┆ … ┆ 34177  ┆ 0.541709 ┆ 74443   ┆ 0.532152 │
│ Altanak, the Thrice-Called ┆ G     ┆ uncommon ┆ 76981    ┆ … ┆ 13393  ┆ 0.513925 ┆ 34525   ┆ 0.539175 │
│ Anthropede                 ┆ G     ┆ common   ┆ 365380   ┆ … ┆ 8075   ┆ 0.479876 ┆ 20189   ┆ 0.502353 │
│ …                          ┆ …     ┆ …        ┆ …        ┆ … ┆ …      ┆ …        ┆ …       ┆ …        │
│ Wildfire Wickerfolk        ┆ GR    ┆ uncommon ┆ 98040    ┆ … ┆ 18654  ┆ 0.592366 ┆ 42251   ┆ 0.588696 │
│ Winter's Intervention      ┆ B     ┆ common   ┆ 318565   ┆ … ┆ 27552  ┆ 0.537638 ┆ 66921   ┆ 0.548453 │
│ Winter, Misanthropic Guide ┆ BGR   ┆ rare     ┆ 52091    ┆ … ┆ 1261   ┆ 0.462331 ┆ 3183    ┆ 0.479422 │
│ Withering Torment          ┆ B     ┆ uncommon ┆ 76237    ┆ … ┆ 15901  ┆ 0.511729 ┆ 39323   ┆ 0.542024 │
│ Zimone, All-Questioning    ┆ GU    ┆ rare     ┆ 20450    ┆ … ┆ 9510   ┆ 0.654574 ┆ 23576   ┆ 0.616686 │
└────────────────────────────┴───────┴──────────┴──────────┴───┴────────┴──────────┴─────────┴──────────┘

In [3]: %time spells.summon('DSK')
CPU times: user 16.3 ms, sys: 66.2 ms, total: 82.5 ms
Wall time: 80.8 ms
```
Coverting to pandas DataFrame is as simple as invoking the chained call `summon(...).to_pandas()`.

Spells is not affiliated with 17Lands. Please review the [Usage Guidelines](https://www.17lands.com/usage_guidelines) for 17lands data before using Spells, and consider supporting their patreon. Spells is free and open-source; please consider contributing and feel free to make use of the source code under the terms of the MIT license.

## spells

- Uses [Polars](https://docs.pola.rs/) for high-performance, multi-threaded aggregations of large datasets
- Uses Polars to power an expressive query language for specifying custom extensions
- Analyzes larger-than-memory datasets using Polars streaming mode
- Converts csv datasets to parquet for 10x faster calculations and 20x smaller file sizes
- Supports calculating the standard aggregations and measures out of the box with no arguments (ALSA, GIH WR, etc)
- Caches aggregate DataFrames in the local file system automatically for instantaneous reproduction of previous analysis
- Manages grouping and filtering by built-in and custom columns at the row level
- Provides 124 explicitly specified, enumerated, documented column definitions
- Can aggregate over multiple sets at once, even all of them, if you want.
- Supports "Deck Color Data" aggregations with built-in column definitions.
- Lets you feed card metrics back in to column definitions to support scientific workflows like MLE
- Provides a CLI tool `spells [add|refresh|clean|remove|info] [SET]` to download and manage external files
- Downloads and manages public datasets from 17Lands
- Retrieves and models booster configuration and card data from [MTGJSON](https://mtgjson.com/)
- Is fully typed, linted, and statically analyzed for support of advanced IDE features
- Provides optional enums for all base columns and built-in extensions, as well as for custom extension parameters
- Uses Polars expressions to support second-stage aggregations and beyond like game-weighted z-scores with one call to summon
- Works on MacOS, Linux, and Windows
- Provides example notebooks to kickstart your exploration

## summon

`summon` takes five optional arguments, allowing a fully declarative specification of your desired analysis. Basic functionality not provided by this api can often be managed by simple chained calls using the polars API, e.g. sorting and post-agg filtering.
  - `columns` specifies the desired output columns
    ```python
    >>> spells.summon('DSK', columns=["num_gp", "pct_gp", "gp_wr", "gp_wr_z"])
    shape: (286, 5)
    ┌────────────────────────────┬────────┬──────────┬──────────┬───────────┐
    │ name                       ┆ num_gp ┆ pct_gp   ┆ gp_wr    ┆ gp_wr_z   │
    │ ---                        ┆ ---    ┆ ---      ┆ ---      ┆ ---       │
    │ str                        ┆ i64    ┆ f64      ┆ f64      ┆ f64       │
    ╞════════════════════════════╪════════╪══════════╪══════════╪═══════════╡
    │ Abandoned Campground       ┆ 114632 ┆ 0.643404 ┆ 0.546444 ┆ 0.12494   │
    │ Abhorrent Oculus           ┆ 26046  ┆ 0.908476 ┆ 0.561852 ┆ 1.245212  │
    │ Acrobatic Cheerleader      ┆ 188674 ┆ 0.705265 ┆ 0.541474 ┆ -0.236464 │
    │ Altanak, the Thrice-Called ┆ 87285  ┆ 0.798662 ┆ 0.538695 ┆ -0.438489 │
    │ Anthropede                 ┆ 50634  ┆ 0.214676 ┆ 0.515444 ┆ -2.129016 │
    │ …                          ┆ …      ┆ …        ┆ …        ┆ …         │
    │ Wildfire Wickerfolk        ┆ 106557 ┆ 0.725806 ┆ 0.565331 ┆ 1.498173  │
    │ Winter's Intervention      ┆ 157534 ┆ 0.616868 ┆ 0.531758 ┆ -0.942854 │
    │ Winter, Misanthropic Guide ┆ 7794   ┆ 0.197207 ┆ 0.479985 ┆ -4.70721  │
    │ Withering Torment          ┆ 92468  ┆ 0.875387 ┆ 0.525858 ┆ -1.371877 │
    │ Zimone, All-Questioning    ┆ 54687  ┆ 0.844378 ┆ 0.560974 ┆ 1.181387  │
    └────────────────────────────┴────────┴──────────┴──────────┴───────────┘
    ```
  - `group_by` specifies the grouping by one or more columns. By default, group by card names, but optionally group by any of a large set of fundamental and derived columns, including card attributes and your own custom extension.
    ```python
    >>> summon('BLB', columns=["num_won", "num_games", "game_wr", "deck_mana_value_avg"], group_by=["main_colors"], filter_spec={"num_colors": 2})
    shape: (10, 5)
    ┌─────────────┬─────────┬───────────┬──────────┬─────────────────────┐
    │ main_colors ┆ num_won ┆ num_games ┆ game_wr  ┆ deck_mana_value_avg │
    │ ---         ┆ ---     ┆ ---       ┆ ---      ┆ ---                 │
    │ str         ┆ u32     ┆ u32       ┆ f64      ┆ f64                 │
    ╞═════════════╪═════════╪═══════════╪══════════╪═════════════════════╡
    │ BG          ┆ 85022   ┆ 152863    ┆ 0.556197 ┆ 2.862305            │
    │ BR          ┆ 45900   ┆ 81966     ┆ 0.559988 ┆ 2.76198             │
    │ RG          ┆ 34641   ┆ 64428     ┆ 0.53767  ┆ 2.852182            │
    │ UB          ┆ 30922   ┆ 57698     ┆ 0.535928 ┆ 3.10409             │
    │ UG          ┆ 59879   ┆ 109145    ┆ 0.548619 ┆ 2.861026            │
    │ UR          ┆ 19638   ┆ 38679     ┆ 0.507717 ┆ 2.908215            │
    │ WB          ┆ 59480   ┆ 107443    ┆ 0.553596 ┆ 2.9217              │
    │ WG          ┆ 76134   ┆ 136832    ┆ 0.556405 ┆ 2.721064            │
    │ WR          ┆ 49712   ┆ 91224     ┆ 0.544944 ┆ 2.5222              │
    │ WU          ┆ 16483   ┆ 31450     ┆ 0.524102 ┆ 2.930967            │
    └─────────────┴─────────┴───────────┴──────────┴─────────────────────┘ 
    ```
  - `filter_spec` specifies a row-level filter for the dataset, using an intuitive custom query formulation
    ```python
    >>> from spells import ColName
    >>> spells.summon('BLB', columns=[ColName.GAME_WR], group_by=[ColName.PLAYER_COHORT], filter_spec={'lhs': ColName.NUM_MULLIGANS, 'op': '>', 'rhs': 0})
    shape: (4, 2)
    ┌───────────────┬──────────┐
    │ player_cohort ┆ game_wr  │
    │ ---           ┆ ---      │
    │ str           ┆ f64      │
    ╞═══════════════╪══════════╡
    │ Bottom        ┆ 0.33233  │
    │ Middle        ┆ 0.405346 │
    │ Other         ┆ 0.406151 │
    │ Top           ┆ 0.475763 │
    └───────────────┴──────────┘
    ```
  - `extensions` allows for the specification of arbitrarily complex derived columns and aggregations, including custom columns built on top of custom columns.
    ```python
    >>> import polars as pl
    >>> from spells import ColSpec, ColType 
    >>> ext = {
    ...     'deq_base': ColSpec(
    ...         col_type=ColType.AGG,
    ...         expr=(pl.col('gp_wr_excess') + 0.03 * (1 - pl.col('ata')/14).pow(2)) * pl.col('pct_gp'),
    ...     )
    ... }
    >>> spells.summon('DSK', columns=['deq_base'], group_by=["name", "color", "rarity"], filter_spec={'player_cohort': 'Top'}, extensions=ext)
    ...     .filter(pl.col('deq_base').is_finite())
    ...     .filter(pl.col('rarity').is_in(['common', 'uncommon'])
    ...     .sort('deq_base', descending=True)
    ...     .head(10)
    shape: (10, 4)
    ┌──────────────────────────┬──────────┬──────────┬───────┐
    │ name                     ┆ deq_base ┆ rarity   ┆ color │
    │ ---                      ┆ ---      ┆ ---      ┆ ---   │
    │ str                      ┆ f64      ┆ str      ┆ str   │
    ╞══════════════════════════╪══════════╪══════════╪═══════╡
    │ Sheltered by Ghosts      ┆ 0.03945  ┆ uncommon ┆ W     │
    │ Optimistic Scavenger     ┆ 0.036131 ┆ uncommon ┆ W     │
    │ Midnight Mayhem          ┆ 0.034278 ┆ uncommon ┆ RW    │
    │ Splitskin Doll           ┆ 0.03423  ┆ uncommon ┆ W     │
    │ Fear of Isolation        ┆ 0.033901 ┆ uncommon ┆ U     │
    │ Floodpits Drowner        ┆ 0.033198 ┆ uncommon ┆ U     │
    │ Gremlin Tamer            ┆ 0.032048 ┆ uncommon ┆ UW    │
    │ Arabella, Abandoned Doll ┆ 0.032008 ┆ uncommon ┆ RW    │
    │ Unnerving Grasp          ┆ 0.030278 ┆ uncommon ┆ U     │
    │ Oblivious Bookworm       ┆ 0.028605 ┆ uncommon ┆ GU    │
    └──────────────────────────┴──────────┴──────────┴───────┘
    ```
  - `card_context` takes a name-indexed DataFrame or name-keyed dict and allows the construction of column definitions based on the results.
    ```python
    >>> deq = spells.summon('DSK', columns=['deq_base'], filter_spec={'player_cohort': 'Top'}, extensions=[ext])
    >>> ext = { 
    ...     'picked_deq_base': ColSpec(
    ...         col_type=ColType.PICK_SUM,
    ...         expr=lambda name, card_context: card_context[name]['deq_base']
    ...     ),
    ...     'picked_deq_base_avg', ColSpec(
    ...         col_type=ColType.AGG,
    ...         expr=pl.col('picked_deq_base') / pl.col('num_taken')
    ...     ),
    ... }
    >>> spells.summon('DSK', columns=['picked_deq_base_avg'], group_by=['player_cohort'], extensions=ext, card_context=deq)
    shape: (4, 2)
    ┌───────────────┬─────────────────────┐
    │ player_cohort ┆ picked_deq_base_avg │
    │ ---           ┆ ---                 │
    │ str           ┆ f64                 │
    ╞═══════════════╪═════════════════════╡
    │ Bottom        ┆ 0.004826            │
    │ Middle        ┆ 0.00532             │
    │ Other         ┆ 0.004895            │
    │ Top           ┆ 0.005659            │
    └───────────────┴─────────────────────┘
    ```
    
## Installation

Spells is available on PyPI as *spells-mtg*, and can be installed using pip or any package manager:

`pip install spells-mtg`

Spells is still in development and could benefit from many new features and improvements. As such, you might rather clone this repository and install locally. It is set up to use pdm, but it's just a regular old python package and you can install with your normal workflow.

If you are new to Python, I recommend using a package manager like poetry, pdm or uv to create a virtual environment and manage your project.

Once Spells is installed, check out the notebooks under [examples](https://github.com/oelarnes/spells/tree/main/examples) for ideas on getting started.

## Why did you make this? Who is it for?

Earlier this year I developed a card quality metric called [DEq](https://docs.google.com/spreadsheets/d/1n1pfrb5q_2ICYk-vfF3Uwo8t61DJU-5T_DFe0dwk8DY/edit), short for "Estimated Draft Equity", which is designed to estimate the average value of selecting a card in draft relative to a comparable baseline, in order to improve on commonly-used metrics like GIH WR, which has a number of major and minor problems when interpreted as a card quality metric. DEq depends on the daily drops from 17Lands.com and won't be replaced by this static kind of analysis.

While the modeling underpinning DEq remains sound, the estimation of the value depends on several parameters which should be inferred statistically, particularly the value of a pick and the pool bias estimate, and that process has been, let's say, somewhat less sound. In order to provide more scientific estimates of the parameters, and to continue on with deeper research, I felt the need to build a python library to enable quicker iteration and concise, portable declarations of analysis. 

That need compounded with a feeling that the barrier to entry to working with these datasets is too high and that a tool like this would benefit the community. So that's what this is. It is for data-curious beginning programmers and professional developers and scientists. I hope you find it useful. 

If you're interested in the fruits of my DEq research, or in checking my work, keep an eye on my [deq](https://GitHub.com/oelarnes/deq) repository.

## Performance

Spells provides several features to optimize performance.

### Parquet Transformation

The most significant optimization used by Spells is the simplest: the csv files are scanned and streamed to Parquet files by Polars. This allows 10x faster compute times with 20x less storage space and lower memory usage compared to csv. Yes, it's twenty times smaller and ten times faster!

### Query Optimization

Spells is built on top of Polars, a modern, well-supported DataFrame engine written for performance in Rust that enables declarative query plans and lazy evaluation, allowing for automatic performance optimization in the execution of the query plan. Spells selects only the necessary columns for your analysis, recursively traversing the dependency tree.

### Local Caching

Spells caches the results of expensive aggregations in the local file system as parquet files, which by default are found under the `data/local` path from the execution directory, which can be configured using the environment variable `SPELLS_PROJECT_DIR`. Query plans which request the same set of first-stage aggregations (sums over base rows) will attempt to locate the aggregate data in the cache before calculating. This guarantees that a repeated call to `summon` returns instantaneously.

### Memory Usage

One of my goals in creating Spells was to eliminate issues with memory pressure by exclusively using the map-reduce paradigm and a technology that supports partitioned/streaming aggregation of larget-than-memory datasets. By default, Polars loads the entire dataset in memory, but the API exposes a parameter `streaming` which I have exposed as `use_streaming` and defaulted to `True` in Spells. Further testing is needed to determine the performance impacts, so you could try turning it off if you have expansive virtual memory. My 16 GB MacBook Air is fine using 60 GB of memory, but my 32 GB homelab is not.

When refreshing a given set's data files from 17Lands using the provided cli, the cache for that set is automatically cleared. The `spells` CLI gives additional tools for managing the local and external caches.

# Documentation
In order to give a valid specification for more complex queries, it's important to understand a bit about what Spells is doing under the hood.

## Basic Concepts
Let's briefly review the structure of the underlying data. Spells supports aggregations on two of the three large data sets provided by 17Lands, which
are identified as "views" within Spells. First there is *draft*, which is the source for information about draft picks. The row model is single draft picks with pack and pool context. Unlike *game*, there are two different paradigms for aggregating over card names. 

First, one can group by the value of the "pick" column and sum numerical column values. This is how ATA is calculated. In Spells, we tag columns to be summed in this way as *pick_sum* columns. For example, "taken_at" is equivalent to "pick_number", but whereas the latter is available for grouping, "taken_at" is summed over groups. 

Second, certain columns are pivoted horizontally within the raw data and suffixed with card names, for example "pack_card_Savor". In Spells we tag such columns as *name_sum*, and group by non-name columns and sum before unpivoting. The columns are identified by their prefix only and Spells handles the mapping. 

A standard way to aggregate information in non-*name_sum* columns over names is to multiply that column over the pivoted column. For example, to calculate the *name_sum* column "last_seen", used in ALSA, we multiply "pack_card" by a modified version of "pick_number".

In the *game* file, the row model represents games, and primarily uses *name_sum* aggregations for the familiar columns, such as "num_gih", from which win rates are derived. For groupings that do not use card names or card attributes (to recreate the "deck color data" page, for example), one can also specify *game_sum* columns which aggregate simply over rows.

### Aggregate View

Once aggregation columns, filters and groupings are determined at the row level for each of the required base views, Spells asks Polars to sum over groups and unpivot as needed to produce the "base aggregate view", which fixes the row model (pre-card attributes) to the provided base groupings. This base aggregate view is cached by default to the local file system, keyed by the *manifest*, which is a function of the specification provided by the user.

Next, card attributes are calculated and joined to the base aggregate view by name, and an additional grouping is performed if requested by the user to produce the *aggregate view*.

A final extension and selection stage is applied to the aggregate view, which is where weighted averages like GIH WR are calculated. Polars expression language enables aggregations to be represented as expressions and broadcast back to the row level, enabling Spells to support arbitrary chains of aggregation and extension at the aggregate view level. For example, one could calculate the mean of a metric over groups by archetype, regress a metric by a function of that mean, then calculate the mean of that regressed metric, all expressed declaratively as column expressions and simply specified by name in the `summon` api call.

So that's it, that's what Spells does from a high level. `summon` will hand off a Polars DataFrame which can be cast to pandas, sorted, filtered, used to be generate plots or whatever you like. If a task can be handled as easily via a chained call or outside library, it should stay that way, but if you have a request for features specific to the structure of limited data that could be handled in a general way, please reach out! In particular I am interested in scientific workflows like maximum likelihood estimation, but I haven't yet considered how to build it into Spells.

## CLI

Spells includes a command-line interface `spells` to manage your external data files and local cache. Spells will download files to an appropriate file location on your system, 
typically `~/.local/share/spells` on Unix-like platforms and `C:\Users\{Username}\AppData\Local\Spells` on Windows, or to a location specified by the environment variable `SPELLS_DATA_HOME`.
To use `spells`, make sure Spells is installed in your environment using pip or a package manager, and type `spells help` into your shell, or dive in with `spells add DSK` or your favorite set. If Spells is installed globally using pipx, any local version of Spells will be able to read the managed files.

## API

### Summon

```python
from spells import summon

summon(
    set_code: list[str] | str,
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: dict[str, ColSpec] | None = None,
    card_context: pl.DataFrame | dict[str, dict[str, Any] | None = None,
    set_context: pl.DataFrame | dict[str, Any] | None = None,
    read_cache: bool = True,
    write_cache: bool = True,
    use_streaming: bool = True,
    log_to_console: int = logging.ERROR,
) -> polars.DataFrame
```

#### parameters

- `set_code`: a set code or list of set codes among those that you have added using `spells add`.
You can use "expansion" as a group_by to separate results from multiple sets, or you can aggregate them together.

- `columns`: a list of string or `ColName` values to select as non-grouped columns. Valid `ColTypes` are `PICK_SUM`, `NAME_SUM`, `GAME_SUM`, and `AGG`. Min/Max/Unique 
aggregations of non-numeric (or numeric) data types are not supported. If `None`, use a set of columns modeled on the commonly used values on 17Lands.com/card_data.

- `group_by`: a list of string or `ColName` values to display as grouped columns. Valid `ColTypes` are `GROUP_BY` and `CARD_ATTR`. By default, group by "name" (card name). For contextual card attrs, include
 in `group_by`, even when grouping by name.

- `filter_spec`: a dictionary specifying a filter, using a small number of paradigms. Columns used must be in each base view ("draft" and "game") that the `columns` and `group_by` columns depend on, so 
`AGG` and `CARD_ATTR` columns are not valid. Functions of card attributes in the base views can be filtered on using `card_context`, see the documentation for `expr` for details. `NAME_SUM` columns are also not supported. Derived columns are supported. No filter is applied by default. Yes, I should rewrite it to use the mongo query language. The specification is best understood with examples:

    - `{'player_cohort': 'Top'}` "player_cohort" value equals "Top".
    - `{'lhs': 'player_cohort', 'op': 'in', 'rhs': ['Top', 'Middle']}` "player_cohort" value is either "Top" or "Middle". Supported values for `op` are `<`, `<=`, `>`, `>=`, `!=`, `=`, `in` and `nin`.
    - `{'$and': [{'lhs': 'draft_date', 'op': '>', 'rhs': datetime.date(2024, 10, 7)}, {'rank': 'Mythic'}]}` Drafts after October 7 by Mythic-ranked players. Supported values for query construction keys are `$and`, `$or`, and `$not`.

- `extensions`: a dict of `spells.columns.ColSpec` objects, keyed by name, which are appended to the definitions built-in columns described below. 

- `card_context`: Typically a Polars DataFrame containing a `"name"` column with one row for each card name in the set, such that any usages of `card_context[name][key]` in column specs reference the column `key`. Typically this will be the output of a call to `summon` requesting cards metrics like `GP_WR`. Can also be a dictionary having the necessary form for the same access pattern.

- `set_context`: Typically, a dict of abitrary values to use in column definitions, for example, you could provide the quick draft release date and have a column that depended on that. You can also provide a one-row dataframe and access the column values.

- `read_cache`/`write_cache`: Use the local file system to cache and retrieve aggregations to minimize expensive reads of the large datasets. You shouldn't need to touch these arguments unless you are debugging.

- 'log_to_console': Set to `logging.INFO` to see useful messages on the progress of your aggregation, or `logging.WARNING` to see warning messages about potentially invalid column definitions.

### Enums

```python
from spells import ColName, ColType
```

Recommended to import `ColName` for any usage of `summon`, and to import `ColType` when defining custom extensions.

### ColSpec

```python
from spells import ColSpec

ColSpec(
    col_type: ColType,
    expr: pl.Expr | Callable[..., pl.Expr] | None = None,
    version: str | None = None
)
```

Used to define extensions in `summon`

#### parameters

- `col_type`: one of the `ColType` enum values, `FILTER_ONLY`, `GROUP_BY`, `PICK_SUM`, `NAME_SUM`, `GAME_SUM`, `CARD_ATTR`, and `AGG`. See documentation for `summon` for usage. All columns except `CARD_ATTR`
and `AGG` must be derivable at the individual row level on one or both base views. `CARD_ATTR` must be derivable at the individual row level from the card file. `AGG` can depend on any column present after 
summing over groups, and can include polars Expression aggregations. Arbitrarily long chains of aggregate dependencies are supported.

- `expr`: A polars expression or function returning a polars expression giving the derivation of the column value at the first level where it is defined. The name is inferred from the dictionary key in the extensions argument, you do not need to specify an alias.
    - For `NAME_SUM` columns, `expr` must be a function of `name` which will result in a list of expressions mapped over all card names.
    - `PICK_SUM` columns can also be functions on `name`, in which case the value will be a function of the value of the `PICK` field. 
    - `AGG` columns that depend on `NAME_SUM` columns reference the prefix (`cdef.name`) only, since the unpivot has occured prior to selection. 
    - `AGG` columns must not be functions, since they may be applied to the aggregation of several sets' data. (And they shouldn't need this anyway)
    - The possible arguments to `expr`, in addition to `name` when appropriate, are as follows:
        - `names`: An array of all card names in the canonical order.
        - `card_context`: A dictionary keyed by card name which contains card dict objects with all `CARD_ATTR` values, including custom extensions and metric columns passed by the `card_context` argument to `summon`. See example notebooks for more details.
        - `set_context`: A dictionary with arbitrary fields provided via the `set_context` argument. Has two built-in attributes, `picks_per_pack` (e.g. 13 or 14), and `release_time`, which is the minimum value of the `draft_time` field.

- `version`: When defining a column using a python function, as opposed to Polars expressions, add a unique version number so that the unique hashed signature of the column specification can be derived 
for caching purposes, since Polars cannot generate a serialization natively. When changing the definition, be sure to increment the version value. Otherwise you do not need to use this parameter.

### Columns

A table of all included columns. Columns can be referenced by enum or by string value in arguments and filter specs. The string value is always the lowercase version of the enum attribute.


| `ColName`                   | **Name**                     | `View`        | `ColType`     | **Description** | **Type**        |     
| --------------------------- | ---------------------------- | ------------- | ------------- | --------------- | --------------- |
| `NAME`                   | `"name"`                   |         | `GROUP_BY`    | Special handling, don't use in `filter_spec` | String   |
| `EXPANSION`                 | `"expansion"`                | `DRAFT, GAME` | `GROUP_BY`    | Dataset Column  | String          |    
| `EVENT_TYPE`                | `"event_type"`               | `DRAFT, GAME` | `GROUP_BY`    | Dataset Column  | String          |    
| `DRAFT_ID`                  | `"draft_id"`                 | `DRAFT, GAME` | `FILTER_ONLY` | Dataset column  | String          |   
| `DRAFT_TIME`                | `"draft_time"`               | `DRAFT, GAME` | `FILTER_ONLY` | Dataset column  | String          |    
| `DRAFT_DATE`                | `"draft_date"`               | `DRAFT, GAME` | `GROUP_BY`    |                 | `datetime.date` |
| `FORMAT_DAY`          | `"format_day"` | `DRAFT, GAME` | `GROUP_BY` | 1 for release day, 2, 3, etc. | Int |
| `DRAFT_DAY_OF_WEEK`         | `"draft_day_of_week`         | `DRAFT, GAME` | `GROUP_BY`    | 1-7 (Mon-Sun)  | Int          |    
| `DRAFT_HOUR`                | `"draft_hour"`               | `DRAFT, GAME` | `GROUP_BY`    | 0-23            | Int             |   
| `DRAFT_WEEK`                | `"draft_week"`               | `DRAFT, GAME` | `GROUP_BY`    | 1-53            | Int             |   
| `FORMAT_WEEK`             | `"format_week"`     | `DRAFT, GAME` | `GROUP_BY` | 1 for `FORMAT_DAY` 1 - 7, etc. | Int | 
| `RANK`                      | `"rank"`                     | `DRAFT, GAME` | `GROUP_BY`    | Dataset column  | String          |    
| `USER_N_GAMES_BUCKET`       | `"user_n_games_bucket"`      | `DRAFT, GAME` | `GROUP_BY`    | Dataset Column  | Int             |    
| `USER_GAME_WIN_RATE_BUCKET` | `"user_game_win_rate_bucket` | `DRAFT, GAME` | `GROUP_BY`    | Dataset Column  | Float           |    
| `PLAYER_COHORT`    | `"player_cohort"`   | `DRAFT, GAME` | `GROUP_BY`    | In-sample version of "Top", "Middle", "Bottom", etc based on `USER_GAME_WIN_RATE_BUCKET`. Thresholds are 49% and 57% and 100 games played. | String          |
| `EVENT_MATCH_WINS`       | `"event_match_wins`        | `DRAFT` | `GROUP_BY`    | Dataset Column                               | Int      |
| `EVENT_MATCH_WINS_SUM`   | `"event_match_wins_sum`    | `DRAFT` | `PICK_SUM`    |                                              | Int      |
| `EVENT_MATCH_LOSSES`     | `"event_match_losses`      | `DRAFT` | `GROUP_BY`    | Dataset Column                               | Int      |
| `EVENT_MATCH_LOSSES_SUM` | `"event_match_losses_sum"` | `DRAFT` | `PICK_SUM`    |                                              | Int      |
| `EVENT_MATCHES`          | `"event_matches"`          | `DRAFT` | `GROUP_BY`    |                                              | Int      |
| `EVENT_MATCHES_SUM`      | `"event_matches_sum"`      | `DRAFT` | `PICK_SUM`    |                                              | Int      |
| `IS_TROPHY`              | `"is_trophy"`              | `DRAFT` | `GROUP_BY`    | 3 Match Wins if "Traditional", 7 if Premier  | Boolean  |
| `IS_TROPHY_SUM`          | `"is_trophy_sum"`          | `DRAFT` | `PICK_SUM`    |                                              | Int      |
| `PACK_NUMBER`            | `"pack_number`             | `DRAFT` | `FILTER_ONLY` | Dataset Column                               | Int      |
| `PACK_NUM`               | `"pack_num"`               | `DRAFT` | `GROUP_BY`    | 1-indexed                                    | Int      |
| `PICK_NUMBER`            | `"pick_number"`            | `DRAFT` | `FILTER_ONLY` | Dataset Column                               | Int      |
| `PICK_NUM`               | `"pick_num"`               | `DRAFT` | `GROUP_BY`    | 1-indexed                                    | Int      |
| `TAKEN_AT`               | `"taken_at`                | `DRAFT` | `PICK_SUM`    | Summable alias of `PICK_NUM`                 | Int      |
| `NUM_DRAFTS`           | `"num_drafts"` | `DRAFT` | `PICK_SUM` | | Int |
| `NUM_TAKEN`              | `"num_taken"`              | `DRAFT` | `PICK_SUM`    | Sum 1 over rows                              | Int      |
| `PICK`                   | `"pick"`                   | `DRAFT` | `FILTER_ONLY` | Dataset Column, joined as "name"             | String   |
| `PICK_MAINDECK_RATE`     | `"pick_maindeck_rate"`     | `DRAFT` | `PICK_SUM`    | Dataset Column                               | Float    |
| `PICK_SIDEBOARD_IN_RATE` | `"pick_sideboard_in_rate`  | `DRAFT` | `PICK_SUM`    | Dataset Column                               | Float    |
| `PACK_CARD`        | `"pack_card`        | `DRAFT`       | `NAME_SUM`    | Dataset Column                                                                                                                             | Int             |
| `LAST_SEEN`        | `"last_seen"`       | `DRAFT`       | `NAME_SUM`    | `PACK_CARD` times `min(8, PICK_NUM)`, add 8 to give last pick num seen when summed                                                         | Int             |
| `NUM_SEEN`         | `"num_seen"`        | `DRAFT`       | `NAME_SUM`    | `PACK_CARD` for `PICK_NUM` less than 9                                                                                                     | Int             |
| `POOL`             | `"pool"`            | `DRAFT`       | `NAME_SUM`    | Dataset Column                                                                                                                             | Int             |
| `GAME_TIME`        | `"game_time"`       | `GAME`        | `FILTER_ONLY` | Dataset Column                                                                                                                             | String          |
| `GAME_DATE`        | `"game_date"`       | `GAME`        | `GROUP_BY`    |                                                                                                                                            | `datetime.date` |
| `GAME_DAY_OF_WEEK` | `"game_day_of_week` | `GAME`        | `GROUP_BY`    | 1-7 (Mon-Sun)                                                                                                                              | Int             |
| `GAME_HOUR`        | `"game_hour"`       | `GAME`        | `GROUP_BY`    | 0-23                                                                                                                                       | Int             |
| `GAME_WEEK`        | `"game_week"`       | `GAME`        | `GROUP_BY`    | 1-53                                                                                                                                       | Int             |
| `BUILD_INDEX`      | `"build_index"`     | `GAME`        | `GROUP_BY`    | Dataset Column                                                                                                                             | Int             |
| `MATCH_NUMBER`     | `"match_number"`    | `GAME`        | `GROUP_BY`    | Dataset Column                                                                                                                             | Int             |
| `GAME_NUMBER`      | `"game_number"`     | `GAME`        | `GROUP_BY`    | Dataset Column                                                                                                                             | Int             |
| `NUM_EVENTS` | `"num_events"` | `GAME` | `GAME_SUM` | | Int |
| `NUM_MATCHES` | `"num_matches"` | `GAME` | `GAME_SUM` | | Int |
| `NUM_GAMES` | `"num_games"` | `GAME` | `GAME_SUM` | | Int |
| `OPP_RANK`         | `"opp_rank"`        | `GAME`        | `GROUP_BY`    | Dataset Column (tends to be blank)                                                                                                         | String          |
| `MAIN_COLORS`      | `"main_colors"`     | `GAME`        | `GROUP_BY`    | Dataset Column                                                                                                                             | String          |
| `NUM_COLORS`       | `"num_colors"`      | `GAME`        | `GROUP_BY`    | `len(MAIN_COLORS)`                                                                                                                         | Int             |
| `SPLASH_COLORS`    | `"splash_colors"`   | `GAME`        | `GROUP_BY`    | Dataset Column                                                                                                                             | String          |
| `HAS_SPLASH`       | `"has_splash"`      | `GAME`        | `GROUP_BY`    |                                                                                                                                            | Boolean         |
| `ON_PLAY`               | `"on_play"`               | `GAME` | `GROUP_BY` | Dataset Column | Boolean |
| `NUM_ON_PLAY`           | `"num_on_play"`           | `GAME` | `GAME_SUM` |                | Int     |
| `NUM_MULLIGANS`         | `"num_mulligans"`         | `GAME` | `GROUP_BY` | Dataset Column | Boolean |
| `NUM_MULLIGANS_SUM`     | `"num_mulligans_sum"`     | `GAME` | `GAME_SUM` |                | Int     |
| `OPP_NUM_MULLIGANS`     | `"opp_num_mulligans"`     | `GAME` | `GROUP_BY` | Dataset Column | Boolean |
| `OPP_NUM_MULLIGANS_SUM` | `"opp_num_mulligans_sum"` | `GAME` | `GAME_SUM` |                | Int     |
| `OPP_COLORS`            | `"opp_colors"`            | `GAME` | `GROUP_BY` | Dataset Column | Boolean |
| `NUM_TURNS`             | `"num_turns"`             | `GAME` | `GROUP_BY` | Dataset Column | Int     |
| `NUM_TURNS_SUM`         | `"num_turns_sum"`         | `GAME` | `GROUP_BY` |                | Int     |
| `WON`             | `"won"`   |   `GAME` | `GROUP_BY` | Dataset Column | Boolean |
| `NUM_WON` |   `"num_won"` | `GAME` | `GAME_SUM` |   | Int |
| `OPENING_HAND` | `"opening_hand"` | `GAME` | `NAME_SUM` | | Int |
| `WON_OPENING_HAND` | `"won_opening_hand"` | `GAME` | `NAME_SUM` | `WON * OPENING_HAND`| Int |
| `DRAWN` | `"drawn"` | `GAME` | `NAME_SUM` | | Int |
| `WON_DRAWN` | `"won_drawn"` | `GAME` | `NAME_SUM` | `WON * DRAWN`| Int |
| `TUTORED` | `"tutored"` | `GAME` | `NAME_SUM` | | Int |
| `WON_TUTORED` | `"won_tutored"` | `GAME` | `NAME_SUM` | `WON * TUTORED`| Int |
| `DECK` | `"deck"` | `GAME` | `NAME_SUM` | | Int |
| `WON_DECK` | `"won_deck"` | `GAME` | `NAME_SUM` | `WON * DECK`| Int |
| `SIDEBOARD` | `"sideboard"` | `GAME` | `NAME_SUM` | | Int |
| `WON_SIDEBOARD` | `"won_sideboard"` | `GAME` | `NAME_SUM` | `WON * SIDEBOARD`| Int |
| `NUM_GNS` | '"num_ns"` | `GAME` | `NAME_SUM` | `max(DECK - TUTORED - DRAWN - OPENING_HAND)` | Int |
| `WON_NUM_GNS` | `"won_num_gms"` | `GAME` | `NAME_SUM` | | Int |
| `SET_CODE` | `"set_code"` | `CARD` | `CARD_ATTR` | | String |
| `COLOR` | `"color"` | `CARD` | `CARD_ATTR` | | String |
| `RARITY` | `"rarity"` | `CARD` | `CARD_ATTR` | | String |
| `COLOR_IDENTITY` | `"color_identity"` | `CARD` | `CARD_ATTR` | | String |
| `CARD_TYPE` | `"card_type"` | `CARD` | `CARD_ATTR` | | String |
| `SUBTYPE` | `"subtype"` | `CARD` | `CARD_ATTR` | | String |
| `MANA_VALUE` | `"mana_value"` | `CARD` | `CARD_ATTR` | | Float |
| `DECK_MANA_VALUE` | `"deck_mana_value"` | | `NAME_SUM` | `DECK` * `MANA_VALUE` | Float |
| `DECK_LANDS` | `"deck_lands"` | | `NAME_SUM` | Number of lands in deck | Float |
| `DECK_SPELLS` | `"deck_spells"` | | `NAME_SUM` | Number of spells in deck | Float |
| `MANA_COST` | `"mana_cost"` | `CARD` | `CARD_ATTR` | | String |
| `POWER` | `"power"` | `CARD` | `CARD_ATTR` | | Float |
| `TOUGHNESS` | `"toughness"` | `CARD` | `CARD_ATTR` | | Float |
| `IS_BONUS_SHEET` | `"is_bonus_sheet"` | `CARD` | `CARD_ATTR` | `SET_CODE` != `EXPANSION` | Boolean |
| `IS_DFC` | `"is_dfc"` | `CARD` | `CARD_ATTR` | Includes split cards | Boolean |
| `ORACLE_TEXT` | `"oracle_text"` | `CARD` | `CARD_ATTR` | | String |
| `CARD_JSON` | `"card_json"` | `CARD` | `CARD_ATTR` | The full dump of the mtgjson entry for the card as printed in the draft booster | String |
| `PICKED_MATCH_WR` | `"picked_match_wr"` | | `AGG` | `EVENT_MATCH_WINS` / `EVENT_MATCHES` | Float |
| `TROPHY_RATE` | `"trophy_rate"` | | `AGG` || Float |
| `GAME_WR` | `"game_wr"` | | `AGG` | `NUM_WON` / `NUM_GAMES` | Float |
| `ALSA` | `"alsa"` | | `AGG` | `LAST_SEEN` / `NUM_SEEN` | Float |
| `ATA` | `"ata"` | | `AGG` | `PICKED_AT` / `NUM_PICKED` | Float |
| `NUM_GP` | `"num_gp"` | | `AGG` | `DECK` | Int |
| `PCT_GP` | `"pct_gp"` | | `AGG` | `DECK` / (`DECK` + `SIDEBOARD`) | Float |
| `GP_WR` | `"gp_wr"` | | `AGG` | `WON_DECK` / `DECK` | Float |
| `NUM_OH` | `"num_oh"` | | `AGG` || Int |
| `OH_WR` | `"oh_wr"` | | `AGG` || Float |
| `NUM_GIH` | `"num_gih"` | | `AGG` |`OPENING_HAND` + `DRAWN`| Int |
| `NUM_GIH_WON` | `"num_gih_won"` | | `AGG` | `WON_OPENING_HAND` + `WON_DRAWN` | Int |
| `GIH_WR` | `"gih_wr"` | | `AGG` | `NUM_GIH_WON` / `NUM_GIH` | Float |
| `GNS_WR` | `"gns_Wr"` | | `AGG` | `WON_NUM_GNS` / `NUM_GNS` | Float |
| `IWD` | `"iwd"` | | `AGG` | `GIH_WR - GNS_WR` | Float |
| `NUM_IN_POOL` | `"num_in_pool"` | | `AGG` | `DECK` + `SIDEBOARD`| Int |
| `IN_POOL_WR` | `"in_pool_wr"` | | `AGG` || Float |
| `DECK_TOTAL` | `"deck_total"` | | `AGG` | Sum `DECK` over all rows and broadcast back to row level| Int |
| `WON_DECK_TOTAL` | `"won_deck_total"` | | `AGG` || Int |
| `GP_WR_MEAN` | `"gp_wr_mean"` | | `AGG` | `WON_DECK_TOTAL` / `DECK_TOTAL` | Float |
| `GP_WR_EXCESS` | `"gp_wr_excess"` | | `AGG` | `GP_WR - GP_WR_MEAN` | Float |
| `GP_WR_VAR` | `"gp_wr_var"` | | `AGG` | Game-weighted Variance | Float |
| `GP_WR_STDEV` | `"gp_wr_stdev"` | | `AGG` | Sqrt of `GP_WR_VAR` | Float |
| `GP_WR_Z` | `"gp_wr_z"` | | `AGG` | `GP_WR_EXCESS` / `GP_WR_STDEV` | Float |
| `GIH_TOTAL` | `"gih_total"` | | `AGG` | Sum `NUM_GIH` over all rows and broadcast back to row level| Float |
| `WON_GIH_TOTAL` | `"won_gih_total"` | | `AGG` | | Float |
| `GIH_WR_MEAN` | `"gih_wr_mean"` | | `AGG` | `WON_GIH_TOTAL` / `GIH_TOTAL` | Float |
| `GIH_WR_EXCESS` | `"gih_wr_excess"` | | `AGG` | `GIH_WR - GIH_WR_MEAN` | Float |
| `GIH_WR_VAR` | `"gih_wr_var"` | | `AGG` | Game-weighted Variance | Float |
| `GIH_WR_STDEV` | `"gh_wr_stdev"` | | `AGG` | Sqrt of `GIH_WR_VAR` | Float |
| `GIH_WR_Z` | `"gih_wr_z"` | | `AGG` |`GIH_WR_EXCESS` / `GIH_WR_STDEV` | Float |
| `DECK_MANA_VALUE_AVG` | `"deck_mana_value_avg"` | | `AGG` | `DECK_MANA_VALUE ` / `DECK_SPELLS` | Float |
| `DECK_LANDS_AVG` | `"deck_lands_avg"` | | `AGG` | `DECK_LANDS ` / `NUM_GAMES` | Float |
| `DECK_SPELLS_AVG` | `"deck_spells_avg"` | | `AGG` | `DECK_SPELLS ` / `NUM_GAMES` | Float |

# Roadmap to 1.0

- [ ] Support Traditional and Premier datasets (currently only Premier is supported)
- [ ] Enable configuration using $XDG_CONFIG_HOME/cfg.toml
- [ ] Enhanced profiling
- [ ] Optimized caching strategy
- [ ] Organize and analyze daily downloads from 17Lands (not a scraper!)
- [ ] Helper functions to generate second-order analysis by card name
- [ ] Helper functions for common plotting paradigms
- [ ] Scientific workflows: regression, MLE, etc
