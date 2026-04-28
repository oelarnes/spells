# CLAUDE.md — spells-mtg

## What this library is

`spells-mtg` is a Python analytics library for MTG draft data from 17Lands.com. The primary
API is `summon(set_code, columns, group_by, filter_spec, extensions, card_context)` which
returns a Polars DataFrame.

There are **two distinct data paths** and it's critical to keep them straight:

### Path 1 — Public parquet files (original design)
17Lands releases full public datasets as CSV/parquet at least **two weeks after** a set's
Arena release. `spells add DSK` downloads these, converts to parquet, and caches locally.
`summon()` without `cdfs` reads from these local parquet files. This is the analytics
library use case.

### Path 2 — Daily web API via `cdfs` (CardDataFileSpec)
The 17Lands website exposes daily aggregated card ratings at
`17lands.com/card_ratings/data?expansion=...&start_date=...&end_date=...`. `cdfs` is a
bolt-on path that makes `summon()` hit this API instead of local parquet. This is how
**DEq uses spells exclusively** — parquet files are generally not present in the DEq
workflow. See `card_data_files.py` for the API client and column mapping.

## MTG Arena release cadence

Sets release on **Arena on Tuesday**, three days before the official **paper release on
Friday**. MTGJSON and Scryfall both use the Friday date. Start dates in DEq's config and in
`START_DATE_MAP` are the Tuesday Arena dates. This means MTGJSON is unavailable when 17Lands
first has data. The goal for DEq is data on Wednesday morning when community interest peaks.

## Key architectural facts

**`card_only=cdfs is not None` (draft_data.py line 544):** When `cdfs` is provided,
`_hydrate_col_defs()` is called with `card_only=True`. This causes `_get_card_context()` to
take the `else` branch — it calls `get_names()` and returns empty context dicts, never
touching the card parquet. CARD_ATTR columns in the cdfs path get their values from the
ratings API response directly, not from card context.

**`get_names()` fallback:** When card parquet doesn't exist, `get_names()` falls back to
`base_ratings_df(set_code)` with no `start_date`. This hits `START_DATE_MAP` in
`card_data_files.py`. For a new set not yet in that map, this raises `KeyError` — the sole
failure point when DEq first processes a new set. Everything else in the cdfs path works
because `start_date` is passed explicitly from DEq's config.

**`START_DATE_MAP` in `card_data_files.py`:** Exists only to support the `get_names()`
fallback. It's a maintenance burden — every new set requires updating it in addition to
`deq/main.py`. The canonical start dates live in `deq/deq/main.py`'s `config` dict.

**`deq/start_days.py`:** Dead file, no imports anywhere.

## Module roles

| Module | Role |
|--------|------|
| `columns.py` | Defines 150+ `ColSpec` objects — the column catalogue |
| `draft_data.py` | `summon()` pipeline: dependency resolution, caching, view selection |
| `manifest.py` | Validates column combinations, resolves which views are needed |
| `filter.py` | Filter DSL: turns dict specs into Polars expressions |
| `cache.py` | Parquet file paths, cache key logic, clean/data-home utilities |
| `external.py` | CLI: `spells add`, `spells clean`, `spells refresh`, `spells info` |
| `cards.py` | Fetches card attributes from MTGJSON; called during `spells add` |
| `card_data_files.py` | 17Lands daily ratings API client; used by DEq via `cdfs` |
| `extension.py` | Pre-built `ColSpec` factories for statistical extensions |

## ColType reference

Seven types drive all pipeline logic:

- `FILTER_ONLY` — can be used in filter_spec, never appears in output
- `GROUP_BY` — goes into `group_by`, not `columns`; available in expressions post-agg
- `PICK_SUM` — summed per pick in draft view
- `GAME_SUM` — summed per game in game view
- `NAME_SUM` — one expression per card name, pivoted back by name after aggregation
- `AGG` — computed post-aggregation from already-aggregated columns
- `CARD_ATTR` — static card attribute; in non-cdfs path comes from card parquet

## MTGJSON — scope and limits

MTGJSON is called in exactly one place: `cards.card_df()` in `cards.py`, invoked only by
`write_card_file()` in `external.py` during `spells add` / `spells refresh`. It is never
called at runtime. The cdfs path has zero dependency on MTGJSON or on the card parquet file
that `write_card_file()` produces.

`_add()` in `external.py` runs:
1. Download draft parquet from 17Lands
2. `write_card_file()` — calls MTGJSON to build the card attribute parquet
3. `get_set_context()` — computes `release_date` (= `min(draft_date)`) and `picks_per_pack`
4. Download game parquet from 17Lands

On Arena release day (Tuesday), MTGJSON returns 404 for the new set (official release is
Friday), so step 2 fails and steps 3–4 are never reached. This only affects the analytics
library use case — DEq's Wednesday 4am run is entirely unaffected since it uses `cdfs`.

## Version and publishing

- Package name on PyPI: `spells-mtg`
- Version source: `[tool.pdm.version] source = "scm"` — version is read from git tags
- `fetch-depth: 0` is required in CI for this to work
- GitHub Actions workflow at `.github/workflows/publish.yml` publishes on `v*` tag push
  using PyPI Trusted Publishing (OIDC, no token needed); configure the publisher at
  pypi.org → spells-mtg → Manage → Publishing

## DEq dependency

`deq` imports `spells` as a local file dependency:
`spells-mtg @ file:///home/joel/dev/spells`. This is a **non-editable copy install** —
spells source files are copied into deq's site-packages at `pdm install` time, not
symlinked. Changes to spells are invisible to deq until `pdm install` is re-run there.

`deq/pyproject.toml` has a `pre_install` hook (`scripts/check_spells_branch.py`) that
aborts `pdm install` if spells is not on `main`. This prevents accidentally snapshotting
a feature branch into deq's venv.

Safe workflow: develop spells on a `cc-*` branch, merge to main, then run `pdm install`
in deq. Test both after any changes to `summon()`, `ColSpec`, `ColType`, or
`card_data_files.py`.
