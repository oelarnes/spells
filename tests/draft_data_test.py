"""
test dataframe outputs
"""

import os
import pytest

import polars as pl

from mdu import draft_data
import mdu.columns as mcol
import mdu.cache_17l as c17
from mdu.enums import View, ColName
from mdu.get_schema import schema

os.environ["MDU_PROJECT_DIR"] = "tests"  # will only work from project directory

df_path = c17.data_file_path("BLB", View.DRAFT)
draft_df = pl.scan_csv(df_path, schema=schema(df_path))

@pytest.mark.parametrize(
    "col, expected",
    [
        (
            ColName.DRAFT_ID,
            """shape: (5, 1)
┌─────────────────────────────────┐
│ draft_id                        │
│ ---                             │
│ str                             │
╞═════════════════════════════════╡
│ deaa4cdcd3e84d8e8b5a0ea34a0f9d… │
│ deaa4cdcd3e84d8e8b5a0ea34a0f9d… │
│ deaa4cdcd3e84d8e8b5a0ea34a0f9d… │
│ deaa4cdcd3e84d8e8b5a0ea34a0f9d… │
│ deaa4cdcd3e84d8e8b5a0ea34a0f9d… │
└─────────────────────────────────┘"""
        ),
        (
            ColName.DRAFT_WEEK,
            """shape: (5, 1)
┌────────────┐
│ draft_week │
│ ---        │
│ i8         │
╞════════════╡
│ 31         │
│ 31         │
│ 31         │
│ 31         │
│ 31         │
└────────────┘"""
        ),
        (
            ColName.LAST_SEEN,
            """shape: (5, 276)
┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
│ last_seen ┆ last_seen ┆ last_seen ┆ last_seen ┆ … ┆ last_seen ┆ last_seen ┆ last_seen ┆ last_see │
│ _Agate    ┆ _Agate-Bl ┆ _Alania's ┆ _Alania,  ┆   ┆ _Wildfire ┆ _Wishing  ┆ _Ygra,    ┆ n_Zorali │
│ Assault   ┆ ade       ┆ Pathmaker ┆ Divergent ┆   ┆ Howl      ┆ Well      ┆ Eater of  ┆ ne,      │
│ ---       ┆ Assassin  ┆ ---       ┆ St…       ┆   ┆ ---       ┆ ---       ┆ All       ┆ Cosmos   │
│ i32       ┆ ---       ┆ i32       ┆ ---       ┆   ┆ i32       ┆ i32       ┆ ---       ┆ Cal…     │
│           ┆ i32       ┆           ┆ i32       ┆   ┆           ┆           ┆ i32       ┆ ---      │
│           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ i32      │
╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
│ 0         ┆ 0         ┆ 0         ┆ 1         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 2         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 5         ┆ 5         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
└───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘"""
        ),
    ]
)
def test_col_df(col, expected):
    result = draft_data.col_df(draft_df, col, mcol.col_def_map, is_base_view=True).head().collect()
    assert str(result) == expected
