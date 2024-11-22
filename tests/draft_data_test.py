"""
test dataframe outputs
"""

import os
import pytest

import polars as pl

from spells import draft_data
import spells.columns as mcol
import spells.external as external
from spells.enums import View, ColName
from spells.schema import schema

os.environ["MDU_PROJECT_DIR"] = "tests"  # will only work from project directory

df_path = external.data_file_path("BLB", View.DRAFT)
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
        (
            ColName.NUM_SEEN,
            """shape: (5, 276)
┌───────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬───────────┬──────────┐
│ num_seen_ ┆ num_seen_ ┆ num_seen_ ┆ num_seen_ ┆ … ┆ num_seen_ ┆ num_seen_ ┆ num_seen_ ┆ num_seen │
│ Agate     ┆ Agate-Bla ┆ Alania's  ┆ Alania,   ┆   ┆ Wildfire  ┆ Wishing   ┆ Ygra,     ┆ _Zoralin │
│ Assault   ┆ de        ┆ Pathmaker ┆ Divergent ┆   ┆ Howl      ┆ Well      ┆ Eater of  ┆ e,       │
│ ---       ┆ Assassin  ┆ ---       ┆ Sto…      ┆   ┆ ---       ┆ ---       ┆ All       ┆ Cosmos   │
│ i8        ┆ ---       ┆ i8        ┆ ---       ┆   ┆ i8        ┆ i8        ┆ ---       ┆ Call…    │
│           ┆ i8        ┆           ┆ i8        ┆   ┆           ┆           ┆ i8        ┆ ---      │
│           ┆           ┆           ┆           ┆   ┆           ┆           ┆           ┆ i8       │
╞═══════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪══════════╡
│ 0         ┆ 0         ┆ 0         ┆ 1         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 1         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 0         ┆ 0         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
│ 1         ┆ 1         ┆ 0         ┆ 0         ┆ … ┆ 0         ┆ 0         ┆ 0         ┆ 0        │
└───────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴───────────┴──────────┘"""
        ),
        (
            ColName.NUM_TAKEN,
            """shape: (5, 1)
┌───────────┐
│ num_taken │
│ ---       │
│ i32       │
╞═══════════╡
│ 1         │
│ 1         │
│ 1         │
│ 1         │
│ 1         │
└───────────┘"""
        ),
        (
            ColName.PLAYER_COHORT,
            """shape: (5, 1)
┌───────────────┐
│ player_cohort │
│ ---           │
│ str           │
╞═══════════════╡
│ Other         │
│ Other         │
│ Other         │
│ Other         │
│ Other         │
└───────────────┘"""
        )
    ]
)
def test_col_df(col, expected):
    result = draft_data.col_df(draft_df, col, mcol.col_def_map, is_view=True).head().collect()

    print(str(result))
    assert str(result) == expected
    

