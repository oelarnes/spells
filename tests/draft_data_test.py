"""
test dataframe outputs
"""

import os

import pandas

import mdu.cache
from mdu import draft_data

os.environ["MDU_PROJECT_DIR"] = "tests"  # will only work from project directory
pandas.options.display.max_rows = 1000
pandas.options.display.max_columns = 100


def test_game_counts():
    ddo = draft_data.DraftData("BLB")
    gc = repr(ddo.game_counts(read_cache=False, write_cache=False).head())
    with open("test.out", "w") as f:
        f.write(gc)

    assert False
