"""
test dataframe outputs
"""

import os
import pytest

import pandas

from mdu import draft_data
import mdu.columns as mcol
from mdu.enums import View, ColName

os.environ["MDU_PROJECT_DIR"] = "tests"  # will only work from project directory
pandas.options.display.max_rows = 1000
pandas.options.display.max_columns = 100

@pytest.mark.parametrize(
    "columns, expected",
    [
        (
            frozenset({ColName.DRAFT_TIME}),
            {
                View.DRAFT: frozenset({ColName.DRAFT_TIME}),
                View.GAME: frozenset({ColName.DRAFT_TIME}),
            }
        ),
        (
            frozenset({ColName.DRAFT_WEEK, ColName.PICK_NUM}),
            {
                View.DRAFT: frozenset({ColName.DRAFT_TIME, ColName.DRAFT_WEEK, ColName.PICK_NUM, ColName.PICK}),
                View.GAME: frozenset({ColName.DRAFT_TIME, ColName.DRAFT_WEEK}),
            }
        )
    ]
)
def test_get_manifest(columns, expected):
    cols_by_view = draft_data.get_manifest(columns, mcol.column_def_map)

    assert cols_by_view == expected
    

