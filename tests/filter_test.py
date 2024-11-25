"""
Test behavior of filters from dict specification
"""

import pytest
import polars as pl

import spells.filter

ROW_0 = {"int": 1, "float": 2.0, "text": "hi"}
ROW_1 = {"int": 0, "float": -0.4, "text": "foo"}
ROW_2 = {"int": -10, "float": 3.14, "text": "bar"}

TEST_DF = pl.DataFrame([ROW_0, ROW_1, ROW_2])


def format_test_string(test_string: str) -> str:
    """
    strip whitespace from each line to test pasted dataframe outputs
    """
    return "\n".join(
        [line.strip() for line in test_string.splitlines() if line.strip()]
    )


@pytest.mark.parametrize(
    "filter_spec, expected",
    [
        (None, None),
        (
            {"int": 1},
            """
shape: (1, 3)
┌─────┬───────┬──────┐
│ int ┆ float ┆ text │
│ --- ┆ ---   ┆ ---  │
│ i64 ┆ f64   ┆ str  │
╞═════╪═══════╪══════╡
│ 1   ┆ 2.0   ┆ hi   │
└─────┴───────┴──────┘
""",
        ),
        (
            {"lhs": "float", "rhs": 3, "op": "<"},
            """
shape: (2, 3)
┌─────┬───────┬──────┐
│ int ┆ float ┆ text │
│ --- ┆ ---   ┆ ---  │
│ i64 ┆ f64   ┆ str  │
╞═════╪═══════╪══════╡
│ 1   ┆ 2.0   ┆ hi   │
│ 0   ┆ -0.4  ┆ foo  │
└─────┴───────┴──────┘
        """,
        ),
        (
            {"$not": {"$or": [{"text": "foo"}, {"int": 1}]}},
            """
shape: (1, 3)
┌─────┬───────┬──────┐
│ int ┆ float ┆ text │
│ --- ┆ ---   ┆ ---  │
│ i64 ┆ f64   ┆ str  │
╞═════╪═══════╪══════╡
│ -10 ┆ 3.14  ┆ bar  │
└─────┴───────┴──────┘
        """,
        ),
        (
            {
                "$and": [
                    {"lhs": "text", "rhs": ["foo", "bar", "hi"], "op": "in"},
                    {"lhs": "int", "rhs": [1, 2], "op": "nin"},
                    {"lhs": "float", "rhs": 2.4, "op": "<"},
                ]
            },
            """
shape: (1, 3)
┌─────┬───────┬──────┐
│ int ┆ float ┆ text │
│ --- ┆ ---   ┆ ---  │
│ i64 ┆ f64   ┆ str  │
╞═════╪═══════╪══════╡
│ 0   ┆ -0.4  ┆ foo  │
└─────┴───────┴──────┘
            """,
        ),
    ],
)
def test_from_spec(filter_spec: dict | None, expected: str | None):
    test_filter = spells.filter.from_spec(filter_spec)

    if expected is None:
        assert test_filter is None
    else:
        assert test_filter is not None
        test_str = str(TEST_DF.filter(test_filter.expr))
        print(test_str)
        assert test_str == format_test_string(expected)


@pytest.mark.parametrize(
    "test_filter, expected",
    [
        (spells.filter.from_spec({"int": 1}), {"int"}),
        (
            spells.filter.from_spec({"$not": {"$or": [{"text": "foo"}, {"int": 1}]}}),
            {"text", "int"},
        ),
        (
            spells.filter.from_spec(
                {
                    "$and": [
                        {"lhs": "text", "rhs": ["foo", "bar", "hi"], "op": "in"},
                        {"lhs": "int", "rhs": [1, 2], "op": "nin"},
                        {"lhs": "float", "rhs": 2.4, "op": "<"},
                    ]
                }
            ),
            {"int", "float", "text"},
        ),
    ],
)
def test_lhs(test_filter, expected):
    assert test_filter.lhs == expected
