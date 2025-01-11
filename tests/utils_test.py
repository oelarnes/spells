
"""
Test behavior of wavg utility for Polars DataFrames
"""

import pytest
import polars as pl

import spells.utils as utils

def format_test_string(test_string: str) -> str:
    """
    strip whitespace from each line to test pasted dataframe outputs
    """
    return "\n".join(
        [line.strip() for line in test_string.splitlines() if line.strip()]
    )

test_df = pl.DataFrame({
    'cat': ['a',    'a',    'b',    'b',    'b',    'c'     ],
    'va1': [1.0,    -1.0,   0.2,    0.4,    0.0,    10.0    ],
    'va2': [4.0,    3.0,    1.0,    -2.0,   2.0,    1.0     ],
    'wt1': [1,      2,      0,      2,      3,      1       ],
    'wt2': [2,      4,      1,      1,      1,      2,      ],
})


# test wavg with default args 
@pytest.mark.parametrize(
    "cols, weights, expected",
    [
        (
            'va1', 
            'wt1', 
            """
shape: (1, 2)
┌─────┬──────────┐
│ wt1 ┆ va1      │
│ --- ┆ ---      │
│ i64 ┆ f64      │
╞═════╪══════════╡
│ 9   ┆ 1.088889 │
└─────┴──────────┘
"""
         ),
        (
            ['va1', 'va2'],
            'wt1',
            """
shape: (1, 3)
┌─────┬──────────┬──────────┐
│ wt1 ┆ va1      ┆ va2      │
│ --- ┆ ---      ┆ ---      │
│ i64 ┆ f64      ┆ f64      │
╞═════╪══════════╪══════════╡
│ 9   ┆ 1.088889 ┆ 1.444444 │
└─────┴──────────┴──────────┘
"""
        ),
        (
            ['va1', 'va2'],
            ['wt1', 'wt2'],
            """
shape: (1, 4)
┌─────┬─────┬──────────┬──────────┐
│ wt1 ┆ wt2 ┆ va1      ┆ va2      │
│ --- ┆ --- ┆ ---      ┆ ---      │
│ i64 ┆ i64 ┆ f64      ┆ f64      │
╞═════╪═════╪══════════╪══════════╡
│ 9   ┆ 11  ┆ 1.088889 ┆ 2.090909 │
└─────┴─────┴──────────┴──────────┘
"""
        ),
        (
            [pl.col('va1') + 1, 'va2'],
            ['wt1', pl.col('wt2') + 1],
            """
shape: (1, 4)
┌─────┬─────┬──────────┬──────────┐
│ wt1 ┆ wt2 ┆ va1      ┆ va2      │
│ --- ┆ --- ┆ ---      ┆ ---      │
│ i64 ┆ i64 ┆ f64      ┆ f64      │
╞═════╪═════╪══════════╪══════════╡
│ 9   ┆ 17  ┆ 2.088889 ┆ 1.882353 │
└─────┴─────┴──────────┴──────────┘
"""
        ),
    ] 
)
def test_wavg_defaults(cols: str | pl.Expr | list[str | pl.Expr], weights: str | pl.Expr | list[str | pl.Expr], expected: str):
    result = utils.wavg(test_df, cols, weights)

    test_str = str(result)
    print(test_str)
    assert test_str == format_test_string(expected)


# test wavg with named args
@pytest.mark.parametrize(
    "cols, weights, group_by, new_names, expected",
    [
        (
            "va1",
            "wt1",
            [],
            "v1",
            """
shape: (1, 2)
┌─────┬──────────┐
│ wt1 ┆ v1       │
│ --- ┆ ---      │
│ i64 ┆ f64      │
╞═════╪══════════╡
│ 9   ┆ 1.088889 │
└─────┴──────────┘
"""
        ),
        (
            "va1",
            "wt1",
            "cat",
            "va1",
            """
shape: (3, 3)
┌─────┬─────┬───────────┐
│ cat ┆ wt1 ┆ va1       │
│ --- ┆ --- ┆ ---       │
│ str ┆ i64 ┆ f64       │
╞═════╪═════╪═══════════╡
│ a   ┆ 3   ┆ -0.333333 │
│ b   ┆ 5   ┆ 0.16      │
│ c   ┆ 1   ┆ 10.0      │
└─────┴─────┴───────────┘
"""
        ),
        (
            ["va1", "va1"],
            ["wt1", "wt2"],
            ["cat"],
            ["v@1", "v@2"],
            """
shape: (3, 5)
┌─────┬─────┬─────┬───────────┬───────────┐
│ cat ┆ wt1 ┆ wt2 ┆ v@1       ┆ v@2       │
│ --- ┆ --- ┆ --- ┆ ---       ┆ ---       │
│ str ┆ i64 ┆ i64 ┆ f64       ┆ f64       │
╞═════╪═════╪═════╪═══════════╪═══════════╡
│ a   ┆ 3   ┆ 6   ┆ -0.333333 ┆ -0.333333 │
│ b   ┆ 5   ┆ 3   ┆ 0.16      ┆ 0.2       │
│ c   ┆ 1   ┆ 2   ┆ 10.0      ┆ 10.0      │
└─────┴─────┴─────┴───────────┴───────────┘
"""
        )
    ]
)
def test_wavg(
    cols: str | pl.Expr | list[str | pl.Expr], 
    weights: str | pl.Expr | list[str | pl.Expr], 
    group_by: str | pl.Expr | list[str | pl.Expr],
    new_names: str | list[str],
    expected: str,
):
    result = utils.wavg(
        test_df, 
        cols, 
        weights,
        group_by=group_by,
        new_names=new_names,
    )

    test_str = str(result)
    print(test_str)
    assert test_str == format_test_string(expected)

