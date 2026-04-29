import polars as pl
import pytest

from spells import summon
from spells.columns import P1P1_MISSING_SETS
from tests.conftest import FAKE_CARD_NAMES


def test_summon_returns_dataframe(fake_draft_sets):
    df = summon("TST", ["num_drafts"])
    assert isinstance(df, pl.DataFrame)
    assert "num_drafts" in df.columns


def test_num_drafts_normal_set_uses_p1p1(fake_draft_sets):
    # TST has 3 p1p1 rows and 2 p1p2 rows; correct result is 3.
    df = summon("TST", ["num_drafts"])
    assert df["num_drafts"].sum() == 3


def test_num_drafts_missing_set_uses_p1p2(fake_draft_sets):
    # TLA has 0 p1p1 rows and 2 p1p2 rows; correct result is 2.
    df = summon("TLA", ["num_drafts"])
    assert df["num_drafts"].sum() == 2


def test_num_drafts_regression_normal_set_not_p1p2(fake_draft_sets):
    # If the bug were present (always p1p2), TST would return 2 not 3.
    df = summon("TST", ["num_drafts"])
    assert df["num_drafts"].sum() != 2


def test_num_drafts_regression_missing_set_not_p1p1(fake_draft_sets):
    # If fixed to always p1p1, TLA would return 0 (no p1p1 rows exist).
    df = summon("TLA", ["num_drafts"])
    assert df["num_drafts"].sum() != 0


def test_num_taken_counts_all_picks(fake_draft_sets):
    # TST fixture has 5 pick rows total.
    df = summon("TST", ["num_taken"])
    assert df["num_taken"].sum() == 5


def test_summon_group_by_expansion(fake_draft_sets):
    df = summon(["TST", "TLA"], ["num_drafts"], group_by=["expansion"])
    tst = df.filter(pl.col("expansion") == "TST")["num_drafts"].sum()
    tla = df.filter(pl.col("expansion") == "TLA")["num_drafts"].sum()
    assert tst == 3
    assert tla == 2


def test_filter_spec_pack_num(fake_draft_sets):
    # All rows are pack 1 (pack_number=0); filtering to pack 2 should give 0.
    df = summon("TST", ["num_taken"], filter_spec={"pack_num": 2})
    assert df["num_taken"].sum() == 0


def test_p1p1_missing_sets_constant():
    assert "TLA" in P1P1_MISSING_SETS
    assert "TMT" in P1P1_MISSING_SETS
    assert "ECL" in P1P1_MISSING_SETS
    assert "BLB" not in P1P1_MISSING_SETS
