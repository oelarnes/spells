import polars as pl

from spells import summon
from spells.columns import P1P1_MISSING_SETS


def test_summon_returns_dataframe(fake_draft_sets):
    df = summon("TST", ["num_drafts"])
    assert isinstance(df, pl.DataFrame)
    assert "num_drafts" in df.columns


def test_num_drafts_normal_set_uses_p1p1(fake_draft_sets):
    # TST has 5 p1p1 rows and 4 p1p2 rows; correct result is 5.
    df = summon("TST", ["num_drafts"])
    assert df["num_drafts"].sum() == 5


def test_num_drafts_missing_set_uses_p1p2(fake_draft_sets):
    # TLA has 0 p1p1 rows and 2 p1p2 rows; correct result is 2.
    df = summon("TLA", ["num_drafts"])
    assert df["num_drafts"].sum() == 2


def test_num_taken_counts_all_picks(fake_draft_sets):
    # TST fixture has 14 pick rows total (see _tst_draft_rows() docstring).
    df = summon("TST", ["num_taken"])
    assert df["num_taken"].sum() == 14


def test_summon_group_by_expansion(fake_draft_sets):
    df = summon(["TST", "TLA"], ["num_drafts"], group_by=["expansion"])
    tst = df.filter(pl.col("expansion") == "TST")["num_drafts"].sum()
    tla = df.filter(pl.col("expansion") == "TLA")["num_drafts"].sum()
    assert tst == 5
    assert tla == 2


def test_filter_spec_pack_num(fake_draft_sets):
    # Pack distribution: 9 picks in pack 1, 3 in pack 2, 2 in pack 3.
    df1 = summon("TST", ["num_taken"], filter_spec={"pack_num": 1})
    df2 = summon("TST", ["num_taken"], filter_spec={"pack_num": 2})
    df3 = summon("TST", ["num_taken"], filter_spec={"pack_num": 3})
    assert df1["num_taken"].sum() == 9
    assert df2["num_taken"].sum() == 3
    assert df3["num_taken"].sum() == 2


def test_summon_traditional_event_type(fake_trad_set):
    # TRD Traditional has 3 pick rows; Premier has 2. Selecting the event_type
    # reads the matching draft parquet.
    trad = summon("TRD", ["num_taken"], event_type="TradDraft")
    premier = summon("TRD", ["num_taken"])
    assert trad["num_taken"].sum() == 3
    assert premier["num_taken"].sum() == 2


def test_summon_aggregates_premier_and_trad(fake_trad_set):
    # A parallel event_type list aggregates the two formats like multiple sets;
    # the cache key includes event_type so the two runs don't collide.
    both = summon(
        ["TRD", "TRD"],
        ["num_taken"],
        event_type=["PremierDraft", "TradDraft"],
    )
    assert both["num_taken"].sum() == 5


def test_traditional_trophy_uses_three_match_wins(fake_trad_set):
    # IS_TROPHY keys off the "TradDraft" event_type string; the trophy draft
    # (event_match_wins=3) contributes both its picks. Would be 0 under the old
    # "Traditional" comparison.
    df = summon("TRD", ["is_trophy_sum"], event_type="TradDraft")
    assert df["is_trophy_sum"].sum() == 2


def test_p1p1_missing_sets_constant():
    assert "TLA" in P1P1_MISSING_SETS
    assert "TMT" in P1P1_MISSING_SETS
    assert "ECL" in P1P1_MISSING_SETS
    assert "BLB" not in P1P1_MISSING_SETS
