"""
Tests for the cdfs (CardDataFileSpec) path through summon() — the DEq workflow.

All tests use fake set codes ("TST", "TS2") with no local parquet files. Ratings
JSON is pre-seeded in a temp directory so no network calls are made. This also
serves as a regression suite for the new-set KeyError that occurred when a set
code was absent from the (now-removed) START_DATE_MAP.

CardDataFileSpec no longer carries set_code/event_type — those come from summon()'s
own arguments, and `cdfs` accepts the same broadcast forms (bare, set-code-keyed,
tuple-keyed) as card_context/set_context. See context_test.py for that mechanism.
"""

import datetime

import polars as pl
import pytest

from spells.draft_data import CardDataFileSpec, _resolve_cdfs_window, summon
from spells.columns import ColSpec
from spells.enums import ColType, EventType

from tests.conftest import (
    FAKE_SET,
    FAKE_START,
    FAKE_END,
    FAKE_CARD_RATINGS,
    FAKE_SET_2,
    FAKE_START_2,
    FAKE_END_2,
    FAKE_CARD_RATINGS_2,
)


def make_cdfs(**kwargs) -> CardDataFileSpec:
    return CardDataFileSpec(
        start_date=FAKE_START,
        end_date=FAKE_END,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_cdfs_summon_returns_dataframe(fake_ratings_file):
    result = summon(FAKE_SET, ["num_gih", "gih_wr"], group_by=["name"], cdfs=make_cdfs())
    assert isinstance(result, pl.DataFrame)
    assert set(result.columns) == {"name", "num_gih", "gih_wr"}


def test_cdfs_summon_one_row_per_card(fake_ratings_file):
    result = summon(FAKE_SET, ["num_gih"], group_by=["name"], cdfs=make_cdfs())
    assert len(result) == len(FAKE_CARD_RATINGS)
    assert result["name"].is_unique().all()


def test_cdfs_summon_card_attr_columns(fake_ratings_file):
    result = summon(FAKE_SET, ["rarity", "color"], group_by=["name"], cdfs=make_cdfs())
    assert "rarity" in result.columns
    assert "color" in result.columns
    assert set(result["rarity"].to_list()) == {"common", "uncommon", "rare"}


def test_cdfs_summon_num_gih_values_match_source(fake_ratings_file):
    result = summon(FAKE_SET, ["num_gih"], group_by=["name"], cdfs=make_cdfs())
    result_map = dict(zip(result["name"].to_list(), result["num_gih"].to_list()))

    for card in FAKE_CARD_RATINGS:
        expected = card["opening_hand_game_count"] + card["drawn_game_count"]
        assert result_map[card["name"]] == expected


def test_cdfs_summon_no_parquet_files_needed(fake_ratings_file):
    # The external/ directory (parquet files) should never be created in the cdfs path.
    external_dir = fake_ratings_file / "external"
    summon(FAKE_SET, ["num_gih", "gih_wr"], group_by=["name"], cdfs=make_cdfs())
    assert not external_dir.exists()


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------
# filter_spec is a pre-aggregation, row-level filter — it removes individual
# draft/game rows before summing. The cdfs API response is already aggregated
# (one row per card), so there is nothing to pre-filter. filter_spec is not
# applicable in the cdfs path; filter the returned DataFrame instead.


def test_cdfs_summon_filter_spec_not_applicable(fake_ratings_file):
    # The manifest rejects filter_spec in the cdfs path when the filter column
    # is not in a resolved base view. This is the correct rejection: there are
    # no pre-aggregation rows to filter against.
    with pytest.raises(AssertionError, match="filter col rarity not found in base view"):
        summon(
            FAKE_SET,
            ["num_gih"],
            group_by=["name"],
            filter_spec={"rarity": "common"},
            cdfs=make_cdfs(),
        )


def test_cdfs_summon_filter_post_summon(fake_ratings_file):
    # Correct pattern for cdfs: fetch all cards, filter the resulting DataFrame.
    result = summon(FAKE_SET, ["num_gih", "rarity"], group_by=["name"], cdfs=make_cdfs())
    filtered = result.filter(pl.col("rarity") == "common")
    assert len(filtered) == 1
    assert filtered["name"][0] == "Aether Sprite"


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------


def test_cdfs_summon_with_extension_column(fake_ratings_file):
    extension = {
        "custom_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col("num_gih_won") / pl.col("num_gih"),
        )
    }
    result = summon(
        FAKE_SET,
        ["num_gih", "custom_rate"],
        group_by=["name"],
        extensions=extension,
        cdfs=make_cdfs(),
    )
    assert "custom_rate" in result.columns
    # custom_rate should equal gih_wr
    gih_wr = summon(FAKE_SET, ["gih_wr"], group_by=["name"], cdfs=make_cdfs())
    merged = result.join(gih_wr, on="name")
    for row in merged.iter_rows(named=True):
        if row["gih_wr"] is not None and row["custom_rate"] is not None:
            assert abs(row["custom_rate"] - row["gih_wr"]) < 1e-9


# ---------------------------------------------------------------------------
# event_type: now a plain summon() argument, not a CardDataFileSpec field
# ---------------------------------------------------------------------------


def test_cdfs_event_type_column_reflects_summon_event_type(fake_ratings_file):
    # fake_ratings_file seeds a ratings JSON for both formats under FAKE_SET, so
    # a bare cdfs (no set/event identity of its own) correctly follows whichever
    # event_type summon() is asked for.
    premier = summon(FAKE_SET, ["num_gih"], group_by=["name", "event_type"], cdfs=make_cdfs())
    trad = summon(
        FAKE_SET,
        ["num_gih"],
        group_by=["name", "event_type"],
        event_type=EventType.TRADITIONAL,
        cdfs=make_cdfs(),
    )
    assert set(premier["event_type"].to_list()) == {"PremierDraft"}
    assert set(trad["event_type"].to_list()) == {"TradDraft"}


def test_cdfs_bare_broadcasts_across_event_types(fake_ratings_file):
    # A single CardDataFileSpec, requested against both event types in one call —
    # this used to be impossible (cdfs was single-cell only).
    result = summon(
        FAKE_SET,
        ["num_gih"],
        group_by=["name", "event_type"],
        event_type=[EventType.PREMIER, EventType.TRADITIONAL],
        cdfs=make_cdfs(),
    )
    assert set(result["event_type"].to_list()) == {"PremierDraft", "TradDraft"}
    assert len(result) == 2 * len(FAKE_CARD_RATINGS)


# ---------------------------------------------------------------------------
# Multi-set broadcast forms (mirrors context_test.py's card_context/set_context
# coverage, applied to cdfs)
# ---------------------------------------------------------------------------


def test_cdfs_bare_broadcasts_same_literal_window_to_every_set(fake_ratings_file, monkeypatch):
    # A bare spec broadcasts the *same* literal window to every set — by design,
    # for cross-set comparisons over one calendar range (day-of-week/seasonal
    # analysis). FAKE_SET_2's ratings are only seeded at FAKE_START_2/FAKE_END_2,
    # so broadcasting FAKE_START/FAKE_END finds no cached file for TS2 and must
    # not silently succeed by reaching the real network.
    def _no_network(*args, **kwargs):
        raise AssertionError("must not hit the network for an unseeded window")

    monkeypatch.setattr("spells.card_data_files.wget.download", _no_network)

    with pytest.raises(AssertionError, match="must not hit the network"):
        summon(
            [FAKE_SET, FAKE_SET_2],
            ["num_gih"],
            group_by=["expansion", "name"],
            cdfs=make_cdfs(),  # literal FAKE_START/FAKE_END, wrong window for TS2
        )


def test_cdfs_set_code_keyed_dict_uses_per_set_window(fake_ratings_file):
    cdfs = {
        FAKE_SET: CardDataFileSpec(start_date=FAKE_START, end_date=FAKE_END),
        FAKE_SET_2: CardDataFileSpec(start_date=FAKE_START_2, end_date=FAKE_END_2),
    }
    result = summon(
        [FAKE_SET, FAKE_SET_2],
        ["num_gih"],
        group_by=["expansion", "name"],
        cdfs=cdfs,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


def test_cdfs_tuple_keyed_dict_uses_per_cell_window(fake_ratings_file):
    cdfs = {
        (FAKE_SET, EventType.PREMIER): CardDataFileSpec(
            start_date=FAKE_START, end_date=FAKE_END
        ),
        (FAKE_SET_2, EventType.PREMIER): CardDataFileSpec(
            start_date=FAKE_START_2, end_date=FAKE_END_2
        ),
    }
    result = summon(
        [FAKE_SET, FAKE_SET_2],
        ["num_gih"],
        group_by=["expansion", "name"],
        cdfs=cdfs,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


# ---------------------------------------------------------------------------
# format_day / num_days: window relative to each set's own release date
# ---------------------------------------------------------------------------


def test_cdfs_format_day_resolves_relative_window(fake_ratings_file, monkeypatch):
    # release_date + 7 days (format_day=8) == FAKE_START; + 1 more day (num_days=2)
    # == FAKE_END. The pre-seeded ratings file only exists at that exact window,
    # so this only passes if resolution computed the right dates.
    release_date = FAKE_START - datetime.timedelta(days=7)
    monkeypatch.setattr(
        "spells.draft_data.expansion_start_date", lambda set_code: release_date
    )

    def _no_network(*args, **kwargs):
        raise AssertionError("must not hit the network — resolved window is wrong")

    monkeypatch.setattr("spells.card_data_files.wget.download", _no_network)

    result = summon(
        FAKE_SET,
        ["num_gih"],
        group_by=["name"],
        cdfs=CardDataFileSpec(format_day=8, num_days=2),
    )
    assert len(result) == len(FAKE_CARD_RATINGS)


def test_cdfs_format_day_without_num_days_defaults_to_yesterday(monkeypatch):
    # num_days is optional: omitting it means "from format_day through yesterday",
    # matching the same default base_ratings_df already uses for a bare start_date.
    release_date = datetime.date(2026, 1, 1)
    monkeypatch.setattr(
        "spells.draft_data.expansion_start_date", lambda set_code: release_date
    )

    start, end = _resolve_cdfs_window(CardDataFileSpec(format_day=3), "TST")
    assert start == release_date + datetime.timedelta(days=2)
    assert end == datetime.date.today() - datetime.timedelta(days=1)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_cdfs_requires_start_date_or_format_day():
    with pytest.raises(AssertionError):
        CardDataFileSpec()


def test_cdfs_rejects_both_start_date_and_format_day():
    with pytest.raises(AssertionError):
        CardDataFileSpec(start_date=FAKE_START, format_day=1, num_days=1)


def test_cdfs_format_day_without_num_days_is_valid():
    CardDataFileSpec(format_day=1)  # should not raise


def test_cdfs_format_day_rejects_non_positive_num_days():
    with pytest.raises(AssertionError):
        CardDataFileSpec(format_day=1, num_days=0)


def test_cdfs_num_days_requires_format_day():
    with pytest.raises(AssertionError):
        CardDataFileSpec(start_date=FAKE_START, num_days=3)
