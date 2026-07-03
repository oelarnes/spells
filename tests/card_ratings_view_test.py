"""
Tests for card_ratings_view() — the live-fetch (17lands daily API) counterpart
to summon(). This is the DEq workflow.

All tests use fake set codes ("TST", "TS2") with no local parquet files. Ratings
JSON is pre-seeded in a temp directory so no network calls are made. This also
serves as a regression suite for the new-set KeyError that occurred when a set
code was absent from the (now-removed) START_DATE_MAP.

card_ratings_view() takes no filter_spec/card_context/set_context: the ratings
API already returns one aggregated row per card, so there's nothing to
pre-aggregation filter, and CARD_ATTR columns (rarity, color, ...) come directly
from the API response — every real caller of this path already does any further
cross-referencing with a plain DataFrame .join() after the call.
"""

import datetime

import polars as pl
import pytest

from spells.draft_data import DateSpec, _resolve_date_window, card_ratings_view
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


def make_date_spec(**kwargs) -> DateSpec:
    return DateSpec(start_date=FAKE_START, end_date=FAKE_END, **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_returns_dataframe(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih", "gih_wr"], group_by=["name"], date_spec=make_date_spec()
    )
    assert isinstance(result, pl.DataFrame)
    assert set(result.columns) == {"name", "num_gih", "gih_wr"}


def test_one_row_per_card(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih"], group_by=["name"], date_spec=make_date_spec()
    )
    assert len(result) == len(FAKE_CARD_RATINGS)
    assert result["name"].is_unique().all()


def test_card_attr_columns(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["rarity", "color"], group_by=["name"], date_spec=make_date_spec()
    )
    assert "rarity" in result.columns
    assert "color" in result.columns
    assert set(result["rarity"].to_list()) == {"common", "uncommon", "rare"}


def test_num_gih_values_match_source(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih"], group_by=["name"], date_spec=make_date_spec()
    )
    result_map = dict(zip(result["name"].to_list(), result["num_gih"].to_list()))

    for card in FAKE_CARD_RATINGS:
        expected = card["opening_hand_game_count"] + card["drawn_game_count"]
        assert result_map[card["name"]] == expected


def test_no_parquet_files_needed(fake_ratings_file):
    # The external/ directory (parquet files) should never be created by this path.
    external_dir = fake_ratings_file / "external"
    card_ratings_view(
        FAKE_SET, columns=["num_gih", "gih_wr"], group_by=["name"], date_spec=make_date_spec()
    )
    assert not external_dir.exists()


def test_post_hoc_filter_is_the_pattern(fake_ratings_file):
    # There's no filter_spec — filter the returned DataFrame instead.
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih", "rarity"], group_by=["name"], date_spec=make_date_spec()
    )
    filtered = result.filter(pl.col("rarity") == "common")
    assert len(filtered) == 1
    assert filtered["name"][0] == "Aether Sprite"


# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------


def test_with_extension_column(fake_ratings_file):
    extension = {
        "custom_rate": ColSpec(
            col_type=ColType.AGG,
            expr=pl.col("num_gih_won") / pl.col("num_gih"),
        )
    }
    result = card_ratings_view(
        FAKE_SET,
        columns=["num_gih", "custom_rate"],
        group_by=["name"],
        extensions=extension,
        date_spec=make_date_spec(),
    )
    assert "custom_rate" in result.columns
    # custom_rate should equal gih_wr
    gih_wr = card_ratings_view(
        FAKE_SET, columns=["gih_wr"], group_by=["name"], date_spec=make_date_spec()
    )
    merged = result.join(gih_wr, on="name")
    for row in merged.iter_rows(named=True):
        if row["gih_wr"] is not None and row["custom_rate"] is not None:
            assert abs(row["custom_rate"] - row["gih_wr"]) < 1e-9


# ---------------------------------------------------------------------------
# event_type / multi-cell
# ---------------------------------------------------------------------------


def test_event_type_column_reflects_requested_event_type(fake_ratings_file):
    # fake_ratings_file seeds a ratings JSON for both formats under FAKE_SET, so
    # a bare date_spec (no set/event identity of its own) correctly follows
    # whichever event_type is requested.
    premier = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name", "event_type"],
        date_spec=make_date_spec(),
    )
    trad = card_ratings_view(
        FAKE_SET,
        event_type=EventType.TRADITIONAL,
        columns=["num_gih"],
        group_by=["name", "event_type"],
        date_spec=make_date_spec(),
    )
    assert set(premier["event_type"].to_list()) == {"PremierDraft"}
    assert set(trad["event_type"].to_list()) == {"TradDraft"}


def test_bare_date_spec_broadcasts_across_event_types(fake_ratings_file):
    # A single DateSpec, requested against both event types in one call.
    result = card_ratings_view(
        FAKE_SET,
        event_type=[EventType.PREMIER, EventType.TRADITIONAL],
        columns=["num_gih"],
        group_by=["name", "event_type"],
        date_spec=make_date_spec(),
    )
    assert set(result["event_type"].to_list()) == {"PremierDraft", "TradDraft"}
    assert len(result) == 2 * len(FAKE_CARD_RATINGS)


# ---------------------------------------------------------------------------
# Multi-set broadcast forms (mirrors context_test.py's card_context/set_context
# coverage, applied to date_spec)
# ---------------------------------------------------------------------------


def test_bare_date_spec_broadcasts_same_literal_window_to_every_set(fake_ratings_file, monkeypatch):
    # A bare spec broadcasts the *same* literal window to every set — by design,
    # for cross-set comparisons over one calendar range (day-of-week/seasonal
    # analysis). FAKE_SET_2's ratings are only seeded at FAKE_START_2/FAKE_END_2,
    # so broadcasting FAKE_START/FAKE_END finds no cached file for TS2 and must
    # not silently succeed by reaching the real network.
    def _no_network(*args, **kwargs):
        raise AssertionError("must not hit the network for an unseeded window")

    monkeypatch.setattr("spells.card_data_files.wget.download", _no_network)

    with pytest.raises(AssertionError, match="must not hit the network"):
        card_ratings_view(
            [FAKE_SET, FAKE_SET_2],
            columns=["num_gih"],
            group_by=["expansion", "name"],
            date_spec=make_date_spec(),  # literal FAKE_START/FAKE_END, wrong window for TS2
        )


def test_set_code_keyed_dict_uses_per_set_window(fake_ratings_file):
    date_spec = {
        FAKE_SET: DateSpec(start_date=FAKE_START, end_date=FAKE_END),
        FAKE_SET_2: DateSpec(start_date=FAKE_START_2, end_date=FAKE_END_2),
    }
    result = card_ratings_view(
        [FAKE_SET, FAKE_SET_2],
        columns=["num_gih"],
        group_by=["expansion", "name"],
        date_spec=date_spec,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


def test_tuple_keyed_dict_uses_per_cell_window(fake_ratings_file):
    date_spec = {
        (FAKE_SET, EventType.PREMIER): DateSpec(start_date=FAKE_START, end_date=FAKE_END),
        (FAKE_SET_2, EventType.PREMIER): DateSpec(
            start_date=FAKE_START_2, end_date=FAKE_END_2
        ),
    }
    result = card_ratings_view(
        [FAKE_SET, FAKE_SET_2],
        columns=["num_gih"],
        group_by=["expansion", "name"],
        date_spec=date_spec,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


def test_date_spec_required(fake_ratings_file):
    with pytest.raises(AssertionError, match="No date_spec resolved"):
        card_ratings_view(FAKE_SET, columns=["num_gih"], group_by=["name"])


# ---------------------------------------------------------------------------
# player_cohort / deck_colors: plain top-level params, not per-cell
# ---------------------------------------------------------------------------


def test_player_cohort_is_top_level_param(fake_ratings_file):
    # "all" (the default) resolves to the ratings file seeded without a cohort
    # suffix; passing player_cohort doesn't require touching date_spec at all.
    result = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name"],
        date_spec=make_date_spec(),
        player_cohort="all",
    )
    assert len(result) == len(FAKE_CARD_RATINGS)


# ---------------------------------------------------------------------------
# format_day / num_days: window relative to each set's own release date
# ---------------------------------------------------------------------------


def test_format_day_resolves_relative_window(fake_ratings_file, monkeypatch):
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

    result = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name"],
        date_spec=DateSpec(format_day=8, num_days=2),
    )
    assert len(result) == len(FAKE_CARD_RATINGS)


def test_format_day_without_num_days_defaults_to_yesterday(monkeypatch):
    # num_days is optional: omitting it means "from format_day through yesterday",
    # matching the same default base_ratings_df already uses for a bare start_date.
    release_date = datetime.date(2026, 1, 1)
    monkeypatch.setattr(
        "spells.draft_data.expansion_start_date", lambda set_code: release_date
    )

    start, end = _resolve_date_window(DateSpec(format_day=3), "TST")
    assert start == release_date + datetime.timedelta(days=2)
    assert end == datetime.date.today() - datetime.timedelta(days=1)


# ---------------------------------------------------------------------------
# DateSpec validation
# ---------------------------------------------------------------------------


def test_requires_start_date_or_format_day():
    with pytest.raises(AssertionError):
        DateSpec()


def test_rejects_both_start_date_and_format_day():
    with pytest.raises(AssertionError):
        DateSpec(start_date=FAKE_START, format_day=1, num_days=1)


def test_format_day_without_num_days_is_valid():
    DateSpec(format_day=1)  # should not raise


def test_format_day_rejects_non_positive_num_days():
    with pytest.raises(AssertionError):
        DateSpec(format_day=1, num_days=0)


def test_num_days_requires_format_day():
    with pytest.raises(AssertionError):
        DateSpec(start_date=FAKE_START, num_days=3)
