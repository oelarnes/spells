"""
Tests for card_ratings_view() — the live-fetch (17lands /api/card_data)
counterpart to summon(). This is the DEq workflow.

All tests use fake set codes ("TST", "TS2") with no local parquet files.
Ratings JSON is pre-seeded as snapshots at past as-of dates, so a cache miss
raises instead of reaching the network (past snapshots cannot be refetched —
17lands resolves time periods against its own current date). Tests that
exercise the fetch path itself monkeypatch wget.download.

card_ratings_view() takes no filter_spec/card_context/set_context: the ratings
API already returns one aggregated row per card, so there's nothing to
pre-aggregation filter, and CARD_ATTR columns (rarity, color, ...) come directly
from the API response — every real caller of this path already does any further
cross-referencing with a plain DataFrame .join() after the call.
"""

import datetime
import json
from pathlib import Path

import polars as pl
import pytest

from spells.card_data_files import base_ratings_df
from spells.draft_data import DateSpec, card_ratings_view
from spells.columns import ColSpec
from spells.enums import ColType, EventType, TimePeriod

from tests.conftest import (
    FAKE_SET,
    FAKE_AS_OF,
    FAKE_CARD_RATINGS,
    FAKE_SET_2,
    FAKE_AS_OF_2,
    FAKE_CARD_RATINGS_2,
)


def make_date_spec(**kwargs) -> DateSpec:
    return DateSpec(as_of=FAKE_AS_OF, **kwargs)


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


def test_bare_date_spec_broadcasts_same_snapshot_to_every_set(fake_ratings_file):
    # A bare spec broadcasts the *same* (time_period, as_of) to every set — by
    # design, for comparing snapshots taken on the same day. FAKE_SET_2's ratings
    # are only seeded at FAKE_AS_OF_2, and a past as_of cannot be refetched, so
    # broadcasting FAKE_AS_OF raises instead of silently reaching the network.
    with pytest.raises(ValueError, match="cannot be fetched"):
        card_ratings_view(
            [FAKE_SET, FAKE_SET_2],
            columns=["num_gih"],
            group_by=["expansion", "name"],
            date_spec=make_date_spec(),
        )


def test_set_code_keyed_dict_uses_per_set_snapshot(fake_ratings_file):
    date_spec = {
        FAKE_SET: DateSpec(as_of=FAKE_AS_OF),
        FAKE_SET_2: DateSpec(as_of=FAKE_AS_OF_2),
    }
    result = card_ratings_view(
        [FAKE_SET, FAKE_SET_2],
        columns=["num_gih"],
        group_by=["expansion", "name"],
        date_spec=date_spec,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


def test_tuple_keyed_dict_uses_per_cell_snapshot(fake_ratings_file):
    date_spec = {
        (FAKE_SET, EventType.PREMIER): DateSpec(as_of=FAKE_AS_OF),
        (FAKE_SET_2, EventType.PREMIER): DateSpec(as_of=FAKE_AS_OF_2),
    }
    result = card_ratings_view(
        [FAKE_SET, FAKE_SET_2],
        columns=["num_gih"],
        group_by=["expansion", "name"],
        date_spec=date_spec,
    )
    assert set(result["expansion"].to_list()) == {FAKE_SET, FAKE_SET_2}
    assert len(result) == len(FAKE_CARD_RATINGS) + len(FAKE_CARD_RATINGS_2)


def test_omitted_date_spec_defaults_to_all_time_as_of_today(fake_ratings_file, monkeypatch):
    # No date_spec means DateSpec(): ALL_TIME as of today. Today's snapshot isn't
    # seeded, so the fetch path runs — the fake download asserts the new /api/card_data
    # query format and writes the payload where the cache expects it.
    def _fake_download(url, out):
        assert "https://www.17lands.com/api/card_data?" in url
        assert f"expansion={FAKE_SET}" in url
        assert "event_type=PremierDraft" in url
        assert "time_period=ALL_TIME" in url
        assert "start_date" not in url
        Path(out).write_text(json.dumps(FAKE_CARD_RATINGS))

    monkeypatch.setattr("spells.card_data_files.wget.download", _fake_download)

    result = card_ratings_view(FAKE_SET, columns=["num_gih"], group_by=["name"])
    assert len(result) == len(FAKE_CARD_RATINGS)


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


def test_cohort_with_colors_empty_response_names_the_gap(fake_ratings_file):
    # 17lands' precompute covers user_group and colors separately but not
    # together; the API answers such queries with an empty list. Seed an empty
    # snapshot to exercise the descriptive error without any network.
    ratings_dir = fake_ratings_file / "ratings" / FAKE_SET
    empty = (
        f"{EventType.PREMIER}_top_WU_{TimePeriod.ALL_TIME}"
        f"_{FAKE_AS_OF.strftime('%Y-%m-%d')}.json"
    )
    (ratings_dir / empty).write_text("[]")

    with pytest.raises(ValueError, match="does not precompute user_group and colors"):
        base_ratings_df(
            FAKE_SET,
            player_cohort="top",
            deck_colors="WU",
            time_period=TimePeriod.ALL_TIME,
            as_of=FAKE_AS_OF,
        )


# ---------------------------------------------------------------------------
# DateSpec / as_of validation
# ---------------------------------------------------------------------------


def test_time_period_accepts_plain_string():
    assert DateSpec(time_period="LAST_WEEK").time_period == TimePeriod.LAST_WEEK


def test_rejects_unknown_time_period():
    with pytest.raises(ValueError):
        DateSpec(time_period="LAST_FORTNIGHT")


def test_defaults_to_all_time_as_of_today():
    spec = DateSpec()
    assert spec.time_period == TimePeriod.ALL_TIME
    assert spec.as_of == datetime.date.today()


def test_future_as_of_rejected(fake_ratings_file):
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    with pytest.raises(ValueError, match="in the future"):
        card_ratings_view(
            FAKE_SET,
            columns=["num_gih"],
            group_by=["name"],
            date_spec=DateSpec(as_of=tomorrow),
        )


def test_past_as_of_cache_miss_cannot_refetch(fake_ratings_file):
    with pytest.raises(ValueError, match="cannot be fetched"):
        card_ratings_view(
            FAKE_SET,
            columns=["num_gih"],
            group_by=["name"],
            date_spec=DateSpec(time_period=TimePeriod.LAST_WEEK, as_of=FAKE_AS_OF),
        )
