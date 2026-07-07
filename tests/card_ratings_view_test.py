"""
Tests for card_ratings_view() — the live-fetch (17lands /api/card_data)
counterpart to summon(). This is the DEq workflow.

All tests use fake set codes ("TST", "TS2") with no local parquet files.
Ratings JSON is pre-seeded as snapshots at a past as-of date, so a cache miss
raises instead of reaching the network (past snapshots cannot be refetched —
17lands resolves time periods against its own current date). Tests that
exercise the fetch path itself monkeypatch wget.download.

card_ratings_view() takes no filter_spec/card_context/set_context: the ratings
API already returns one aggregated row per card, so there's nothing to
pre-aggregation filter, and CARD_ATTR columns (rarity, color, ...) come directly
from the API response — every real caller of this path already does any further
cross-referencing with a plain DataFrame .join() after the call.

time_period and as_of are plain top-level params (like player_cohort and
deck_colors), applied uniformly to every set/event_type in a single call —
there's no per-cell dict-keyed broadcasting for these, unlike card_context/
set_context in summon().
"""

import datetime
import json
from pathlib import Path

import polars as pl
import pytest

from spells.card_data_files import base_ratings_df
from spells.draft_data import card_ratings_view
from spells.columns import ColSpec
from spells.enums import ColType, EventType, TimePeriod

from tests.conftest import (
    FAKE_SET,
    FAKE_AS_OF,
    FAKE_CARD_RATINGS,
    FAKE_SET_2,
)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_returns_dataframe(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih", "gih_wr"], group_by=["name"], as_of=FAKE_AS_OF
    )
    assert isinstance(result, pl.DataFrame)
    assert set(result.columns) == {"name", "num_gih", "gih_wr"}


def test_one_row_per_card(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih"], group_by=["name"], as_of=FAKE_AS_OF
    )
    assert len(result) == len(FAKE_CARD_RATINGS)
    assert result["name"].is_unique().all()


def test_card_attr_columns(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["rarity", "color"], group_by=["name"], as_of=FAKE_AS_OF
    )
    assert "rarity" in result.columns
    assert "color" in result.columns
    assert set(result["rarity"].to_list()) == {"common", "uncommon", "rare"}


def test_num_gih_values_match_source(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih"], group_by=["name"], as_of=FAKE_AS_OF
    )
    result_map = dict(zip(result["name"].to_list(), result["num_gih"].to_list()))

    for card in FAKE_CARD_RATINGS:
        expected = card["opening_hand_game_count"] + card["drawn_game_count"]
        assert result_map[card["name"]] == expected


def test_no_parquet_files_needed(fake_ratings_file):
    # The external/ directory (parquet files) should never be created by this path.
    external_dir = fake_ratings_file / "external"
    card_ratings_view(
        FAKE_SET, columns=["num_gih", "gih_wr"], group_by=["name"], as_of=FAKE_AS_OF
    )
    assert not external_dir.exists()


def test_post_hoc_filter_is_the_pattern(fake_ratings_file):
    # There's no filter_spec — filter the returned DataFrame instead.
    result = card_ratings_view(
        FAKE_SET, columns=["num_gih", "rarity"], group_by=["name"], as_of=FAKE_AS_OF
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
        as_of=FAKE_AS_OF,
    )
    assert "custom_rate" in result.columns
    # custom_rate should equal gih_wr
    gih_wr = card_ratings_view(
        FAKE_SET, columns=["gih_wr"], group_by=["name"], as_of=FAKE_AS_OF
    )
    merged = result.join(gih_wr, on="name")
    for row in merged.iter_rows(named=True):
        if row["gih_wr"] is not None and row["custom_rate"] is not None:
            assert abs(row["custom_rate"] - row["gih_wr"]) < 1e-9


# ---------------------------------------------------------------------------
# event_type / multi-cell
# ---------------------------------------------------------------------------


def test_event_type_column_reflects_requested_event_type(fake_ratings_file):
    # fake_ratings_file seeds a ratings JSON for both formats under FAKE_SET.
    premier = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name", "event_type"],
        as_of=FAKE_AS_OF,
    )
    trad = card_ratings_view(
        FAKE_SET,
        event_type=EventType.TRADITIONAL,
        columns=["num_gih"],
        group_by=["name", "event_type"],
        as_of=FAKE_AS_OF,
    )
    assert set(premier["event_type"].to_list()) == {"PremierDraft"}
    assert set(trad["event_type"].to_list()) == {"TradDraft"}


def test_as_of_broadcasts_across_event_types(fake_ratings_file):
    # A single as_of, requested against both event types in one call.
    result = card_ratings_view(
        FAKE_SET,
        event_type=[EventType.PREMIER, EventType.TRADITIONAL],
        columns=["num_gih"],
        group_by=["name", "event_type"],
        as_of=FAKE_AS_OF,
    )
    assert set(result["event_type"].to_list()) == {"PremierDraft", "TradDraft"}
    assert len(result) == 2 * len(FAKE_CARD_RATINGS)


def test_as_of_broadcasts_across_sets(fake_ratings_file):
    # as_of applies uniformly to every set in the call — this only reaches both
    # sets' pre-seeded snapshots because they happen to share FAKE_AS_OF... which
    # they don't (FAKE_SET_2 is seeded at FAKE_AS_OF_2), so a shared as_of across
    # sets with different snapshot dates raises instead of silently hitting the
    # network for the unseeded one.
    with pytest.raises(ValueError, match="cannot be fetched"):
        card_ratings_view(
            [FAKE_SET, FAKE_SET_2],
            columns=["num_gih"],
            group_by=["expansion", "name"],
            as_of=FAKE_AS_OF,
        )


def test_omitted_as_of_defaults_to_today(fake_ratings_file, monkeypatch):
    # No as_of means today's ALL_TIME snapshot. Today's snapshot isn't seeded,
    # so the fetch path runs — the fake download asserts the new /api/card_data
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
# player_cohort / deck_colors / time_period: plain top-level params
# ---------------------------------------------------------------------------


def test_player_cohort_is_top_level_param(fake_ratings_file):
    # "all" (the default) resolves to the ratings file seeded without a cohort
    # suffix.
    result = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name"],
        as_of=FAKE_AS_OF,
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
# as_of="LAST_CACHED": most recently cached snapshot, whatever its age
# ---------------------------------------------------------------------------


def test_last_cached_picks_the_most_recent_snapshot(fake_ratings_file):
    # Seed a second, newer snapshot alongside the fixture's FAKE_AS_OF one, with
    # different data so the two are distinguishable.
    ratings_dir = fake_ratings_file / "ratings" / FAKE_SET
    newer_as_of = FAKE_AS_OF + datetime.timedelta(days=5)
    newer_ratings = [{**card, "ever_drawn_game_count": 999} for card in FAKE_CARD_RATINGS]
    newer_filename = (
        f"{EventType.PREMIER}_all_any_{TimePeriod.ALL_TIME}"
        f"_{newer_as_of.strftime('%Y-%m-%d')}.json"
    )
    (ratings_dir / newer_filename).write_text(json.dumps(newer_ratings))

    result = card_ratings_view(
        FAKE_SET, columns=["num_gih"], group_by=["name"], as_of="LAST_CACHED"
    )
    assert set(result["num_gih"].to_list()) == {999}


def test_last_cached_falls_back_to_live_query_when_nothing_cached(monkeypatch, tmp_path):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))

    def _fake_download(url, out):
        assert "time_period=ALL_TIME" in url
        Path(out).write_text(json.dumps(FAKE_CARD_RATINGS))

    monkeypatch.setattr("spells.card_data_files.wget.download", _fake_download)

    result = card_ratings_view(FAKE_SET, columns=["num_gih"], group_by=["name"], as_of="LAST_CACHED")
    assert len(result) == len(FAKE_CARD_RATINGS)


def test_last_cached_ignores_other_deck_colors_stub(fake_ratings_file):
    # A cached snapshot for a different deck_color shouldn't be picked up when
    # resolving "LAST_CACHED" for deck_colors="any" — the stub match must be exact.
    ratings_dir = fake_ratings_file / "ratings" / FAKE_SET
    newer_as_of = FAKE_AS_OF + datetime.timedelta(days=5)
    other_color_filename = (
        f"{EventType.PREMIER}_all_WU_{TimePeriod.ALL_TIME}"
        f"_{newer_as_of.strftime('%Y-%m-%d')}.json"
    )
    (ratings_dir / other_color_filename).write_text(json.dumps(FAKE_CARD_RATINGS))

    result = card_ratings_view(FAKE_SET, columns=["num_gih"], group_by=["name"], as_of="LAST_CACHED")
    result_map = dict(zip(result["name"].to_list(), result["num_gih"].to_list()))
    for card in FAKE_CARD_RATINGS:
        expected = card["opening_hand_game_count"] + card["drawn_game_count"]
        assert result_map[card["name"]] == expected


# ---------------------------------------------------------------------------
# time_period / as_of validation
# ---------------------------------------------------------------------------


def test_time_period_accepts_plain_string(fake_ratings_file):
    result = card_ratings_view(
        FAKE_SET,
        columns=["num_gih"],
        group_by=["name"],
        time_period="ALL_TIME",
        as_of=FAKE_AS_OF,
    )
    assert len(result) == len(FAKE_CARD_RATINGS)


def test_rejects_unknown_time_period():
    with pytest.raises(ValueError):
        card_ratings_view(
            FAKE_SET, columns=["num_gih"], group_by=["name"], time_period="LAST_FORTNIGHT"
        )


def test_future_as_of_rejected(fake_ratings_file):
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    with pytest.raises(ValueError, match="in the future"):
        card_ratings_view(
            FAKE_SET, columns=["num_gih"], group_by=["name"], as_of=tomorrow
        )


def test_past_as_of_cache_miss_cannot_refetch(fake_ratings_file):
    with pytest.raises(ValueError, match="cannot be fetched"):
        card_ratings_view(
            FAKE_SET,
            columns=["num_gih"],
            group_by=["name"],
            time_period=TimePeriod.LAST_WEEK,
            as_of=FAKE_AS_OF,
        )
