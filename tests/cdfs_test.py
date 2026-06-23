"""
Tests for the cdfs (CardDataFileSpec) path through summon() — the DEq workflow.

All tests use a fake set code "TST" with no local parquet files. The ratings
JSON is pre-seeded in a temp directory so no network calls are made. This also
serves as a regression suite for the new-set KeyError that occurred when a set
code was absent from the (now-removed) START_DATE_MAP.
"""

import polars as pl
import pytest

from spells.draft_data import CardDataFileSpec, summon
from spells.columns import ColSpec
from spells.enums import ColType, EventType

from tests.conftest import FAKE_SET, FAKE_START, FAKE_END, FAKE_CARD_RATINGS


def make_cdfs(**kwargs) -> CardDataFileSpec:
    return CardDataFileSpec(
        set_code=FAKE_SET,
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
# event_type / format
# ---------------------------------------------------------------------------


def test_cdfs_format_coerced_to_event_type():
    # A plain 17Lands string (as deq passes) is coerced to the enum.
    assert make_cdfs(format="TradDraft").format is EventType.TRADITIONAL
    assert make_cdfs().format is EventType.PREMIER


def test_cdfs_event_type_column_reflects_format(fake_ratings_file):
    premier = summon(FAKE_SET, ["num_gih"], group_by=["name", "event_type"], cdfs=make_cdfs())
    trad = summon(
        FAKE_SET,
        ["num_gih"],
        group_by=["name", "event_type"],
        cdfs=make_cdfs(format=EventType.TRADITIONAL),
    )
    assert set(premier["event_type"].to_list()) == {"PremierDraft"}
    assert set(trad["event_type"].to_list()) == {"TradDraft"}


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_cdfs_summon_wrong_set_code_raises(fake_ratings_file):
    wrong_cdfs = CardDataFileSpec(
        set_code="OTH",
        start_date=FAKE_START,
        end_date=FAKE_END,
    )
    with pytest.raises(AssertionError):
        summon(FAKE_SET, ["num_gih"], group_by=["name"], cdfs=wrong_cdfs)
