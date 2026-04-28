"""
Tests for the cdfs (CardDataFileSpec) path through summon() — the DEq workflow.

All tests use a fake set code "TST" with no local parquet files. The ratings
JSON is pre-seeded in a temp directory so no network calls are made. This also
serves as a regression suite for the new-set KeyError that occurred when a set
code was absent from the (now-removed) START_DATE_MAP.
"""

import datetime

import polars as pl
import pytest

from spells.draft_data import CardDataFileSpec, summon
from spells.columns import ColSpec
from spells.enums import ColType

from tests.conftest import FAKE_SET, FAKE_START, FAKE_END, FAKE_CARDS


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
    assert len(result) == len(FAKE_CARDS)
    assert result["name"].is_unique().all()


def test_cdfs_summon_card_attr_columns(fake_ratings_file):
    result = summon(FAKE_SET, ["rarity", "color"], group_by=["name"], cdfs=make_cdfs())
    assert "rarity" in result.columns
    assert "color" in result.columns
    assert set(result["rarity"].to_list()) == {"common", "uncommon", "rare"}


def test_cdfs_summon_num_gih_values_match_source(fake_ratings_file):
    result = summon(FAKE_SET, ["num_gih"], group_by=["name"], cdfs=make_cdfs())
    result_map = dict(zip(result["name"].to_list(), result["num_gih"].to_list()))

    for card in FAKE_CARDS:
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
# Note: filtering by CARD_ATTR columns (rarity, color, etc.) is not supported
# in the cdfs path. The manifest requires filter columns to be present in the
# resolved base views; CARD_ATTR columns only appear in View.CARD, which is
# never populated when cdfs is used. Filter after summon() instead.


def test_cdfs_summon_filter_card_attr_raises(fake_ratings_file):
    # Demonstrates the current limitation: CARD_ATTR filter cols are rejected
    # by the manifest when a non-CARD view is also present.
    with pytest.raises(AssertionError, match="filter col rarity not found in base view"):
        summon(
            FAKE_SET,
            ["num_gih"],
            group_by=["name"],
            filter_spec={"rarity": "common"},
            cdfs=make_cdfs(),
        )


def test_cdfs_summon_filter_post_summon(fake_ratings_file):
    # The practical workaround: fetch all cards, then filter the DataFrame.
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
