"""
Tests for counting both picks of a pick-two row.

A pick-two row records two picks, so aggregating it as one understated every
per-pick total by half. The draft view becomes one row per pick, and
`pick_ordinal` says which of the two a row is, for the columns where that
matters.
"""

import polars as pl
import pytest

from spells import summon
from spells.draft_data import PICK_ORDINAL_FIRST, _pick_events
from spells.enums import ColName, EventType, View

PREMIER = EventType.PREMIER
PICK_TWO = EventType.PICK_TWO


@pytest.fixture
def rows() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            ColName.PICK: ["Alpha", "Gamma"],
            ColName.PICK_2: ["Beta", "Delta"],
            ColName.PICK_MAINDECK_RATE: [1.0, 0.5],
            ColName.PICK_SIDEBOARD_IN_RATE: [0.25, 0.75],
            ColName.PACK_NUMBER: [0, 0],
            ColName.PICK_NUMBER: [0, 1],
        }
    )


# ---------------------------------------------------------------------------
# The projection
# ---------------------------------------------------------------------------


def test_a_pick_two_row_becomes_two_pick_events(rows):
    out = _pick_events(rows, View.DRAFT, PICK_TWO).collect()

    assert len(out) == 4
    assert out[ColName.PICK].to_list() == ["Alpha", "Gamma", "Beta", "Delta"]


def test_the_second_pick_stands_in_as_the_pick(rows):
    """Renaming on the raw frame is what lets every expression written against
    `pick` count both without knowing there were two."""
    out = _pick_events(rows, View.DRAFT, PICK_TWO).collect()

    assert ColName.PICK_2 not in out.columns
    assert out.filter(pl.col(ColName.PICK_ORDINAL) == 2)[ColName.PICK].to_list() == [
        "Beta",
        "Delta",
    ]


def test_a_single_pick_row_stays_one_event(rows):
    out = _pick_events(rows, View.DRAFT, PREMIER).collect()

    assert len(out) == 2
    assert out[ColName.PICK_ORDINAL].to_list() == [1, 1]


def test_the_game_view_is_never_split():
    """It carries the ordinal too, which costs nothing because a view only ever
    selects the columns asked of it — but a game is one row however it drafted."""
    game = pl.LazyFrame({"draft_id": ["a"], "won": [1]})
    out = _pick_events(game, View.GAME, PICK_TWO).collect()

    assert out[["draft_id", "won"]].to_dicts() == [{"draft_id": "a", "won": 1}]


def test_first_pick_only_columns_are_absent_from_the_second_event(rows):
    """17Lands derives both rates from the first pick and publishes no
    counterpart, so carrying them over would describe the wrong card."""
    out = _pick_events(rows, View.DRAFT, PICK_TWO).collect()
    second = out.filter(pl.col(ColName.PICK_ORDINAL) == 2)

    assert second[ColName.PICK_MAINDECK_RATE].to_list() == [None, None]
    assert second[ColName.PICK_SIDEBOARD_IN_RATE].to_list() == [None, None]


def test_first_pick_only_columns_survive_on_the_first_event(rows):
    out = _pick_events(rows, View.DRAFT, PICK_TWO).collect()
    first = out.filter(pl.col(ColName.PICK_ORDINAL) == 1)

    assert first[ColName.PICK_MAINDECK_RATE].to_list() == [1.0, 0.5]


def test_a_row_level_view_keeps_one_row_and_both_picks(rows):
    """`lazy_select` reconstructs drafts, so it must not split anything — but
    the column still has to resolve, since one set of definitions serves both."""
    out = rows.with_columns(PICK_ORDINAL_FIRST).collect()

    assert len(out) == 2
    assert ColName.PICK_2 in out.columns
    assert out[ColName.PICK_ORDINAL].to_list() == [1, 1]


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


def test_pick_two_counts_both_picks(fake_pick_two):
    """Two picks per row, so twice the row count."""
    df = summon(
        "TST",
        columns=[ColName.NUM_TAKEN],
        event_type=PICK_TWO,
        read_cache=False,
        write_cache=False,
    )
    assert df[ColName.NUM_TAKEN].sum() == 2 * fake_pick_two


def test_num_drafts_counts_drafts_not_cards(fake_pick_two):
    """The second pick is the same draft, so this one must not double."""
    df = summon(
        "TST",
        columns=[ColName.NUM_DRAFTS],
        group_by=[ColName.EVENT_TYPE],
        event_type=PICK_TWO,
        read_cache=False,
        write_cache=False,
    )
    assert df[ColName.NUM_DRAFTS].sum() == 1


def test_grouping_by_ordinal_separates_the_two_picks(fake_pick_two):
    df = summon(
        "TST",
        columns=[ColName.NUM_TAKEN],
        group_by=[ColName.PICK_ORDINAL],
        event_type=PICK_TWO,
        read_cache=False,
        write_cache=False,
    ).sort(ColName.PICK_ORDINAL)

    assert df[ColName.PICK_ORDINAL].to_list() == [1, 2]
    assert df[ColName.NUM_TAKEN].to_list() == [fake_pick_two, fake_pick_two]


def test_single_pick_formats_are_unchanged(fake_pick_two):
    """Everything above must leave Premier exactly as it was."""
    df = summon(
        "TST",
        columns=[ColName.NUM_TAKEN],
        event_type=PREMIER,
        read_cache=False,
        write_cache=False,
    )
    assert df[ColName.NUM_TAKEN].sum() > 0
