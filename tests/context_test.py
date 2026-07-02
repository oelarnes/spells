"""Tests for card/set context resolution.

The canonical index for card_context and set_context is (set_code, event_type),
matching the data files. `_normalize_context` takes a user-provided context — given
at that full index or in a broadcast form — and resolves it up front into a dict
keyed by every (set_code, event_type) cell the caller is about to process.
"""

import polars as pl

from spells import summon
from spells.draft_data import _normalize_context
from spells.enums import EventType


def test_none_returns_none_for_every_cell():
    assert _normalize_context(None, [("TST", EventType.PREMIER)]) == {
        ("TST", EventType.PREMIER): None
    }


def test_bare_set_context_broadcasts_to_every_cell():
    # Field-keyed set_context applies to every (set, event_type).
    ctx = {"picks_per_pack": 14}
    cells = [("TST", EventType.PREMIER), ("TS2", EventType.TRADITIONAL)]
    assert _normalize_context(ctx, cells) == {cell: ctx for cell in cells}


def test_bare_card_context_broadcasts_to_every_cell():
    # {name: {...}} card_context is bare (names are not set codes) and broadcasts.
    ctx = {"Aether Sprite": {"mana_value": 2}}
    cell = ("TST", EventType.PREMIER)
    assert _normalize_context(ctx, [cell]) == {cell: ctx}


def test_set_code_keyed_broadcasts_across_event_types():
    ctx = {"TST": {"picks_per_pack": 14}, "TS2": {"picks_per_pack": 8}}
    cells = [
        ("TST", EventType.PREMIER),
        ("TST", EventType.TRADITIONAL),
        ("TS2", EventType.PREMIER),
    ]
    resolved = _normalize_context(ctx, cells)
    assert resolved[("TST", EventType.PREMIER)] == {"picks_per_pack": 14}
    # Same set, different event type → same set-level value (broadcast).
    assert resolved[("TST", EventType.TRADITIONAL)] == {"picks_per_pack": 14}
    assert resolved[("TS2", EventType.PREMIER)] == {"picks_per_pack": 8}


def test_tuple_keyed_is_canonical_index():
    ctx = {
        ("TST", EventType.PREMIER): {"picks_per_pack": 14},
        ("TST", EventType.TRADITIONAL): {"picks_per_pack": 8},
    }
    cells = [("TST", EventType.PREMIER), ("TST", EventType.TRADITIONAL)]
    resolved = _normalize_context(ctx, cells)
    assert resolved[("TST", EventType.PREMIER)] == {"picks_per_pack": 14}
    assert resolved[("TST", EventType.TRADITIONAL)] == {"picks_per_pack": 8}


def test_tuple_keyed_accepts_plain_string_event_type():
    # Keys stored with a plain 17Lands string resolve too (StrEnum hashing/equality),
    # and lookup works whether the queried cell carries an enum or a string.
    ctx = {("TST", "PremierDraft"): {"picks_per_pack": 14}}
    assert _normalize_context(ctx, [("TST", EventType.PREMIER)]) == {
        ("TST", EventType.PREMIER): {"picks_per_pack": 14}
    }
    assert _normalize_context(ctx, [("TST", "PremierDraft")]) == {
        ("TST", "PremierDraft"): {"picks_per_pack": 14}
    }


def test_tuple_keyed_missing_cell_returns_none():
    ctx = {("TST", EventType.PREMIER): {"picks_per_pack": 14}}
    assert _normalize_context(ctx, [("TST", EventType.TRADITIONAL)]) == {
        ("TST", EventType.TRADITIONAL): None
    }


def test_dataframe_filters_by_expansion():
    df = pl.DataFrame(
        {"expansion": ["TST", "TS2"], "name": ["Aether Sprite", "Other"], "x": [1, 2]}
    )
    cells = [("TST", EventType.PREMIER), ("TS2", EventType.PREMIER)]
    resolved = _normalize_context(df, cells)
    out = resolved[("TST", EventType.PREMIER)]
    assert out["expansion"].to_list() == ["TST"]
    assert out["x"].to_list() == [1]


def test_dataframe_filters_by_event_type_when_present():
    df = pl.DataFrame(
        {
            "expansion": ["TST", "TST"],
            "event_type": ["PremierDraft", "TradDraft"],
            "x": [1, 2],
        }
    )
    cells = [("TST", EventType.PREMIER), ("TST", EventType.TRADITIONAL)]
    resolved = _normalize_context(df, cells)
    assert resolved[("TST", EventType.PREMIER)]["x"].to_list() == [1]
    assert resolved[("TST", EventType.TRADITIONAL)]["x"].to_list() == [2]


def test_summon_accepts_tuple_keyed_set_context(fake_trad_set):
    # End-to-end: a canonical (set, event_type)-keyed set_context is accepted and
    # the multi-event query routes each cell correctly (premier=2 picks, trad=3).
    set_ctx = {
        ("TRD", EventType.PREMIER): {"picks_per_pack": 14},
        ("TRD", EventType.TRADITIONAL): {"picks_per_pack": 14},
    }
    df = summon(
        "TRD",
        ["num_taken"],
        group_by=["event_type"],
        event_type=[EventType.PREMIER, EventType.TRADITIONAL],
        set_context=set_ctx,
    )
    counts = dict(zip(df["event_type"].to_list(), df["num_taken"].to_list()))
    assert counts["PremierDraft"] == 2
    assert counts["TradDraft"] == 3
