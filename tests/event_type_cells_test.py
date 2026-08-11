"""
Tests for choosing which (set, event type) pairs a query covers.

The product of every set and every event type is only right when every set
holds every one of them. A collection where it does not — some sets Premier
only, one also Traditional, one Pick Two — makes the product ask for files that
were never downloaded, which fails the read rather than returning nothing for
those pairs.
"""

import pytest

from spells.draft_data import _event_type_cells
from spells.enums import EventType

PREMIER = EventType.PREMIER
TRAD = EventType.TRADITIONAL
PICK_TWO = EventType.PICK_TWO


def test_one_event_type_applies_to_every_set():
    assert _event_type_cells(PREMIER, ["AAA", "BBB"]) == [
        ("AAA", PREMIER),
        ("BBB", PREMIER),
    ]


def test_a_list_applies_to_every_set():
    assert _event_type_cells([PREMIER, TRAD], ["AAA", "BBB"]) == [
        ("AAA", PREMIER),
        ("AAA", TRAD),
        ("BBB", PREMIER),
        ("BBB", TRAD),
    ]


def test_a_dict_gives_each_set_its_own():
    cells = _event_type_cells(
        {"AAA": PREMIER, "BBB": [PREMIER, TRAD], "CCC": PICK_TWO},
        ["AAA", "BBB", "CCC"],
    )

    assert cells == [
        ("AAA", PREMIER),
        ("BBB", PREMIER),
        ("BBB", TRAD),
        ("CCC", PICK_TWO),
    ]


def test_a_dict_asks_for_nothing_a_set_does_not_have():
    """The point of the mapping: no (BBB, PickTwoDraft) pair to fail on."""
    cells = _event_type_cells(
        {"AAA": [PREMIER, PICK_TWO], "BBB": PREMIER}, ["AAA", "BBB"]
    )

    assert ("BBB", PICK_TWO) not in cells
    assert ("AAA", PICK_TWO) in cells


def test_strings_are_accepted_like_every_other_enum_argument():
    assert _event_type_cells({"AAA": "PremierDraft"}, ["AAA"]) == [("AAA", PREMIER)]
    assert _event_type_cells("TradDraft", ["AAA"]) == [("AAA", TRAD)]


def test_cell_order_follows_the_sets_asked_for():
    """Cell order drives the order of the concatenated frames."""
    cells = _event_type_cells({"BBB": PREMIER, "AAA": PREMIER}, ["BBB", "AAA"])
    assert [code for code, _ in cells] == ["BBB", "AAA"]


def test_repeats_within_one_set_collapse():
    assert _event_type_cells({"AAA": [PREMIER, PREMIER]}, ["AAA"]) == [("AAA", PREMIER)]


def test_a_dict_must_name_every_set():
    """Defaulting the rest would turn a mistyped set code into a silently
    different query rather than an error."""
    with pytest.raises(ValueError, match="BBB"):
        _event_type_cells({"AAA": PREMIER}, ["AAA", "BBB"])


def test_extra_keys_are_ignored_so_one_mapping_serves_many_queries():
    assert _event_type_cells({"AAA": PREMIER, "ZZZ": TRAD}, ["AAA"]) == [
        ("AAA", PREMIER)
    ]


def test_an_empty_choice_is_an_error_not_an_empty_frame():
    with pytest.raises(ValueError, match="AAA"):
        _event_type_cells({"AAA": []}, ["AAA"])

    with pytest.raises(ValueError, match="AAA"):
        _event_type_cells([], ["AAA"])


def test_an_unknown_event_type_still_raises():
    with pytest.raises(ValueError):
        _event_type_cells({"AAA": "NotADraft"}, ["AAA"])
