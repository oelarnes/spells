"""
Tests for the `spells add` orchestration in external.py. These pin the call
pattern (which files get downloaded/written) without touching the network.
"""

import pytest

from spells import external
from spells.enums import View, EventType


@pytest.fixture()
def record_io(monkeypatch: pytest.MonkeyPatch):
    """Replace the IO steps of `_add` with recorders, returning the call log."""
    calls: dict[str, list] = {"download": [], "card": [], "context": []}

    def fake_download(set_code, dataset_type, event_type=EventType.PREMIER, **kw):
        calls["download"].append((set_code, dataset_type, event_type))
        return 0

    def fake_card(set_code, event_type=EventType.PREMIER, **kw):
        calls["card"].append((set_code, event_type))
        return 0

    def fake_context(set_code, event_type=EventType.PREMIER, **kw):
        calls["context"].append((set_code, event_type))
        return 0

    monkeypatch.setattr(external, "download_data_set", fake_download)
    monkeypatch.setattr(external.cards, "write_card_file", fake_card)
    monkeypatch.setattr(external, "get_set_context", fake_context)
    return calls


def test_add_premier_downloads_full_set(record_io):
    external._add("TST")

    assert record_io["download"] == [
        ("TST", View.DRAFT, EventType.PREMIER),
        ("TST", View.GAME, EventType.PREMIER),
    ]
    assert record_io["card"] == [("TST", EventType.PREMIER)]
    assert record_io["context"] == [("TST", EventType.PREMIER)]


def test_add_traditional_downloads_full_set_with_context(record_io):
    external._add("TST", event_type=EventType.TRADITIONAL)

    # Trad is single-pick like Premier, so set context is generated (not skipped)
    # and threaded with the Trad event_type.
    assert record_io["download"] == [
        ("TST", View.DRAFT, EventType.TRADITIONAL),
        ("TST", View.GAME, EventType.TRADITIONAL),
    ]
    assert record_io["card"] == [("TST", EventType.TRADITIONAL)]
    assert record_io["context"] == [("TST", EventType.TRADITIONAL)]


def test_add_pick_two_skips_only_set_context(record_io):
    external._add("OM1", event_type=EventType.PICK_TWO)

    # draft + game + card download identically; only the summon-driven set
    # context is skipped for multi-pick formats
    assert record_io["download"] == [
        ("OM1", View.DRAFT, EventType.PICK_TWO),
        ("OM1", View.GAME, EventType.PICK_TWO),
    ]
    assert record_io["card"] == [("OM1", EventType.PICK_TWO)]
    assert record_io["context"] == []
