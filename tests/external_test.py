"""
Tests for the `spells add` orchestration in external.py. These pin the call
pattern (which files get downloaded/written) without touching the network.
"""

import polars as pl
import pytest

from spells import external
from spells.enums import View, EventType


FAKE_NAMES = ["Card A", "Card B"]


@pytest.fixture()
def record_io(monkeypatch: pytest.MonkeyPatch):
    """Replace the IO steps of `_add` with recorders, returning the call log."""
    calls: dict[str, list] = {"download": [], "card": [], "context": []}

    def fake_download(set_code, dataset_type, event_type=EventType.PREMIER, **kw):
        calls["download"].append((set_code, dataset_type, event_type))
        return 0

    def fake_names(set_code, event_type=EventType.PREMIER, **kw):
        return FAKE_NAMES

    def fake_card(set_code, names, **kw):
        calls["card"].append((set_code, names))
        return 0

    def fake_context(set_code, event_type=EventType.PREMIER, **kw):
        calls["context"].append((set_code, event_type))
        return 0

    monkeypatch.setattr(external, "download_data_set", fake_download)
    monkeypatch.setattr(external.cards, "names_from_parquet", fake_names)
    monkeypatch.setattr(external.cards, "write_card_file", fake_card)
    monkeypatch.setattr(external, "get_set_context", fake_context)
    return calls


def test_add_premier_downloads_full_set(record_io):
    external._add("TST", event_type=EventType.PREMIER)

    assert record_io["download"] == [
        ("TST", View.DRAFT, EventType.PREMIER),
        ("TST", View.GAME, EventType.PREMIER),
    ]
    assert record_io["card"] == [("TST", FAKE_NAMES)]
    assert record_io["context"] == [("TST", EventType.PREMIER)]


def test_add_traditional_downloads_full_set_with_context(record_io):
    external._add("TST", event_type=EventType.TRADITIONAL)

    # Trad is single-pick like Premier, so set context is generated (not skipped)
    # and threaded with the Trad event_type.
    assert record_io["download"] == [
        ("TST", View.DRAFT, EventType.TRADITIONAL),
        ("TST", View.GAME, EventType.TRADITIONAL),
    ]
    assert record_io["card"] == [("TST", FAKE_NAMES)]
    assert record_io["context"] == [("TST", EventType.TRADITIONAL)]


def test_add_pick_two_skips_only_set_context(record_io):
    external._add("OM1", event_type=EventType.PICK_TWO)

    # draft + game + card download identically; only the summon-driven set
    # context is skipped for multi-pick formats
    assert record_io["download"] == [
        ("OM1", View.DRAFT, EventType.PICK_TWO),
        ("OM1", View.GAME, EventType.PICK_TWO),
    ]
    assert record_io["card"] == [("OM1", FAKE_NAMES)]
    assert record_io["context"] == []


def test_refresh_card_only_rebuilds_from_existing_draft_file(record_io, monkeypatch):
    cleaned = []
    monkeypatch.setattr(external.cache, "clean", lambda set_code: cleaned.append(set_code))

    result = external._refresh_card_only("TST", event_type=EventType.PREMIER)

    assert result == 0
    assert record_io["card"] == [("TST", FAKE_NAMES)]
    # no draft/game download, no set context — only the card file is touched
    assert record_io["download"] == []
    assert record_io["context"] == []
    assert cleaned == ["TST"]


def test_refresh_card_only_requires_existing_draft_file(monkeypatch):
    def boom(set_code, event_type=EventType.PREMIER, **kw):
        raise FileNotFoundError(f"No {event_type} draft file for {set_code}")

    monkeypatch.setattr(external.cards, "names_from_parquet", boom)

    assert external._refresh_card_only("TST", event_type=EventType.PREMIER) == 1


def test_cli_dispatches_refresh_card_only(monkeypatch):
    calls = []
    monkeypatch.setattr(
        external,
        "_refresh_card_only",
        lambda set_code, event_type: calls.append((set_code, event_type)) or 0,
    )
    monkeypatch.setattr(external, "_refresh", lambda *a, **kw: pytest.fail("full refresh ran"))
    monkeypatch.setattr(external.sys, "argv", ["spells", "refresh", "TST", "--card-only"])

    assert external.cli() == 0
    assert calls == [("TST", EventType.PREMIER)]


def test_cli_rejects_card_only_with_add(monkeypatch):
    monkeypatch.setattr(external.sys, "argv", ["spells", "add", "TST", "--card-only"])

    assert external.cli() == 1


def test_write_card_file_validates_consistent_names(tmp_path, monkeypatch):
    from spells.cards import BASIC_LANDS, write_card_file

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external" / "TST"
    ext.mkdir(parents=True)
    pl.DataFrame({"name": ["Card A", "Card B", *BASIC_LANDS]}).write_parquet(
        ext / "TST_card.parquet"
    )

    assert write_card_file("TST", ["Card A", "Card B"]) == 1


def test_write_card_file_tolerates_names_without_basics(tmp_path, monkeypatch):
    from spells.cards import BASIC_LANDS, write_card_file

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external" / "TST"
    ext.mkdir(parents=True)
    pl.DataFrame({"name": ["Card A", *BASIC_LANDS]}).write_parquet(
        ext / "TST_card.parquet"
    )

    # the card ratings API omits basics; the file still validates because
    # write_card_file canonicalizes the incoming names to include them
    assert write_card_file("TST", ["Card A"]) == 1


def test_write_card_file_raises_on_inconsistent_names(tmp_path, monkeypatch):
    from spells.cards import BASIC_LANDS, write_card_file

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external" / "TST"
    ext.mkdir(parents=True)
    pl.DataFrame({"name": ["Card A", "Card B", *BASIC_LANDS]}).write_parquet(
        ext / "TST_card.parquet"
    )

    with pytest.raises(ValueError, match="inconsistent"):
        write_card_file("TST", ["Card A", "Card C"])
