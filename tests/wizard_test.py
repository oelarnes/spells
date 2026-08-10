"""
Tests for the interactive walkthrough.

`_ask` is the single place questionary is called, so stubbing it drives every
flow without a terminal. Nothing here reaches the network or downloads.
"""

import datetime

import pytest

from spells import catalog, external, inventory, wizard
from spells.catalog import Catalog, Dataset
from spells.enums import EventType

LEGACY_SNAPSHOT = "PremierDraft_all_any_2026-06-23_2026-06-24.json"
VALID_SNAPSHOT = "PremierDraft_all_any_ALL_TIME_2026-07-14.json"


def _dataset(expansion, event_type, updated=datetime.date(2026, 7, 26)):
    stub = f"https://example/{{}}_data_public.{expansion}.{event_type}.csv.gz"
    return Dataset(
        expansion=expansion,
        format_name=str(event_type),
        event_type=event_type,
        last_updated=updated,
        draft_url=stub.format("draft"),
        game_url=stub.format("game"),
    )


@pytest.fixture
def cat():
    return Catalog(
        datasets=(
            _dataset("OLD", EventType.PREMIER, datetime.date(2024, 1, 1)),
            _dataset("TST", EventType.PREMIER),
            _dataset("TST", EventType.TRADITIONAL),
            _dataset("NEW", EventType.PREMIER, datetime.date(2026, 8, 1)),
        )
    )


@pytest.fixture
def data_home(tmp_path, monkeypatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def answers(monkeypatch):
    """Queue replies for each prompt, in order."""
    queued = []

    def fake_ask(prompt):
        return queued.pop(0) if queued else None

    monkeypatch.setattr(wizard, "_ask", fake_ask)
    return queued


@pytest.fixture
def no_network(monkeypatch, cat):
    monkeypatch.setattr(catalog, "fetch", lambda *a, **k: cat)
    monkeypatch.setattr(wizard.catalog, "fetch", lambda *a, **k: cat)
    monkeypatch.setattr(wizard.catalog, "head", lambda url: None)
    monkeypatch.setattr(catalog, "head", lambda url: None)


@pytest.fixture
def downloads(monkeypatch):
    calls = []
    monkeypatch.setattr(
        external, "_add", lambda s, event_type, **kw: calls.append((s, event_type)) or 0
    )
    monkeypatch.setattr(
        wizard.external,
        "_add",
        lambda s, event_type, **kw: calls.append((s, event_type)) or 0,
    )
    return calls


def write_set(home, set_code, event_type=EventType.PREMIER):
    d = home / "external" / set_code
    d.mkdir(parents=True, exist_ok=True)
    for view in ("draft", "game", "context"):
        (d / f"{set_code}_{event_type}_{view}.parquet").write_bytes(b"x" * 10)
    (d / f"{set_code}_card.parquet").write_bytes(b"x" * 10)
    return d


# ---------------------------------------------------------------------------
# Bootstrapping
# ---------------------------------------------------------------------------


def test_empty_data_home_goes_straight_to_adding(
    data_home, answers, no_network, downloads
):
    """A first run has exactly one useful action, so no menu is shown."""
    answers.extend(["NEW", True])  # set, then confirm the download

    wizard.run()

    assert downloads == [("NEW", EventType.PREMIER)]


def test_bootstrap_picks_the_only_event_type_without_asking(
    data_home, answers, no_network, downloads
):
    """NEW publishes one event type; asking would be a prompt with one answer."""
    answers.extend(["NEW", True])

    wizard.run()

    assert downloads == [("NEW", EventType.PREMIER)]
    assert answers == []  # both replies consumed: set and confirm, nothing else


def test_bootstrap_asks_for_event_type_when_several_are_published(
    data_home, answers, no_network, downloads
):
    answers.extend(["TST", EventType.TRADITIONAL, True])

    wizard.run()

    assert downloads == [("TST", EventType.TRADITIONAL)]


def test_declining_the_download_adds_nothing(data_home, answers, no_network, downloads):
    answers.extend(["NEW", False])

    wizard.run()

    assert downloads == []


def test_backing_out_of_the_set_list_adds_nothing(
    data_home, answers, no_network, downloads
):
    answers.append(wizard.CANCEL)

    wizard.run()

    assert downloads == []


# ---------------------------------------------------------------------------
# The menu
# ---------------------------------------------------------------------------


def test_menu_offers_only_what_the_data_home_needs(data_home, no_network, cat):
    write_set(data_home, "TST")
    inv = inventory.scan()

    keys = [a.key for a in wizard._menu(inv, cat)]

    # nothing stale and nothing to clean, so neither is offered as a chore
    assert "update" not in keys
    assert "clean" not in keys
    assert keys[0] == "add"
    assert keys[-1] == "quit"


def test_menu_offers_cleanup_when_there_are_dead_files(data_home, no_network, cat):
    write_set(data_home, "TST")
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / LEGACY_SNAPSHOT).write_text("[]")
    (snaps / VALID_SNAPSHOT).write_text("[]")

    keys = [a.key for a in wizard._menu(inventory.scan(), cat)]
    assert "clean" in keys


def test_menu_counts_sets_you_could_add(data_home, no_network, cat):
    write_set(data_home, "TST")

    actions = {a.key: a.detail for a in wizard._menu(inventory.scan(), cat)}
    assert "1" in actions["add"]  # NEW is published and absent; OLD predates TST


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def test_free_space_deletes_only_after_confirmation(
    data_home, answers, no_network, cat
):
    write_set(data_home, "TST")
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / LEGACY_SNAPSHOT).write_text("[]")
    (snaps / VALID_SNAPSHOT).write_text("[]")

    answers.append(False)
    wizard.free_space(inventory.scan(), cat)
    assert (snaps / LEGACY_SNAPSHOT).exists()

    answers.append(True)
    wizard.free_space(inventory.scan(), cat)
    assert not (snaps / LEGACY_SNAPSHOT).exists()
    assert (snaps / VALID_SNAPSHOT).exists()


def test_free_space_leaves_advisories_alone(data_home, answers, no_network, cat):
    """An unrecognized directory is reported, never deleted — same rule doctor
    follows."""
    write_set(data_home, "TST")
    (data_home / "mystery").mkdir()
    (data_home / "mystery" / "notes.txt").write_bytes(b"x")

    answers.append(True)
    wizard.free_space(inventory.scan(), cat)

    assert (data_home / "mystery").exists()


# ---------------------------------------------------------------------------
# Commands are taught, not hidden
# ---------------------------------------------------------------------------


def test_actions_print_the_equivalent_command(
    data_home, answers, no_network, downloads, capsys
):
    answers.extend(["NEW", True])
    wizard.run()

    assert "spells add NEW PremierDraft" in capsys.readouterr().out


def test_cleanup_prints_the_equivalent_command(
    data_home, answers, no_network, cat, capsys
):
    write_set(data_home, "TST")
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / LEGACY_SNAPSHOT).write_text("[]")

    answers.append(True)
    wizard.free_space(inventory.scan(), cat)

    assert "spells doctor --yes" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Offline
# ---------------------------------------------------------------------------


def test_unreachable_catalog_still_allows_local_actions(
    data_home, answers, monkeypatch, capsys
):
    write_set(data_home, "TST")
    fallback = Catalog(datasets=(), is_fallback=True)
    monkeypatch.setattr(wizard.catalog, "fetch", lambda *a, **k: fallback)
    answers.append("quit")

    wizard.run()

    assert "Could not reach 17Lands" in capsys.readouterr().err
