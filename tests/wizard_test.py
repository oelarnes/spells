"""
Tests for the interactive walkthrough.

`_ask` is the single place questionary is called, so stubbing it drives every
flow without a terminal. Nothing here reaches the network or downloads.
"""

import datetime
import os
import pathlib

import pytest

from spells import catalog, inventory, wizard
from spells.catalog import Catalog, Dataset, RemoteFile
from spells.enums import EventType

LEGACY_SNAPSHOT = "PremierDraft_all_any_2026-06-23_2026-06-24.json"
VALID_SNAPSHOT = "PremierDraft_all_any_ALL_TIME_2026-07-14.json"

UTC = datetime.timezone.utc
REMOTE_TIME = datetime.datetime(2026, 7, 27, tzinfo=UTC)
OLDER = datetime.datetime(2026, 1, 1, tzinfo=UTC).timestamp()
NEWER = datetime.datetime(2026, 8, 5, tzinfo=UTC).timestamp()


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
    """OLD predates the collection, TST is held, NEW is published after it."""
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
    monkeypatch.setattr(wizard, "_ask", lambda p: queued.pop(0) if queued else None)
    return queued


@pytest.fixture
def no_network(monkeypatch, cat):
    def remote(url):
        return RemoteFile(url=url, last_modified=REMOTE_TIME)

    monkeypatch.setattr(wizard.catalog, "fetch", lambda *a, **k: cat)
    monkeypatch.setattr(wizard.catalog, "head", remote)
    monkeypatch.setattr(catalog, "head", remote)


@pytest.fixture
def downloads(monkeypatch):
    calls = []
    monkeypatch.setattr(
        wizard.external,
        "_add",
        lambda s, event_type, **kw: calls.append((s, event_type)) or 0,
    )
    return calls


@pytest.fixture
def removals(monkeypatch):
    calls = []
    monkeypatch.setattr(wizard.external, "_remove", lambda s: calls.append(s) or 0)
    return calls


def write_set(home, set_code, event_type=EventType.PREMIER, mtime=NEWER):
    d = home / "external" / set_code
    d.mkdir(parents=True, exist_ok=True)
    for view in ("draft", "game", "context"):
        path = d / f"{set_code}_{event_type}_{view}.parquet"
        path.write_bytes(b"x" * 10)
        os.utime(path, (mtime, mtime))
    (d / f"{set_code}_card.parquet").write_bytes(b"x" * 10)
    return d


def offers_for(inv, cat):
    return wizard.candidates(inv, cat, wizard._check(inv, cat))


# ---------------------------------------------------------------------------
# What gets offered, and what is ticked
# ---------------------------------------------------------------------------


def test_a_first_run_pre_selects_the_two_newest_sets(cat):
    """Enough to start without knowing which codes are current, and far short
    of the tens of gigabytes that ticking everything would queue."""
    empty = inventory.Inventory(data_home=pathlib.Path("/tmp/nothing-here"))

    ticked = [c for c in wizard.candidates(empty, cat, []) if c.wanted]

    assert [c.expansion for c in ticked] == ["NEW", "TST"]
    assert all(c.event_type == EventType.PREMIER for c in ticked)


def test_a_first_run_ticks_only_premier_for_those_sets(cat):
    """TST publishes TradDraft too; a newcomer wants one format, not both."""
    empty = inventory.Inventory(data_home=pathlib.Path("/tmp/nothing-here"))

    offers = wizard.candidates(empty, cat, [])
    trad = [c for c in offers if c.event_type == EventType.TRADITIONAL]

    assert trad and not any(c.wanted for c in trad)


def test_a_first_run_does_not_call_anything_new(cat):
    """Nothing can be new to a data home with nothing in it."""
    empty = inventory.Inventory(data_home=pathlib.Path("/tmp/nothing-here"))

    assert not any(c.reason == "new set" for c in wizard.candidates(empty, cat, []))


def test_a_set_published_since_you_started_is_pre_selected(data_home, no_network, cat):
    write_set(data_home, "TST")

    new = [c for c in offers_for(inventory.scan(), cat) if c.expansion == "NEW"]
    assert [c.reason for c in new] == ["new set"]
    assert all(c.wanted for c in new)


def test_a_set_predating_your_collection_is_not_offered(data_home, no_network, cat):
    write_set(data_home, "TST")
    assert "OLD" not in {c.expansion for c in offers_for(inventory.scan(), cat)}


def test_stale_data_is_pre_selected(data_home, no_network, cat):
    write_set(data_home, "TST", mtime=OLDER)

    stale = [
        c
        for c in offers_for(inventory.scan(), cat)
        if c.expansion == "TST" and c.reason == "update available"
    ]
    assert stale and all(c.wanted for c in stale)


def test_an_event_type_you_never_wanted_is_offered_unticked(data_home, no_network, cat):
    """TST publishes TradDraft too, but only Premier was ever downloaded."""
    write_set(data_home, "TST")

    trad = [
        c
        for c in offers_for(inventory.scan(), cat)
        if c.expansion == "TST" and c.event_type == EventType.TRADITIONAL
    ]
    assert trad and not any(c.wanted for c in trad)


def test_offers_are_newest_set_first(data_home, no_network, cat):
    write_set(data_home, "TST")
    assert offers_for(inventory.scan(), cat)[0].expansion == "NEW"


# ---------------------------------------------------------------------------
# The menu
# ---------------------------------------------------------------------------


def test_menu_offers_only_what_applies(data_home, no_network, cat):
    write_set(data_home, "TST")
    inv = inventory.scan()

    keys = [a.key for a in wizard._menu(inv, cat, wizard._check(inv, cat))]
    assert keys == ["download", "remove", "quit"]  # nothing dead to clean up


def test_menu_offers_cleanup_when_there_are_dead_files(data_home, no_network, cat):
    write_set(data_home, "TST")
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / LEGACY_SNAPSHOT).write_text("[]")

    inv = inventory.scan()
    assert "clean" in [a.key for a in wizard._menu(inv, cat, wizard._check(inv, cat))]


def test_menu_has_nothing_to_remove_on_a_first_run(data_home, no_network, cat):
    assert "remove" not in [a.key for a in wizard._menu(inventory.scan(), cat, [])]


# ---------------------------------------------------------------------------
# Downloading
# ---------------------------------------------------------------------------


def test_download_queues_the_chosen_sets(
    data_home, answers, no_network, cat, downloads
):
    write_set(data_home, "TST")
    inv = inventory.scan()
    answers.extend([["NEW"], True])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert downloads == [("NEW", EventType.PREMIER)]


def test_download_asks_for_event_types_when_several_are_offered(
    data_home, answers, no_network, cat, downloads
):
    write_set(data_home, "TST", mtime=OLDER)
    inv = inventory.scan()
    answers.extend([["TST"], [EventType.TRADITIONAL], True])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert downloads == [("TST", EventType.TRADITIONAL)]


def test_download_skips_the_event_type_step_when_there_is_one(
    data_home, answers, no_network, cat, downloads
):
    write_set(data_home, "TST")
    inv = inventory.scan()
    answers.extend([["NEW"], True])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert answers == []  # the confirm took the second reply, not an event step
    assert downloads == [("NEW", EventType.PREMIER)]


def test_selecting_no_sets_downloads_nothing(
    data_home, answers, no_network, cat, downloads
):
    write_set(data_home, "TST")
    inv = inventory.scan()
    answers.append([])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert downloads == []


def test_declining_the_confirmation_downloads_nothing(
    data_home, answers, no_network, cat, downloads
):
    write_set(data_home, "TST")
    inv = inventory.scan()
    answers.extend([["NEW"], False])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert downloads == []


# ---------------------------------------------------------------------------
# Removing
# ---------------------------------------------------------------------------


def test_remove_deletes_the_chosen_sets(data_home, answers, no_network, cat, removals):
    write_set(data_home, "TST")
    write_set(data_home, "TS2")
    answers.extend([["TST"], True])

    wizard.remove(inventory.scan(), cat, [])
    assert removals == ["TST"]


def test_remove_defaults_to_declining(data_home, answers, no_network, cat, removals):
    """Nothing is pre-ticked and the confirmation defaults to no: this is the
    one action in the walkthrough that destroys downloaded data."""
    write_set(data_home, "TST")
    answers.extend([["TST"], False])

    wizard.remove(inventory.scan(), cat, [])
    assert removals == []


def test_remove_with_nothing_selected_deletes_nothing(
    data_home, answers, no_network, cat, removals
):
    write_set(data_home, "TST")
    answers.append([])

    wizard.remove(inventory.scan(), cat, [])
    assert removals == []


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
    wizard.free_space(inventory.scan(), cat, [])
    assert (snaps / LEGACY_SNAPSHOT).exists()

    answers.append(True)
    wizard.free_space(inventory.scan(), cat, [])
    assert not (snaps / LEGACY_SNAPSHOT).exists()
    assert (snaps / VALID_SNAPSHOT).exists()


def test_free_space_leaves_advisories_alone(data_home, answers, no_network, cat):
    """An unrecognized directory is reported, never deleted — the rule doctor
    follows."""
    write_set(data_home, "TST")
    (data_home / "mystery").mkdir()
    (data_home / "mystery" / "notes.txt").write_bytes(b"x")

    answers.append(True)
    wizard.free_space(inventory.scan(), cat, [])

    assert (data_home / "mystery").exists()


# ---------------------------------------------------------------------------
# Commands are taught, not hidden
# ---------------------------------------------------------------------------


def test_downloading_prints_the_equivalent_command(
    data_home, answers, no_network, cat, downloads, capsys
):
    write_set(data_home, "TST")
    inv = inventory.scan()
    answers.extend([["NEW"], True])

    wizard.download(inv, cat, wizard._check(inv, cat))
    assert "spells add NEW PremierDraft" in capsys.readouterr().out


def test_removing_prints_the_equivalent_command(
    data_home, answers, no_network, cat, removals, capsys
):
    write_set(data_home, "TST")
    answers.extend([["TST"], True])

    wizard.remove(inventory.scan(), cat, [])
    assert "spells remove TST --yes" in capsys.readouterr().out


def test_cleanup_prints_the_equivalent_command(
    data_home, answers, no_network, cat, capsys
):
    write_set(data_home, "TST")
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / LEGACY_SNAPSHOT).write_text("[]")

    answers.append(True)
    wizard.free_space(inventory.scan(), cat, [])
    assert "spells doctor --yes" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# The opening screen
# ---------------------------------------------------------------------------


def test_opening_shows_the_status_table_when_there_is_data(
    data_home, no_network, cat, capsys
):
    write_set(data_home, "TST")
    wizard._opening(inventory.scan(), cat)

    out = capsys.readouterr().out
    assert "TST" in out
    assert "event types" in out


def test_opening_explains_itself_when_there_is_no_data(
    data_home, no_network, cat, capsys
):
    wizard._opening(inventory.scan(), cat)

    out = capsys.readouterr().out
    assert "No 17Lands data yet" in out
    assert "published, not downloaded" in out


def test_opening_names_sets_with_newer_data(data_home, no_network, cat, capsys):
    write_set(data_home, "TST", mtime=OLDER)
    wizard._opening(inventory.scan(), cat)

    assert "have newer data" in capsys.readouterr().out


def test_unreachable_catalog_still_allows_local_actions(data_home, monkeypatch, capsys):
    write_set(data_home, "TST")
    fallback = Catalog(datasets=(), is_fallback=True)
    monkeypatch.setattr(wizard.catalog, "fetch", lambda *a, **k: fallback)

    rows = wizard._opening(inventory.scan(), fallback)

    assert rows == []
    assert "Could not reach 17Lands" in capsys.readouterr().err


def test_quitting_leaves_immediately(data_home, answers, no_network, cat, downloads):
    write_set(data_home, "TST")
    answers.append("quit")

    wizard.run()
    assert downloads == []


def test_a_partly_held_set_is_labelled_by_event_type(data_home, no_network, cat):
    """ "not downloaded" beside a set you plainly have reads as though the whole
    thing were missing, when only an event type is."""
    write_set(data_home, "TST")
    offers = offers_for(inventory.scan(), cat)

    assert wizard._reasons_for(offers, "TST") == "TradDraft"


def test_a_pre_selected_set_is_labelled_by_reason(data_home, no_network, cat):
    write_set(data_home, "TST")
    offers = offers_for(inventory.scan(), cat)

    assert wizard._reasons_for(offers, "NEW") == "new set"
