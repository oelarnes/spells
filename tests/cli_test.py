"""
Tests for the `spells` command line. CliRunner drives the real typer app; the
download orchestration is monkeypatched, so nothing reaches the network.

Note that CliRunner's stdin is never a tty, which is exactly the unattended
case the destructive commands have to refuse rather than hang on.
"""

import json

import pytest
from typer.testing import CliRunner

from spells import cli, external
from spells.enums import EventType

runner = CliRunner()


@pytest.fixture
def data_home(tmp_path, monkeypatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    (tmp_path / "external" / "TST").mkdir(parents=True)
    for view in ("draft", "game", "context"):
        (
            tmp_path / "external" / "TST" / f"TST_PremierDraft_{view}.parquet"
        ).write_bytes(b"x" * 10)
    (tmp_path / "external" / "TST" / "TST_card.parquet").write_bytes(b"x" * 10)
    return tmp_path


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_bare_invocation_shows_status(data_home):
    result = runner.invoke(cli.app, [])
    assert result.exit_code == 0
    assert "TST" in result.stdout


def test_status_json_is_parseable(data_home):
    result = runner.invoke(cli.app, ["status", "--json"])
    assert result.exit_code == 0

    payload = json.loads(result.stdout)
    assert payload["data_home"] == str(data_home)
    (entry,) = payload["sets"]
    assert entry["set_code"] == "TST"
    assert entry["card_file"] is True
    assert entry["event_types"]["PremierDraft"]["complete"] is True


def test_status_for_one_set(data_home):
    assert runner.invoke(cli.app, ["status", "TST"]).exit_code == 0


def test_status_for_unknown_set_exits_1(data_home):
    result = runner.invoke(cli.app, ["status", "NOPE"])
    assert result.exit_code == 1


def test_status_reports_anomalies(data_home):
    (data_home / "external" / "ECL").mkdir(parents=True)
    (
        data_home / "external" / "ECL" / "draft_data_public.ECL.PremierDraft.csv"
    ).write_bytes(b"x" * 10)

    result = runner.invoke(cli.app, ["status"])
    assert "stray-download" in result.stdout


def test_status_renders_an_empty_set_directory(data_home):
    """The anomaly summary looks up help text per kind, so a kind with no entry
    took down every caller that renders status — the walkthrough included."""
    (data_home / "external" / "STH").mkdir(parents=True)

    result = runner.invoke(cli.app, ["status"])
    assert result.exit_code == 0, result.exception
    assert "empty-set" in result.stdout


# ---------------------------------------------------------------------------
# add / refresh dispatch
# ---------------------------------------------------------------------------


def test_add_card_only_dispatches(monkeypatch, data_home):
    calls = []
    monkeypatch.setattr(
        external,
        "_add_card_only",
        lambda set_code, event_type: calls.append((set_code, event_type)) or 0,
    )
    monkeypatch.setattr(external, "_add", lambda *a, **kw: pytest.fail("full add ran"))

    result = runner.invoke(cli.app, ["add", "TST", "--card-only"])
    assert result.exit_code == 0
    assert calls == [("TST", EventType.PREMIER)]


def test_refresh_card_only_dispatches(monkeypatch, data_home):
    calls = []
    monkeypatch.setattr(
        external,
        "_refresh_card_only",
        lambda set_code, event_type: calls.append((set_code, event_type)) or 0,
    )
    monkeypatch.setattr(
        external, "_refresh", lambda *a, **kw: pytest.fail("full refresh ran")
    )

    result = runner.invoke(cli.app, ["refresh", "TST", "--card-only"])
    assert result.exit_code == 0
    assert calls == [("TST", EventType.PREMIER)]


def test_add_passes_event_type(monkeypatch, data_home):
    calls = []
    monkeypatch.setattr(
        external, "_add", lambda set_code, event_type: calls.append(event_type) or 0
    )

    runner.invoke(cli.app, ["add", "OM1", "PickTwoDraft"])
    assert calls == [EventType.PICK_TWO]


def test_add_rejects_unknown_event_type(data_home):
    result = runner.invoke(cli.app, ["add", "TST", "NotARealDraft"])
    assert result.exit_code == 2


def test_card_only_is_not_accepted_by_remove(data_home):
    result = runner.invoke(cli.app, ["remove", "TST", "--card-only"])
    assert result.exit_code == 2


# ---------------------------------------------------------------------------
# Destructive commands and headless behavior
# ---------------------------------------------------------------------------


def test_remove_refuses_without_confirmation_when_piped(monkeypatch, data_home):
    monkeypatch.setattr(external, "_remove", lambda *a: pytest.fail("removed anyway"))

    result = runner.invoke(cli.app, ["remove", "TST"])
    assert result.exit_code == 2


def test_remove_proceeds_with_yes(monkeypatch, data_home):
    calls = []
    monkeypatch.setattr(
        external, "_remove", lambda set_code: calls.append(set_code) or 0
    )

    result = runner.invoke(cli.app, ["remove", "TST", "--yes"])
    assert result.exit_code == 0
    assert calls == ["TST"]


def test_clean_is_not_gated(data_home):
    # derived cache rebuilds on demand, so it never needs confirmation
    (data_home / "cache" / "TST").mkdir(parents=True)
    (data_home / "cache" / "TST" / "abc.parquet").write_bytes(b"x" * 10)

    result = runner.invoke(cli.app, ["clean", "TST"])
    assert result.exit_code == 0
    assert not (data_home / "cache" / "TST").exists()


# ---------------------------------------------------------------------------
# path
# ---------------------------------------------------------------------------


def test_path_reports_data_home(data_home):
    result = runner.invoke(cli.app, ["path"])
    assert result.stdout.strip() == str(data_home)


def test_path_reports_external_set_dir(data_home):
    result = runner.invoke(cli.app, ["path", "TST"])
    assert result.stdout.strip() == str(data_home / "external" / "TST")


def test_path_reports_store_dir(data_home):
    result = runner.invoke(cli.app, ["path", "--kind", "ratings"])
    assert result.stdout.strip() == str(data_home / "ratings")


def test_path_reports_store_set_dir(data_home):
    result = runner.invoke(cli.app, ["path", "TST", "--kind", "cache"])
    assert result.stdout.strip() == str(data_home / "cache" / "TST")


def test_status_omits_draft_logs_when_reporting_on_one_set(data_home):
    """Cached logs are keyed by draft id with no set attribution, so showing a
    count under `status TST` implies an association that does not exist."""
    (data_home / "draft").mkdir()
    (data_home / "draft" / "abc123.json").write_bytes(b"x" * 10)

    assert "cached draft logs" in runner.invoke(cli.app, ["status"]).stdout
    assert "cached draft logs" not in runner.invoke(cli.app, ["status", "TST"]).stdout


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_catalog(monkeypatch):
    """A catalog publishing TST in two event types, and nothing for KHM."""
    import datetime

    from spells.catalog import Catalog, Dataset, RemoteFile

    def dataset(event_type, draft=True):
        stub = f"https://example/{{}}_data_public.TST.{event_type}.csv.gz"
        return Dataset(
            expansion="TST",
            format_name=str(event_type),
            event_type=event_type,
            last_updated=datetime.date(2026, 7, 26),
            draft_url=stub.format("draft") if draft else None,
            game_url=stub.format("game"),
        )

    cat = Catalog(datasets=(dataset(EventType.PREMIER), dataset(EventType.TRADITIONAL)))
    monkeypatch.setattr(cli.catalog, "fetch", lambda *a, **k: cat)
    monkeypatch.setattr(
        cli.catalog,
        "head",
        lambda url: RemoteFile(
            url=url,
            last_modified=datetime.datetime(2026, 7, 27, tzinfo=datetime.timezone.utc),
        ),
    )
    return cat


REMOTE_MTIME = 1785110400  # 2026-07-27, what fake_catalog's HEAD reports


def _set_mtime(data_home, when: int) -> None:
    import os

    for path in (data_home / "external" / "TST").glob("*.parquet"):
        os.utime(path, (when, when))


def test_check_flags_stale_local_data_and_exits_3(data_home, fake_catalog):
    _set_mtime(data_home, REMOTE_MTIME - 86400)

    result = runner.invoke(cli.app, ["check"])
    assert result.exit_code == 3
    assert "stale" in result.stdout


def test_check_reports_an_event_type_published_but_never_added(data_home, fake_catalog):
    """The whole point of the catalog: TradDraft exists and we do not have it."""
    result = runner.invoke(cli.app, ["check", "--json"])
    payload = json.loads(result.stdout)

    trad = [d for d in payload["datasets"] if d["event_type"] == "TradDraft"]
    assert trad and all(d["status"] == "absent" for d in trad)


def test_check_json_carries_the_url_and_remote_timestamp(data_home, fake_catalog):
    result = runner.invoke(cli.app, ["check", "--json"])
    payload = json.loads(result.stdout)

    assert payload["catalog_reachable"] is True
    draft = next(
        d
        for d in payload["datasets"]
        if d["event_type"] == "PremierDraft" and d["view"] == "draft"
    )
    assert draft["url"].endswith("draft_data_public.TST.PremierDraft.csv.gz")
    assert draft["remote_last_modified"].startswith("2026-07-27")


def test_check_reports_current_when_local_is_newer(data_home, fake_catalog):
    _set_mtime(data_home, REMOTE_MTIME + 86400)

    result = runner.invoke(cli.app, ["check", "TST", "--json"])
    payload = json.loads(result.stdout)
    held = [
        d
        for d in payload["datasets"]
        if d["event_type"] == "PremierDraft" and d["view"] in ("draft", "game")
    ]
    assert all(d["status"] == "current" for d in held)


def test_check_for_a_set_17lands_does_not_publish_exits_1(data_home, fake_catalog):
    result = runner.invoke(cli.app, ["check", "NOPE"])
    assert result.exit_code == 1


def test_check_warns_but_does_not_claim_unpublished_when_offline(
    monkeypatch, data_home
):
    from spells.catalog import Catalog

    monkeypatch.setattr(
        cli.catalog, "fetch", lambda *a, **k: Catalog(datasets=(), is_fallback=True)
    )
    result = runner.invoke(cli.app, ["check", "--json"])
    payload = json.loads(result.stdout)

    assert payload["catalog_reachable"] is False
    assert all(d["status"] == "unknown" for d in payload["datasets"])


def test_check_ignores_event_types_never_added_when_setting_exit_code(
    data_home, fake_catalog
):
    """Nearly every set publishes TradDraft. Counting one the user never added
    as "work to do" would make exit 3 fire permanently and be useless to cron."""
    _set_mtime(data_home, REMOTE_MTIME + 86400)

    result = runner.invoke(cli.app, ["check"])
    assert result.exit_code == 0

    payload = json.loads(runner.invoke(cli.app, ["check", "--json"]).stdout)
    trad = [d for d in payload["datasets"] if d["event_type"] == "TradDraft"]
    assert trad
    assert all(d["status"] == "absent" for d in trad)
    assert all(d["tracked"] is False for d in trad)
    assert all(d["actionable"] is False for d in trad)


def test_check_counts_a_missing_file_in_a_tracked_event_type_as_work(
    data_home, fake_catalog
):
    """A half-downloaded event type is a real defect, unlike one never added."""
    _set_mtime(data_home, REMOTE_MTIME + 86400)
    (data_home / "external" / "TST" / "TST_PremierDraft_game.parquet").unlink()

    result = runner.invoke(cli.app, ["check", "--json"])
    assert result.exit_code == 3

    payload = json.loads(result.stdout)
    game = next(
        d
        for d in payload["datasets"]
        if d["event_type"] == "PremierDraft" and d["view"] == "game"
    )
    assert game["status"] == "absent"
    assert game["tracked"] is True
    assert game["actionable"] is True


# ---------------------------------------------------------------------------
# doctor / snapshots
# ---------------------------------------------------------------------------


LEGACY_SNAPSHOT = "PremierDraft_all_any_2026-06-23_2026-06-24.json"
VALID_SNAPSHOT = "PremierDraft_all_any_ALL_TIME_2026-07-14.json"


@pytest.fixture
def snapshots(data_home):
    d = data_home / "ratings" / "TST"
    d.mkdir(parents=True)
    for name in (LEGACY_SNAPSHOT, VALID_SNAPSHOT):
        (d / name).write_text("[]")
    return d


def test_doctor_is_a_dry_run_by_default(snapshots):
    result = runner.invoke(cli.app, ["doctor"])
    assert result.exit_code == 0
    assert "would remove" in result.stdout
    assert (snapshots / LEGACY_SNAPSHOT).exists()


def test_doctor_tells_you_how_to_act_when_headless(snapshots):
    """Nothing can be confirmed without a terminal, so the dry run has to name
    the flag that would have done it."""
    result = runner.invoke(cli.app, ["doctor"])
    assert "--yes" in result.stdout
    assert (snapshots / LEGACY_SNAPSHOT).exists()


def test_doctor_execute_with_yes_deletes_only_the_dead_snapshot(snapshots):
    result = runner.invoke(cli.app, ["doctor", "--yes"])
    assert result.exit_code == 0
    assert not (snapshots / LEGACY_SNAPSHOT).exists()
    assert (snapshots / VALID_SNAPSHOT).exists()


def test_doctor_reports_advisories_without_offering_to_delete(data_home):
    (data_home / "mystery").mkdir()
    (data_home / "mystery" / "notes.txt").write_bytes(b"x")

    result = runner.invoke(cli.app, ["doctor"])
    assert "needs your judgement" in result.stdout
    assert (data_home / "mystery").exists()


def test_doctor_json_lists_paths(snapshots):
    result = runner.invoke(cli.app, ["doctor", "--json"])
    payload = json.loads(result.stdout)

    (entry,) = payload["repairs"]
    assert entry["kind"] == "legacy-snapshot"
    assert entry["paths"] == [str(snapshots / LEGACY_SNAPSHOT)]


def test_snapshots_list_separates_keep_from_dead(snapshots):
    result = runner.invoke(cli.app, ["snapshots", "--json"])
    payload = json.loads(result.stdout)

    (entry,) = payload["sets"]
    assert entry["ratings"]["valid"] == 1
    assert entry["ratings"]["legacy"] == 1


@pytest.fixture
def at_a_terminal(monkeypatch):
    """CliRunner's stdin is never a tty, so the interactive branch has to be
    asked for explicitly."""
    monkeypatch.setattr(cli, "_is_interactive", lambda: True)


def test_doctor_offers_the_deletion_at_a_terminal(snapshots, at_a_terminal):
    """At a terminal the prompt is the confirmation; no second invocation."""
    result = runner.invoke(cli.app, ["doctor"], input="y\n")

    assert result.exit_code == 0
    assert not (snapshots / LEGACY_SNAPSHOT).exists()
    assert (snapshots / VALID_SNAPSHOT).exists()


def test_doctor_declining_the_prompt_deletes_nothing(snapshots, at_a_terminal):
    result = runner.invoke(cli.app, ["doctor"], input="n\n")

    assert result.exit_code == 0
    assert (snapshots / LEGACY_SNAPSHOT).exists()


def test_doctor_prompt_defaults_to_declining(snapshots, at_a_terminal):
    """A bare newline must not destroy anything."""
    result = runner.invoke(cli.app, ["doctor"], input="\n")

    assert result.exit_code == 0
    assert (snapshots / LEGACY_SNAPSHOT).exists()


def test_doctor_json_never_prompts_even_at_a_terminal(snapshots, at_a_terminal):
    result = runner.invoke(cli.app, ["doctor", "--json"])

    assert result.exit_code == 0
    json.loads(result.stdout)
    assert (snapshots / LEGACY_SNAPSHOT).exists()


# ---------------------------------------------------------------------------
# cards
# ---------------------------------------------------------------------------


import polars as pl  # noqa: E402

from spells import cards as cards_module  # noqa: E402

BASICS = ["Plains", "Island", "Swamp", "Mountain", "Forest"]


@pytest.fixture
def card_set(data_home):
    """Real parquets: the card commands read them, unlike the stub files the
    scanner tests get away with."""

    def build(draft_names, card_names):
        d = data_home / "external" / "TST"
        d.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({f"pack_card_{n}": [1] for n in draft_names}).write_parquet(
            d / "TST_PremierDraft_draft.parquet"
        )
        if card_names is not None:
            pl.DataFrame({"name": sorted(set(card_names) | set(BASICS))}).write_parquet(
                d / "TST_card.parquet"
            )
        return d

    return build


def test_cards_validates_a_consistent_file(card_set):
    """Exit 0: a card file that checks out is success, not a runtime error."""
    card_set(["Alpha", "Beta"], ["Alpha", "Beta"])

    result = runner.invoke(cli.app, ["cards", "TST"])
    assert result.exit_code == 0
    assert "validated" in result.output


def test_cards_reports_a_mismatch_without_a_traceback(card_set):
    card_set(["Alpha", "Gamma"], ["Alpha", "Delta"])

    result = runner.invoke(cli.app, ["cards", "TST"])

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "Traceback" not in result.output
    assert "does not match" in result.output
    assert "Gamma" in result.output and "Delta" in result.output


def test_cards_mismatch_names_the_side_each_difference_came_from(card_set):
    card_set(["Alpha", "Gamma"], ["Alpha", "Delta"])

    out = runner.invoke(cli.app, ["cards", "TST"]).output
    assert "1 only in draft data" in out
    assert "1 only in card file" in out


def test_cards_mismatch_truncates_long_name_lists(card_set):
    """Comparing the wrong set puts hundreds of names on each side."""
    card_set(
        [f"Card{i:03d}" for i in range(200)], [f"Other{i:03d}" for i in range(150)]
    )

    out = runner.invoke(cli.app, ["cards", "TST"]).output
    assert "200 only in draft data" in out
    assert "more" in out
    assert out.count("Card0") <= cli.NAMES_SHOWN


def test_cards_reports_a_missing_draft_file(card_set):
    result = runner.invoke(cli.app, ["cards", "NOPE"])
    assert result.exit_code == 1
    assert "Traceback" not in result.output


def test_cards_rebuild_dispatches_to_the_rebuild_path(monkeypatch, card_set):
    calls = []
    monkeypatch.setattr(
        external,
        "_refresh_card_only",
        lambda set_code, event_type: calls.append((set_code, event_type)) or 0,
    )
    result = runner.invoke(cli.app, ["cards", "TST", "--rebuild"])

    assert result.exit_code == 0
    assert calls == [("TST", EventType.PREMIER)]


def test_cards_takes_an_event_type(monkeypatch, card_set):
    calls = []
    monkeypatch.setattr(
        external,
        "_add_card_only",
        lambda set_code, event_type: calls.append(event_type) or 0,
    )
    runner.invoke(cli.app, ["cards", "TST", "TradDraft"])
    assert calls == [EventType.TRADITIONAL]


def test_add_presents_a_card_mismatch_cleanly(card_set):
    """The guard covers every entry point, not just `cards`."""
    card_set(["Alpha", "Gamma"], ["Alpha", "Delta"])

    result = runner.invoke(cli.app, ["add", "TST", "--card-only"])
    assert result.exit_code == 1
    assert "does not match" in result.output
    assert "Traceback" not in result.output


def test_mismatch_error_carries_both_sides():
    e = cards_module.CardFileMismatch("TST", ["a"], ["b", "c"])
    assert e.only_in_data == ["a"]
    assert e.only_in_file == ["b", "c"]
    assert "TST" in str(e)


# ---------------------------------------------------------------------------
# --quiet
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _loud():
    """--quiet is process-global, so a test that sets it must not leak."""
    from spells import console as spells_console

    spells_console.set_quiet(False)
    yield
    spells_console.set_quiet(False)


def test_quiet_suppresses_library_progress(data_home):
    loud = runner.invoke(cli.app, ["clean", "TST"])
    quiet = runner.invoke(cli.app, ["--quiet", "clean", "TST"])

    assert loud.output.strip() != ""
    assert quiet.output.strip() == ""
    assert quiet.exit_code == loud.exit_code


def test_quiet_still_reports_errors(card_set):
    card_set(["Alpha", "Gamma"], ["Alpha", "Delta"])

    result = runner.invoke(cli.app, ["--quiet", "cards", "TST"])

    assert result.exit_code == 1
    assert "does not match" in result.output
    assert "Checking card file" not in result.output


def test_quiet_does_not_swallow_a_missing_file_error(data_home):
    """These went through info(), so --quiet reduced them to a bare exit 1."""
    result = runner.invoke(cli.app, ["--quiet", "cards", "NOPE"])

    assert result.exit_code == 1
    assert "No PremierDraft draft file for NOPE" in result.output


def test_cards_does_not_report_itself_as_add(card_set):
    card_set(["Alpha"], ["Alpha"])

    output = runner.invoke(cli.app, ["cards", "TST"]).output
    assert "Checking card file" in output
    assert "add" not in output.split("Checking")[0]


def test_status_omits_the_snapshots_column_when_there_are_none(data_home):
    """Snapshots only exist for callers of the private ratings API, so for most
    users the column is a header over a column of dashes."""
    out = runner.invoke(cli.app, ["status"]).output

    assert "snapshots" not in out
    assert "event types" in out


def test_status_shows_the_snapshots_column_when_there_are_some(data_home):
    snaps = data_home / "ratings" / "TST"
    snaps.mkdir(parents=True)
    (snaps / "PremierDraft_all_any_ALL_TIME_2026-07-14.json").write_text("[]")

    assert "snapshots" in runner.invoke(cli.app, ["status"]).output
