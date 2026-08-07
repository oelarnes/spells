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
