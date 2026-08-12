"""
Tests for the data-home scanner. Everything runs against a fabricated data
home in tmp_path — no network, no real files.

The snapshot classifier gets the most attention: Phase 2's prune deletes
whatever it calls legacy, and a false positive there destroys data that cannot
be refetched (17lands resolves time periods against its own current date).
"""

import json

import pytest

from spells import inventory, repair
from spells.enums import EventType, TimePeriod
from spells.inventory import AnomalyKind, Remedy, is_valid_snapshot


@pytest.fixture
def data_home(tmp_path, monkeypatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    return tmp_path


def write_external(home, set_code, *names):
    d = home / "external" / set_code
    d.mkdir(parents=True, exist_ok=True)
    for name in names:
        (d / name).write_bytes(b"x" * 10)
    return d


def write_snapshots(home, store, set_code, *names):
    d = home / store / set_code
    d.mkdir(parents=True, exist_ok=True)
    for name in names:
        (d / name).write_text(json.dumps([{"name": "Card A"}]))
    return d


# ---------------------------------------------------------------------------
# Snapshot classifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("period", list(TimePeriod))
def test_every_time_period_is_valid(period):
    assert is_valid_snapshot(f"PremierDraft_all_any_{period}_2026-07-14.json")


@pytest.mark.parametrize("period", list(TimePeriod))
def test_deck_color_shape_is_valid(period):
    # deck_color snapshots have no deck-color token
    assert is_valid_snapshot(f"PremierDraft_top_{period}_2026-07-14.json")


@pytest.mark.parametrize(
    "name",
    [
        "PremierDraft_all_any_2026-06-23_2026-06-24.json",
        "PremierDraft_top_2024-09-07_2024-09-07.json",
        "PremierDraft_top_BRG_2024-10-08_2025-10-14 (1).json",
        "PremierDraft_all_any_ALL_TIME.json",
        "PremierDraft_all_any_NOT_A_PERIOD_2026-07-14.json",
        "notes.txt",
    ],
)
def test_legacy_and_junk_are_not_valid(name):
    assert not is_valid_snapshot(name)


def test_punctuated_set_codes_do_not_confuse_the_classifier():
    # `Cube+-+Powered` is a real set directory; set code never appears in the
    # filename, but guard against a future regex that assumes [A-Z]{3}
    assert is_valid_snapshot(
        f"PremierDraft_all_any_{TimePeriod.ALL_TIME}_2026-07-14.json"
    )


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------


def test_empty_data_home(data_home):
    inv = inventory.scan()
    assert inv.sets == {}
    assert inv.all_anomalies == []


def test_complete_premier_set(data_home):
    write_external(
        data_home,
        "TST",
        "TST_card.parquet",
        "TST_PremierDraft_draft.parquet",
        "TST_PremierDraft_game.parquet",
        "TST_PremierDraft_context.parquet",
    )
    inv = inventory.scan()

    tst = inv.sets["TST"]
    assert tst.card_file is not None
    assert tst.events[EventType.PREMIER].is_complete
    assert tst.anomalies == []


def test_incomplete_set_is_flagged(data_home):
    write_external(
        data_home, "TST", "TST_card.parquet", "TST_PremierDraft_draft.parquet"
    )
    inv = inventory.scan()

    (anomaly,) = inv.sets["TST"].anomalies
    assert anomaly.kind == AnomalyKind.INCOMPLETE_SET
    assert "game" in anomaly.detail and "context" in anomaly.detail


def test_pick_two_set_is_complete_without_context(data_home):
    # summon skips set context for multi-pick formats
    write_external(
        data_home,
        "OM1",
        "OM1_card.parquet",
        "OM1_PickTwoDraft_draft.parquet",
        "OM1_PickTwoDraft_game.parquet",
    )
    inv = inventory.scan()

    assert inv.sets["OM1"].events[EventType.PICK_TWO].is_complete
    assert inv.sets["OM1"].anomalies == []


def test_multiple_event_types(data_home):
    write_external(
        data_home,
        "SOS",
        "SOS_card.parquet",
        *[
            f"SOS_{event}_{view}.parquet"
            for event in (EventType.PREMIER, EventType.TRADITIONAL)
            for view in ("draft", "game", "context")
        ],
    )
    inv = inventory.scan()

    sos = inv.sets["SOS"]
    assert set(sos.events) == {EventType.PREMIER, EventType.TRADITIONAL}
    assert all(e.is_complete for e in sos.events.values())
    assert sos.anomalies == []


def test_stray_download_is_flagged(data_home):
    write_external(
        data_home,
        "ECL",
        "ECL_PremierDraft_draft.parquet",
        "draft_data_public.ECL.PremierDraft.csv",
    )
    inv = inventory.scan()

    kinds = {a.kind for a in inv.sets["ECL"].anomalies}
    assert AnomalyKind.STRAY_DOWNLOAD in kinds


def test_legacy_context_file_is_flagged(data_home):
    write_external(
        data_home,
        "TDM",
        "TDM_card.parquet",
        "TDM_context.parquet",
        "TDM_PremierDraft_draft.parquet",
        "TDM_PremierDraft_game.parquet",
        "TDM_PremierDraft_context.parquet",
    )
    inv = inventory.scan()

    legacy = [
        a for a in inv.sets["TDM"].anomalies if a.kind == AnomalyKind.LEGACY_CONTEXT
    ]
    assert len(legacy) == 1


def test_legacy_snapshots_aggregate_to_one_anomaly(data_home):
    write_snapshots(
        data_home,
        "ratings",
        "BLB",
        *[
            f"PremierDraft_all_any_2024-08-13_2024-09-{d:02d}.json"
            for d in range(10, 30)
        ],
        f"PremierDraft_all_any_{TimePeriod.ALL_TIME}_2026-07-14.json",
    )
    inv = inventory.scan()

    blb = inv.sets["BLB"]
    assert blb.ratings.valid == 1
    assert blb.ratings.legacy == 20

    (anomaly,) = blb.anomalies
    assert anomaly.kind == AnomalyKind.LEGACY_SNAPSHOT
    assert "20" in anomaly.detail


def test_orphan_cache_is_flagged(data_home):
    cache_dir = data_home / "cache" / "GONE"
    cache_dir.mkdir(parents=True)
    (cache_dir / "abc123.parquet").write_bytes(b"x" * 10)

    inv = inventory.scan()

    kinds = {a.kind for a in inv.sets["GONE"].anomalies}
    assert AnomalyKind.ORPHAN_CACHE in kinds


def test_cache_with_external_data_is_not_orphaned(data_home):
    write_external(data_home, "TST", "TST_card.parquet")
    cache_dir = data_home / "cache" / "TST"
    cache_dir.mkdir(parents=True)
    (cache_dir / "abc123.parquet").write_bytes(b"x" * 10)

    inv = inventory.scan()

    kinds = {a.kind for a in inv.sets["TST"].anomalies}
    assert AnomalyKind.ORPHAN_CACHE not in kinds


def test_orphan_top_level_dir_is_flagged(data_home):
    (data_home / "filters").mkdir(parents=True)
    (data_home / "filters" / "filters_2026-07-03.json").write_text("{}")

    inv = inventory.scan()

    (anomaly,) = inv.anomalies
    assert anomaly.kind == AnomalyKind.ORPHAN_DIR
    assert anomaly.path.name == "filters"


def test_log_dir_is_not_an_anomaly(data_home):
    (data_home / ".logs").mkdir(parents=True)
    (data_home / ".logs" / "spells.log").write_text("")

    assert inventory.scan().anomalies == []


def test_draft_logs_are_counted(data_home):
    draft_dir = data_home / "draft"
    draft_dir.mkdir(parents=True)
    for i in range(3):
        (draft_dir / f"{i}.json").write_text("{}")

    inv = inventory.scan()
    assert inv.draft_logs == 3


# ---------------------------------------------------------------------------
# Empty set directories
# ---------------------------------------------------------------------------


def _empty_anomalies(inv):
    return [a for a in inv.all_anomalies if a.kind == AnomalyKind.EMPTY_SET]


def test_an_empty_set_directory_is_reported(data_home):
    """`add` makes the directory before downloading, so a 404 leaves a shell.
    Nothing else catches it: no files means no event, so no incomplete set."""
    (data_home / "external" / "Powered Cube").mkdir(parents=True)

    (anomaly,) = _empty_anomalies(inventory.scan())
    assert anomaly.set_code == "Powered Cube"
    assert anomaly.path == data_home / "external" / "Powered Cube"


def test_an_empty_set_directory_is_removable(data_home):
    """Advisory is the default for anything spells did not write, but an empty
    directory has no contents to judge."""
    (data_home / "external" / "TST").mkdir(parents=True)

    (anomaly,) = _empty_anomalies(inventory.scan())
    assert anomaly.is_repairable
    assert anomaly.remedy == Remedy.DELETE_PATH


def test_a_set_directory_with_files_is_not_empty(data_home):
    write_external(data_home, "TST", "TST_card.parquet")

    assert _empty_anomalies(inventory.scan()) == []


@pytest.mark.parametrize("store", ["external", "cache", "ratings", "deck_color"])
def test_every_per_set_store_is_checked(data_home, store):
    (data_home / store / "TST").mkdir(parents=True)

    (anomaly,) = _empty_anomalies(inventory.scan())
    assert store in anomaly.detail


def test_draft_logs_are_not_mistaken_for_set_directories(data_home):
    """`draft` holds log files directly, so its subdirectories are not sets."""
    (data_home / "draft" / "somedir").mkdir(parents=True)

    assert _empty_anomalies(inventory.scan()) == []


def test_repair_deletes_an_empty_set_directory(data_home):
    shell = data_home / "external" / "Powered Cube"
    shell.mkdir(parents=True)

    repairs = repair.plan(inventory.scan())
    assert [p for r in repairs for p in r.paths] == [shell]

    repair.apply(repairs)
    assert not shell.exists()


def test_sets_are_unioned_across_stores(data_home):
    write_external(data_home, "TST", "TST_card.parquet")
    write_snapshots(
        data_home,
        "ratings",
        "Cube+-+Powered",
        f"PremierDraft_all_any_{TimePeriod.ALL_TIME}_2026-07-14.json",
    )
    inv = inventory.scan()

    assert set(inv.sets) == {"TST", "Cube+-+Powered"}
    assert not inv.sets["Cube+-+Powered"].has_external
