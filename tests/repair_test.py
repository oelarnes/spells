"""
Tests for the layer that actually deletes.

The prune-safety test is the point of this file. A valid `{time_period}_{as_of}`
snapshot cannot be refetched — 17Lands resolves a time period against its own
current date — so a false positive in the legacy classifier destroys data
permanently. Everything else here is comparatively cheap to get wrong.
"""

import json

import pytest

from spells import inventory, repair
from spells.enums import TimePeriod
from spells.inventory import AnomalyKind, Remedy

VALID_SHAPES = [f"PremierDraft_all_any_{p}_2026-07-14.json" for p in TimePeriod] + [
    f"PremierDraft_top_{p}_2026-07-14.json" for p in TimePeriod
]

LEGACY_SHAPES = [
    "PremierDraft_all_any_2026-06-23_2026-06-24.json",
    "PremierDraft_top_2024-09-07_2024-09-07.json",
    "PremierDraft_top_BRG_2024-10-08_2025-10-14 (1).json",
    "PremierDraft_all_any_ALL_TIME.json",
]


@pytest.fixture
def data_home(tmp_path, monkeypatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    return tmp_path


def write_snapshots(home, store, set_code, *names):
    d = home / store / set_code
    d.mkdir(parents=True, exist_ok=True)
    for name in names:
        (d / name).write_text(json.dumps([{"name": "Card A"}]))
    return d


# ---------------------------------------------------------------------------
# Prune safety
# ---------------------------------------------------------------------------


def test_plan_never_schedules_a_valid_snapshot_for_deletion(data_home):
    """Across every TimePeriod, in both snapshot shapes, mixed with legacy
    files in the same directory."""
    write_snapshots(data_home, "ratings", "TST", *VALID_SHAPES, *LEGACY_SHAPES)

    planned = {p.name for r in repair.plan(inventory.scan()) for p in r.paths}

    assert planned == set(LEGACY_SHAPES)
    assert not planned & set(VALID_SHAPES)


def test_plan_is_safe_for_a_set_code_containing_punctuation(data_home):
    """`Cube+-+Powered` is a real directory name."""
    write_snapshots(
        data_home, "ratings", "Cube+-+Powered", *VALID_SHAPES, *LEGACY_SHAPES
    )

    planned = {p.name for r in repair.plan(inventory.scan()) for p in r.paths}
    assert planned == set(LEGACY_SHAPES)


def test_a_directory_of_only_valid_snapshots_yields_no_repair(data_home):
    write_snapshots(data_home, "ratings", "TST", *VALID_SHAPES)
    assert repair.plan(inventory.scan()) == []


def test_apply_leaves_valid_snapshots_on_disk(data_home):
    d = write_snapshots(data_home, "ratings", "TST", *VALID_SHAPES, *LEGACY_SHAPES)

    repair.apply(repair.plan(inventory.scan()))

    survivors = {p.name for p in d.iterdir()}
    assert survivors == set(VALID_SHAPES)


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def test_advisory_anomalies_are_never_planned(data_home):
    """An unrecognized directory is reported by `status` but must not be
    scheduled for deletion — spells did not write it and cannot judge it."""
    (data_home / "mystery").mkdir(parents=True)
    (data_home / "mystery" / "notes.txt").write_bytes(b"x" * 10)

    inv = inventory.scan()
    assert any(a.kind == AnomalyKind.ORPHAN_DIR for a in inv.all_anomalies)
    assert repair.plan(inv) == []


def test_advisory_anomalies_are_marked_as_such():
    assert AnomalyKind.ORPHAN_DIR.__class__ is AnomalyKind
    for kind in AnomalyKind:
        assert inventory.REMEDIES[kind] in tuple(Remedy)


def test_plan_reports_the_paths_it_would_delete(data_home):
    write_snapshots(data_home, "ratings", "TST", *LEGACY_SHAPES)

    (r,) = repair.plan(inventory.scan())
    assert r.files == len(LEGACY_SHAPES)
    assert r.size > 0
    assert all(p.exists() for p in r.paths)


def test_plan_can_be_limited_to_one_set(data_home):
    write_snapshots(data_home, "ratings", "TST", *LEGACY_SHAPES)
    write_snapshots(data_home, "ratings", "TS2", *LEGACY_SHAPES)

    planned = repair.plan(inventory.scan(), set_code="TST")
    assert {r.anomaly.set_code for r in planned} == {"TST"}


def test_apply_removes_files_and_reports_what_it_freed(data_home):
    write_snapshots(data_home, "ratings", "TST", *LEGACY_SHAPES)
    repairs = repair.plan(inventory.scan())

    outcome = repair.apply(repairs)

    assert outcome.removed == len(LEGACY_SHAPES)
    assert outcome.freed > 0
    assert not outcome.failures
    assert not any(p.exists() for r in repairs for p in r.paths)


def test_apply_continues_past_a_failure(data_home, monkeypatch):
    """One unremovable file must not strand the rest of a cleanup half-done."""
    write_snapshots(data_home, "ratings", "TST", *LEGACY_SHAPES)
    repairs = repair.plan(inventory.scan())
    doomed = repairs[0].paths[0]

    real_remove = repair.os.remove

    def flaky(path):
        if str(path) == str(doomed):
            raise OSError("permission denied")
        return real_remove(path)

    monkeypatch.setattr(repair.os, "remove", flaky)
    outcome = repair.apply(repairs)

    assert outcome.removed == len(LEGACY_SHAPES) - 1
    assert [p for p, _ in outcome.failures] == [doomed]
    assert doomed.exists()


def test_planning_does_not_delete_anything(data_home):
    d = write_snapshots(data_home, "ratings", "TST", *LEGACY_SHAPES)
    repair.plan(inventory.scan())
    assert len(list(d.iterdir())) == len(LEGACY_SHAPES)
