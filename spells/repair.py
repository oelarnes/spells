"""Turning anomalies into concrete deletions.

`inventory` describes what is wrong and never touches disk; everything that
removes a file lives here. Planning is separate from applying so that the
dry-run a caller sees is the exact list that would be deleted, rather than a
description of what a second, independent walk might find.

The one genuinely dangerous judgement is which snapshots are dead. A valid
`{time_period}_{as_of}` snapshot cannot be refetched — 17Lands resolves a time
period against its own current date, so a past window is gone once deleted.
Only files `inventory.is_valid_snapshot` rejects are ever removed.
"""

from dataclasses import dataclass
import os
from pathlib import Path
import shutil

from spells.inventory import Anomaly, Inventory, Remedy, is_valid_snapshot


@dataclass(frozen=True)
class Repair:
    anomaly: Anomaly
    paths: tuple[Path, ...]
    size: int

    @property
    def files(self) -> int:
        return len(self.paths)


@dataclass(frozen=True)
class Outcome:
    removed: int = 0
    freed: int = 0
    failures: tuple[tuple[Path, str], ...] = ()


def _legacy_snapshots(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return [
        entry
        for entry in sorted(directory.iterdir())
        if entry.is_file() and not is_valid_snapshot(entry.name)
    ]


def _tree_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def plan(inv: Inventory, set_code: str | None = None) -> list[Repair]:
    """Resolve repairable anomalies into the exact paths that would be deleted.

    Anomalies whose remedy is advisory are absent: a caller acting on this list
    should never have to re-check whether an entry is safe to remove.
    """
    repairs = []
    for anomaly in inv.all_anomalies:
        if not anomaly.is_repairable:
            continue
        if set_code is not None and anomaly.set_code != set_code:
            continue

        if anomaly.remedy == Remedy.PRUNE_LEGACY_SNAPSHOTS:
            paths = _legacy_snapshots(anomaly.path)
        elif anomaly.path.exists():
            paths = [anomaly.path]
        else:
            continue

        if paths:
            repairs.append(
                Repair(
                    anomaly=anomaly,
                    paths=tuple(paths),
                    size=sum(_tree_size(p) for p in paths),
                )
            )
    return repairs


def apply(repairs: list[Repair]) -> Outcome:
    """Delete every planned path, continuing past individual failures so one
    permission error cannot strand the rest of a cleanup half-done."""
    removed = freed = 0
    failures: list[tuple[Path, str]] = []

    for repair in repairs:
        for path in repair.paths:
            try:
                size = _tree_size(path)
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except OSError as e:
                failures.append((path, str(e)))
                continue
            removed += 1
            freed += size

    return Outcome(removed=removed, freed=freed, failures=tuple(failures))
