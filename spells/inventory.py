"""What is on disk, and what looks wrong with it.

A single scan of the data home feeds every reporting command: `status`,
`snapshots`, and `doctor` are renderings of the same `Inventory`, not
independent directory walks. Nothing here writes, deletes, or reaches the
network.
"""

from dataclasses import dataclass, field
from enum import StrEnum
import os
from pathlib import Path
import re

from spells import cache
from spells.cache import DataDir
from spells.enums import EventType, TimePeriod

# {set}_{event_type}_{view}.parquet, and the set-level {set}_card.parquet
DATASET_VIEWS = ("draft", "game", "context")

# 17lands snapshots are {query stub}_{as_of}.json. The pre-0.14 scheme encoded a
# {start}_{end} date range instead of a time period, and nothing can read those.
_SNAPSHOT_RE = re.compile(r"^(?P<stub>.+)_(?P<as_of>\d{4}-\d{2}-\d{2})\.json$")

# created by log.setup_logging, not a data store
_LOG_DIR = ".logs"

KNOWN_DIRS = frozenset({*(d.value for d in DataDir), "ad_hoc", _LOG_DIR})


class AnomalyKind(StrEnum):
    STRAY_DOWNLOAD = "stray-download"
    LEGACY_CONTEXT = "legacy-context"
    LEGACY_SNAPSHOT = "legacy-snapshot"
    ORPHAN_CACHE = "orphan-cache"
    ORPHAN_DIR = "orphan-dir"
    UNKNOWN_FILE = "unknown-file"
    INCOMPLETE_SET = "incomplete-set"


@dataclass(frozen=True)
class Anomaly:
    kind: AnomalyKind
    path: Path
    detail: str
    set_code: str | None = None
    size: int = 0


@dataclass(frozen=True)
class FileInfo:
    path: Path
    size: int
    mtime: float = 0.0


@dataclass(frozen=True)
class EventFiles:
    event_type: EventType
    draft: FileInfo | None = None
    game: FileInfo | None = None
    context: FileInfo | None = None

    @property
    def missing(self) -> tuple[str, ...]:
        # summon skips set context for multi-pick formats, so pick-two sets are
        # complete without one.
        expected = ("draft", "game") if self.is_pick_two else DATASET_VIEWS
        return tuple(v for v in expected if getattr(self, v) is None)

    @property
    def is_pick_two(self) -> bool:
        return self.event_type == EventType.PICK_TWO

    @property
    def is_complete(self) -> bool:
        return not self.missing


@dataclass(frozen=True)
class SnapshotStats:
    valid: int = 0
    valid_bytes: int = 0
    legacy: int = 0
    legacy_bytes: int = 0

    @property
    def total(self) -> int:
        return self.valid + self.legacy

    @property
    def total_bytes(self) -> int:
        return self.valid_bytes + self.legacy_bytes


@dataclass
class SetInventory:
    set_code: str
    events: dict[EventType, EventFiles] = field(default_factory=dict)
    card_file: FileInfo | None = None
    external_bytes: int = 0
    cache_files: int = 0
    cache_bytes: int = 0
    ratings: SnapshotStats = field(default_factory=SnapshotStats)
    deck_color: SnapshotStats = field(default_factory=SnapshotStats)
    anomalies: list[Anomaly] = field(default_factory=list)

    @property
    def has_external(self) -> bool:
        return bool(self.events) or self.card_file is not None

    @property
    def snapshot_bytes(self) -> int:
        return self.ratings.total_bytes + self.deck_color.total_bytes

    @property
    def total_bytes(self) -> int:
        return self.external_bytes + self.cache_bytes + self.snapshot_bytes


@dataclass
class Inventory:
    data_home: Path
    sets: dict[str, SetInventory] = field(default_factory=dict)
    draft_logs: int = 0
    draft_log_bytes: int = 0
    anomalies: list[Anomaly] = field(default_factory=list)

    @property
    def all_anomalies(self) -> list[Anomaly]:
        return self.anomalies + [a for s in self.sets.values() for a in s.anomalies]

    @property
    def total_bytes(self) -> int:
        return sum(s.total_bytes for s in self.sets.values()) + self.draft_log_bytes


def is_valid_snapshot(filename: str) -> bool:
    """A snapshot is current-scheme iff it ends in `_{time_period}_{as_of}.json`.

    Parsing from the right avoids having to enumerate event types, cohorts, and
    deck colors, and avoids assuming anything about set code punctuation
    (`Cube+-+Powered` is a real set directory).
    """
    match = _SNAPSHOT_RE.match(filename)
    if match is None:
        return False
    stub = match.group("stub")
    return any(stub.endswith(f"_{period}") for period in TimePeriod)


def _file_info(entry: os.DirEntry) -> FileInfo:
    stat = entry.stat()
    return FileInfo(path=Path(entry.path), size=stat.st_size, mtime=stat.st_mtime)


def _scan_external_set(inv: SetInventory, set_dir: Path) -> None:
    events: dict[EventType, dict[str, FileInfo]] = {}

    with os.scandir(set_dir) as entries:
        for entry in entries:
            info = _file_info(entry)
            inv.external_bytes += info.size

            if not entry.name.endswith(".parquet"):
                inv.anomalies.append(
                    Anomaly(
                        AnomalyKind.STRAY_DOWNLOAD,
                        info.path,
                        "leftover download artifact, not a dataset",
                        inv.set_code,
                        info.size,
                    )
                )
                continue

            stem = entry.name[: -len(".parquet")]
            if not stem.startswith(f"{inv.set_code}_"):
                inv.anomalies.append(
                    Anomaly(
                        AnomalyKind.UNKNOWN_FILE,
                        info.path,
                        f"does not belong to set {inv.set_code}",
                        inv.set_code,
                        info.size,
                    )
                )
                continue

            rest = stem[len(inv.set_code) + 1 :]
            if rest == "card":
                inv.card_file = info
                continue

            event_type, _, view = rest.rpartition("_")
            if view in DATASET_VIEWS and event_type in tuple(EventType):
                events.setdefault(EventType(event_type), {})[view] = info
            elif rest in DATASET_VIEWS:
                inv.anomalies.append(
                    Anomaly(
                        AnomalyKind.LEGACY_CONTEXT,
                        info.path,
                        f"pre-event_type {rest} file, superseded by "
                        f"{inv.set_code}_<event_type>_{rest}.parquet",
                        inv.set_code,
                        info.size,
                    )
                )
            else:
                inv.anomalies.append(
                    Anomaly(
                        AnomalyKind.UNKNOWN_FILE,
                        info.path,
                        "unrecognized dataset name",
                        inv.set_code,
                        info.size,
                    )
                )

    for event_type, views in sorted(events.items()):
        inv.events[event_type] = EventFiles(event_type=event_type, **views)

    for event_files in inv.events.values():
        if missing := event_files.missing:
            inv.anomalies.append(
                Anomaly(
                    AnomalyKind.INCOMPLETE_SET,
                    set_dir,
                    f"{event_files.event_type} is missing {', '.join(missing)}",
                    inv.set_code,
                )
            )


def _scan_snapshots(inv: SetInventory, set_dir: Path, store: DataDir) -> SnapshotStats:
    valid = valid_bytes = legacy = legacy_bytes = 0

    with os.scandir(set_dir) as entries:
        for entry in entries:
            info = _file_info(entry)
            if is_valid_snapshot(entry.name):
                valid += 1
                valid_bytes += info.size
            else:
                legacy += 1
                legacy_bytes += info.size

    # one anomaly per store, not per file — these run to thousands, and the
    # paths are cheap to re-derive when something actually acts on them
    if legacy:
        inv.anomalies.append(
            Anomaly(
                AnomalyKind.LEGACY_SNAPSHOT,
                set_dir,
                f"{legacy} pre-0.14 {store} snapshots, unreadable by any "
                "current code path",
                inv.set_code,
                legacy_bytes,
            )
        )

    return SnapshotStats(valid, valid_bytes, legacy, legacy_bytes)


def _scan_cache_set(inv: SetInventory, set_dir: Path) -> None:
    with os.scandir(set_dir) as entries:
        for entry in entries:
            info = _file_info(entry)
            if entry.name.endswith(".parquet"):
                inv.cache_files += 1
                inv.cache_bytes += info.size
            else:
                inv.anomalies.append(
                    Anomaly(
                        AnomalyKind.UNKNOWN_FILE,
                        info.path,
                        "not a cache file",
                        inv.set_code,
                        info.size,
                    )
                )


def _set_dirs(store: DataDir) -> list[Path]:
    store_path = Path(cache.data_dir_path(store))
    if not store_path.is_dir():
        return []
    return sorted(p for p in store_path.iterdir() if p.is_dir())


def scan() -> Inventory:
    """Walk the data home once and classify everything in it."""
    home = Path(cache.data_home())
    inventory = Inventory(data_home=home)

    def set_inv(set_code: str) -> SetInventory:
        return inventory.sets.setdefault(set_code, SetInventory(set_code=set_code))

    for set_dir in _set_dirs(DataDir.EXTERNAL):
        _scan_external_set(set_inv(set_dir.name), set_dir)

    for set_dir in _set_dirs(DataDir.CACHE):
        _scan_cache_set(set_inv(set_dir.name), set_dir)

    for store, attr in (
        (DataDir.RATINGS, "ratings"),
        (DataDir.DECK_COLOR, "deck_color"),
    ):
        for set_dir in _set_dirs(store):
            inv = set_inv(set_dir.name)
            setattr(inv, attr, _scan_snapshots(inv, set_dir, store))

    for inv in inventory.sets.values():
        if inv.cache_files and not inv.has_external:
            inv.anomalies.append(
                Anomaly(
                    AnomalyKind.ORPHAN_CACHE,
                    Path(cache.cache_dir_for_set(inv.set_code)),
                    "derived cache with no external data to rebuild it from",
                    inv.set_code,
                    inv.cache_bytes,
                )
            )

    draft_dir = Path(cache.data_dir_path(DataDir.DRAFT))
    if draft_dir.is_dir():
        logs = [p for p in draft_dir.iterdir() if p.is_file()]
        inventory.draft_logs = len(logs)
        inventory.draft_log_bytes = sum(p.stat().st_size for p in logs)

    if home.is_dir():
        for entry in sorted(home.iterdir()):
            if entry.is_dir() and entry.name not in KNOWN_DIRS:
                size = sum(p.stat().st_size for p in entry.rglob("*") if p.is_file())
                inventory.anomalies.append(
                    Anomaly(
                        AnomalyKind.ORPHAN_DIR,
                        entry,
                        "not a store spells writes to",
                        size=size,
                    )
                )

    inventory.sets = dict(sorted(inventory.sets.items()))
    return inventory
