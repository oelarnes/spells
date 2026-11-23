"""What 17Lands publishes, and whether our copy of it is current.

17Lands lists its public datasets at `/api/public_datasets`, one entry per
expansion and event type, newest expansion first, with each file's URL and the
date it was last published. Two things follow from using it:

- It *enumerates*. The bucket is not listable, so nothing else can answer
  "what sets could I add?" It also distinguishes "not published" from
  "published and you don't have it": old expansions have no draft data at all.
- It reports formats spells does not model (Sealed, TradSealed, QuickDraft).
  Those are surfaced as unsupported rather than dropped, so `check` never
  silently hides published data.

Freshness compares a local file's write time with the entry's `last_updated`,
which 17Lands sets each time it publishes a file. Nothing here writes to the
data home.
"""

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from email.utils import parsedate_to_datetime
from enum import StrEnum
import re

import requests

from spells import http
from spells.enums import EventType, View

PUBLIC_DATASETS_API = "https://www.17lands.com/api/public_datasets"

# Where `add` downloads from, and the fallback when the catalog is unreachable.
DATASET_TEMPLATE = "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
RESOURCE_TEMPLATE = "https://data.17landscapes.com/analysis_data/{dataset_type}_data/"


class Freshness(StrEnum):
    CURRENT = "current"
    STALE = "stale"
    ABSENT = "absent"
    UNPUBLISHED = "unpublished"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RemoteFile:
    url: str
    last_modified: datetime | None = None
    etag: str | None = None
    size: int | None = None


@dataclass(frozen=True)
class Dataset:
    expansion: str
    format_name: str
    event_type: EventType | None
    last_updated: date | None
    draft_url: str | None
    game_url: str | None

    @property
    def is_supported(self) -> bool:
        return self.event_type is not None

    def url(self, view: View) -> str | None:
        return {View.DRAFT: self.draft_url, View.GAME: self.game_url}.get(view)


@dataclass(frozen=True)
class Catalog:
    datasets: tuple[Dataset, ...]
    is_fallback: bool = False

    @property
    def expansions(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(d.expansion for d in self.datasets))

    def for_expansion(self, expansion: str) -> tuple[Dataset, ...]:
        return tuple(d for d in self.datasets if d.expansion == expansion)

    def get(self, expansion: str, event_type: EventType) -> Dataset | None:
        for dataset in self.datasets:
            if dataset.expansion == expansion and dataset.event_type == event_type:
                return dataset
        return None

    def is_addable(self, expansion: str) -> bool:
        """Whether any event type spells models has draft data published."""
        return any(
            d.is_supported and d.draft_url for d in self.for_expansion(expansion)
        )

    def updated(self, expansion: str) -> date | None:
        dates = [
            d.last_updated for d in self.for_expansion(expansion) if d.last_updated
        ]
        return max(dates) if dates else None


_URL_EXPANSION = re.compile(r"_data_public\.(?P<expansion>.+)\.[^.]+\.csv\.gz$")


def _expansion(label: str, *urls: str | None) -> str:
    """The expansion as it appears in the filename, not as it is displayed.

    The catalog's own field is prose ("Powered Cube" is published as
    `Cube_-_Powered`), and the filename is what has to match both the download
    URL and the directory it lands in.
    """
    for url in urls:
        if url and (match := _URL_EXPANSION.search(url)):
            return match.group("expansion")
    return label


def _event_type(format_name: str) -> EventType | None:
    try:
        return EventType(format_name)
    except ValueError:
        return None


def _parse_date(text: str) -> date | None:
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse(entries: list[dict]) -> Catalog:
    """Build a Catalog from the `/api/public_datasets` response.

    Kept separate from fetching so it can be tested against a saved response.
    """
    datasets = []
    for entry in entries:
        label = entry.get("expansion")
        event_type = entry.get("event_type")
        if not label or not event_type:
            continue
        urls = entry.get("urls") or {}
        draft_url = urls.get("draft")
        game_url = urls.get("game")
        datasets.append(
            Dataset(
                expansion=_expansion(label, draft_url, game_url),
                format_name=event_type,
                event_type=_event_type(event_type),
                last_updated=_parse_date(entry.get("last_updated") or ""),
                draft_url=draft_url,
                game_url=game_url,
            )
        )
    return Catalog(datasets=tuple(datasets))


_cached: Catalog | None = None


def fetch(refresh: bool = False) -> Catalog:
    """The published catalog, memoized for the process lifetime.

    Returns an empty fallback Catalog if 17Lands is unreachable; callers should
    check `is_fallback` before reporting "not published".
    """
    global _cached
    if _cached is not None and not refresh:
        return _cached

    try:
        _cached = parse(http.get_json(PUBLIC_DATASETS_API))
    except (requests.RequestException, AttributeError, TypeError, ValueError):
        _cached = Catalog(datasets=(), is_fallback=True)
    return _cached


def fallback_url(expansion: str, view: View, event_type: EventType) -> str:
    return RESOURCE_TEMPLATE.format(dataset_type=view) + DATASET_TEMPLATE.format(
        dataset_type=view, set_code=expansion, event_type=event_type
    )


def head(url: str) -> RemoteFile | None:
    """Remote metadata, or None if the object is missing or unreachable."""
    response = http.head(url)
    if response is None:
        return None
    headers = response.headers

    last_modified = None
    if raw := headers.get("Last-Modified"):
        try:
            last_modified = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            last_modified = None

    size = None
    if raw := headers.get("Content-Length"):
        try:
            size = int(raw)
        except ValueError:
            size = None

    return RemoteFile(
        url=url, last_modified=last_modified, etag=headers.get("ETag"), size=size
    )


@dataclass(frozen=True)
class Target:
    expansion: str
    event_type: EventType
    view: View
    local_mtime: float | None = None


@dataclass(frozen=True)
class CheckRow:
    target: Target
    freshness: Freshness
    remote: RemoteFile | None = None

    @property
    def needs_download(self) -> bool:
        return self.freshness in (Freshness.STALE, Freshness.ABSENT)


def resolve(targets: list[Target], cat: Catalog) -> list[CheckRow]:
    """Classify each target against the catalog's publication dates."""
    rows = []
    for target in targets:
        dataset = cat.get(target.expansion, target.event_type)
        url = dataset.url(target.view) if dataset else None
        if dataset is None or url is None:
            freshness = Freshness.UNKNOWN if cat.is_fallback else Freshness.UNPUBLISHED
            rows.append(CheckRow(target, freshness))
            continue
        published = (
            datetime.combine(dataset.last_updated, time(), tzinfo=timezone.utc)
            if dataset.last_updated
            else None
        )
        remote = RemoteFile(url=url, last_modified=published)
        rows.append(CheckRow(target, compare(target.local_mtime, remote), remote))
    return rows


def in_release_order(cat: Catalog, expansions: Iterable[str]) -> list[str]:
    """Newest expansion first, by catalog position.

    17Lands lists expansions newest first, which is the order a drafter thinks
    in; alphabetical ordering puts whatever they are currently drafting at an
    arbitrary place in the list. Expansions the catalog no longer lists sort to
    the end alphabetically rather than vanishing.
    """
    order = {expansion: i for i, expansion in enumerate(cat.expansions)}
    known = sorted((e for e in expansions if e in order), key=lambda e: order[e])
    return known + sorted(e for e in expansions if e not in order)


def unadded(cat: Catalog, held: Collection[str]) -> list[str]:
    """Published expansions the caller does not have, newest first.

    Anchored to the oldest expansion they already hold, so sets that predate
    their collection are not reported as things they are missing. Without that
    anchor the answer would be every expansion 17Lands has ever published,
    which is noise rather than news.

    Ordering comes from the catalog's position, which 17Lands keeps in release
    order, rather than from `last_updated`: regenerating an old set's files
    bumps that date without making the set new.
    """
    order = cat.expansions
    position = {expansion: i for i, expansion in enumerate(order)}
    oldest_held = max(
        (position[e] for e in held if e in position), default=len(order) - 1
    )

    return [
        expansion
        for expansion in order[: oldest_held + 1]
        if expansion not in held and cat.is_addable(expansion)
    ]


def compare(local_mtime: float | None, remote: RemoteFile | None) -> Freshness:
    """Classify one local file against its published counterpart.

    Compares the local parquet's mtime, which is when we wrote it, with when
    17Lands published the file. A conversion always postdates the download, so
    a local file written after publication is genuinely current.
    """
    if remote is None:
        return Freshness.UNPUBLISHED if local_mtime is None else Freshness.UNKNOWN
    if local_mtime is None:
        return Freshness.ABSENT
    if remote.last_modified is None:
        return Freshness.UNKNOWN
    local = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
    return Freshness.STALE if local < remote.last_modified else Freshness.CURRENT
