"""What 17Lands publishes, and whether our copy of it is current.

The Public Datasets page is a Prismic CMS document, not a 17Lands API. That
document is public and unauthenticated, and its hyperlink spans carry the
canonical S3 URLs — so it is the sanctioned way to discover what exists, rather
than guessing filenames from a template. Two things follow from using it:

- It *enumerates*. S3 objects are `public-read` but the bucket is not listable,
  so nothing else can answer "what sets could I add?" It also distinguishes
  "not published" from "published and you don't have it" — old expansions have
  no draft data at all, and requesting one returns 403.
- It reports formats spells does not model (Sealed, TradSealed, QuickDraft).
  Those are surfaced as unsupported rather than dropped, so `check` never
  silently hides published data.

The catalog's own `last_updated` field is hand-maintained and lags, so it is
display-only. Freshness decisions come from a HEAD on the URL the catalog gives
us. Nothing here writes to the data home.
"""

from collections.abc import Collection, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from enum import StrEnum
import json
import urllib.error
import urllib.parse
import urllib.request

from spells.enums import EventType, View

PRISMIC_API = "https://17lands.cdn.prismic.io/api/v2"
PRISMIC_QUERY = '[[at(document.type,"public-data")]]'

# Used when the catalog is unreachable. The catalog is authoritative when it
# answers; these only keep `add` working offline-ish, and cannot enumerate.
DATASET_TEMPLATE = "{dataset_type}_data_public.{set_code}.{event_type}.csv.gz"
RESOURCE_TEMPLATE = (
    "https://17lands-public.s3.amazonaws.com/analysis_data/{dataset_type}_data/"
)

USER_AGENT = "spells-mtg"
TIMEOUT = 30


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


def _text(field) -> str:
    return "".join(block.get("text", "") for block in field or []).strip()


def _link(field) -> str | None:
    """A dataset that is not published renders as a literal "-" with no span."""
    for block in field or []:
        for span in block.get("spans", []):
            if span.get("type") == "hyperlink":
                if url := span.get("data", {}).get("url"):
                    return url
    return None


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


def parse(document: dict) -> Catalog:
    """Build a Catalog from a Prismic `public-data` document.

    Kept separate from fetching so it can be tested against a saved response.
    """
    datasets = []
    for entry in document.get("data", {}).get("datasets", []):
        expansion = _text(entry.get("expansion"))
        format_name = _text(entry.get("format"))
        if not expansion or not format_name:
            continue
        datasets.append(
            Dataset(
                expansion=expansion,
                format_name=format_name,
                event_type=_event_type(format_name),
                last_updated=_parse_date(_text(entry.get("last_updated"))),
                draft_url=_link(entry.get("draft_data")),
                game_url=_link(entry.get("game_data")),
            )
        )
    return Catalog(datasets=tuple(datasets))


def _get_json(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def _fetch_document() -> dict:
    ref = _get_json(PRISMIC_API)["refs"][0]["ref"]
    query = urllib.parse.urlencode({"ref": ref, "q": PRISMIC_QUERY, "pageSize": 100})
    results = _get_json(f"{PRISMIC_API}/documents/search?{query}")["results"]
    if not results:
        raise ValueError("no public-data document in the 17Lands catalog")
    return results[0]


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
        _cached = parse(_fetch_document())
    except (
        urllib.error.URLError,
        TimeoutError,
        KeyError,
        ValueError,
        json.JSONDecodeError,
    ):
        _cached = Catalog(datasets=(), is_fallback=True)
    return _cached


def fallback_url(expansion: str, view: View, event_type: EventType) -> str:
    return RESOURCE_TEMPLATE.format(dataset_type=view) + DATASET_TEMPLATE.format(
        dataset_type=view, set_code=expansion, event_type=event_type
    )


def head(url: str) -> RemoteFile | None:
    """Remote metadata, or None if the object is missing or unreachable.

    Unpublished datasets are `public-read` only for objects that exist, so a
    stale or guessed URL comes back 403 rather than 404.
    """
    request = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": USER_AGENT}
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            headers = response.headers
    except (urllib.error.URLError, TimeoutError):
        return None

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


def resolve(
    targets: list[Target], cat: Catalog, max_workers: int = 8
) -> list[CheckRow]:
    """Classify each target against the catalog, doing the HEADs concurrently.

    Only datasets we already hold are HEADed: the catalog alone is enough to
    say a missing one is published, and skipping those keeps `check` to one
    request per file actually on disk.
    """
    rows: list[CheckRow] = []
    to_head: list[tuple[Target, str]] = []

    for target in targets:
        dataset = cat.get(target.expansion, target.event_type)
        url = dataset.url(target.view) if dataset else None
        if url is None:
            freshness = Freshness.UNKNOWN if cat.is_fallback else Freshness.UNPUBLISHED
            rows.append(CheckRow(target, freshness))
        elif target.local_mtime is None:
            rows.append(CheckRow(target, Freshness.ABSENT, RemoteFile(url=url)))
        else:
            to_head.append((target, url))

    if to_head:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            remotes = pool.map(head, [url for _, url in to_head])
        for (target, url), remote in zip(to_head, remotes):
            rows.append(CheckRow(target, compare(target.local_mtime, remote), remote))

    order = {t: i for i, t in enumerate(targets)}
    return sorted(rows, key=lambda row: order[row.target])


def in_release_order(cat: Catalog, expansions: Iterable[str]) -> list[str]:
    """Newest expansion first, by catalog array position.

    17Lands authors the array in release order, which is the order a drafter
    thinks in; alphabetical ordering puts whatever they are currently drafting
    at an arbitrary place in the list. Expansions the catalog no longer lists
    sort to the end alphabetically rather than vanishing.
    """
    order = {expansion: i for i, expansion in enumerate(cat.expansions)}
    known = sorted((e for e in expansions if e in order), key=lambda e: -order[e])
    return known + sorted(e for e in expansions if e not in order)


def unadded(cat: Catalog, held: Collection[str]) -> list[str]:
    """Published expansions the caller does not have, newest first.

    Anchored to the oldest expansion they already hold, so sets that predate
    their collection are not reported as things they are missing. Without that
    anchor the answer would be every expansion 17Lands has ever published,
    which is noise rather than news.

    Ordering comes from the catalog array, which 17Lands authors in release
    order, rather than from `last_updated` — regenerating an old set's files
    bumps that date without making the set new.
    """
    order = cat.expansions
    position = {expansion: i for i, expansion in enumerate(order)}
    anchor = min((position[e] for e in held if e in position), default=0)

    return in_release_order(
        cat,
        (
            expansion
            for expansion in order[anchor:]
            if expansion not in held and cat.is_addable(expansion)
        ),
    )


def compare(local_mtime: float | None, remote: RemoteFile | None) -> Freshness:
    """Classify one local file against its published counterpart.

    Compares against the local parquet's mtime, which is when we wrote it — a
    conversion always postdates the download, so a local file newer than the
    remote object is genuinely current.
    """
    if remote is None:
        return Freshness.UNPUBLISHED if local_mtime is None else Freshness.UNKNOWN
    if local_mtime is None:
        return Freshness.ABSENT
    if remote.last_modified is None:
        return Freshness.UNKNOWN
    local = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
    return Freshness.STALE if local < remote.last_modified else Freshness.CURRENT
