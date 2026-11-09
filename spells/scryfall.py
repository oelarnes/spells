"""Local cache of Scryfall's bulk card data, the card source for spells and other apps.

One SQLite file holds every printing exactly as Scryfall sent it (`printings.data`),
next to indexed columns for lookup by id, name, face name, set, collector number and
language. Its `meta` table records which bulk file and language it came from and
Scryfall's `updated_at`, which doubles as the cache version.
"""

import datetime as dt
import gzip
import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing

from spells import cache, console, http
from spells.cache import DataDir

BULK_DATA_URL = "https://api.scryfall.com/bulk-data"
DEFAULT_CARDS = "default_cards"
ALL_CARDS = "all_cards"

# sync() keeps whatever bulk file and language the cache already holds
KEEP = object()

SCHEMA = """
create table printings (
    id text primary key,
    oracle_id text,
    name text not null,
    set_code text not null,
    collector_number text not null,
    lang text not null,
    data text not null
);
create table face_names (id text not null, name text not null);
create table meta (key text primary key, value text);
"""

INDEXES = """
create index printings_name on printings (name);
create index printings_set on printings (set_code, collector_number);
create index printings_lang on printings (lang);
create index printings_oracle on printings (oracle_id);
create index face_names_name on face_names (name);
"""


def scryfall_dir() -> str:
    return cache.data_dir_path(DataDir.SCRYFALL)


def db_path() -> str:
    return os.path.join(scryfall_dir(), "cards.sqlite")


def connect() -> sqlite3.Connection:
    """A read-only connection to the cache."""
    return sqlite3.connect(f"file:{db_path()}?mode=ro", uri=True)


def meta() -> dict | None:
    """What the cache holds, or None if it has never been synced."""
    if not os.path.isfile(db_path()):
        return None
    with closing(connect()) as db:
        stored = dict(db.execute("select key, value from meta"))
    return {**stored, "lang": stored.get("lang") or None}


def _bulk_entry(bulk_type: str) -> dict:
    listing = http.get_json(BULK_DATA_URL)["data"]
    return next(entry for entry in listing if entry["type"] == bulk_type)


def _printings(gz_path: str, lang: str | None) -> Iterator[tuple[dict, str]]:
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            card = json.loads(line)
            if lang is None or card["lang"] == lang:
                yield card, line.rstrip("\n")


def _build(gz_path: str, path: str, lang: str | None, meta_values: dict) -> int:
    if os.path.exists(path):
        os.remove(path)
    db = sqlite3.connect(path)
    try:
        db.executescript(SCHEMA)
        count = 0
        for card, line in _printings(gz_path, lang):
            db.execute(
                "insert into printings values (?, ?, ?, ?, ?, ?, ?)",
                (
                    card["id"],
                    card.get("oracle_id"),
                    card["name"],
                    card["set"],
                    card["collector_number"],
                    card["lang"],
                    line,
                ),
            )
            db.executemany(
                "insert into face_names values (?, ?)",
                [(card["id"], face["name"]) for face in card.get("card_faces", [])],
            )
            count += 1
        db.executescript(INDEXES)
        db.executemany(
            "insert into meta values (?, ?)",
            [
                (key, value)
                for key, value in {**meta_values, "printings": count}.items()
            ],
        )
        db.commit()
    finally:
        db.close()
    return count


def sync(lang: str | None | object = KEEP, force: bool = False) -> int:
    """Bring the cache up to date with Scryfall, downloading only when Scryfall has
    published newer bulk data than the cache holds.

    `lang=None` uses Scryfall's `default_cards` (English where an English printing
    exists). A language code such as "ja" keeps only that language's printings from
    `all_cards`. By default the cache keeps its current choice.
    """
    current = meta()
    if lang is KEEP:
        lang = current["lang"] if current else None
    bulk_type = DEFAULT_CARDS if lang is None else ALL_CARDS

    entry = _bulk_entry(bulk_type)
    if (
        not force
        and current is not None
        and (current["bulk_type"], current["lang"], current["updated_at"])
        == (bulk_type, lang, entry["updated_at"])
    ):
        console.info(f"Scryfall cache is current ({bulk_type}, {entry['updated_at']})")
        return 0

    os.makedirs(scryfall_dir(), exist_ok=True)
    gz_path = os.path.join(scryfall_dir(), f"{bulk_type}.jsonl.gz")
    part = f"{db_path()}.part"
    http.download(entry["jsonl_download_uri"], gz_path, description=bulk_type)
    try:
        count = _build(
            gz_path,
            part,
            lang,
            {
                "bulk_type": bulk_type,
                "lang": lang or "",
                "updated_at": entry["updated_at"],
                "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            },
        )
        if count == 0:
            raise ValueError(
                f"Scryfall's bulk data has no printings in language {lang!r}"
            )
        os.replace(part, db_path())
    finally:
        os.remove(gz_path)
        if os.path.exists(part):
            os.remove(part)

    console.info(
        f"Wrote {count} printings to {db_path()} "
        f"({bulk_type}{f', lang {lang}' if lang else ''}, {entry['updated_at']})"
    )
    return 0
