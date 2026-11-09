"""Local cache of Scryfall's bulk card data, the card source for spells and other apps.

One Parquet file holds every printing in Scryfall's own schema, nested fields included,
sorted by set and collector number. A `meta.json` beside it records which bulk file and
language it came from and Scryfall's `updated_at`, which doubles as the cache version.
"""

import datetime as dt
import gzip
import json
import os

import polars as pl

from spells import cache, console, http
from spells.cache import DataDir

BULK_DATA_URL = "https://api.scryfall.com/bulk-data"
DEFAULT_CARDS = "default_cards"
ALL_CARDS = "all_cards"

# sync() keeps whatever bulk file and language the cache already holds
KEEP = object()


def scryfall_dir() -> str:
    return cache.data_dir_path(DataDir.SCRYFALL)


def printings_path() -> str:
    return os.path.join(scryfall_dir(), "printings.parquet")


def _meta_path() -> str:
    return os.path.join(scryfall_dir(), "meta.json")


def meta() -> dict | None:
    """What the cache holds, or None if it has never been synced."""
    if not (os.path.isfile(_meta_path()) and os.path.isfile(printings_path())):
        return None
    with open(_meta_path()) as f:
        return json.load(f)


def _bulk_entry(bulk_type: str) -> dict:
    listing = http.get_json(BULK_DATA_URL)["data"]
    return next(entry for entry in listing if entry["type"] == bulk_type)


def _read_bulk(gz_path: str, lang: str | None) -> pl.DataFrame:
    with gzip.open(gz_path, "rb") as f:
        if lang is None:
            data = f.read()
        else:
            data = b"".join(line for line in f if json.loads(line)["lang"] == lang)
    if not data:
        raise ValueError(f"Scryfall's bulk data has no printings in language {lang!r}")
    return pl.read_ndjson(data, infer_schema_length=None)


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
    http.download(entry["jsonl_download_uri"], gz_path, description=bulk_type)
    try:
        printings = _read_bulk(gz_path, lang).sort(
            "set",
            pl.col("collector_number")
            .str.extract(r"^(\d+)")
            .cast(pl.Int64, strict=False),
            "collector_number",
        )
        part = f"{printings_path()}.part"
        printings.write_parquet(part, compression="zstd")
        os.replace(part, printings_path())
    finally:
        os.remove(gz_path)

    with open(_meta_path(), "w") as f:
        json.dump(
            {
                "bulk_type": bulk_type,
                "lang": lang,
                "updated_at": entry["updated_at"],
                "synced_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "printings": len(printings),
            },
            f,
            indent=2,
        )
    console.info(
        f"Wrote {len(printings)} printings to {printings_path()} "
        f"({bulk_type}{f', lang {lang}' if lang else ''}, {entry['updated_at']})"
    )
    return 0
