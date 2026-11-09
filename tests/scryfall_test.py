import gzip
import json

import polars as pl
import pytest
import requests

from spells import external, scryfall

PRINTINGS = [
    {"id": "3", "name": "Shock", "set": "tst", "collector_number": "10", "lang": "en"},
    {"id": "1", "name": "Opt", "set": "tst", "collector_number": "2", "lang": "en"},
    {"id": "2", "name": "Opt", "set": "abc", "collector_number": "5", "lang": "en"},
    {"id": "4", "name": "Opt", "set": "tst", "collector_number": "2", "lang": "ja"},
]


@pytest.fixture
def bulk(monkeypatch, tmp_path):
    """A fake Scryfall: the bulk-data listing plus downloads, recorded."""
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    state = {"updated_at": "2026-11-09T10:00:00", "downloads": []}

    def get_json(url):
        assert url == scryfall.BULK_DATA_URL
        return {
            "data": [
                {
                    "type": bulk_type,
                    "updated_at": state["updated_at"],
                    "jsonl_download_uri": f"https://data.example/{bulk_type}.jsonl.gz",
                }
                for bulk_type in (scryfall.DEFAULT_CARDS, scryfall.ALL_CARDS)
            ]
        }

    def download(url, path, **kwargs):
        state["downloads"].append(url)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for printing in PRINTINGS:
                f.write(json.dumps(printing) + "\n")
        return path

    monkeypatch.setattr(scryfall.http, "get_json", get_json)
    monkeypatch.setattr(scryfall.http, "download", download)
    return state


def test_sync_writes_printings_sorted_by_set_and_collector_number(bulk):
    scryfall.sync()

    printings = pl.read_parquet(scryfall.printings_path())
    assert printings["id"].to_list() == ["2", "1", "4", "3"]
    assert scryfall.meta()["bulk_type"] == scryfall.DEFAULT_CARDS
    assert scryfall.meta()["updated_at"] == bulk["updated_at"]


def test_a_current_cache_is_not_downloaded_again(bulk):
    scryfall.sync()
    scryfall.sync()
    assert len(bulk["downloads"]) == 1


def test_newer_bulk_data_is_downloaded(bulk):
    scryfall.sync()
    bulk["updated_at"] = "2026-11-10T10:00:00"
    scryfall.sync()
    assert len(bulk["downloads"]) == 2
    assert scryfall.meta()["updated_at"] == "2026-11-10T10:00:00"


def test_a_language_comes_from_all_cards(bulk):
    scryfall.sync(lang="ja")

    assert bulk["downloads"] == ["https://data.example/all_cards.jsonl.gz"]
    assert pl.read_parquet(scryfall.printings_path())["id"].to_list() == ["4"]
    assert scryfall.meta()["lang"] == "ja"


def test_sync_keeps_the_cached_language_unless_told_otherwise(bulk):
    scryfall.sync(lang="ja")
    bulk["updated_at"] = "2026-11-10T10:00:00"
    scryfall.sync()
    assert scryfall.meta()["lang"] == "ja"

    scryfall.sync(lang=None)
    assert scryfall.meta()["bulk_type"] == scryfall.DEFAULT_CARDS


def test_a_language_with_no_printings_leaves_the_cache_alone(bulk, tmp_path):
    scryfall.sync()
    with pytest.raises(ValueError, match="xx"):
        scryfall.sync(lang="xx")

    assert scryfall.meta()["bulk_type"] == scryfall.DEFAULT_CARDS
    assert len(pl.read_parquet(scryfall.printings_path())) == len(PRINTINGS)
    assert not list((tmp_path / "scryfall").glob("*.gz"))


def test_a_scryfall_outage_does_not_fail_add(monkeypatch):
    def outage(**kwargs):
        raise requests.ConnectionError("unreachable")

    monkeypatch.setattr(external.scryfall, "sync", outage)
    external._sync_scryfall()
