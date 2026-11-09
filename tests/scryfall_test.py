import gzip
import json
from contextlib import closing

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


def stored(sql: str) -> list:
    with closing(scryfall.connect()) as db:
        return [row[0] for row in db.execute(sql)]


def test_sync_stores_each_printing_as_scryfall_sent_it(bulk):
    scryfall.sync()

    assert sorted(stored("select id from printings")) == ["1", "2", "3", "4"]
    raw = stored("select data from printings where id = '3'")
    assert json.loads(raw[0]) == PRINTINGS[0]
    assert stored("select id from printings where name = 'Opt' and set_code = 'tst'")
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
    assert stored("select id from printings") == ["4"]
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
    assert len(stored("select id from printings")) == len(PRINTINGS)
    assert sorted(p.name for p in (tmp_path / "scryfall").iterdir()) == ["cards.sqlite"]


def test_a_scryfall_outage_does_not_fail_add(monkeypatch):
    def outage(**kwargs):
        raise requests.ConnectionError("unreachable")

    monkeypatch.setattr(external.scryfall, "sync", outage)
    external._sync_scryfall()


def test_multi_face_cards_are_found_by_face_name(bulk, monkeypatch):
    PRINTINGS.append(
        {
            "id": "5",
            "name": "Fire // Ice",
            "set": "tst",
            "collector_number": "20",
            "lang": "en",
            "card_faces": [{"name": "Fire"}, {"name": "Ice"}],
        }
    )
    try:
        scryfall.sync()
        assert stored("select id from face_names where name = 'Ice'") == ["5"]
    finally:
        PRINTINGS.pop()
