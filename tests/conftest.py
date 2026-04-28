import datetime
import json
import os

import pytest

import spells.cache as cache
from spells.draft_data import get_names


FAKE_SET = "TST"
FAKE_START = datetime.date(2026, 1, 1)
FAKE_END = datetime.date(2026, 1, 2)

FAKE_CARDS = [
    {
        "name": "Aether Sprite",
        "color": "U",
        "rarity": "common",
        "url": "https://example.com/aether_sprite.jpg",
        "seen_count": 1000,
        "avg_seen": 3.5,
        "pick_count": 800,
        "avg_pick": 2.1,
        "game_count": 400,
        "win_rate": 0.55,
        "pool_count": 600,
        "opening_hand_game_count": 80,
        "opening_hand_win_rate": 0.60,
        "drawn_game_count": 120,
        "drawn_win_rate": 0.53,
        "ever_drawn_game_count": 200,
        "ever_drawn_win_rate": 0.56,
        "never_drawn_game_count": 200,
        "never_drawn_win_rate": 0.48,
    },
    {
        "name": "Blazing Howl",
        "color": "R",
        "rarity": "uncommon",
        "url": "https://example.com/blazing_howl.jpg",
        "seen_count": 900,
        "avg_seen": 4.0,
        "pick_count": 500,
        "avg_pick": 3.2,
        "game_count": 300,
        "win_rate": 0.52,
        "pool_count": 450,
        "opening_hand_game_count": 60,
        "opening_hand_win_rate": 0.57,
        "drawn_game_count": 90,
        "drawn_win_rate": 0.50,
        "ever_drawn_game_count": 150,
        "ever_drawn_win_rate": 0.53,
        "never_drawn_game_count": 150,
        "never_drawn_win_rate": 0.51,
    },
    {
        "name": "Crystal Warden",
        "color": "W",
        "rarity": "rare",
        "url": "https://example.com/crystal_warden.jpg",
        "seen_count": 700,
        "avg_seen": 5.1,
        "pick_count": 650,
        "avg_pick": 1.8,
        "game_count": 500,
        "win_rate": 0.61,
        "pool_count": 700,
        "opening_hand_game_count": 100,
        "opening_hand_win_rate": 0.65,
        "drawn_game_count": 150,
        "drawn_win_rate": 0.59,
        "ever_drawn_game_count": 250,
        "ever_drawn_win_rate": 0.62,
        "never_drawn_game_count": 250,
        "never_drawn_win_rate": 0.54,
    },
]


@pytest.fixture()
def fake_ratings_file(tmp_path, monkeypatch):
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))

    ratings_dir = tmp_path / "ratings" / FAKE_SET
    ratings_dir.mkdir(parents=True)

    filename = (
        f"PremierDraft_all_any"
        f"_{FAKE_START.strftime('%Y-%m-%d')}"
        f"_{FAKE_END.strftime('%Y-%m-%d')}.json"
    )
    (ratings_dir / filename).write_text(json.dumps(FAKE_CARDS))

    yield tmp_path


@pytest.fixture(autouse=True)
def clear_lru_caches():
    get_names.cache_clear()
    yield
    get_names.cache_clear()
