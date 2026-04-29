import datetime
import json
import os

import polars as pl
import pytest

import spells.cache as cache
from spells.draft_data import get_names


FAKE_SET = "TST"
FAKE_START = datetime.date(2026, 1, 1)
FAKE_END = datetime.date(2026, 1, 2)

FAKE_CARD_NAMES = ["Aether Sprite", "Blazing Howl", "Crystal Warden"]

# Card attribute rows matching the CardAttr enum fields written by cards.py.
FAKE_CARD_ATTR_ROWS = [
    {
        "name": "Aether Sprite",
        "set_code": "TST",
        "color": "U",
        "rarity": "common",
        "color_identity": "U",
        "card_type": "Creature",
        "subtype": "Faerie",
        "mana_value": 2.0,
        "mana_cost": "{1}{U}",
        "power": "1",
        "toughness": "1",
        "is_bonus_sheet": False,
        "is_dfc": False,
        "oracle_text": "Flying",
        "card_json": "{}",
        "scryfall_id": "",
        "image_url": "",
    },
    {
        "name": "Blazing Howl",
        "set_code": "TST",
        "color": "R",
        "rarity": "uncommon",
        "color_identity": "R",
        "card_type": "Instant",
        "subtype": "",
        "mana_value": 1.0,
        "mana_cost": "{R}",
        "power": None,
        "toughness": None,
        "is_bonus_sheet": False,
        "is_dfc": False,
        "oracle_text": "Target creature gets +2/+0 until end of turn.",
        "card_json": "{}",
        "scryfall_id": "",
        "image_url": "",
    },
    {
        "name": "Crystal Warden",
        "set_code": "TST",
        "color": "W",
        "rarity": "rare",
        "color_identity": "W",
        "card_type": "Creature",
        "subtype": "Human Soldier",
        "mana_value": 3.0,
        "mana_cost": "{2}{W}",
        "power": "3",
        "toughness": "3",
        "is_bonus_sheet": False,
        "is_dfc": False,
        "oracle_text": "Vigilance",
        "card_json": "{}",
        "scryfall_id": "",
        "image_url": "",
    },
]


def _make_draft_row(set_code, draft_id, pack_number, pick_number, pick):
    """One pick row: all 3 fake cards present in pack, one picked."""
    row = {
        "expansion": set_code,
        "event_type": "PremierDraft",
        "draft_id": draft_id,
        "draft_time": "2026-01-01",
        "rank": "Gold",
        "event_match_wins": 0,
        "event_match_losses": 0,
        "pack_number": pack_number,
        "pick_number": pick_number,
        "pick": pick,
        "pick_maindeck_rate": 1.0,
        "pick_sideboard_in_rate": 0.0,
        "user_n_games_bucket": 200,
        "user_game_win_rate_bucket": 0.55,
    }
    for name in FAKE_CARD_NAMES:
        row[f"pack_card_{name}"] = 1
        row[f"pool_{name}"] = 0
    return row


def _write_set_parquets(external_dir, set_code, draft_rows):
    """Write card, context, and draft parquets for a fake set."""
    set_dir = external_dir / set_code
    set_dir.mkdir(parents=True)

    card_rows = [{**r, "set_code": set_code} for r in FAKE_CARD_ATTR_ROWS]
    pl.DataFrame(card_rows).write_parquet(set_dir / f"{set_code}_card.parquet")

    pl.DataFrame(
        {"release_date": [datetime.date(2026, 1, 1)], "picks_per_pack": [14]}
    ).write_parquet(set_dir / f"{set_code}_context.parquet")

    schema = {
        "expansion": pl.String,
        "event_type": pl.String,
        "draft_id": pl.String,
        "draft_time": pl.String,
        "rank": pl.String,
        "event_match_wins": pl.Int8,
        "event_match_losses": pl.Int8,
        "pack_number": pl.Int8,
        "pick_number": pl.Int8,
        "pick": pl.String,
        "pick_maindeck_rate": pl.Float64,
        "pick_sideboard_in_rate": pl.Float64,
        "user_n_games_bucket": pl.Int16,
        "user_game_win_rate_bucket": pl.Float64,
        **{f"pack_card_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
        **{f"pool_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
    }
    pl.DataFrame(draft_rows, schema=schema).write_parquet(
        set_dir / f"{set_code}_PremierDraft_draft.parquet"
    )

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
def fake_draft_sets(tmp_path, monkeypatch):
    """
    Writes fake parquet files for two sets under SPELLS_DATA_HOME:

    TST (normal set, not in P1P1_MISSING_SETS):
      3 drafts — all have p1p1 (pick_number=0), only 2 have p1p2 (pick_number=1).
      Expected num_drafts = 3 (p1p1 count).

    TLA (missing p1p1 set, in P1P1_MISSING_SETS):
      2 drafts — p1p2 only (pick_number=1), no p1p1 rows.
      Expected num_drafts = 2 (p1p2 count).
    """
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external"

    tst_rows = [
        _make_draft_row("TST", "d001", 0, 0, "Aether Sprite"),   # p1p1
        _make_draft_row("TST", "d001", 0, 1, "Blazing Howl"),    # p1p2
        _make_draft_row("TST", "d002", 0, 0, "Blazing Howl"),    # p1p1
        _make_draft_row("TST", "d002", 0, 1, "Crystal Warden"),  # p1p2
        _make_draft_row("TST", "d003", 0, 0, "Crystal Warden"),  # p1p1 only
    ]
    _write_set_parquets(ext, "TST", tst_rows)

    tla_rows = [
        _make_draft_row("TLA", "d001", 0, 1, "Aether Sprite"),   # p1p2 only
        _make_draft_row("TLA", "d002", 0, 1, "Blazing Howl"),    # p1p2 only
    ]
    _write_set_parquets(ext, "TLA", tla_rows)

    yield tmp_path


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
