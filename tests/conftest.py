import datetime
import json
from collections.abc import Generator
from pathlib import Path

import polars as pl
import pytest

from spells.draft_data import get_names
from spells.enums import EventType


FAKE_SET = "TST"
FAKE_START = datetime.date(2026, 1, 1)
FAKE_END = datetime.date(2026, 1, 2)

# 6 cards: 2 U, 2 R, 1 UR multicolor, 1 colorless. Supports group_by/filter on
# color with 4 distinct groups (U, R, UR, "") and main_colors permutations
# U / R / UR. Multi-card groups exist within U and R.
FAKE_CARD_NAMES = [
    "Aether Sprite",
    "Tidal Reckoning",
    "Blazing Howl",
    "Ember Brute",
    "Stormcrash Wyvern",
    "Crystal Idol",
]

FAKE_CARD_ATTR_ROWS = [
    {
        "name": "Aether Sprite", "color": "U", "rarity": "common", "color_identity": "U",
        "card_type": "Creature", "subtype": "Faerie",
        "mana_value": 2.0, "mana_cost": "{1}{U}", "power": "1", "toughness": "1",
        "is_bonus_sheet": False, "is_dfc": False, "oracle_text": "Flying",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
    {
        "name": "Tidal Reckoning", "color": "U", "rarity": "rare", "color_identity": "U",
        "card_type": "Instant", "subtype": "",
        "mana_value": 4.0, "mana_cost": "{2}{U}{U}", "power": None, "toughness": None,
        "is_bonus_sheet": False, "is_dfc": False, "oracle_text": "Counter target spell.",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
    {
        "name": "Blazing Howl", "color": "R", "rarity": "uncommon", "color_identity": "R",
        "card_type": "Instant", "subtype": "",
        "mana_value": 1.0, "mana_cost": "{R}", "power": None, "toughness": None,
        "is_bonus_sheet": False, "is_dfc": False,
        "oracle_text": "Target creature gets +2/+0 until end of turn.",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
    {
        "name": "Ember Brute", "color": "R", "rarity": "common", "color_identity": "R",
        "card_type": "Creature", "subtype": "Goblin",
        "mana_value": 3.0, "mana_cost": "{2}{R}", "power": "3", "toughness": "2",
        "is_bonus_sheet": False, "is_dfc": False, "oracle_text": "Haste",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
    {
        "name": "Stormcrash Wyvern", "color": "UR", "rarity": "rare", "color_identity": "UR",
        "card_type": "Creature", "subtype": "Dragon",
        "mana_value": 4.0, "mana_cost": "{2}{U}{R}", "power": "3", "toughness": "3",
        "is_bonus_sheet": False, "is_dfc": False, "oracle_text": "Flying. When ETB, deal 2 damage.",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
    {
        "name": "Crystal Idol", "color": "", "rarity": "common", "color_identity": "",
        "card_type": "Artifact", "subtype": "",
        "mana_value": 2.0, "mana_cost": "{2}", "power": None, "toughness": None,
        "is_bonus_sheet": False, "is_dfc": False, "oracle_text": "{T}: Add {C}.",
        "card_json": "{}", "scryfall_id": "", "image_url": "",
    },
]


def _make_draft_row(
    set_code,
    draft_id,
    pack_number,
    pick_number,
    pick,
    *,
    draft_time="2026-01-01 12:00:00",
    rank="Gold",
    user_n_games_bucket=200,
    user_game_win_rate_bucket=0.55,
    event_match_wins=0,
    event_match_losses=0,
    event_type=EventType.PREMIER,
):
    """One pick row. All cards are present in the pack; one is picked."""
    row = {
        "expansion": set_code,
        "event_type": event_type,
        "draft_id": draft_id,
        "draft_time": draft_time,
        "rank": rank,
        "event_match_wins": event_match_wins,
        "event_match_losses": event_match_losses,
        "pack_number": pack_number,
        "pick_number": pick_number,
        "pick": pick,
        "pick_maindeck_rate": 1.0,
        "pick_sideboard_in_rate": 0.0,
        "user_n_games_bucket": user_n_games_bucket,
        "user_game_win_rate_bucket": user_game_win_rate_bucket,
    }
    for name in FAKE_CARD_NAMES:
        row[f"pack_card_{name}"] = 1
        row[f"pool_{name}"] = 0
    return row


def _make_game_row(
    set_code,
    draft_id,
    game_number,
    match_number,
    *,
    won=True,
    on_play=True,
    main_colors="U",
    splash_colors="",
    opp_colors="R",
    num_mulligans=0,
    opp_num_mulligans=0,
    num_turns=10,
    rank="Gold",
    opp_rank="Gold",
    build_index=0,
    draft_time="2026-01-01 12:00:00",
    game_time="2026-01-01 13:00:00",
    user_n_games_bucket=200,
    user_game_win_rate_bucket=0.55,
    deck=None,
    drawn=None,
    opening_hand=None,
    sideboard=None,
    tutored=None,
    event_type=EventType.PREMIER,
):
    """One game row. Per-card counts default to zero unless overridden."""
    deck = deck or {}
    drawn = drawn or {}
    opening_hand = opening_hand or {}
    sideboard = sideboard or {}
    tutored = tutored or {}
    row = {
        "expansion": set_code,
        "event_type": event_type,
        "draft_id": draft_id,
        "draft_time": draft_time,
        "game_time": game_time,
        "build_index": build_index,
        "match_number": match_number,
        "game_number": game_number,
        "rank": rank,
        "opp_rank": opp_rank,
        "main_colors": main_colors,
        "splash_colors": splash_colors,
        "on_play": on_play,
        "num_mulligans": num_mulligans,
        "opp_num_mulligans": opp_num_mulligans,
        "opp_colors": opp_colors,
        "num_turns": num_turns,
        "won": won,
        "user_n_games_bucket": user_n_games_bucket,
        "user_game_win_rate_bucket": user_game_win_rate_bucket,
    }
    for name in FAKE_CARD_NAMES:
        row[f"deck_{name}"] = deck.get(name, 0)
        row[f"drawn_{name}"] = drawn.get(name, 0)
        row[f"opening_hand_{name}"] = opening_hand.get(name, 0)
        row[f"sideboard_{name}"] = sideboard.get(name, 0)
        row[f"tutored_{name}"] = tutored.get(name, 0)
    return row


def _draft_schema():
    return {
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


def _game_schema():
    return {
        "expansion": pl.String,
        "event_type": pl.String,
        "draft_id": pl.String,
        "draft_time": pl.String,
        "game_time": pl.String,
        "build_index": pl.Int8,
        "match_number": pl.Int8,
        "game_number": pl.Int8,
        "rank": pl.String,
        "opp_rank": pl.String,
        "main_colors": pl.String,
        "splash_colors": pl.String,
        "on_play": pl.Boolean,
        "num_mulligans": pl.Int8,
        "opp_num_mulligans": pl.Int8,
        "opp_colors": pl.String,
        "num_turns": pl.Int8,
        "won": pl.Boolean,
        "user_n_games_bucket": pl.Int16,
        "user_game_win_rate_bucket": pl.Float64,
        **{f"deck_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
        **{f"drawn_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
        **{f"opening_hand_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
        **{f"sideboard_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
        **{f"tutored_{n}": pl.Int8 for n in FAKE_CARD_NAMES},
    }


def _write_set_parquets(
    external_dir,
    set_code,
    draft_rows,
    game_rows=None,
    release_date=None,
    event_type=EventType.PREMIER,
):
    """Write card, context, draft, and (optionally) game parquets for a fake set.

    The card and context files are set-level (shared across event types), so a
    second call for the same set with a different event_type only adds the
    event-type-specific draft/game parquets.
    """
    set_dir = external_dir / set_code
    if not set_dir.is_dir():
        set_dir.mkdir(parents=True)

        card_rows = [{**r, "set_code": set_code} for r in FAKE_CARD_ATTR_ROWS]
        pl.DataFrame(card_rows).write_parquet(set_dir / f"{set_code}_card.parquet")

        pl.DataFrame(
            {
                "release_date": [release_date or datetime.date(2026, 1, 1)],
                "picks_per_pack": [14],
            }
        ).write_parquet(set_dir / f"{set_code}_context.parquet")

    pl.DataFrame(draft_rows, schema=_draft_schema()).write_parquet(
        set_dir / f"{set_code}_{event_type}_draft.parquet"
    )

    if game_rows is not None:
        pl.DataFrame(game_rows, schema=_game_schema()).write_parquet(
            set_dir / f"{set_code}_{event_type}_game.parquet"
        )


# PLAYER_COHORT thresholds (from columns.py):
#   user_n_games_bucket  <  100  ->  "Other"
#   bucket >= 100, win_rate >  0.57  ->  "Top"
#   bucket >= 100, win_rate <  0.49  ->  "Bottom"
#   bucket >= 100, otherwise         ->  "Middle"
_TOP    = {"user_n_games_bucket": 200, "user_game_win_rate_bucket": 0.60}
_BOTTOM = {"user_n_games_bucket": 200, "user_game_win_rate_bucket": 0.45}
_MIDDLE = {"user_n_games_bucket": 150, "user_game_win_rate_bucket": 0.52}
_OTHER  = {"user_n_games_bucket":  50, "user_game_win_rate_bucket": 0.55}


def _tst_draft_rows():
    """5 drafts spanning all four PLAYER_COHORTs and 3 format weeks.

    Counts (computed by hand, verified in tests):
      total picks       : 14
      p1p1 rows         :  5    (pack_number=0, pick_number=0)
      p1p2 rows         :  4    (pack_number=0, pick_number=1)
      pack 1 picks      :  9    (pack_number=0)
      pack 2 picks      :  3    (pack_number=1)
      pack 3 picks      :  2    (pack_number=2)
      Top cohort picks  :  5    (d001 + d005)
      Bottom cohort     :  4    (d002)
      Middle cohort     :  3    (d003)
      Other cohort      :  2    (d004)
    """
    return [
        # d001 — Top, 2026-01-01 (format_day=1, week=1) — full pack 1+2+3
        _make_draft_row("TST", "d001", 0, 0, "Aether Sprite",   draft_time="2026-01-01 12:00:00", **_TOP),
        _make_draft_row("TST", "d001", 0, 1, "Tidal Reckoning", draft_time="2026-01-01 12:00:00", **_TOP),
        _make_draft_row("TST", "d001", 1, 0, "Crystal Idol",    draft_time="2026-01-01 12:00:00", **_TOP),
        _make_draft_row("TST", "d001", 2, 0, "Aether Sprite",   draft_time="2026-01-01 12:00:00", **_TOP),
        # d002 — Bottom, 2026-01-02 (format_day=2, week=1) — R-leaning
        _make_draft_row("TST", "d002", 0, 0, "Blazing Howl",    draft_time="2026-01-02 14:00:00", **_BOTTOM),
        _make_draft_row("TST", "d002", 0, 1, "Ember Brute",     draft_time="2026-01-02 14:00:00", **_BOTTOM),
        _make_draft_row("TST", "d002", 1, 0, "Ember Brute",     draft_time="2026-01-02 14:00:00", **_BOTTOM),
        _make_draft_row("TST", "d002", 2, 0, "Crystal Idol",    draft_time="2026-01-02 14:00:00", **_BOTTOM),
        # d003 — Middle, 2026-01-08 (format_day=8, week=2) — UR (drafts the UR card)
        _make_draft_row("TST", "d003", 0, 0, "Tidal Reckoning",   draft_time="2026-01-08 10:00:00", **_MIDDLE),
        _make_draft_row("TST", "d003", 0, 1, "Blazing Howl",      draft_time="2026-01-08 10:00:00", **_MIDDLE),
        _make_draft_row("TST", "d003", 1, 0, "Stormcrash Wyvern", draft_time="2026-01-08 10:00:00", **_MIDDLE),
        # d004 — Other (< 100 games), 2026-01-09 (format_day=9, week=2)
        _make_draft_row("TST", "d004", 0, 0, "Crystal Idol",    draft_time="2026-01-09 11:00:00", **_OTHER),
        _make_draft_row("TST", "d004", 0, 1, "Aether Sprite",   draft_time="2026-01-09 11:00:00", **_OTHER),
        # d005 — Top, 2026-01-15 (format_day=15, week=3) — only p1p1
        _make_draft_row("TST", "d005", 0, 0, "Tidal Reckoning", draft_time="2026-01-15 09:00:00", **_TOP),
    ]


def _tst_game_rows():
    """8 games across the 5 drafts above.

    Counts (verified in tests):
      NUM_GAMES         : 8
      NUM_WON           : 6    (d001g1, d002g2, d003g1, d003g2, d004g1, d005g1)
      NUM_ON_PLAY       : 6    (d001g1, d002g1, d002g2, d003g2, d004g1, d005g1)
      NUM_MULLIGANS_SUM : 1    (d002g1)

    main_colors distribution:
      U   : 4 games (3 won)   d001g1, d001g2, d004g1, d005g1
      R   : 2 games (1 won)   d002g1, d002g2
      UR  : 2 games (2 won)   d003g1, d003g2
    num_colors:
      1   : 6 games (4 won)
      2   : 2 games (2 won)
    """
    return [
        # d001 (U deck): 2 games, 1 win
        _make_game_row(
            "TST", "d001", game_number=1, match_number=1,
            won=True, on_play=True, main_colors="U", opp_colors="R",
            draft_time="2026-01-01 12:00:00", game_time="2026-01-01 13:00:00", **_TOP,
            deck={"Aether Sprite": 4, "Tidal Reckoning": 2, "Crystal Idol": 2},
            drawn={"Aether Sprite": 2, "Tidal Reckoning": 1},
            opening_hand={"Aether Sprite": 1},
            sideboard={"Blazing Howl": 1, "Ember Brute": 1},
        ),
        _make_game_row(
            "TST", "d001", game_number=2, match_number=2,
            won=False, on_play=False, main_colors="U", opp_colors="R",
            draft_time="2026-01-01 12:00:00", game_time="2026-01-01 13:30:00", **_TOP,
            deck={"Aether Sprite": 4, "Tidal Reckoning": 2, "Crystal Idol": 2},
            drawn={"Aether Sprite": 1, "Crystal Idol": 1},
            opening_hand={"Tidal Reckoning": 1},
            sideboard={"Blazing Howl": 1, "Ember Brute": 1},
        ),
        # d002 (R deck): 2 games, 1 win
        _make_game_row(
            "TST", "d002", game_number=1, match_number=1,
            won=False, on_play=True, main_colors="R", opp_colors="U",
            num_mulligans=1, draft_time="2026-01-02 14:00:00",
            game_time="2026-01-02 15:00:00", **_BOTTOM,
            deck={"Blazing Howl": 3, "Ember Brute": 4, "Crystal Idol": 2},
            drawn={"Ember Brute": 2, "Blazing Howl": 1},
            opening_hand={"Ember Brute": 1},
            sideboard={"Aether Sprite": 1, "Tidal Reckoning": 1},
        ),
        _make_game_row(
            "TST", "d002", game_number=2, match_number=2,
            won=True, on_play=True, main_colors="R", opp_colors="U",
            draft_time="2026-01-02 14:00:00", game_time="2026-01-02 15:30:00", **_BOTTOM,
            deck={"Blazing Howl": 3, "Ember Brute": 4, "Crystal Idol": 2},
            drawn={"Blazing Howl": 1, "Crystal Idol": 1},
            opening_hand={"Blazing Howl": 1},
            sideboard={"Aether Sprite": 1, "Tidal Reckoning": 1},
        ),
        # d003 (UR deck, includes the multicolor card): 2 games, 2 wins
        _make_game_row(
            "TST", "d003", game_number=1, match_number=1,
            won=True, on_play=False, main_colors="UR", opp_colors="",
            draft_time="2026-01-08 10:00:00", game_time="2026-01-08 11:00:00", **_MIDDLE,
            deck={"Aether Sprite": 2, "Tidal Reckoning": 2, "Blazing Howl": 2, "Ember Brute": 2, "Stormcrash Wyvern": 1, "Crystal Idol": 1},
            drawn={"Aether Sprite": 1, "Blazing Howl": 1, "Stormcrash Wyvern": 1},
            opening_hand={"Tidal Reckoning": 1},
        ),
        _make_game_row(
            "TST", "d003", game_number=2, match_number=2,
            won=True, on_play=True, main_colors="UR", opp_colors="",
            draft_time="2026-01-08 10:00:00", game_time="2026-01-08 11:30:00", **_MIDDLE,
            deck={"Aether Sprite": 2, "Tidal Reckoning": 2, "Blazing Howl": 2, "Ember Brute": 2, "Stormcrash Wyvern": 1, "Crystal Idol": 1},
            drawn={"Ember Brute": 2},
            opening_hand={"Stormcrash Wyvern": 1, "Crystal Idol": 1},
        ),
        # d004 (U deck): 1 game, 1 win
        _make_game_row(
            "TST", "d004", game_number=1, match_number=1,
            won=True, on_play=True, main_colors="U", opp_colors="R",
            draft_time="2026-01-09 11:00:00", game_time="2026-01-09 12:00:00", **_OTHER,
            deck={"Aether Sprite": 4, "Tidal Reckoning": 2, "Crystal Idol": 4},
            drawn={"Aether Sprite": 1, "Crystal Idol": 1},
            opening_hand={"Crystal Idol": 1},
            sideboard={"Blazing Howl": 2, "Ember Brute": 2},
        ),
        # d005 (U deck): 1 game, 1 win
        _make_game_row(
            "TST", "d005", game_number=1, match_number=1,
            won=True, on_play=True, main_colors="U", opp_colors="R",
            draft_time="2026-01-15 09:00:00", game_time="2026-01-15 10:00:00", **_TOP,
            deck={"Aether Sprite": 4, "Tidal Reckoning": 1, "Crystal Idol": 1},
            drawn={"Tidal Reckoning": 1},
            opening_hand={"Aether Sprite": 1},
            sideboard={"Blazing Howl": 2, "Ember Brute": 2, "Crystal Idol": 1},
        ),
    ]


def _tla_draft_rows():
    """TLA: missing-p1p1 set, 2 drafts, p1p2 only. No game data needed."""
    return [
        _make_draft_row(
            "TLA", "d001", 0, 1, "Aether Sprite",
            draft_time="2026-01-15 09:00:00", **_OTHER,
        ),
        _make_draft_row(
            "TLA", "d002", 0, 1, "Blazing Howl",
            draft_time="2026-01-16 09:00:00", **_OTHER,
        ),
    ]


def _trd_premier_draft_rows():
    """TRD Premier: 1 draft, 2 picks. event_match_wins=5 (not a Bo1 trophy)."""
    return [
        _make_draft_row("TRD", "p001", 0, 0, "Aether Sprite", event_match_wins=5),
        _make_draft_row("TRD", "p001", 0, 1, "Blazing Howl", event_match_wins=5),
    ]


def _trd_trad_draft_rows():
    """TRD Traditional: 3 picks across 2 drafts.

    t001 is a Bo3 trophy (event_match_wins == 3), contributing both its picks to
    IS_TROPHY_SUM; t002 is not. Verified counts: num_taken=3, is_trophy_sum=2.
    Under the old `== "Traditional"` bug IS_TROPHY would use the Bo1 `== 7`
    branch and sum to 0, so this pins the fix.
    """
    return [
        _make_draft_row(
            "TRD", "t001", 0, 0, "Aether Sprite",
            event_match_wins=3, event_type=EventType.TRADITIONAL,
        ),
        _make_draft_row(
            "TRD", "t001", 0, 1, "Blazing Howl",
            event_match_wins=3, event_type=EventType.TRADITIONAL,
        ),
        _make_draft_row(
            "TRD", "t002", 0, 0, "Aether Sprite",
            event_match_wins=1, event_type=EventType.TRADITIONAL,
        ),
    ]


@pytest.fixture()
def fake_trad_set(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Writes set TRD with both Premier and Traditional draft parquets.

    Premier has 2 picks (num_taken=2); Traditional has 3 picks (num_taken=3)
    including a trophy draft (is_trophy_sum=2). Shared card/context written once.
    """
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external"

    _write_set_parquets(ext, "TRD", _trd_premier_draft_rows())
    _write_set_parquets(
        ext, "TRD", _trd_trad_draft_rows(), event_type=EventType.TRADITIONAL
    )

    yield tmp_path


@pytest.fixture()
def fake_draft_sets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """
    Writes fake parquet files for two sets under SPELLS_DATA_HOME:

    TST (normal set, not in P1P1_MISSING_SETS):
      - 5 drafts spanning all 4 PLAYER_COHORTs and 3 format weeks
      - Picks across all 3 packs, 6 cards (2 U, 2 R, 1 UR multicolor, 1 colorless)
      - 8 games with main_colors in {U, R, UR}
      - See _tst_draft_rows() and _tst_game_rows() docstrings for verified counts.

    TLA (missing p1p1, in P1P1_MISSING_SETS):
      - 2 drafts, p1p2 only — drives the num_drafts regression test.
      - No game parquet.
    """
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    ext = tmp_path / "external"

    _write_set_parquets(ext, "TST", _tst_draft_rows(), game_rows=_tst_game_rows())
    _write_set_parquets(ext, "TLA", _tla_draft_rows())

    yield tmp_path


FAKE_CARD_RATINGS = [
    {
        "name": "Aether Sprite", "color": "U", "rarity": "common",
        "url": "https://example.com/aether_sprite.jpg",
        "seen_count": 1000, "avg_seen": 3.5,
        "pick_count": 800, "avg_pick": 2.1,
        "game_count": 400, "win_rate": 0.55, "pool_count": 600,
        "opening_hand_game_count": 80, "opening_hand_win_rate": 0.60,
        "drawn_game_count": 120, "drawn_win_rate": 0.53,
        "ever_drawn_game_count": 200, "ever_drawn_win_rate": 0.56,
        "never_drawn_game_count": 200, "never_drawn_win_rate": 0.48,
    },
    {
        "name": "Blazing Howl", "color": "R", "rarity": "uncommon",
        "url": "https://example.com/blazing_howl.jpg",
        "seen_count": 900, "avg_seen": 4.0,
        "pick_count": 500, "avg_pick": 3.2,
        "game_count": 300, "win_rate": 0.52, "pool_count": 450,
        "opening_hand_game_count": 60, "opening_hand_win_rate": 0.57,
        "drawn_game_count": 90, "drawn_win_rate": 0.50,
        "ever_drawn_game_count": 150, "ever_drawn_win_rate": 0.53,
        "never_drawn_game_count": 150, "never_drawn_win_rate": 0.51,
    },
    {
        # Distinct from the parquet-path "Crystal Idol" — FAKE_CARD_RATINGS feeds the
        # cdfs/ratings-JSON fixture, which is independent of the card parquet.
        "name": "Crystal Warden", "color": "W", "rarity": "rare",
        "url": "https://example.com/crystal_warden.jpg",
        "seen_count": 700, "avg_seen": 5.1,
        "pick_count": 650, "avg_pick": 1.8,
        "game_count": 500, "win_rate": 0.61, "pool_count": 700,
        "opening_hand_game_count": 100, "opening_hand_win_rate": 0.65,
        "drawn_game_count": 150, "drawn_win_rate": 0.59,
        "ever_drawn_game_count": 250, "ever_drawn_win_rate": 0.62,
        "never_drawn_game_count": 250, "never_drawn_win_rate": 0.54,
    },
]


@pytest.fixture()
def fake_ratings_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))

    ratings_dir = tmp_path / "ratings" / FAKE_SET
    ratings_dir.mkdir(parents=True)

    filename = (
        f"PremierDraft_all_any"
        f"_{FAKE_START.strftime('%Y-%m-%d')}"
        f"_{FAKE_END.strftime('%Y-%m-%d')}.json"
    )
    (ratings_dir / filename).write_text(json.dumps(FAKE_CARD_RATINGS))

    yield tmp_path


@pytest.fixture(autouse=True)
def clear_lru_caches() -> Generator[None, None, None]:
    get_names.cache_clear()
    yield
    get_names.cache_clear()
