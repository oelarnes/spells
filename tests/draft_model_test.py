"""
Test the Draft object model built from 17lands draft data responses.
"""

import json
import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import spells.draft_model as draft_model
from spells import cache
from spells.draft_model import (
    Draft,
    DraftCard,
    _draft_from_data,
    draft_view_df,
    fetch_draft,
    view_draft,
)
from spells.enums import View


def card(name: str) -> dict:
    return {"name": name, "image_url": f"https://img/{name}.jpg"}


def pick_obj(
    pack_number: int,
    pick_number: int,
    available: list[str],
    pick: str | None,
    picks: list[str] | None = None,
) -> dict:
    return {
        "pack_number": pack_number,
        "pick_number": pick_number,
        "available": [card(n) for n in available],
        "pick": card(pick) if pick is not None else None,
        "picks": [card(n) for n in (picks if picks is not None else ([pick] if pick else []))],
    }


FAKE_DATA = {
    "expansion": "TST",
    "num_seats": 8,
    "picks": [
        pick_obj(0, 0, ["Aether Sprite", "Blazing Howl", "Crystal Idol"], "Aether Sprite"),
        pick_obj(0, 1, ["Ember Brute", "Tidal Reckoning"], "Tidal Reckoning"),
        pick_obj(1, 0, ["Stormcrash Wyvern", "Crystal Idol"], "Crystal Idol"),
    ],
}


def test_draft_from_data_structure():
    draft = _draft_from_data("abc123", FAKE_DATA)

    assert draft.expansion == "TST"
    assert draft.draft_id == "abc123"
    assert len(draft.picks) == 3

    first = draft.picks[0]
    assert (first.pack_num, first.pick_num) == (1, 1)  # 0-indexed feed -> 1-indexed model
    assert [c.name for c in first.pack_cards] == ["Aether Sprite", "Blazing Howl", "Crystal Idol"]
    assert first.pick_ind == 0
    assert first.picks_ind == [0]
    assert first.pool == []

    # cards carry identity only; no environment or metrics attached
    assert first.pack_cards[0].set_code is None
    assert first.pack_cards[0].image_url == "https://img/Aether Sprite.jpg"


def test_pool_accumulates_prior_picks():
    draft = _draft_from_data("abc123", FAKE_DATA)

    assert [c.name for c in draft.picks[1].pool] == ["Aether Sprite"]
    assert [c.name for c in draft.picks[2].pool] == ["Aether Sprite", "Tidal Reckoning"]


def test_picks_sorted_by_pack_then_pick():
    shuffled = {**FAKE_DATA, "picks": list(reversed(FAKE_DATA["picks"]))}
    draft = _draft_from_data("abc123", shuffled)

    assert [(s.pack_num, s.pick_num) for s in draft.picks] == [(1, 1), (1, 2), (2, 1)]
    assert [c.name for c in draft.picks[2].pool] == ["Aether Sprite", "Tidal Reckoning"]


def test_duplicate_names_consume_distinct_indices():
    data = {
        "expansion": "TST",
        "picks": [
            pick_obj(
                0, 0,
                ["Crystal Idol", "Crystal Idol", "Aether Sprite"],
                "Crystal Idol",
                picks=["Crystal Idol", "Crystal Idol"],
            ),
        ],
    }
    state = _draft_from_data("abc123", data).picks[0]

    # two picks -> pick-two semantics: pick_ind is None, picks_ind holds both
    assert state.pick_ind is None
    assert state.picks_ind == [0, 1]
    assert [c.name for c in _draft_from_data("abc123", data).picks[0].pack_cards] == [
        "Crystal Idol", "Crystal Idol", "Aether Sprite",
    ]


def test_missing_pick_gives_none_ind():
    data = {
        "expansion": "TST",
        "picks": [pick_obj(0, 0, ["Aether Sprite"], None)],
    }
    state = _draft_from_data("abc123", data).picks[0]

    assert state.pick_ind is None
    assert state.picks_ind == []
    assert state.pool == []


FAKE_ATTR_MAP = {
    "Aether Sprite": {
        "set_code": "TST",
        "color": "U",
        "rarity": "common",
        "card_type": "Creature",
        "mana_value": 2.0,
        "mana_cost": "{1}{U}",
        "oracle_text": "Flying",
        "image_url": "https://attr-img/Aether Sprite.jpg",
    },
}


def test_attr_map_populates_card_fields():
    draft = _draft_from_data("abc123", FAKE_DATA, FAKE_ATTR_MAP)
    sprite = draft.picks[0].pack_cards[0]

    assert sprite.set_code == "TST"
    assert sprite.color == "U"
    assert sprite.rarity == "common"
    assert sprite.card_type == "Creature"
    assert sprite.mana_value == 2.0
    assert sprite.oracle_text == "Flying"
    # the feed image_url wins over the card-file one
    assert sprite.image_url == "https://img/Aether Sprite.jpg"

    # cards missing from the attr map parse bare
    howl = draft.picks[0].pack_cards[1]
    assert howl.name == "Blazing Howl"
    assert howl.set_code is None
    assert howl.rarity is None


def test_card_attr_map_reads_card_file(tmp_path, monkeypatch: pytest.MonkeyPatch):
    import polars as pl

    from spells import cache
    from spells.enums import View

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))

    file_path = cache.data_file_path("TST", View.CARD)
    os.makedirs(os.path.dirname(file_path))
    pl.DataFrame(
        [{"name": "Aether Sprite", "set_code": "TST", "rarity": "common"}]
    ).write_parquet(file_path)

    attr_map = draft_model._card_attr_map("TST", ["Aether Sprite"])

    assert attr_map["Aether Sprite"]["rarity"] == "common"


def test_card_attr_map_falls_back_to_mtgjson(tmp_path, monkeypatch: pytest.MonkeyPatch):
    import polars as pl

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    monkeypatch.setattr(
        draft_model,
        "card_df",
        lambda expansion, names: pl.DataFrame(
            [{"name": n, "set_code": expansion, "rarity": "rare"} for n in names]
        ),
    )

    attr_map = draft_model._card_attr_map("TST", ["Aether Sprite"])

    assert attr_map["Aether Sprite"]["set_code"] == "TST"


def test_card_attr_map_empty_when_unavailable(tmp_path, monkeypatch: pytest.MonkeyPatch):
    def boom(expansion, names):
        raise ValueError("no such set")

    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    monkeypatch.setattr(draft_model, "card_df", boom)

    assert draft_model._card_attr_map("Cube+-+Powered", ["Aether Sprite"]) == {}


def test_fetch_draft_reads_cached_file(tmp_path, monkeypatch: pytest.MonkeyPatch):
    file_path = tmp_path / "abc123.json"
    file_path.write_text(json.dumps(FAKE_DATA))

    monkeypatch.setattr(
        draft_model, "download_data_file", lambda url, target_dir, filename: str(file_path)
    )
    monkeypatch.setattr(
        draft_model, "_card_attr_map", lambda expansion, names: FAKE_ATTR_MAP
    )

    draft = fetch_draft("abc123")

    assert isinstance(draft, Draft)
    assert draft.draft_id == "abc123"
    assert len(draft.picks) == 3
    assert isinstance(draft.picks[0].pack_cards[0], DraftCard)
    # card attrs joined through the fetch path
    assert draft.picks[0].pack_cards[0].rarity == "common"


def test_fetch_draft_without_card_data(tmp_path, monkeypatch: pytest.MonkeyPatch):
    file_path = tmp_path / "abc123.json"
    file_path.write_text(json.dumps(FAKE_DATA))

    monkeypatch.setattr(
        draft_model, "download_data_file", lambda url, target_dir, filename: str(file_path)
    )

    def no_call(expansion, names):
        raise AssertionError("_card_attr_map should not be called")

    monkeypatch.setattr(draft_model, "_card_attr_map", no_call)

    draft = fetch_draft("abc123", card_data=False)

    assert draft.picks[0].pack_cards[0].rarity is None


VIEW_NAMES = ["Aether Sprite", "Blazing Howl", "Crystal Idol"]

VIEW_SCHEMA = {
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
    **{f"pool_{n}": pl.Int8 for n in VIEW_NAMES},
    **{f"pack_card_{n}": pl.Int8 for n in VIEW_NAMES},
}


def view_row(draft_id, pack_number, pick_number, pack, pool, pick, rank="gold"):
    row = {
        "expansion": "TST",
        "event_type": "PremierDraft",
        "draft_id": draft_id,
        "draft_time": "2026-01-01 10:00:00",
        "rank": rank,
        "event_match_wins": 3,
        "event_match_losses": 1,
        "pack_number": pack_number,
        "pick_number": pick_number,
        "pick": pick,
        "pick_maindeck_rate": 1.0,
        "pick_sideboard_in_rate": 0.0,
        "user_n_games_bucket": 100,
        "user_game_win_rate_bucket": 0.55,
    }
    for n in VIEW_NAMES:
        row[f"pool_{n}"] = pool.get(n, 0)
        row[f"pack_card_{n}"] = pack.get(n, 0)
    return row


@pytest.fixture()
def fake_view(tmp_path, monkeypatch: pytest.MonkeyPatch) -> pl.DataFrame:
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    rows = [
        view_row("d1", 0, 0, {"Crystal Idol": 2, "Aether Sprite": 1}, {}, "Crystal Idol"),
        view_row("d1", 0, 1, {"Blazing Howl": 1}, {"Crystal Idol": 1}, "Blazing Howl"),
        # d2's pool is seeded with a promo card never picked (TLA-style)
        view_row(
            "d2", 0, 0, {"Aether Sprite": 1}, {"Blazing Howl": 1},
            "Aether Sprite", rank="bronze",
        ),
    ]
    df = pl.DataFrame(rows, schema=VIEW_SCHEMA)
    fp = cache.data_file_path("TST", View.DRAFT)
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    df.write_parquet(fp)
    return df


def test_view_draft_by_id(fake_view):
    draft = view_draft("TST", "d1", card_data=False)

    assert draft.expansion == "TST"
    assert draft.draft_id == "d1"
    assert draft.rank == "gold"
    assert draft.event_match_wins == 3
    assert draft.user_n_games_bucket == 100

    first = draft.picks[0]
    assert (first.pack_num, first.pick_num) == (1, 1)
    # pack expands count columns with multiplicity, in column (name) order
    assert [c.name for c in first.pack_cards] == [
        "Aether Sprite", "Crystal Idol", "Crystal Idol",
    ]
    assert first.pick_ind == 1
    assert first.picks_ind == [1]
    assert first.pick_maindeck_rate == 1.0

    second = draft.picks[1]
    assert [c.name for c in second.pool] == ["Crystal Idol"]


def test_view_draft_sampling(fake_view):
    draft = view_draft(
        "TST", filter_expr=pl.col("rank") == "bronze", seed=1, card_data=False
    )
    assert draft.draft_id == "d2"

    # seeded sampling is deterministic
    a = view_draft("TST", seed=7, card_data=False)
    b = view_draft("TST", seed=7, card_data=False)
    assert a.draft_id == b.draft_id


def test_view_draft_no_match_raises(fake_view):
    with pytest.raises(ValueError):
        view_draft("TST", filter_expr=pl.col("rank") == "mythic", card_data=False)


def test_view_round_trip(fake_view):
    draft = view_draft("TST", "d1", card_data=False)
    df = draft_view_df(draft)

    expected = fake_view.filter(pl.col("draft_id") == "d1")
    assert_frame_equal(df, expected)


def test_view_pool_promo_cards(fake_view):
    """Cards seeded into the pool outside of picks are recovered from pool_."""
    draft = view_draft("TST", "d2", card_data=False)

    assert [c.name for c in draft.picks[0].pool] == ["Blazing Howl"]

    # and they round-trip exactly
    df = draft_view_df(draft)
    assert_frame_equal(df, fake_view.filter(pl.col("draft_id") == "d2"))


def test_draft_view_df_constructed_schema(tmp_path, monkeypatch: pytest.MonkeyPatch):
    # no local view file: schema is constructed from the draft's own cards
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    draft = _draft_from_data("abc123", FAKE_DATA)

    df = draft_view_df(draft)

    assert df.height == 3
    assert df["pack_number"].to_list() == [0, 0, 1]
    assert df["pick"].to_list() == ["Aether Sprite", "Tidal Reckoning", "Crystal Idol"]
    assert df["pool_Aether Sprite"].to_list() == [0, 1, 1]
    assert df["rank"][0] is None
    assert df.schema["pack_card_Crystal Idol"] == pl.Int8


PICK_TWO_SCHEMA = {}
for _k, _v in VIEW_SCHEMA.items():
    PICK_TWO_SCHEMA[_k] = _v
    if _k == "pick":
        PICK_TWO_SCHEMA["pick_2"] = pl.String


@pytest.fixture()
def fake_pick_two_view(tmp_path, monkeypatch: pytest.MonkeyPatch) -> pl.DataFrame:
    """Mimics the OM1 PickTwoDraft file: pick_2 column present, and the
    pool_ columns only track `pick` cards (the 17lands undercount)."""
    monkeypatch.setenv("SPELLS_DATA_HOME", str(tmp_path))
    rows = [
        view_row(
            "d1", 0, 0,
            {"Crystal Idol": 2, "Aether Sprite": 1, "Blazing Howl": 1},
            {}, "Crystal Idol",
        ),
        view_row(
            "d1", 0, 1,
            {"Aether Sprite": 1, "Blazing Howl": 1},
            {"Crystal Idol": 1},  # undercount: the pick_2 copy is missing
            "Aether Sprite",
        ),
    ]
    rows[0]["pick_2"] = "Crystal Idol"  # two copies of the same card
    rows[1]["pick_2"] = "Blazing Howl"
    df = pl.DataFrame(rows, schema=PICK_TWO_SCHEMA)
    fp = cache.data_file_path("TS2", View.DRAFT, cache.EventType.PICK_TWO)
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    df.write_parquet(fp)
    return df


def test_view_draft_pick_two(fake_pick_two_view):
    draft = view_draft(
        "TS2", "d1", event_type=cache.EventType.PICK_TWO, card_data=False
    )

    first = draft.picks[0]
    # pack in column order: Aether Sprite, Blazing Howl, Crystal Idol x2
    assert [c.name for c in first.pack_cards] == [
        "Aether Sprite", "Blazing Howl", "Crystal Idol", "Crystal Idol",
    ]
    # pick-two: pick_ind None, both copies consume distinct indices
    assert first.pick_ind is None
    assert first.picks_ind == [2, 3]

    second = draft.picks[1]
    assert second.pick_ind is None
    assert second.picks_ind == [0, 1]
    # pool corrected by accumulation: both Crystal Idol copies, not the
    # file's undercounted single copy
    assert [c.name for c in second.pool] == ["Crystal Idol", "Crystal Idol"]


def test_pick_two_round_trip(fake_pick_two_view):
    draft = view_draft(
        "TS2", "d1", event_type=cache.EventType.PICK_TWO, card_data=False
    )
    df = draft_view_df(draft)

    # non-pool columns round-trip exactly, including pick_2
    non_pool = [c for c in fake_pick_two_view.columns if not c.startswith("pool_")]
    assert_frame_equal(df.select(non_pool), fake_pick_two_view.select(non_pool))
    assert df["pick_2"].to_list() == ["Crystal Idol", "Blazing Howl"]

    # pool columns come out corrected relative to the source file
    assert df["pool_Crystal Idol"].to_list() == [0, 2]
    assert df["pool_Blazing Howl"].to_list() == [0, 0]


def test_live_pick_two_feed():
    data = {
        "expansion": "OM1",
        "picks": [
            pick_obj(
                0, 0,
                ["Aether Sprite", "Blazing Howl", "Crystal Idol", "Ember Brute"],
                "Aether Sprite",
                picks=["Aether Sprite", "Crystal Idol"],
            ),
        ],
    }
    state = _draft_from_data("abc123", data).picks[0]

    assert state.pick_ind is None
    assert state.picks_ind == [0, 2]


def test_live_feed_pool_promo_from_sections():
    p1 = pick_obj(0, 0, ["Aether Sprite", "Blazing Howl"], "Aether Sprite")
    p2 = pick_obj(0, 1, ["Crystal Idol"], "Crystal Idol")
    # the feed's sections show a promo card that never appears in picks
    p2["sections"] = [
        {
            "title": "Possible Maindeck",
            "cards": [[card("Ember Brute")], [card("Aether Sprite")]],
        },
    ]
    draft = _draft_from_data("abc123", {"expansion": "TST", "picks": [p1, p2]})

    # promo prepended, accumulated picks follow in pick order
    assert [c.name for c in draft.picks[1].pool] == ["Ember Brute", "Aether Sprite"]
