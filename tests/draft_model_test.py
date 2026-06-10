"""
Test the Draft object model built from 17lands draft data responses.
"""

import json
import os

import pytest

import spells.draft_model as draft_model
from spells.draft_model import Draft, DraftCard, _draft_from_data, fetch_draft


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

    assert state.pick_ind == 0
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
