"""Object model for individual drafts.

A `Draft` is a succession of `DraftState`s, one per pack/pick index. The
chosen card is identified by index into `pack_cards` (`pick_ind`, or
`picks_ind` for pick-two formats), and `pool` accumulates prior picks.

Cards carry identity and static card attributes (joined from MTGJSON via the
spells card file), but no metrics; annotate with metric data by joining on
name downstream.
"""

import json
import os
from dataclasses import dataclass

import polars as pl

from spells import cache
from spells.card_data_files import download_data_file
from spells.cards import card_df, write_card_file
from spells.enums import ColName, View

DRAFT_DATA_TEMPLATE = "https://www.17lands.com/data/draft?draft_id={draft_id}"


@dataclass
class DraftCard:
    name: str
    set_code: str | None = None  # the card's own printing set, from MTGJSON
    image_url: str = ""
    color: str | None = None
    rarity: str | None = None
    color_identity: str | None = None
    card_type: str | None = None
    subtype: str | None = None
    mana_value: float | None = None
    mana_cost: str | None = None
    power: str | None = None
    toughness: str | None = None
    is_bonus_sheet: bool | None = None
    is_dfc: bool | None = None
    oracle_text: str | None = None
    scryfall_id: str | None = None


@dataclass
class DraftState:
    pack_num: int  # 1-indexed, as ColName.PACK_NUM
    pick_num: int  # 1-indexed, as ColName.PICK_NUM
    pack_cards: list[DraftCard]
    pick_ind: int | None  # None for pick two draft, use picks_ind
    picks_ind: list[int]
    pool: list[DraftCard]


@dataclass
class Draft:
    expansion: str
    draft_id: str
    picks: list[DraftState]


# DraftCard fields populated from the card file rather than the draft feed
CARD_ATTR_FIELDS = (
    ColName.SET_CODE,
    ColName.COLOR,
    ColName.RARITY,
    ColName.COLOR_IDENTITY,
    ColName.CARD_TYPE,
    ColName.SUBTYPE,
    ColName.MANA_VALUE,
    ColName.MANA_COST,
    ColName.POWER,
    ColName.TOUGHNESS,
    ColName.IS_BONUS_SHEET,
    ColName.IS_DFC,
    ColName.ORACLE_TEXT,
    ColName.SCRYFALL_ID,
)


def _card_attr_map(expansion: str, names: list[str]) -> dict[str, dict]:
    """name -> card attribute row, preferring the local spells card file.

    When no card file exists, write one if the public draft file is present;
    otherwise fall back to a live MTGJSON fetch for the draft's own names.
    Returns an empty map when card data can't be sourced (e.g. cubes, which
    are not MTGJSON sets).
    """
    file_path = cache.data_file_path(expansion, View.CARD)

    if not os.path.isfile(file_path) and os.path.isfile(
        cache.data_file_path(expansion, View.DRAFT)
    ):
        write_card_file(expansion)

    if os.path.isfile(file_path):
        df = pl.read_parquet(file_path)
    else:
        try:
            df = card_df(expansion, names)
        except Exception:
            cache.spells_print("draft", f"No card data available for {expansion}")
            return {}

    return {row[ColName.NAME]: row for row in df.to_dicts()}


def _draft_card(card: dict, attr_map: dict[str, dict]) -> DraftCard:
    attrs = attr_map.get(card["name"], {})
    return DraftCard(
        name=card["name"],
        image_url=card.get("image_url", "") or attrs.get(ColName.IMAGE_URL, ""),
        **{field: attrs.get(field) for field in CARD_ATTR_FIELDS},
    )


def _pick_indices(pack_cards: list[DraftCard], names: list[str]) -> list[int]:
    """Indices of picked cards within the pack; duplicate names consume distinct indices."""
    used: set[int] = set()
    indices = []
    for name in names:
        ind = next(
            (i for i, c in enumerate(pack_cards) if c.name == name and i not in used),
            None,
        )
        if ind is not None:
            used.add(ind)
            indices.append(ind)
    return indices


def _draft_state(
    pick_data: dict, pool: list[DraftCard], attr_map: dict[str, dict]
) -> DraftState:
    """Build one DraftState from a 17lands pick object.

    17lands pack/pick numbers are 0-indexed; the model is 1-indexed.
    """
    pack_cards = [_draft_card(c, attr_map) for c in pick_data["available"]]

    picked_names = [c["name"] for c in pick_data.get("picks") or []]
    picks_ind = _pick_indices(pack_cards, picked_names)

    pick = pick_data.get("pick")
    pick_ind = (
        next((i for i, c in enumerate(pack_cards) if c.name == pick["name"]), None)
        if pick
        else None
    )

    return DraftState(
        pack_num=pick_data["pack_number"] + 1,
        pick_num=pick_data["pick_number"] + 1,
        pack_cards=pack_cards,
        pick_ind=pick_ind,
        picks_ind=picks_ind,
        pool=pool,
    )


def _draft_from_data(
    draft_id: str, data: dict, attr_map: dict[str, dict] | None = None
) -> Draft:
    attr_map = attr_map or {}
    states = []
    pool: list[DraftCard] = []
    for pick_data in sorted(
        data["picks"], key=lambda p: (p["pack_number"], p["pick_number"])
    ):
        state = _draft_state(pick_data, list(pool), attr_map)
        states.append(state)

        picked_ind = state.picks_ind or (
            [state.pick_ind] if state.pick_ind is not None else []
        )
        pool.extend(state.pack_cards[i] for i in picked_ind)

    return Draft(expansion=data["expansion"], draft_id=draft_id, picks=states)


def fetch_draft(draft_id: str, card_data: bool = True) -> Draft:
    """Build a Draft from the 17lands live draft endpoint, cached locally by id.

    Card attributes are joined from MTGJSON data (via the spells card file)
    unless card_data=False.
    """
    target_dir, filename = cache.draft_file_path(draft_id)
    file_path = download_data_file(
        DRAFT_DATA_TEMPLATE.format(draft_id=draft_id), target_dir, filename
    )
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)

    attr_map = None
    if card_data:
        names = sorted(
            {c["name"] for p in data["picks"] for c in p["available"]}
        )
        attr_map = _card_attr_map(data["expansion"], names)

    return _draft_from_data(draft_id, data, attr_map)
