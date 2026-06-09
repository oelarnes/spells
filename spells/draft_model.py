"""Object model for individual drafts.

A `Draft` is a succession of `DraftState`s, one per pack/pick index. The
chosen card is identified by index into `pack_cards` (`pick_ind`, or
`picks_ind` for pick-two formats), and `pool` accumulates prior picks.

Cards carry identity only (no metrics); annotate with metric data by joining
on name downstream.
"""

import json
from dataclasses import dataclass

from spells import cache
from spells.card_data_files import download_data_file

DRAFT_DATA_TEMPLATE = "https://www.17lands.com/data/draft?draft_id={draft_id}"


@dataclass
class DraftCard:
    name: str
    set_code: str | None = None  # the card's own printing set; not in the draft feed
    image_url: str = ""


@dataclass
class DraftState:
    pack_num: int  # 1-indexed, as ColName.PACK_NUM
    pick_num: int  # 1-indexed, as ColName.PICK_NUM
    pack_cards: list[DraftCard]
    pick_ind: int | None
    picks_ind: list[int]
    pool: list[DraftCard]


@dataclass
class Draft:
    expansion: str
    draft_id: str
    picks: list[DraftState]


def _draft_card(card: dict) -> DraftCard:
    return DraftCard(name=card["name"], image_url=card.get("image_url", ""))


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


def _draft_state(pick_data: dict, pool: list[DraftCard]) -> DraftState:
    """Build one DraftState from a 17lands pick object.

    17lands pack/pick numbers are 0-indexed; the model is 1-indexed.
    """
    pack_cards = [_draft_card(c) for c in pick_data["available"]]

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


def _draft_from_data(draft_id: str, data: dict) -> Draft:
    states = []
    pool: list[DraftCard] = []
    for pick_data in sorted(
        data["picks"], key=lambda p: (p["pack_number"], p["pick_number"])
    ):
        state = _draft_state(pick_data, list(pool))
        states.append(state)

        picked_ind = state.picks_ind or (
            [state.pick_ind] if state.pick_ind is not None else []
        )
        pool.extend(state.pack_cards[i] for i in picked_ind)

    return Draft(expansion=data["expansion"], draft_id=draft_id, picks=states)


def fetch_draft(draft_id: str) -> Draft:
    """Build a Draft from the 17lands live draft endpoint, cached locally by id."""
    target_dir, filename = cache.draft_file_path(draft_id)
    file_path = download_data_file(
        DRAFT_DATA_TEMPLATE.format(draft_id=draft_id), target_dir, filename
    )
    with open(file_path, encoding="utf-8") as f:
        data = json.load(f)
    return _draft_from_data(draft_id, data)
