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
import random
import warnings
from collections import Counter
from dataclasses import dataclass
import itertools

import polars as pl

from spells import cache
from spells.card_data_files import download_data_file
from spells.cards import card_df, write_card_file
from spells.enums import ColName, View

DRAFT_DATA_TEMPLATE = "https://www.17lands.com/data/draft?draft_id={draft_id}"

PACK_CARD_PREFIX = f"{ColName.PACK_CARD}_"
POOL_PREFIX = f"{ColName.POOL}_"


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
    # per-pick annotations from the draft view; None on the live feed path
    pick_maindeck_rate: float | None = None
    pick_sideboard_in_rate: float | None = None


@dataclass
class Draft:
    expansion: str
    draft_id: str
    picks: list[DraftState]
    # draft-level metadata from the draft view; None on the live feed path
    event_type: str | None = None
    draft_time: str | None = None
    rank: str | None = None
    event_match_wins: int | None = None
    event_match_losses: int | None = None
    user_n_games_bucket: int | None = None
    user_game_win_rate_bucket: float | None = None


# Draft fields read from / written to the draft view by column name
DRAFT_META_FIELDS = (
    ColName.EVENT_TYPE,
    ColName.DRAFT_TIME,
    ColName.RANK,
    ColName.EVENT_MATCH_WINS,
    ColName.EVENT_MATCH_LOSSES,
    ColName.USER_N_GAMES_BUCKET,
    ColName.USER_GAME_WIN_RATE_BUCKET,
)


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
    attrs = attr_map.get(card[ColName.NAME], {})
    return DraftCard(
        name=card[ColName.NAME],
        image_url=card.get(ColName.IMAGE_URL, "") or attrs.get(ColName.IMAGE_URL, ""),
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


def _pick_inds(
    pack_cards: list[DraftCard], picked_names: list[str]
) -> tuple[int | None, list[int]]:
    """pick_ind is the single pick; None for pick-two, where picks_ind holds both."""
    picks_ind = _pick_indices(pack_cards, picked_names)
    pick_ind = picks_ind[0] if len(picks_ind) == 1 else None
    return pick_ind, picks_ind


def _reconcile_pool(
    pool_counts: Counter, acc_pool: list[DraftCard], attr_map: dict[str, dict]
) -> list[DraftCard]:
    """Accumulated picks, prepended with pool cards picks don't explain.

    Each source's own pool is incomplete in one direction: the view's pool_
    columns omit pick_2 cards in PickTwoDraft files (kept here via the
    accumulation), while pure accumulation misses cards seeded into the pool
    outside of picks, e.g. starting promos (recovered here from pool_counts).
    """
    extra = pool_counts - Counter(c.name for c in acc_pool)
    extra_cards = [
        card
        for name, count in extra.items()
        for card in [_draft_card({ColName.NAME: name}, attr_map)] * count
    ]
    return extra_cards + list(acc_pool)


def _gap_states(
    pack_num_0: int,
    first_pick_0: int,
    pool_counts: Counter,
    acc_pool: list[DraftCard],
    attr_map: dict[str, dict],
) -> list[DraftState]:
    """Synthesize DraftStates for pick rows absent from the draft view.

    A known Arena client bug caused picks to go unrecorded in several sets;
    the 17lands parquet strips those rows entirely while the live API returns
    them with available=[picked_card]. The picked card is inferred from the
    pool_ columns at the first recorded row minus accumulated pool.
    """
    extra = pool_counts - Counter(c.name for c in acc_pool)
    inferred = [
        _draft_card({ColName.NAME: name}, attr_map)
        for name, count in extra.items()
        for _ in range(count)
    ]
    states: list[DraftState] = []
    local_pool = list(acc_pool)
    for i in range(first_pick_0):
        picked = inferred[i] if i < len(inferred) else None
        pack_cards = [picked] if picked else []
        states.append(
            DraftState(
                pack_num=pack_num_0 + 1,
                pick_num=i + 1,
                pack_cards=pack_cards,
                pick_ind=0 if picked else None,
                picks_ind=[0] if picked else [],
                pool=list(local_pool),
            )
        )
        if picked:
            local_pool.append(picked)
    return states


def _draft_state(
    pick_data: dict, pool: list[DraftCard], attr_map: dict[str, dict]
) -> DraftState:
    """Build one DraftState from a 17lands pick object.

    17lands pack/pick numbers are 0-indexed; the model is 1-indexed.
    """
    pack_cards = [_draft_card(c, attr_map) for c in pick_data["available"]]

    picked_names = [c[ColName.NAME] for c in pick_data.get("picks") or []]
    if not picked_names and (pick := pick_data.get(ColName.PICK)):
        picked_names = [pick[ColName.NAME]]
    pick_ind, picks_ind = _pick_inds(pack_cards, picked_names)

    # the feed's sections (maindeck/sideboard columns) are its view of the pool
    section_counts = Counter(
        c[ColName.NAME]
        for section in pick_data.get("sections") or []
        for column in section.get("cards") or []
        for c in column
    )

    return DraftState(
        pack_num=pick_data[ColName.PACK_NUMBER] + 1,
        pick_num=pick_data[ColName.PICK_NUMBER] + 1,
        pack_cards=pack_cards,
        pick_ind=pick_ind,
        picks_ind=picks_ind,
        pool=_reconcile_pool(section_counts, pool, attr_map),
    )


def _draft_from_data(
    draft_id: str, data: dict, attr_map: dict[str, dict] | None = None
) -> Draft:
    attr_map = attr_map or {}
    states = []
    pool: list[DraftCard] = []
    for pick_data in sorted(
        data["picks"],
        key=lambda p: (p[ColName.PACK_NUMBER], p[ColName.PICK_NUMBER]),
    ):
        state = _draft_state(pick_data, list(pool), attr_map)
        states.append(state)
        pool.extend(state.pack_cards[i] for i in state.picks_ind)

    return Draft(expansion=data[ColName.EXPANSION], draft_id=draft_id, picks=states)


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
            {c[ColName.NAME] for p in data["picks"] for c in p["available"]}
        )
        attr_map = _card_attr_map(data[ColName.EXPANSION], names)

    return _draft_from_data(draft_id, data, attr_map)


def _collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect with the streaming engine so large draft views stay off-heap."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            "The old streaming engine is being deprecated",
            DeprecationWarning,
        )
        return lf.collect(streaming=True)


def _cards_from_counts(
    row: dict, prefix: str, attr_map: dict[str, dict]
) -> list[DraftCard]:
    """Expand a view row's prefixed count columns into cards, with multiplicity."""
    return [
        card
        for col, count in row.items()
        if col.startswith(prefix) and count
        for card in [_draft_card({ColName.NAME: col[len(prefix):]}, attr_map)] * count
    ]


def _state_from_row(
    row: dict, pool: list[DraftCard], attr_map: dict[str, dict]
) -> DraftState:
    """Build one DraftState from a draft view row (0-indexed -> 1-indexed).

    The pool reconciles the caller-accumulated picks against the row's pool_
    columns; see _reconcile_pool.
    """
    pack_cards = _cards_from_counts(row, PACK_CARD_PREFIX, attr_map)
    picked_names = [
        name for name in (row.get(ColName.PICK), row.get(ColName.PICK_2)) if name
    ]
    pick_ind, picks_ind = _pick_inds(pack_cards, picked_names)

    pool_counts = Counter(
        {
            col[len(POOL_PREFIX):]: count
            for col, count in row.items()
            if col.startswith(POOL_PREFIX) and count
        }
    )

    return DraftState(
        pack_num=row[ColName.PACK_NUMBER] + 1,
        pick_num=row[ColName.PICK_NUMBER] + 1,
        pack_cards=pack_cards,
        pick_ind=pick_ind,
        picks_ind=picks_ind,
        pool=_reconcile_pool(pool_counts, pool, attr_map),
        pick_maindeck_rate=row.get(ColName.PICK_MAINDECK_RATE),
        pick_sideboard_in_rate=row.get(ColName.PICK_SIDEBOARD_IN_RATE),
    )


def view_draft(
    set_code: str,
    draft_id: str | None = None,
    *,
    event_type: cache.EventType = cache.EventType.PREMIER,
    filter_expr: pl.Expr | None = None,
    seed: int | None = None,
    card_data: bool = True,
) -> Draft:
    """Build a Draft from the local draft view (public 17lands dataset).

    Look up by draft_id, or sample a random draft from the rows matching
    filter_expr (a polars expression over the raw view columns). Sampling is
    two-pass and streaming: first collect just the matching draft ids, then
    the ~45 rows of the chosen draft -- the full view is never materialized.
    """
    lf = pl.scan_parquet(cache.data_file_path(set_code, View.DRAFT, event_type))

    if draft_id is None:
        id_lf = lf if filter_expr is None else lf.filter(filter_expr)
        ids = _collect(id_lf.select(ColName.DRAFT_ID).unique().sort(ColName.DRAFT_ID))[
            ColName.DRAFT_ID
        ]
        if len(ids) == 0:
            raise ValueError(f"no drafts match filter for {set_code}")
        draft_id = ids[random.Random(seed).randrange(len(ids))]

    rows = (
        _collect(lf.filter(pl.col(ColName.DRAFT_ID) == draft_id))
        .sort(ColName.PACK_NUMBER, ColName.PICK_NUMBER)
        .to_dicts()
    )
    if not rows:
        raise ValueError(f"draft {draft_id} not found in {set_code} draft view")

    attr_map = {}
    if card_data:
        names = sorted(
            col[len(PACK_CARD_PREFIX):]
            for col in rows[0]
            if col.startswith(PACK_CARD_PREFIX)
        )
        attr_map = _card_attr_map(set_code, names)

    states = []
    pool: list[DraftCard] = []
    for pack_num_0, pack_rows in itertools.groupby(rows, key=lambda r: r[ColName.PACK_NUMBER]):
        pack_rows = list(pack_rows)
        first_pick = pack_rows[0][ColName.PICK_NUMBER]
        if first_pick > 0:
            pool_counts = Counter(
                {
                    col[len(POOL_PREFIX):]: count
                    for col, count in pack_rows[0].items()
                    if col.startswith(POOL_PREFIX) and count
                }
            )
            for s in _gap_states(pack_num_0, first_pick, pool_counts, list(pool), attr_map):
                states.append(s)
                pool.extend(s.pack_cards[i] for i in s.picks_ind)
        for row in pack_rows:
            state = _state_from_row(row, list(pool), attr_map)
            states.append(state)
            pool.extend(state.pack_cards[i] for i in state.picks_ind)

    return Draft(
        expansion=rows[0][ColName.EXPANSION],
        draft_id=draft_id,
        picks=states,
        **{field: rows[0].get(field) for field in DRAFT_META_FIELDS},
    )


def draft_view_df(draft: Draft) -> pl.DataFrame:
    """Render a Draft as a dataframe matching the draft view schema.

    One row per DraftState, with the model's 1-indexed numbers written back
    as the view's 0-indexed ones. When the local draft view exists for the
    expansion its exact schema (column order and dtypes) is used; otherwise
    a conforming schema is constructed from the draft's own card names.
    Fields the model doesn't carry come out null; the view's pool ordering
    (counts) loses the model's pick-order pool.
    """
    file_path = cache.data_file_path(draft.expansion, View.DRAFT)
    if os.path.isfile(file_path):
        target_schema = pl.scan_parquet(file_path).collect_schema()
    else:
        names = sorted(
            {c.name for s in draft.picks for c in [*s.pack_cards, *s.pool]}
        )
        has_pick_2 = any(len(s.picks_ind) > 1 for s in draft.picks)
        meta_schema = {
            ColName.EXPANSION: pl.String,
            ColName.EVENT_TYPE: pl.String,
            ColName.DRAFT_ID: pl.String,
            ColName.DRAFT_TIME: pl.String,
            ColName.RANK: pl.String,
            ColName.EVENT_MATCH_WINS: pl.Int8,
            ColName.EVENT_MATCH_LOSSES: pl.Int8,
            ColName.PACK_NUMBER: pl.Int8,
            ColName.PICK_NUMBER: pl.Int8,
            ColName.PICK: pl.String,
            **({ColName.PICK_2: pl.String} if has_pick_2 else {}),
            ColName.PICK_MAINDECK_RATE: pl.Float64,
            ColName.PICK_SIDEBOARD_IN_RATE: pl.Float64,
            ColName.USER_N_GAMES_BUCKET: pl.Int16,
            ColName.USER_GAME_WIN_RATE_BUCKET: pl.Float64,
        }
        target_schema = pl.Schema(
            {
                **{str(k): v for k, v in meta_schema.items()},
                **{f"{POOL_PREFIX}{n}": pl.Int8 for n in names},
                **{f"{PACK_CARD_PREFIX}{n}": pl.Int8 for n in names},
            }
        )

    rows = []
    for state in draft.picks:
        pack_counts = Counter(c.name for c in state.pack_cards)
        pool_counts = Counter(c.name for c in state.pool)
        picked = [state.pack_cards[i].name for i in state.picks_ind]
        row = {
            ColName.EXPANSION: draft.expansion,
            ColName.DRAFT_ID: draft.draft_id,
            ColName.PACK_NUMBER: state.pack_num - 1,
            ColName.PICK_NUMBER: state.pick_num - 1,
            ColName.PICK: picked[0] if picked else None,
            ColName.PICK_2: picked[1] if len(picked) > 1 else None,
            ColName.PICK_MAINDECK_RATE: state.pick_maindeck_rate,
            ColName.PICK_SIDEBOARD_IN_RATE: state.pick_sideboard_in_rate,
            **{field: getattr(draft, field) for field in DRAFT_META_FIELDS},
        }
        rows.append(
            {
                col: row.get(
                    col,
                    pack_counts.get(col[len(PACK_CARD_PREFIX):], 0)
                    if col.startswith(PACK_CARD_PREFIX)
                    else pool_counts.get(col[len(POOL_PREFIX):], 0)
                    if col.startswith(POOL_PREFIX)
                    else None,
                )
                for col in target_schema.names()
            }
        )

    return pl.DataFrame(rows, schema=target_schema)
