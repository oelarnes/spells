from dataclasses import dataclass
from collections.abc import Callable

import polars as pl

from spells.enums import View, ColName, ColType


@dataclass(frozen=True)
class ColSpec:
    col_type: ColType
    expr: pl.Expr | Callable[..., pl.Expr] | None = None
    views: list[View] | None = None
    version: str | None = None


@dataclass(frozen=True)
class ColDef:
    name: str
    col_type: ColType
    expr: pl.Expr | tuple[pl.Expr, ...]
    views: set[View]
    dependencies: set[str]
    signature: str


default_columns = [
    ColName.COLOR,
    ColName.RARITY,
    ColName.NUM_SEEN,
    ColName.ALSA,
    ColName.NUM_TAKEN,
    ColName.ATA,
    ColName.NUM_GP,
    ColName.PCT_GP,
    ColName.GP_WR,
    ColName.NUM_OH,
    ColName.OH_WR,
    ColName.NUM_GIH,
    ColName.GIH_WR,
]

_specs: dict[str, ColSpec] = {
    ColName.NAME: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.CARD],
    ),
    ColName.EXPANSION: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColName.EVENT_TYPE: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColName.DRAFT_ID: ColSpec(
        views=[View.GAME, View.DRAFT],
        col_type=ColType.FILTER_ONLY,
    ),
    ColName.DRAFT_TIME: ColSpec(
        col_type=ColType.FILTER_ONLY,
        views=[View.GAME, View.DRAFT],
    ),
    ColName.DRAFT_DATE: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.date(),
    ),
    ColName.FORMAT_DAY: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=lambda set_context: (
            pl.col(ColName.DRAFT_DATE) - pl.lit(set_context["release_date"])
        ).dt.total_days()
        + 1,
    ),
    ColName.DRAFT_DAY_OF_WEEK: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME)
        .str.to_datetime("%Y-%m-%d %H:%M:%S")
        .dt.weekday(),
    ),
    ColName.DRAFT_HOUR: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.hour(),
    ),
    ColName.DRAFT_WEEK: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.week(),
    ),
    ColName.FORMAT_WEEK: ColSpec(
        col_type=ColType.GROUP_BY, expr=(pl.col(ColName.FORMAT_DAY) - 1) // 7 + 1
    ),
    ColName.RANK: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColName.USER_N_GAMES_BUCKET: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT, View.GAME],
    ),
    ColName.USER_GAME_WIN_RATE_BUCKET: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT, View.GAME],
    ),
    ColName.PLAYER_COHORT: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.when(pl.col(ColName.USER_N_GAMES_BUCKET) < 100)
        .then(pl.lit("Other"))
        .otherwise(
            pl.when(pl.col(ColName.USER_GAME_WIN_RATE_BUCKET) > 0.57)
            .then(pl.lit("Top"))
            .otherwise(
                pl.when(pl.col(ColName.USER_GAME_WIN_RATE_BUCKET) < 0.49)
                .then(pl.lit("Bottom"))
                .otherwise(pl.lit("Middle"))
            )
        ),
    ),
    ColName.EVENT_MATCH_WINS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT],
    ),
    ColName.EVENT_MATCH_WINS_SUM: ColSpec(
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
        expr=pl.col(ColName.EVENT_MATCH_WINS),
    ),
    ColName.EVENT_MATCH_LOSSES: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT],
    ),
    ColName.EVENT_MATCH_LOSSES_SUM: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.EVENT_MATCH_LOSSES),
    ),
    ColName.EVENT_MATCHES: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.EVENT_MATCH_WINS) + pl.col(ColName.EVENT_MATCH_LOSSES),
    ),
    ColName.EVENT_MATCHES_SUM: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.EVENT_MATCHES),
    ),
    ColName.IS_TROPHY: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.when(pl.col(ColName.EVENT_TYPE) == "Traditional")
        .then(pl.col(ColName.EVENT_MATCH_WINS) == 3)
        .otherwise(pl.col(ColName.EVENT_MATCH_WINS) == 7),
    ),
    ColName.IS_TROPHY_SUM: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.IS_TROPHY),
    ),
    ColName.PACK_NUMBER: ColSpec(
        col_type=ColType.FILTER_ONLY,  # use pack_num
        views=[View.DRAFT],
    ),
    ColName.PACK_NUM: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.PACK_NUMBER) + 1,
    ),
    ColName.PICK_NUMBER: ColSpec(
        col_type=ColType.FILTER_ONLY,  # use pick_num
        views=[View.DRAFT],
    ),
    ColName.PICK_NUM: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.PICK_NUMBER) + 1,
    ),
    ColName.TAKEN_AT: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.PICK_NUM),
    ),
    ColName.NUM_TAKEN: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.when(pl.col(ColName.PICK).is_not_null()).then(1).otherwise(0),
    ),
    ColName.NUM_DRAFTS: ColSpec(
        col_type=ColType.PICK_SUM,
        expr=pl.when(
            (pl.col(ColName.PACK_NUMBER) == 0) & (pl.col(ColName.PICK_NUMBER) == 0)
        )
        .then(1)
        .otherwise(0),
    ),
    ColName.PICK: ColSpec(
        col_type=ColType.FILTER_ONLY,
        views=[View.DRAFT],
    ),
    ColName.PICK_MAINDECK_RATE: ColSpec(
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
    ),
    ColName.PICK_SIDEBOARD_IN_RATE: ColSpec(
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
    ),
    ColName.PACK_CARD: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.DRAFT],
    ),
    ColName.LAST_SEEN: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"pack_card_{name}")
        * pl.min_horizontal(ColName.PICK_NUM, 8),
    ),
    ColName.NUM_SEEN: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"pack_card_{name}") * (pl.col(ColName.PICK_NUM) <= 8),
    ),
    ColName.POOL: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.DRAFT],
    ),
    ColName.GAME_TIME: ColSpec(
        col_type=ColType.FILTER_ONLY,
        views=[View.GAME],
    ),
    ColName.GAME_DATE: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.date(),
    ),
    ColName.GAME_DAY_OF_WEEK: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME)
        .str.to_datetime("%Y-%m-%d %H-%M-%S")
        .dt.weekday(),
    ),
    ColName.GAME_HOUR: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.hour(),
    ),
    ColName.GAME_WEEK: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.week(),
    ),
    ColName.BUILD_INDEX: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.MATCH_NUMBER: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.GAME_NUMBER: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_GAMES: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.GAME_NUMBER).is_not_null(),
    ),
    ColName.NUM_MATCHES: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.GAME_NUMBER) == 1,
    ),
    ColName.NUM_EVENTS: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=(pl.col(ColName.GAME_NUMBER) == 1) & (pl.col(ColName.MATCH_NUMBER) == 1),
    ),
    ColName.OPP_RANK: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.MAIN_COLORS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_COLORS: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.MAIN_COLORS).str.len_chars(),
    ),
    ColName.SPLASH_COLORS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.HAS_SPLASH: ColSpec(
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.SPLASH_COLORS).str.len_chars() > 0,
    ),
    ColName.ON_PLAY: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_ON_PLAY: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.ON_PLAY),
    ),
    ColName.NUM_MULLIGANS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_MULLIGANS_SUM: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.NUM_MULLIGANS),
    ),
    ColName.OPP_NUM_MULLIGANS: ColSpec(
        col_type=ColType.GAME_SUM,
        views=[View.GAME],
    ),
    ColName.OPP_NUM_MULLIGANS_SUM: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.OPP_NUM_MULLIGANS),
    ),
    ColName.OPP_COLORS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_TURNS: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_TURNS_SUM: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.NUM_TURNS),
    ),
    ColName.WON: ColSpec(
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColName.NUM_WON: ColSpec(
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.WON),
    ),
    ColName.OPENING_HAND: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColName.WON_OPENING_HAND: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"opening_hand_{name}") * pl.col(ColName.WON),
    ),
    ColName.DRAWN: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColName.WON_DRAWN: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"drawn_{name}") * pl.col(ColName.WON),
    ),
    ColName.TUTORED: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColName.WON_TUTORED: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"tutored_{name}") * pl.col(ColName.WON),
    ),
    ColName.DECK: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColName.WON_DECK: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"deck_{name}") * pl.col(ColName.WON),
    ),
    ColName.SIDEBOARD: ColSpec(
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColName.WON_SIDEBOARD: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"sideboard_{name}") * pl.col(ColName.WON),
    ),
    ColName.NUM_GNS: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.max_horizontal(
            0,
            pl.col(f"deck_{name}")
            - pl.col(f"drawn_{name}")
            - pl.col(f"tutored_{name}")
            - pl.col(f"opening_hand_{name}"),
        ),
    ),
    ColName.WON_NUM_GNS: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(ColName.WON) * pl.col(f"num_gns_{name}"),
    ),
    ColName.SET_CODE: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.COLOR: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.RARITY: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.COLOR_IDENTITY: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.CARD_TYPE: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.SUBTYPE: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.MANA_VALUE: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.DECK_MANA_VALUE: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name, card_context: card_context[name][ColName.MANA_VALUE]
        * pl.col(f"deck_{name}"),
    ),
    ColName.DECK_LANDS: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name, card_context: pl.col(f"deck_{name}")
        * (1 if "Land" in card_context[name][ColName.CARD_TYPE] else 0),
    ),
    ColName.DECK_SPELLS: ColSpec(
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"deck_{name}") - pl.col(f"deck_lands_{name}"),
    ),
    ColName.MANA_COST: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.POWER: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.TOUGHNESS: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.IS_BONUS_SHEET: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.IS_DFC: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.ORACLE_TEXT: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.CARD_JSON: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.SCRYFALL_ID: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.IMAGE_URL: ColSpec(
        col_type=ColType.CARD_ATTR,
    ),
    ColName.PICKED_MATCH_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.EVENT_MATCH_WINS_SUM) / pl.col(ColName.EVENT_MATCHES_SUM),
    ),
    ColName.TROPHY_RATE: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.IS_TROPHY_SUM) / pl.col(ColName.NUM_TAKEN),
    ),
    ColName.GAME_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_WON) / pl.col(ColName.NUM_GAMES),
    ),
    ColName.ALSA: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.LAST_SEEN) / pl.col(ColName.NUM_SEEN),
    ),
    ColName.ATA: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.TAKEN_AT) / pl.col(ColName.NUM_TAKEN),
    ),
    ColName.NUM_GP: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK),
    ),
    ColName.PCT_GP: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) / (pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD)),
    ),
    ColName.GP_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK) / pl.col(ColName.DECK),
    ),
    ColName.NUM_OH: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND),
    ),
    ColName.OH_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) / pl.col(ColName.OPENING_HAND),
    ),
    ColName.NUM_GIH: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND) + pl.col(ColName.DRAWN),
    ),
    ColName.NUM_GIH_WON: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) + pl.col(ColName.WON_DRAWN),
    ),
    ColName.GIH_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH_WON) / pl.col(ColName.NUM_GIH),
    ),
    ColName.GNS_WR: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_NUM_GNS) / pl.col(ColName.NUM_GNS),
    ),
    ColName.IWD: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR) - pl.col(ColName.GNS_WR),
    ),
    ColName.NUM_IN_POOL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD),
    ),
    ColName.NUM_IN_POOL_TOTAL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_IN_POOL).sum(),
    ),
    ColName.IN_POOL_WR: ColSpec(
        col_type=ColType.AGG,
        expr=(pl.col(ColName.WON_DECK) + pl.col(ColName.WON_SIDEBOARD))
        / pl.col(ColName.NUM_IN_POOL),
    ),
    ColName.DECK_TOTAL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK).sum(),
    ),
    ColName.WON_DECK_TOTAL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK).sum(),
    ),
    ColName.GP_WR_MEAN: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK_TOTAL) / pl.col(ColName.DECK_TOTAL),
    ),
    ColName.GP_WR_EXCESS: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR) - pl.col(ColName.GP_WR_MEAN),
    ),
    ColName.GP_WR_VAR: ColSpec(
        col_type=ColType.AGG,
        expr=(pl.col(ColName.GP_WR_EXCESS).pow(2) * pl.col(ColName.NUM_GP)).sum()
        / pl.col(ColName.DECK_TOTAL),
    ),
    ColName.GP_WR_STDEV: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR_VAR).sqrt(),
    ),
    ColName.GP_WR_Z: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR_EXCESS) / pl.col(ColName.GP_WR_STDEV),
    ),
    ColName.GIH_TOTAL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH).sum(),
    ),
    ColName.WON_GIH_TOTAL: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH_WON).sum(),
    ),
    ColName.GIH_WR_MEAN: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_GIH_TOTAL) / pl.col(ColName.GIH_TOTAL),
    ),
    ColName.GIH_WR_EXCESS: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR) - pl.col(ColName.GIH_WR_MEAN),
    ),
    ColName.GIH_WR_VAR: ColSpec(
        col_type=ColType.AGG,
        expr=(pl.col(ColName.GIH_WR_EXCESS).pow(2) * pl.col(ColName.NUM_GIH)).sum()
        / pl.col(ColName.GIH_TOTAL),
    ),
    ColName.GIH_WR_STDEV: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR_VAR).sqrt(),
    ),
    ColName.GIH_WR_Z: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR_EXCESS) / pl.col(ColName.GIH_WR_STDEV),
    ),
    ColName.DECK_MANA_VALUE_AVG: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_MANA_VALUE) / pl.col(ColName.DECK_SPELLS),
    ),
    ColName.DECK_LANDS_AVG: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_LANDS) / pl.col(ColName.NUM_GAMES),
    ),
    ColName.DECK_SPELLS_AVG: ColSpec(
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_SPELLS) / pl.col(ColName.NUM_GAMES),
    ),
}

for item in ColName:
    assert item in _specs, f"column {item} enumerated but not specified"


def get_specs():
    return dict(_specs)
