from dataclasses import dataclass
from collections.abc import Callable

import polars as pl

from spells.enums import View, ColName, ColType


@dataclass(frozen=True)
class ColSpec:
    name: str
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

_column_specs = [
    ColSpec(
        name=ColName.NAME,
        col_type=ColType.GROUP_BY,
        views=[View.CARD],
    ),
    ColSpec(
        name=ColName.EXPANSION,
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColSpec(
        name=ColName.EVENT_TYPE,
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColSpec(
        name=ColName.DRAFT_ID,
        views=[View.GAME, View.DRAFT],
        col_type=ColType.FILTER_ONLY,
    ),
    ColSpec(
        name=ColName.DRAFT_TIME,
        col_type=ColType.FILTER_ONLY,
        views=[View.GAME, View.DRAFT],
    ),
    ColSpec(
        name=ColName.DRAFT_DATE,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.date(),
    ),
    ColSpec(
        name=ColName.DRAFT_DAY_OF_WEEK,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.weekday(),
    ),
    ColSpec(
        name=ColName.DRAFT_HOUR,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.hour(),
    ),
    ColSpec(
        name=ColName.DRAFT_WEEK,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.DRAFT_TIME).str.to_datetime("%Y-%m-%d %H:%M:%S").dt.week(),
    ),
    ColSpec(
        name=ColName.RANK,
        col_type=ColType.GROUP_BY,
        views=[View.GAME, View.DRAFT],
    ),
    ColSpec(
        name=ColName.USER_N_GAMES_BUCKET,
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT, View.GAME],
    ),
    ColSpec(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT, View.GAME],
    ),
    ColSpec(
        name=ColName.PLAYER_COHORT,
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
    ColSpec(
        name=ColName.EVENT_MATCH_WINS,
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.EVENT_MATCH_WINS_SUM,
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
        expr=pl.col(ColName.EVENT_MATCH_WINS),
    ),
    ColSpec(
        name=ColName.EVENT_MATCH_LOSSES,
        col_type=ColType.GROUP_BY,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.EVENT_MATCH_LOSSES_SUM,
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.EVENT_MATCH_LOSSES),
    ),
    ColSpec(
        name=ColName.EVENT_MATCHES,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.EVENT_MATCH_WINS) + pl.col(ColName.EVENT_MATCH_LOSSES),
    ),
    ColSpec(
        name=ColName.EVENT_MATCHES_SUM,
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.EVENT_MATCHES),
    ),
    ColSpec(
        name=ColName.IS_TROPHY,
        col_type=ColType.GROUP_BY,
        expr=pl.when(pl.col(ColName.EVENT_TYPE) == "Traditional")
        .then(pl.col(ColName.EVENT_MATCH_WINS) == 3)
        .otherwise(pl.col(ColName.EVENT_MATCH_WINS) == 7),
    ),
    ColSpec(
        name=ColName.IS_TROPHY_SUM,
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.IS_TROPHY),
    ),
    ColSpec(
        name=ColName.PACK_NUMBER,
        col_type=ColType.FILTER_ONLY,  # use pack_num
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.PACK_NUM,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.PACK_NUMBER) + 1,
    ),
    ColSpec(
        name=ColName.PICK_NUMBER,
        col_type=ColType.FILTER_ONLY,  # use pick_num
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.PICK_NUM,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.PICK_NUMBER) + 1,
    ),
    ColSpec(
        name=ColName.TAKEN_AT,
        col_type=ColType.PICK_SUM,
        expr=pl.col(ColName.PICK_NUM),
    ),
    ColSpec(
        name=ColName.NUM_TAKEN,
        col_type=ColType.PICK_SUM,
        expr=pl.when(pl.col(ColName.PICK).is_not_null())
        .then(1)
        .otherwise(0),
    ),
    ColSpec(
        name=ColName.NUM_DRAFTS,
        col_type=ColType.PICK_SUM,
        expr=pl.when((pl.col(ColName.PACK_NUMBER) == 0) & (pl.col(ColName.PICK_NUMBER) == 0)).then(1).otherwise(0),
    ),
    ColSpec(
        name=ColName.PICK,
        col_type=ColType.FILTER_ONLY,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.PICK_MAINDECK_RATE,
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.PICK_SIDEBOARD_IN_RATE,
        col_type=ColType.PICK_SUM,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.PACK_CARD,
        col_type=ColType.NAME_SUM,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.LAST_SEEN,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"pack_card_{name}")
        * pl.min_horizontal(ColName.PICK_NUM, 8),
    ),
    ColSpec(
        name=ColName.NUM_SEEN,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"pack_card_{name}") * (pl.col(ColName.PICK_NUM) <= 8),
    ),
    ColSpec(
        name=ColName.POOL,
        col_type=ColType.NAME_SUM,
        views=[View.DRAFT],
    ),
    ColSpec(
        name=ColName.GAME_TIME,
        col_type=ColType.FILTER_ONLY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.GAME_DATE,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.date(),
    ),
    ColSpec(
        name=ColName.GAME_DAY_OF_WEEK,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.weekday(),
    ),
    ColSpec(
        name=ColName.GAME_HOUR,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.hour(),
    ),
    ColSpec(
        name=ColName.GAME_WEEK,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.GAME_TIME).str.to_datetime("%Y-%m-%d %H-%M-%S").dt.week(),
    ),
    ColSpec(
        name=ColName.BUILD_INDEX,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.MATCH_NUMBER,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.GAME_NUMBER,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_GAMES,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.GAME_NUMBER).is_not_null(),
    ),
    ColSpec(
        name=ColName.NUM_MATCHES,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.GAME_NUMBER) == 1,
    ),
    ColSpec(
        name=ColName.NUM_EVENTS,
        col_type=ColType.GAME_SUM,
        expr=(pl.col(ColName.GAME_NUMBER) == 1) & (pl.col(ColName.MATCH_NUMBER) == 1),
    ),
    ColSpec(
        name=ColName.OPP_RANK,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.MAIN_COLORS,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_COLORS,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.MAIN_COLORS).str.len_chars(),
    ),
    ColSpec(
        name=ColName.SPLASH_COLORS,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.HAS_SPLASH,
        col_type=ColType.GROUP_BY,
        expr=pl.col(ColName.SPLASH_COLORS).str.len_chars() > 0,
    ),
    ColSpec(
        name=ColName.ON_PLAY,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_ON_PLAY,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.ON_PLAY),
    ),
    ColSpec(
        name=ColName.NUM_MULLIGANS,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_MULLIGANS_SUM,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.NUM_MULLIGANS),
    ),
    ColSpec(
        name=ColName.OPP_NUM_MULLIGANS,
        col_type=ColType.GAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.OPP_NUM_MULLIGANS_SUM,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.OPP_NUM_MULLIGANS),
    ),
    ColSpec(
        name=ColName.OPP_COLORS,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_TURNS,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_TURNS_SUM,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.NUM_TURNS),
    ),
    ColSpec(
        name=ColName.WON,
        col_type=ColType.GROUP_BY,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.NUM_WON,
        col_type=ColType.GAME_SUM,
        expr=pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.OPENING_HAND,
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.WON_OPENING_HAND,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"opening_hand_{name}") * pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.DRAWN,
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.WON_DRAWN,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"drawn_{name}") * pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.TUTORED,
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.WON_TUTORED,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"tutored_{name}") * pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.DECK,
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.WON_DECK,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"deck_{name}") * pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.SIDEBOARD,
        col_type=ColType.NAME_SUM,
        views=[View.GAME],
    ),
    ColSpec(
        name=ColName.WON_SIDEBOARD,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"sideboard_{name}") * pl.col(ColName.WON),
    ),
    ColSpec(
        name=ColName.NUM_GNS,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.max_horizontal(
            0,
            pl.col(f"deck_{name}")
            - pl.col(f"drawn_{name}")
            - pl.col(f"tutored_{name}")
            - pl.col(f"opening_hand_{name}"),
        ),
    ),
    ColSpec(
        name=ColName.WON_NUM_GNS,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(ColName.WON) * pl.col(f"num_gns_{name}"),
    ),
    ColSpec(
        name=ColName.SET_CODE,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.COLOR,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.RARITY,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.COLOR_IDENTITY,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.CARD_TYPE,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.SUBTYPE,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.MANA_VALUE,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.DECK_MANA_VALUE,
        col_type=ColType.NAME_SUM,
        expr=lambda name, card_context: card_context[name][ColName.MANA_VALUE] * pl.col(f"deck_{name}"),
    ),
    ColSpec(
        name=ColName.DECK_LANDS,
        col_type=ColType.NAME_SUM,
        expr=lambda name, card_context: pl.col(f"deck_{name}") * ( 1 if 'Land' in card_context[name][ColName.CARD_TYPE] else 0 )
    ),
    ColSpec(
        name=ColName.DECK_SPELLS,
        col_type=ColType.NAME_SUM,
        expr=lambda name: pl.col(f"deck_{name}") - pl.col(f"deck_lands_{name}"),
    ),
    ColSpec(
        name=ColName.MANA_COST,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.POWER,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.TOUGHNESS,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.IS_BONUS_SHEET,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.IS_DFC,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.ORACLE_TEXT,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.CARD_JSON,
        col_type=ColType.CARD_ATTR,
    ),
    ColSpec(
        name=ColName.PICKED_MATCH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.EVENT_MATCH_WINS_SUM) / pl.col(ColName.EVENT_MATCHES_SUM),
    ),
    ColSpec(
        name=ColName.TROPHY_RATE,
        col_type=ColType.AGG,
        expr=pl.col(ColName.IS_TROPHY_SUM) / pl.col(ColName.NUM_TAKEN),
    ),
    ColSpec(
        name=ColName.GAME_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_WON) / pl.col(ColName.NUM_GAMES),
    ),
    ColSpec(
        name=ColName.ALSA,
        col_type=ColType.AGG,
        expr=pl.col(ColName.LAST_SEEN) / pl.col(ColName.NUM_SEEN),
    ),
    ColSpec(
        name=ColName.ATA,
        col_type=ColType.AGG,
        expr=pl.col(ColName.TAKEN_AT) / pl.col(ColName.NUM_TAKEN),
    ),
    ColSpec(
        name=ColName.NUM_GP,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK),
    ),
    ColSpec(
        name=ColName.PCT_GP,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) / (pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD)),
    ),
    ColSpec(
        name=ColName.GP_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK) / pl.col(ColName.DECK),
    ),
    ColSpec(
        name=ColName.NUM_OH,
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND),
    ),
    ColSpec(
        name=ColName.OH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) / pl.col(ColName.OPENING_HAND),
    ),
    ColSpec(
        name=ColName.NUM_GIH,
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND) + pl.col(ColName.DRAWN),
    ),
    ColSpec(
        name=ColName.NUM_GIH_WON,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) + pl.col(ColName.WON_DRAWN),
    ),
    ColSpec(
        name=ColName.GIH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH_WON) / pl.col(ColName.NUM_GIH),
    ),
    ColSpec(
        name=ColName.GNS_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_NUM_GNS) / pl.col(ColName.NUM_GNS),
    ),
    ColSpec(
        name=ColName.IWD,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR) - pl.col(ColName.GNS_WR),
    ),
    ColSpec(
        name=ColName.NUM_IN_POOL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD),
    ),
    ColSpec(
        name=ColName.IN_POOL_WR,
        col_type=ColType.AGG,
        expr=(pl.col(ColName.WON_DECK) + pl.col(ColName.WON_SIDEBOARD))
        / pl.col(ColName.NUM_IN_POOL),
    ),
    ColSpec(
        name=ColName.DECK_TOTAL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK).sum(),
    ),
    ColSpec(
        name=ColName.WON_DECK_TOTAL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK).sum(),
    ),
    ColSpec(
        name=ColName.GP_WR_MEAN,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK_TOTAL) / pl.col(ColName.DECK_TOTAL),
    ),
    ColSpec(
        name=ColName.GP_WR_EXCESS,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR) - pl.col(ColName.GP_WR_MEAN),
    ),
    ColSpec(
        name=ColName.GP_WR_VAR,
        col_type=ColType.AGG,
        expr=(pl.col(ColName.GP_WR_EXCESS).pow(2) * pl.col(ColName.NUM_GP)).sum()
        / pl.col(ColName.DECK_TOTAL),
    ),
    ColSpec(
        name=ColName.GP_WR_STDEV,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR_VAR).sqrt(),
    ),
    ColSpec(
        name=ColName.GP_WR_Z,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GP_WR_EXCESS) / pl.col(ColName.GP_WR_STDEV),
    ),
    ColSpec(
        name=ColName.GIH_TOTAL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH).sum(),
    ),
    ColSpec(
        name=ColName.WON_GIH_TOTAL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH_WON).sum(),
    ),
    ColSpec(
        name=ColName.GIH_WR_MEAN,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_GIH_TOTAL) / pl.col(ColName.GIH_TOTAL),
    ),
    ColSpec(
        name=ColName.GIH_WR_EXCESS,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR) - pl.col(ColName.GIH_WR_MEAN),
    ),
    ColSpec(
        name=ColName.GIH_WR_VAR,
        col_type=ColType.AGG,
        expr=(pl.col(ColName.GIH_WR_EXCESS).pow(2) * pl.col(ColName.NUM_GIH)).sum()
        / pl.col(ColName.GIH_TOTAL),
    ),
    ColSpec(
        name=ColName.GIH_WR_STDEV,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR_VAR).sqrt(),
    ),
    ColSpec(
        name=ColName.GIH_WR_Z,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR_EXCESS) / pl.col(ColName.GIH_WR_STDEV),
    ),
    ColSpec(
        name=ColName.DECK_MANA_VALUE_AVG,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_MANA_VALUE) / pl.col(ColName.DECK_SPELLS),
    ),
    ColSpec(
        name=ColName.DECK_LANDS_AVG,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_LANDS) / pl.col(ColName.NUM_GAMES),
    ),
    ColSpec(
        name=ColName.DECK_SPELLS_AVG,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK_SPELLS) / pl.col(ColName.NUM_GAMES),
    ),
]

col_spec_map = {col.name: col for col in _column_specs}

for item in ColName:
    assert item in col_spec_map, f"column {item} enumerated but not specified"
