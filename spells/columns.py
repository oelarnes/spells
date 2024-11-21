"""
extension configuration for custom columns.

The extensions defined here can be passed to the appropriate
dataframe function by enum or enum value, as `extensions = []`, etc.

By default no extensions are used, since they are not needed for the base metrics.

Custom extensions can be defined via draft_data_obj.register_extension(
    <view name>, <extension name>, <extension expr>
)

Note that the extension expr must match the signature provided in the examples here.
"""

import re
from dataclasses import dataclass

import polars as pl

from spells.enums import View, ColName, ColType

@dataclass
class ColumnDefinition:
    """
    if a column has dependencies that are not in the base view, all of its immediate
    dependencies must be called out.
    If all dependencies are in the base view, they do not need to be called out.
    """

    name: str
    col_type: ColType
    expr: pl.functions.col.Col | None = None
    views: tuple[View, ...] = ()
    dependencies: list[str] | None = None
    
    def name_sum_rename(
        self,
        old_name: str,
    ):
        if self.dependencies is None:
            raise ValueError("name_sum columns must name their dependencies")
        name_sum_dep = self.dependencies[0]
        name_pattern = f"^{name_sum_dep}_"
        card_name = re.split(name_pattern, old_name)[1]
        return self.name + "_" + card_name

    def __post_init__(self):
        if self.expr is not None:
            if self.col_type == ColType.NAME_SUM:
                self.expr = self.expr.name.map(self.name_sum_rename)
            else:
                self.expr = self.expr.alias(self.name)
        else:
            if self.col_type == ColType.NAME_SUM:
                self.expr = pl.col(f"^{self.name}_.*$")
            else:
                self.expr = pl.col(self.name)


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

_column_defs = [
    ColumnDefinition(
        name=ColName.DRAFT_ID,
        views=(View.GAME, View.DRAFT),
        col_type=ColType.FILTER_ONLY,
    ),
    ColumnDefinition(
        name=ColName.DRAFT_TIME,
        col_type=ColType.FILTER_ONLY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    ),
    ColumnDefinition(
        name=ColName.DRAFT_DATE,
        col_type=ColType.GROUP_BY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.date(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_DAY_OF_WEEK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.weekday(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_HOUR,
        col_type=ColType.GROUP_BY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.hour(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_WEEK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.week(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.RANK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME, View.DRAFT),
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_WINS,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_WINS_SUM,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.col(ColName.EVENT_MATCH_WINS),
        dependencies=[ColName.EVENT_MATCH_WINS],
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_LOSSES,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_LOSSES_SUM,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.col(ColName.EVENT_MATCH_LOSSES),
        dependencies=[ColName.EVENT_MATCH_LOSSES],
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCHES,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
        expr=pl.col("event_match_wins") + pl.col("event_match_losses"),
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCHES_SUM,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.col(ColName.EVENT_MATCHES),
        dependencies=[ColName.EVENT_MATCHES],
    ),
    ColumnDefinition(
        name=ColName.IS_TROPHY,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
        expr=pl.when(pl.col("event_type") == "Traditional")
        .then(pl.col("event_match_wins") == 3)
        .otherwise(pl.col("event_match_wins") == 7),
    ),
    ColumnDefinition(
        name=ColName.IS_TROPHY_SUM,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.col(ColName.IS_TROPHY),
        dependencies=[ColName.IS_TROPHY],
    ),
    ColumnDefinition(
        name=ColName.PACK_NUMBER,
        col_type=ColType.FILTER_ONLY,  # use pack_num
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PACK_NUM,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
        expr=pl.col("pack_number") + 1,
    ),
    ColumnDefinition(
        name=ColName.PICK_NUMBER,
        col_type=ColType.FILTER_ONLY,  # use pick_num
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PICK_NUM,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT,),
        expr=pl.col("pick_number") + 1,
    ),
    ColumnDefinition(
        name=ColName.TAKEN_AT,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.col(ColName.PICK_NUM),
        dependencies=[ColName.PICK_NUM],
    ),
    ColumnDefinition(
        name=ColName.NUM_TAKEN,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
        expr=pl.when(pl.col(ColName.PICK).is_not_null())
        .then(1)
        .otherwise(0),  # a literal returns one row under select alone
    ),
    ColumnDefinition(
        name=ColName.PICK,
        col_type=ColType.FILTER_ONLY,  # aggregated as "name"
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.NAME,
        col_type=ColType.GROUP_BY,
        views=(),
        # handled by internals, derived from both 'pick' and "name mapped" columns
    ),
    ColumnDefinition(
        name=ColName.PICK_MAINDECK_RATE,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PICK_SIDEBOARD_IN_RATE,
        col_type=ColType.PICK_SUM,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PACK_CARD,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PACK_NUM_CARD,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.col("pack_num"),
        dependencies=[ColName.PACK_CARD, ColName.PACK_NUM],
    ),
    ColumnDefinition(
        name=ColName.PICK_NUM_CARD,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.col("pick_num"),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
    ),
    ColumnDefinition(
        name=ColName.LAST_SEEN,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.min_horizontal("pick_num", 8),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
    ),
    ColumnDefinition(
        name=ColName.NUM_SEEN,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * (pl.col("pick_num") <= 8),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
    ),
    ColumnDefinition(
        name=ColName.POOL,
        col_type=ColType.NAME_SUM,
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.USER_N_GAMES_BUCKET,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT, View.GAME),
    ),
    ColumnDefinition(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT, View.GAME),
    ),
    ColumnDefinition(
        name=ColName.PLAYER_COHORT,
        col_type=ColType.GROUP_BY,
        views=(View.DRAFT, View.GAME),
        expr=pl.when(pl.col("user_n_games_bucket") < 100)
        .then(pl.lit("All"))
        .otherwise(
            pl.when(pl.col("user_game_win_rate_bucket") > 0.57)
            .then(pl.lit("Top"))
            .otherwise(
                pl.when(pl.col("user_game_win_rate_bucket") < 0.49)
                .then(pl.lit("Bottom"))
                .otherwise(pl.lit("Middle"))
            )
        ),
    ),
    ColumnDefinition(
        name=ColName.GAME_TIME,
        col_type=ColType.FILTER_ONLY,
        views=(View.GAME,),
        expr=pl.col("game_time").str.to_datetime("%Y-%m-%d %H-%M-%S"),
    ),
    ColumnDefinition(
        name=ColName.GAME_DATE,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.date(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_DAY_OF_WEEK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.weekday(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_HOUR,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.hour(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_WEEK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.week(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.BUILD_INDEX,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.MATCH_NUMBER,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.GAME_NUMBER,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.OPP_RANK,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.MAIN_COLORS,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.SPLASH_COLORS,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.ON_PLAY,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.NUM_MULLIGANS,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.OPP_NUM_MULLIGANS ,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.OPP_COLORS,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.NUM_TURNS,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON,
        col_type=ColType.GROUP_BY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.OPENING_HAND,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON_OPENING_HAND,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^opening_hand_.*$") * pl.col(ColName.WON),
        dependencies=[ColName.OPENING_HAND, ColName.WON]
    ),
    ColumnDefinition(
        name=ColName.DRAWN,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON_DRAWN,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^drawn_.*$") * pl.col(ColName.WON),
        dependencies=[ColName.DRAWN, ColName.WON]
    ),
    ColumnDefinition(
        name=ColName.TUTORED,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON_TUTORED,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^tutored_.*$") * pl.col(ColName.WON),
        dependencies=[ColName.TUTORED, ColName.WON]
    ),
    ColumnDefinition(
        name=ColName.DECK,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON_DECK,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^deck_.*$") * pl.col(ColName.WON),
        dependencies=[ColName.DECK, ColName.WON]
    ),
    ColumnDefinition(
        name=ColName.SIDEBOARD,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.WON_SIDEBOARD,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^sideboard_.*$") * pl.col(ColName.WON),
        dependencies=[ColName.SIDEBOARD, ColName.WON]
    ),
    ColumnDefinition(
        name=ColName.ALSA,
        col_type=ColType.AGG,
        views=(),
        expr=pl.col(ColName.LAST_SEEN) / pl.col(ColName.NUM_SEEN),
        dependencies=[ColName.LAST_SEEN, ColName.NUM_SEEN],
    ),
    ColumnDefinition(
        name=ColName.ATA,
        col_type=ColType.AGG,
        expr=pl.col(ColName.TAKEN_AT) / pl.col(ColName.NUM_TAKEN),
        dependencies=[ColName.TAKEN_AT, ColName.NUM_TAKEN],
    ),
    ColumnDefinition(
        name=ColName.NUM_GP,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK),
        dependencies=[ColName.DECK],
    ),
    ColumnDefinition(
        name=ColName.PCT_GP,        
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) / (pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD)),
        dependencies=[ColName.DECK, ColName.SIDEBOARD],
    ),
    ColumnDefinition(
        name=ColName.GP_WR,        
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_DECK) / pl.col(ColName.DECK),
        dependencies=[ColName.WON_DECK, ColName.DECK],
    ),
    ColumnDefinition(
        name=ColName.NUM_IN_POOL,
        col_type=ColType.AGG,
        expr=pl.col(ColName.DECK) + pl.col(ColName.SIDEBOARD),
        dependencies=[ColName.DECK, ColName.SIDEBOARD]
    ),
    ColumnDefinition(
        name=ColName.IN_POOL_WR,
        col_type=ColType.AGG,
        expr=(pl.col(ColName.WON_DECK) + pl.col(ColName.WON_SIDEBOARD))/pl.col(ColName.NUM_IN_POOL),
        dependencies=[ColName.WON_DECK, ColName.WON_SIDEBOARD, ColName.NUM_IN_POOL],
    ),
    ColumnDefinition(
        name=ColName.NUM_OH,
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND),
        dependencies=[ColName.OPENING_HAND],
    ),
    ColumnDefinition(
        name=ColName.OH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) / pl.col(ColName.OPENING_HAND),
        dependencies=[ColName.WON_OPENING_HAND, ColName.OPENING_HAND],
    ),
    ColumnDefinition(
        name=ColName.NUM_GIH,
        col_type=ColType.AGG,
        expr=pl.col(ColName.OPENING_HAND) + pl.col(ColName.DRAWN),
        dependencies=[ColName.OPENING_HAND, ColName.DRAWN],
    ),
    ColumnDefinition(
        name=ColName.NUM_GIH_WON,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_OPENING_HAND) + pl.col(ColName.WON_DRAWN),
        dependencies=[ColName.WON_OPENING_HAND, ColName.WON_DRAWN],
    ),
    ColumnDefinition(
        name=ColName.GIH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.NUM_GIH_WON) / pl.col(ColName.NUM_GIH),
        dependencies=[ColName.NUM_GIH_WON, ColName.NUM_GIH],
    ),
    ColumnDefinition(
        name=ColName.SET_CODE,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.RARITY,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.COLOR,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.COLOR_IDENTITY,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.CARD_TYPE,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.SUBTYPE,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.MANA_VALUE,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.MANA_COST,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.POWER,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.TOUGHNESS,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.IS_BONUS_SHEET,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.IS_DFC,
        col_type=ColType.CARD_ATTR,
        views=(View.CARD,),
    ),
    ColumnDefinition(
        name=ColName.IHD,
        col_type=ColType.AGG,
        expr=pl.col(ColName.GIH_WR) - pl.col(ColName.GP_WR),
        dependencies=[ColName.GIH_WR, ColName.GP_WR],
    ),
    ColumnDefinition(
        name=ColName.NUM_DECK_GIH,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^deck_.*$") * (pl.sum_horizontal("^opening_hand_.*$") + pl.sum_horizontal("^drawn_.*$")),
        dependencies=[ColName.DECK, ColName.OPENING_HAND, ColName.DRAWN],
    ),
    ColumnDefinition(
        name=ColName.WON_NUM_DECK_GIH,
        col_type=ColType.NAME_SUM,
        views=(View.GAME,),
        expr=pl.col("^won_deck_.*$") * (pl.sum_horizontal("^won_opening_hand_.*$") + pl.sum_horizontal("^won_drawn_.*$")),
        dependencies=[ColName.WON_DECK, ColName.WON_OPENING_HAND, ColName.WON_DRAWN],
    ),
    ColumnDefinition(
        name=ColName.DECK_GIH_WR,
        col_type=ColType.AGG,
        expr=pl.col(ColName.WON_NUM_DECK_GIH) / pl.col(ColName.NUM_DECK_GIH),
        dependencies=[ColName.WON_NUM_DECK_GIH, ColName.NUM_DECK_GIH]
    ),
    ColumnDefinition(
        name=ColName.TROPHY_RATE,
        col_type=ColType.AGG,
        expr=pl.col(ColName.IS_TROPHY_SUM) / pl.col(ColName.NUM_TAKEN),
        dependencies=[ColName.IS_TROPHY_SUM, ColName.NUM_TAKEN]
    ),
]

col_def_map = {col.name: col for col in _column_defs}

for item in ColName:
    assert item in col_def_map, f"column {item} enumerated but not defined"
