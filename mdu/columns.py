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

from dataclasses import dataclass

import polars as pl

from mdu.enums import View, ColName


@dataclass
class DDColumn:
    name: str
    expr: pl.functions.col.Col | None = None
    base_views: tuple[View,...] = ()
    dependencies: list[str] | None = None
    is_groupby: bool = False
    is_name_sum: bool = False
    is_picked_sum: bool = False


default_columns = [
    ColName.ALSA,
    ColName.PACK_CARD,
    ColName.ATA,
    ColName.NUM_PICKED,
]

column_defs = [
    DDColumn(
        name=ColName.DRAFT_ID,
        base_views=(View.GAME, View.DRAFT),
    ),
    DDColumn(
        name=ColName.DRAFT_TIME,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").str.to_time("%+"),
    ),
    DDColumn(
        name=ColName.DRAFT_DATE,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.date(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby = True,
    ),
    DDColumn(
        name=ColName.DRAFT_DAY_OF_WEEK,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.weekday(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby = True,
    ),
    DDColumn(
        name=ColName.DRAFT_HOUR,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.hour(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.DRAFT_WEEK,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.week(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.RANK,
        base_views=(View.GAME, View.DRAFT),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.EVENT_MATCH_WINS,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.EVENT_MATCH_LOSSES,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.EVENT_MATCHES,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_picked_sum=True,
        expr=pl.col("event_match_wins") + pl.col("event_match_losses"),
    ),
    DDColumn(
        name=ColName.IS_TROPHY,
        base_views=(View.DRAFT,),
        expr=pl.when(pl.col("event_type") == "Traditional")
            .then(pl.col("event_match_wins") == 3)
            .otherwise(pl.col("event_match_wins") == 7),
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.PACK_NUMBER,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.PACK_NUM,
        base_views=(View.DRAFT,),
        expr=pl.col("pack_number") + 1,
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.PICK_NUMBER,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.PICK_NUM,
        base_views=(View.DRAFT,),
        expr=pl.col("pick_number") + 1,
        is_groupby=True,
        is_picked_sum=True,
    ),
    DDColumn(
        name=ColName.PICKED,
        base_views=(View.DRAFT,),
    ),
    DDColumn(
        name=ColName.NAME,
        base_views=(View.DRAFT, View.GAME),
        # handled by internals, derived from both 'picked' and "name mapped" columns
    ),
    DDColumn(
        name=ColName.PICK_MAINDECK_RATE,
        base_views=(View.DRAFT,),
    ),
    DDColumn(
        name=ColName.PICK_SIDEBOARD_IN_RATE,
        base_views=(View.DRAFT,),
    ),
    DDColumn(
        name=ColName.PACK_CARD,
        base_views=(View.DRAFT,),
    ),
    DDColumn(
        name=ColName.PACK_NUM_CARD,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.col("pack_num"),
    ),
    DDColumn(
        name=ColName.LAST_SEEN,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.min_horizontal("pack_num", 8),
    ),
    DDColumn(
        name=ColName.POOL,
        base_views=(View.DRAFT,),
        is_name_sum=True,
    ),
    DDColumn(
        name=ColName.USER_N_GAMES_BUCKET,
        base_views=(View.DRAFT, View.GAME),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        base_views=(View.DRAFT, View.GAME),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.PLAYER_COHORT,
        base_views=(View.DRAFT, View.GAME),
        expr=pl.when(pl.col("user_n_games_bucket") < 100)
        .then("All")
        .otherwise(
            pl.when(pl.col("user_game_win_rate_bucket") > 0.57)
            .then("Top")
            .otherwise(
                pl.when(pl.col("user_game_win_rate_bucket") < 0.49)
                .then("Bottom")
                .otherwise("Middle")
            )
        ),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.GAME_TIME,
        base_views=(View.GAME,),
        expr=pl.col("game_time").str.to_time("%+"),
    ),
    DDColumn(
        name=ColName.GAME_DATE,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.date(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.GAME_DAY_OF_WEEK,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.weekday(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.GAME_HOUR,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.hour(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.GAME_WEEK,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.week(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.BUILD_INDEX,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.MATCH_NUMBER,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.GAME_NUMBER,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.OPP_RANK,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.MAIN_COLORS,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    DDColumn(
        name=ColName.SPLASH_COLORS,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
]

for column in column_defs:
    if column.expr is not None:
        if column.is_name_sum:
            column.expr = column.expr.name.map(lambda x: column.name + '_' + x)
        else:
            column.expr = column.expr.alias(column.name)

column_def_map = {col.name: col for col in column_defs}
