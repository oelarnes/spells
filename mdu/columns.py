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

from mdu.enums import View, ColType, ColName


@dataclass
class DDColumn:
    """
    used to classify existing columns, define library-included columns,
    and register new extensions interactively

    Arguments:
    view: a single view or list of views which may contain this column. Either base views or
    the agg view need to be specified, not both.

    name: the column name, or column prefix for `name_sum` type columns (e.g.
    "deck" for "deck_<cardname")

    col_type: one more of the following. A column may be both groupby and picked_sum, otherwise
    one type:
        "filter_type": used only in filters and exprulated columns
        "groupby": used to group results in game and draft views
        "name_sum": mapped over card names and summed. Cannot reuse existing column names,
            and `.col` must be a sensible column function of an expression like pl.col("^deck_.*$"),
            noting all elements of the regular expression.
        "picked_sum": a single numerical column in the draft view which will be groupbed by "pick"
            along with other groupbys and summed
        "agg_expr": a function of the sum columns generated by the game and draft agg steps. Can
            be a function of columns from either view. In this step, name_sum columns are accessed
            simply by name, e.g. col('deck') will have value equal to the sum of the column
            'deck_<card name>' in the row with index name value '<card name>'. So
            GIHWR = col('in_hand_won') / col('in_hand')
    """

    name: str
    view: View | tuple[View, View]
    col_type: ColType | tuple[ColType, ColType]
    expr: pl.functions.col.Col | None = None
    dependencies: list[str] | None = None


default_columns = [
    ColName.ALSA,
    ColName.PACK_CARD,
    ColName.ATA,
    ColName.NUM_PICKED,
]

column_defs = [
    DDColumn(
        name=ColName.DRAFT_ID,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.FILTER_ONLY,
    ),
    DDColumn(
        name=ColName.DRAFT_TIME,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.FILTER_ONLY,
        expr=pl.col("draft_time").str.to_time("%+"),
    ),
    DDColumn(
        name=ColName.DRAFT_DATE,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.GROUPBY,
        expr=pl.col("draft_time").dt.date(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    DDColumn(
        name=ColName.DRAFT_DAY_OF_WEEK,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.GROUPBY,
        expr=pl.col("draft_time").dt.weekday(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    DDColumn(
        name=ColName.DRAFT_HOUR,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.GROUPBY,
        expr=pl.col("draft_time").dt.hour(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    DDColumn(
        name=ColName.DRAFT_WEEK,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.GROUPBY,
        expr=pl.col("draft_time").dt.week(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    DDColumn(
        name=ColName.RANK,
        view=(View.GAME, View.DRAFT),
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.EVENT_MATCH_WINS,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
    ),
    DDColumn(
        name=ColName.EVENT_MATCH_LOSSES,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
    ),
    DDColumn(
        name=ColName.EVENT_MATCHES,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
        expr=pl.col("event_match_wins") + pl.col("event_match_losses"),
    ),
    DDColumn(
        name=ColName.IS_TROPHY,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
        expr=pl.when(pl.col("event_type") == "Traditional")
        .then(pl.col("event_match_wins") == 3)
        .otherwise(pl.col("event_match_wins") == 7),
    ),
    DDColumn(
        name=ColName.PACK_NUMBER,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
    ),
    DDColumn(
        name=ColName.PACK_NUM,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
        expr=pl.col("pack_number") + 1,
    ),
    DDColumn(
        name=ColName.PICK_NUMBER,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
    ),
    DDColumn(
        name=ColName.PICK_NUM,
        view=View.DRAFT,
        col_type=(ColType.GROUPBY, ColType.PICKED_SUM),
        expr=pl.col("pick_number") + 1,
    ),
    DDColumn(
        name=ColName.PICKED,
        view=View.DRAFT,
        col_type=ColType.FILTER_ONLY,  # will be renamed to "name" for groupby
    ),
    DDColumn(
        name=ColName.NAME,
        view=(View.DRAFT, View.GAME),
        col_type=ColType.GROUPBY,
        # handled by internals, derived from both 'picked' and "name mapped" columns
    ),
    DDColumn(
        name=ColName.PICK_MAINDECK_RATE,
        view=View.DRAFT,
        col_type=ColType.PICKED_SUM,
    ),
    DDColumn(
        name=ColName.PICK_SIDEBOARD_IN_RATE,
        view=View.DRAFT,
        col_type=ColType.PICKED_SUM,
    ),
    DDColumn(
        name=ColName.PACK_CARD,
        view=View.DRAFT,
        col_type=ColType.NAME_SUM,
    ),
    DDColumn(
        name=ColName.PACK_NUM_CARD,
        view=View.DRAFT,
        col_type=ColType.NAME_SUM,
        expr=pl.col("^pack_card_.*$") * pl.col("pack_num"),
    ),
    DDColumn(
        name=ColName.LAST_SEEN,
        view=View.DRAFT,
        col_type=ColType.NAME_SUM,
        expr=pl.col("^pack_card_.*$") * pl.min_horizontal("pack_num", 8),
    ),
    DDColumn(
        name=ColName.POOL,
        view=View.DRAFT,
        col_type=ColType.NAME_SUM,
    ),
    DDColumn(
        name=ColName.USER_N_GAMES_BUCKET,
        view=(View.DRAFT, View.GAME),
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        view=(View.DRAFT, View.GAME),
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.PLAYER_COHORT,
        view=(View.DRAFT, View.GAME),
        col_type=ColType.GROUPBY,
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
    ),
    DDColumn(
        name=ColName.GAME_TIME,
        view=View.GAME,
        col_type=ColType.FILTER_ONLY,
        expr=pl.col("game_time").str.to_time("%+"),
    ),
    DDColumn(
        name=ColName.GAME_DATE,
        view=View.GAME,
        col_type=ColType.GROUPBY,
        expr=pl.col("game_time").dt.date(),
        dependencies=[ColName.GAME_TIME],
    ),
    DDColumn(
        name=ColName.GAME_DAY_OF_WEEK,
        view=View.GAME,
        col_type=ColType.GROUPBY,
        expr=pl.col("game_time").dt.weekday(),
        dependencies=[ColName.GAME_TIME],
    ),
    DDColumn(
        name=ColName.GAME_HOUR,
        view=View.GAME,
        col_type=ColType.GROUPBY,
        expr=pl.col("game_time").dt.hour(),
        dependencies=[ColName.GAME_TIME],
    ),
    DDColumn(
        name=ColName.GAME_WEEK,
        view=View.GAME,
        col_type=ColType.GROUPBY,
        expr=pl.col("game_time").dt.week(),
        dependencies=[ColName.GAME_TIME],
    ),
    DDColumn(
        name=ColName.BUILD_INDEX,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.MATCH_NUMBER,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.GAME_NUMBER,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.OPP_RANK,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.MAIN_COLORS,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
    DDColumn(
        name=ColName.SPLASH_COLORS,
        view=View.GAME,
        col_type=ColType.GROUPBY,
    ),
]

for column in column_defs:
    if column.expr:
        if column.col_type != ColType.NAME_SUM:
            column.expr = column.expr.alias(column.name)
        else:
            column.expr = column.expr.name.map(lambda x: column.name + '_' + x)

column_def_map = {col.name: col for col in column_defs}
