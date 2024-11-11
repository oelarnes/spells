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

import functools
import re
from dataclasses import dataclass

import polars as pl

from mdu.enums import View, ColName


@dataclass
class ColumnDefinition:
    """
    if a column has dependencies that are not in the base view, all of its immediate
    dependencies must be called out.
    If all dependencies are in the base view, they do not need to be called out.
    """
    name: str
    expr: pl.functions.col.Col | None = None
    base_views: tuple[View,...] = ()
    dependencies: list[str] | None = None
    is_groupby: bool = False
    is_name_sum: bool = False
    is_pick_sum: bool = False


default_columns = [
    ColName.ALSA,
    ColName.PACK_CARD,
    ColName.ATA,
    ColName.NUM_PICKED,
]

column_defs = [
    ColumnDefinition(
        name=ColName.DRAFT_ID,
        base_views=(View.GAME, View.DRAFT),
    ),
    ColumnDefinition(
        name=ColName.DRAFT_TIME,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    ),
    ColumnDefinition(
        name=ColName.DRAFT_DATE,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.date(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby = True,
    ),
    ColumnDefinition(
        name=ColName.DRAFT_DAY_OF_WEEK,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.weekday(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby = True,
    ),
    ColumnDefinition(
        name=ColName.DRAFT_HOUR,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.hour(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.DRAFT_WEEK,
        base_views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.week(),
        dependencies=[ColName.DRAFT_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.RANK,
        base_views=(View.GAME, View.DRAFT),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_WINS,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_LOSSES,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCHES,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_pick_sum=True,
        expr=pl.col("event_match_wins") + pl.col("event_match_losses"),
    ),
    ColumnDefinition(
        name=ColName.IS_TROPHY,
        base_views=(View.DRAFT,),
        expr=pl.when(pl.col("event_type") == "Traditional")
            .then(pl.col("event_match_wins") == 3)
            .otherwise(pl.col("event_match_wins") == 7),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PACK_NUMBER,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PACK_NUM,
        base_views=(View.DRAFT,),
        expr=pl.col("pack_number") + 1,
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PICK_NUMBER,
        base_views=(View.DRAFT,),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PICK_NUM,
        base_views=(View.DRAFT,),
        expr=pl.col("pick_number") + 1,
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PICK,
        base_views=(View.DRAFT,),
        # handled by internals, don't use
    ),
    ColumnDefinition(
        name=ColName.NAME,
        base_views=(View.DRAFT, View.GAME),
        # handled by internals, derived from both 'picked' and "name mapped" columns
    ),
    ColumnDefinition(
        name=ColName.PICK_MAINDECK_RATE,
        base_views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PICK_SIDEBOARD_IN_RATE,
        base_views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PACK_CARD,
        base_views=(View.DRAFT,),
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PACK_NUM_CARD,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.col("pack_num"),
        dependencies=[ColName.PACK_CARD, ColName.PACK_NUM],
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.PICK_NUM_CARD,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.col("pick_num"),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.LAST_SEEN,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.*$") * pl.min_horizontal("pick_num", 8),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.NUM_SEEN,
        base_views=(View.DRAFT,),
        expr=pl.col("^pack_card_.$") * (pl.col("pick_num")<=8),
        dependencies=[ColName.PACK_CARD, ColName.PICK_NUM],
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.POOL,
        base_views=(View.DRAFT,),
        is_name_sum=True,
    ),
    ColumnDefinition(
        name=ColName.USER_N_GAMES_BUCKET,
        base_views=(View.DRAFT, View.GAME),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        base_views=(View.DRAFT, View.GAME),
        is_groupby=True,
        is_pick_sum=True,
    ),
    ColumnDefinition(
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
    ColumnDefinition(
        name=ColName.GAME_TIME,
        base_views=(View.GAME,),
        expr=pl.col("game_time").str.to_datetime("%Y-%m-%d %H-%M-%S"),
    ),
    ColumnDefinition(
        name=ColName.GAME_DATE,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.date(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.GAME_DAY_OF_WEEK,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.weekday(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.GAME_HOUR,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.hour(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.GAME_WEEK,
        base_views=(View.GAME,),
        expr=pl.col("game_time").dt.week(),
        dependencies=[ColName.GAME_TIME],
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.BUILD_INDEX,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.MATCH_NUMBER,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.GAME_NUMBER,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.OPP_RANK,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.MAIN_COLORS,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
    ColumnDefinition(
        name=ColName.SPLASH_COLORS,
        base_views=(View.GAME,),
        is_groupby=True,
    ),
]

def name_sum_rename(
    cdef: ColumnDefinition,
    old_name: str,
    ):
    if cdef.dependencies is None:
        raise ValueError("name_sum columns must name their dependencies")
    name_sum_dep = cdef.dependencies[0]
    name_pattern = f'^{name_sum_dep}_'
    card_name = re.split(name_pattern, old_name)[1]
    return cdef.name + '_' + card_name


col_def_map = {col.name: col for col in column_defs}

for cdef in column_defs:
    if cdef.expr is not None:
        if cdef.is_name_sum:
            if not cdef.dependencies or not col_def_map[cdef.dependencies[0]].is_name_sum:
                raise ValueError("dependency 0 of a name_sum column with an expr must be a name_sum column to derive names")
            cdef.expr = cdef.expr.name.map(functools.partial(name_sum_rename, cdef))
        else:
            cdef.expr = cdef.expr.alias(cdef.name)
    else:
        if cdef.is_name_sum:
            cdef.expr = pl.col(f"^{cdef.name}_.*$")
        else:
            cdef.expr = pl.col(cdef.name)

