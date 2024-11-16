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

from mdu.enums import View, ColName, ColType

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
    ColName.ALSA,
    ColName.NUM_SEEN,
    ColName.ATA,
    ColName.NUM_TAKEN,
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
        col_type=ColType.GROUPBY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.date(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_DAY_OF_WEEK,
        col_type=ColType.GROUPBY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.weekday(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_HOUR,
        col_type=ColType.GROUPBY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.hour(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.DRAFT_WEEK,
        col_type=ColType.GROUPBY,
        views=(View.GAME, View.DRAFT),
        expr=pl.col("draft_time").dt.week(),
        dependencies=[ColName.DRAFT_TIME],
    ),
    ColumnDefinition(
        name=ColName.RANK,
        col_type=ColType.GROUPBY,
        views=(View.GAME, View.DRAFT),
    ),
    ColumnDefinition(
        name=ColName.EVENT_MATCH_WINS,
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
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
    ),
    ColumnDefinition(
        name=ColName.PACK_NUMBER,
        col_type=ColType.FILTER_ONLY,  # use pack_num
        views=(View.DRAFT,),
    ),
    ColumnDefinition(
        name=ColName.PACK_NUM,
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
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
        expr=pl.when(pl.col("pick").is_not_null())
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
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
        views=(View.DRAFT, View.GAME),
    ),
    ColumnDefinition(
        name=ColName.USER_GAME_WIN_RATE_BUCKET,
        col_type=ColType.GROUPBY,
        views=(View.DRAFT, View.GAME),
    ),
    ColumnDefinition(
        name=ColName.PLAYER_COHORT,
        col_type=ColType.GROUPBY,
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
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.date(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_DAY_OF_WEEK,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.weekday(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_HOUR,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.hour(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.GAME_WEEK,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
        expr=pl.col("game_time").dt.week(),
        dependencies=[ColName.GAME_TIME],
    ),
    ColumnDefinition(
        name=ColName.BUILD_INDEX,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.MATCH_NUMBER,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.GAME_NUMBER,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.OPP_RANK,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.MAIN_COLORS,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
    ),
    ColumnDefinition(
        name=ColName.SPLASH_COLORS,
        col_type=ColType.GROUPBY,
        views=(View.GAME,),
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
        views=(),
        expr=pl.col(ColName.TAKEN_AT) / pl.col(ColName.NUM_TAKEN),
        dependencies=[ColName.TAKEN_AT, ColName.NUM_TAKEN],
    ),
]


col_def_map = {col.name: col for col in _column_defs}
