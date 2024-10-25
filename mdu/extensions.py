"""
extension configuration for custom columns.

The extensions defined here can be passed to the appropriate
dataframe function by enum or enum value, as `extensions = []`, etc.

By default no extensions are used, since they are not needed for the base metrics.

Custom extensions can be defined via draft_data_obj.register_extension(
    <view name>, <extension name>, <extension calc>
)

Note that the extension calc must match the signature provided in the examples here.
"""

import datetime
from enum import StrEnum

import pandas
import dask.dataframe as dd


class View(StrEnum):
    GAME = "game_view"
    DRAFT = "draft_view"
    GAME_COUNTS = "game_counts"
    SEEN_COUNTS = "seen_counts"
    GAME_AGG = "game_agg"
    PICKED_AGG = "picked_agg"
    SEEN_AGG = "seen_agg"


class CommonExt(StrEnum):
    PLAYER_COHORT = "player_cohort"
    DRAFT_DATE = "draft_date"
    DRAFT_WEEK = "draft_week"


class GCExt(StrEnum):
    MULL = "gc_mull"
    TURNS = "gc_turns"
    NUMOH = "gc_numoh"
    NUMDR = "gc_numdr"


def _player_cohort(df: dd.DataFrame):
    def cohort(row):
        if row["user_n_games_bucket"] < 100:
            return "All"
        if row["user_game_win_rate_bucket"] < 0.49:
            return "Bottom"
        if row["user_game_win_rate_bucket"] > 0.57:
            return "Top"
        return "Middle"

    return df.apply(cohort, axis=1, meta=pandas.Series(dtype="object"))


def _draft_date(df: dd.DataFrame):
    return df["draft_time"].apply(lambda t: str(t[0:10]), meta=pandas.Series(dtype="object"))


def _draft_week(df: dd.DataFrame):
    def week(date_str):
        date = datetime.date.fromisoformat(date_str)
        return (date - datetime.timedelta(days=date.weekday())).isoformat()

    return df["draft_date"].apply(week, meta=pandas.Series(dtype="object"))


# game_counts_extensions take the game view data frame with previous
# extensions and return a dataframe of named count columns, eg "deck_<card name>",
# which will be renamed with the extension name as a prefix, e.g. "mull_<card name>"
# Linear extensions (e.g. GIH = OH + GD) should be defined in the "game agg" extensions
def _game_counts_mull(game_view: dd.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view["num_mulligans"], axis=1)


def _game_counts_turns(game_view: dd.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view["num_turns"], axis=1)


def _game_counts_numoh(game_view: dd.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(
        game_view[prefix_names["opening_hand"]].sum(axis=1), axis=0
    )


def _game_counts_numdr(game_view: dd.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view[prefix_names["drawn"]].sum(axis=1), axis=0)


extensions = {
    View.GAME: {
        CommonExt.PLAYER_COHORT: _player_cohort,
        CommonExt.DRAFT_DATE: _draft_date,
        CommonExt.DRAFT_WEEK: _draft_week,
    },
    View.GAME_COUNTS: {
        GCExt.MULL: _game_counts_mull,
        GCExt.TURNS: _game_counts_turns,
        GCExt.NUMOH: _game_counts_numoh,
        GCExt.NUMDR: _game_counts_numdr,
    },
}
