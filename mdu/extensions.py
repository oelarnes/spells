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

from enum import StrEnum

import pandas
import dask.dataframe as dd

class View(StrEnum):
    game = "game_view"
    draft = "draft_view"
    game_counts = "game_counts"
    seen_counts = "seen_counts"
    game_agg = "game_agg"
    picked_agg = "picked_agg"
    seen_agg = "seen_agg"

class CommonExt(StrEnum):
    player_cohort = 'player_cohort'
    draft_date = 'draft_date'
    draft_week = 'draft_week'

class GCExt(StrEnum):
    mull = "gc_mull"
    turns = "gc_turns"
    numoh = "gc_numoh"
    numdr = "gc_numdr"

def _player_cohort(df: dd.DataFrame):
    def cohort(row):
        if row["user_n_games_bucket"] < 100:
            return "All"
        if row["user_game_win_rate_bucket"] < 0.49:
            return "Bottom"
        if row["user_game_win_rate_bucket"] > 0.57:
            return "Top"
        return "Middle"

    return df.apply(cohort, axis=1, meta=pandas.Series(dtype="object"))A

def _draft_date(df: dd.DataFrame):
    return df["draft_time"].apply(
        lambda t: str(t[0:10]), meta=pandas.Series(dtype="object")
    )

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
    View.game: {
        SharedExt.player_cohort: _player_cohort,
        SharedExt.draft_date: _draft_date,
        SharedExt.draft_week: _draft_week,
    },
    View.game_counts: {
        GCExt.mull: _game_counts_mull,
        GCExt.turns: _game_counts_turns,
        GCExt.numoh: _game_counts_numoh,
        GCExt.numdr: _game_counts_numdr,
    },
}
