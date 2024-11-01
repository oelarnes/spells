import datetime

import polars as pl

# game_counts_extensions take the game view data frame with previous
# extensions and return a dataframe of named count columns, eg "deck_<card name>",
# which will be renamed with the extension name as a prefix, e.g. "mull_<card name>"
# Linear extensions (e.g. GIH = OH + GD) should be defined in the "game agg" extensions
def game_counts_mull(game_view: dd.DataFrame, names: list[str]):
    deck_names = ['deck_' + name for name in names]
    return game_view[deck_names].mul(game_view["num_mulligans"], axis=1)


def game_counts_turns(game_view: dd.DataFrame, names: list[str]):
    deck_names = ['deck_' + name for name in names]
    return game_view[deck_names].mul(game_view["num_turns"], axis=1)


def game_counts_numoh(game_view: dd.DataFrame, names: list[str]):
    deck_names = ['deck_' + name for name in names]
    opening_hand_names = ['opening_hand_' + name for name in names]
    return game_view[deck_names].mul(
        game_view[opening_hand_names].sum(axis=1), axis=0
    )


def game_counts_numdr(game_view: dd.DataFrame, names: list[str]):
    deck_names = ['deck_' + name for name in names]
    drawn_names = ['drawn_' + name for name in names]
    return game_view[deck_names].mul(game_view[drawn_names].sum(axis=1), axis=0)

