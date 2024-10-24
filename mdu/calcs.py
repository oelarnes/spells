"""
column calculations to extend data frames
"""

import pandas


def game_counts_mull(game_view: pandas.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view["num_mulligans"], axis=1)


def game_counts_turns(game_view: pandas.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view["num_turns"], axis=1)


def game_counts_numoh(game_view: pandas.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(
        game_view[prefix_names["opening_hand"]].sum(axis=1), axis=0
    )


def game_counts_numdr(game_view: pandas.DataFrame, prefix_names: list):
    return game_view[prefix_names["deck"]].mul(game_view[prefix_names["drawn"]].sum(axis=1), axis=0)
