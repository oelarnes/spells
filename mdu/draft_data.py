import os
import functools
import re
import datetime

import dask.dataframe as dd
import numpy
import pandas

from mdu.cache_17l import data_file_path
from mdu.config.mdu_cfg import CARDS_PER_PACK_MAP
import mdu.filter as filter

SUPPORTED_GROUPBYS = {
    'draft': {'name', 'rank', 'pack_number', 'pick_number', 'user_n_games_bucket', 'user_game_win_rate_bucket', 'player_cohort', 
              'draft_date', 'draft_week'},
    'game': {'name', 'build_index', 'match_number', 'game_number', 'rank', 'opp_rank', 'main_colors', 'splash_colors', 'on_play', 
             'num_mulligans', 'opp_num_mulligans', 'opp_colors', 'user_n_games_bucket', 'user_game_win_rate_bucket', 'player_cohort', 
             'draft_date', 'draft_week', 'game_length'},
    'card': {'rarity', 'color_identity_str', 'type', 'cmc'}
}

def player_cohort(row):
    if row['user_n_games_bucket'] < 100:
        return 'All'
    if row['user_game_win_rate_bucket'] < 0.49:
        return 'Bottom'
    if row['user_game_win_rate_bucket'] > 0.57:
        return 'Top'
    return 'Middle'


def game_length(num_turns):
    if num_turns < 6: 
        return 'Very Short'
    if num_turns < 9:
        return 'Short'
    if num_turns < 12:
        return 'Medium'
    return 'Long'
        

def week_from_date(date_str):
    date = datetime.date.fromisoformat(date_str)
    return (date - datetime.timedelta(days = date.weekday())).isoformat()

def extend_shared_columns(df: dd.DataFrame):
    df['player_cohort'] = df.apply(player_cohort, axis=1, meta=pandas.Series(dtype="object"))
    df['draft_date'] = df['draft_time'].apply(lambda t: str(t[0:10]), meta=pandas.Series(dtype="object"))
    df['draft_week'] = df['draft_date'].apply(week_from_date, meta=pandas.Series(dtype="object"))

def extend_draft_columns(draft_df: dd.DataFrame):
    extend_shared_columns(draft_df)
    draft_df['event_matches'] = draft_df['event_match_wins'] + draft_df['event_match_losses']
    draft_df['name'] = draft_df['pick']

def extend_game_columns(game_df: dd.DataFrame):
    extend_shared_columns(game_df)
    game_df['game_length'] = game_df['num_turns'].apply(game_length)
    
def picked_counts(draft_view: dd.DataFrame, groupbys=['name']):
    df = draft_view.groupby(groupbys)[['event_matches', 'event_match_wins', 'pick_number']]\
        .agg({'event_matches': 'sum', 'event_match_wins': 'sum', 'pick_number': ['sum', 'count']})\
        .compute()
    df['num_picked'] = df[('pick_number', 'count')]
    df['num_matches'] = df[('event_matches', 'sum')]
    df['sum_pick_num'] = df[('pick_number', 'sum')] + df['num_picked']
    df['num_match_wins'] = df[('event_match_wins', 'sum')]

    df = df[['num_picked', 'sum_pick_num', 'num_matches', 'num_match_wins']].sort_index()
    df.columns = ['num_picked', 'sum_pick_num', 'num_matches', 'num_match_wins'] # remove multiindex

    return df


class DraftData:
    def __init__(
        self,
        set_code: str, 
        filter_spec: any = None
    ):
        self.set_code = set_code.upper()
        
        self.set_filter(filter_spec)

        self._draft_df = dd.read_csv(data_file_path(set_code, 'draft'))
        self._card_df = pandas.read_csv(data_file_path(set_code, 'card'))
        self._game_df = dd.read_csv(data_file_path(set_code, 'game'))
        self._dv = None

    @property
    def card_names(self):
        """
        The card file is generated from the draft data file, so this is exactly the list of card names used in the datasets
        """
        return list(self._card_df['name'].values)

    @property
    def draft_view(self):
        if self._dv is None:
            dv = self._draft_df.copy()
            dv.set_index('draft_id_idx', sorted=True)
            extend_draft_columns(dv)
            if self._filter is not None:
                dv = dv.loc[self._filter]
            self._dv = dv
        return self._dv
    
    @property
    def game_view(self):
        if self._gv is None:
            gv = self._game_df.copy()
            extend_game_columns(gv)
            if self._filter is not None:
                gv = gv.loc[self._filter]
            self._gv = gv
        return self._gv
    
    def set_filter(self, filter_spec: any):
        if type(filter_spec) == dict:
            filter_spec = filter.from_spec(filter_spec)
        self._filter = filter_spec

    def picked_stats(self):
        return picked_counts(self.draft_view)
 
    def game_stats(self, game_view: pandas.DataFrame):
        """
        A data frame of statistics easily aggregated from the 'game' file.
        Generally, statistics are "card-weighted", meaning two copies of a card in a deck contribute two games and two wins, etc

        num_in_pool
        num_wins_in_pool
        in_pool_gwr             := num_win_in_pool / num_in_pool
        num_in_deck
        num_wins_in_deck
        gpwr                    := num_wins_in_deck / num_in_deck
        gp_pct                  := num_games_in_deck / num_games_in_pool
        num_oh                  # number of appearances in opening hand after mulligans
        ohwr                    := <num_wins_oh> / num_oh
        num_drawn               # number of appearances as drawn after OH but not tutored
        gdwr                    := <num_wins_drawn> / num_drawn
        num_gih                 := num_oh + num_drawn                                       # "Game In Hand"
        gihwr                   := <num_wins_gih> / num_gih
        num_gns                 := num_games_in_deck - num_gih - <num_tutored>
        gnswr                   := <num_wins_gns> / num_gns
        iwd                     := gihwr - gnswr                                            # "Improvement When Drawn"
        ihd                     := gihwr - gpwr                                             # "In-Hand Delta
        mull_in_deck            # mulligans in deck
        mull_rate               := mull_in_deck / num_in_deck
        turns_in_deck           # total turns taken with card in deck
        turns_per_game          := turns_in_deck / num_in_deck
        """

        names = self.card_names

        prefix_by_type = {
            'deck': 'deck_',
            'sb': 'sb_',
            'oh': 'opening_hand_',
            'drawn': 'drawn_',
            'tutored': 'tutored_',
            'mull': 'mull_',
            'turn': 'turn_'
        }

        count_types = list(prefix_by_type.keys())

        names_by_type = {
            t: [f"{prefix_by_type[t]}{name}" for name in names] for t in count_types
        }

        df = game_view.copy()
        df[names_by_type['mull']] = df[names_by_type['deck']] * df['num_mulligans']
        df[names_by_type['turn']] = df[names_by_type['deck']] * df['num_turns']

        games_df = df[
            functools.reduce(lambda curr, prev: prev + curr, names_by_type.values())
        ]

        win_df = games_df[df['won']]

        games_result = games_df.sum().compute()
        win_result = win_df.sum().compute()

        game_stat_df = pandas.DataFrame(
            {
                'num_in_pool': games_result[names_by_type['deck']].values + games_result[names_by_type['sideboard']].values,
                'num_wins_in_pool': win_result[names_by_type['deck']].values + win_result[names_by_type['sideboard']].values,
                'num_in_deck': games_result[names_by_type['deck']].values,
                'num_wins_in_deck': win_result[names_by_type['deck']].values,
                'num_oh': games_result[names_by_type['oh']].values,
                'num_wins_oh': win_result[names_by_type['oh']].values,
                'num_drawn': games_result[names_by_type['drawn']].values,
                'num_win_drawn': win_result[names_by_type['drawn']].values,
                'num_tutored': games_result[names_by_type['tutored']].values,
                'num_win_tutored': win_result[names_by_type['tutored']].values,
                'num_mull': games_result[names_by_type['mull']].values,
                'num_turns': games_result[names_by_type['turn']].values
                

            }
        )