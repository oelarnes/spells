import os
import functools
import re
import datetime

import dask.dataframe as dd
import numpy
import pandas

from mdu.cache_17l import data_file_path
from mdu.config.mdu_cfg import CARDS_PER_PACK_MAP
import mdu.filter

SUPPORTED_GROUPBYS = {
    'draft': {'name', 'rank', 'pack_number', 'pick_number', 'user_n_games_bucket', 'user_game_win_rate_bucket', 'player_cohort', 
              'draft_date', 'draft_week'},
    'game': {'name', 'build_index', 'match_number', 'game_number', 'rank', 'opp_rank', 'main_colors', 'splash_colors', 'on_play', 
             'num_mulligans', 'opp_num_mulligans', 'opp_colors', 'user_n_games_bucket', 'user_game_win_rate_bucket', 'player_cohort', 
             'draft_date', 'draft_week'},
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
        self._gv = None

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
            filter_spec = mdu.filter.from_spec(filter_spec)
        self._filter = filter_spec

    def picked_stats(self):
        return picked_counts(self.draft_view)
 
    def game_rates(self, game_counts: pandas.DataFrame):
        """
        in_pool_gwr             := num_win_in_pool / num_in_pool
        gpwr                    := num_wins_in_deck / num_in_deck
        gp_pct                  := num_games_in_deck / num_games_in_pool
        ohwr                    := <num_wins_oh> / num_oh
        gdwr                    := <num_wins_drawn> / num_drawn
        gihwr                   := <num_wins_gih> / num_gih
        gnswr                   := <num_wins_gns> / num_gns
        iwd                     := gihwr - gnswr                                            # "Improvement When Drawn"
        ihd                     := gihwr - gpwr                                             # "In-Hand Delta
        mull_rate               := mull_in_deck / num_in_deck
        turns_per_game          := turns_in_deck / num_in_deck
        """
        pass

    def game_counts(self, game_view: pandas.DataFrame, groupbys:list =['name'], use_index=True):
        """
        A data frame of counts easily aggregated from the 'game' file.
        Card-attribute groupbys can be applied after this stage to be filtered through a rates aggregator.
        """
        names = self.card_names
        nonname_groupbys = [c for c in groupbys if c != 'name']

        prefix_by_type = {
            'deck': 'deck_',
            'sb': 'sideboard_',
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
            functools.reduce(lambda curr, prev: prev + curr, names_by_type.values()) +
            nonname_groupbys
        ]

        win_df = games_df[df['won']]

        if nonname_groupbys:
            games_result = games_df.groupby(nonname_groupbys).sum().compute()
            win_result = win_df.groupby(nonname_groupbys).sum().compute()
        else:
            games_sum = games_df.sum().compute()
            games_result = pandas.DataFrame(numpy.expand_dims(games_sum.values, 0), columns=games_sum.index)
            win_sum = win_df.sum().compute()
            win_result = pandas.DataFrame(numpy.expand_dims(win_sum.values, 0), columns=win_sum.index)

        count_cols = {}
        for count_type in count_types:
            for outcome, df in {'all': games_result, 'win': win_result }.items():
                count_df = df[names_by_type[count_type]]
                count_df.columns = names
                melt_df = pandas.melt(count_df, var_name='name', ignore_index=False)
                count_cols[f'{count_type}_{outcome}'] = melt_df['value'].reset_index(drop=True)
        # grab the indexes from the last one, they are all the same
        index_df = melt_df.reset_index().drop('value', axis='columns')

        by_name_df = pandas.DataFrame(count_cols, index=pandas.MultiIndex.from_frame(index_df))
        if 'name' in groupbys:
            return by_name_df
        
        return by_name_df.groupby(groupbys).sum()