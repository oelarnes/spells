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

PACK_CARD_PREFIX = 'pack_card_'
POOL_PREFIX = 'pool_'

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
    df['date'] = df['draft_time'].apply(lambda t: str(t[0:10]), meta=pandas.Series(dtype="object"))
    df['week'] = df['date'].apply(week_from_date, meta=pandas.Series(dtype="object"))

def extend_draft_columns(draft_df: dd.DataFrame):
    extend_shared_columns(draft_df)
    draft_df['event_matches'] = draft_df['event_match_wins'] + draft_df['event_match_losses']

def extend_game_columns(game_df: dd.DataFrame):
    extend_shared_columns(game_df)
    
def picked_stats(draft_view: dd.DataFrame):
    df = draft_view.groupby('pick')[['event_matches', 'event_match_wins', 'pick_number']]\
        .agg({'event_matches': 'sum', 'event_match_wins': 'sum', 'pick_number': ['sum', 'count']})\
        .compute()
    df['num_picked'] = df[('pick_number', 'count')]
    df['num_matches'] = df[('event_matches', 'sum')]
    df['ata'] = df[('pick_number', 'sum')] / df['num_picked'] + 1
    df['apmwr'] = df[('event_match_wins', 'sum')] / df['num_matches']

    df = df[['num_picked', 'ata', 'num_matches', 'apmwr']].sort_index()
    df.columns = ['num_picked', 'ata', 'num_matches', 'apmwr'] # remove multiindex
    df.index.name = 'name'

    return df


def seen_stats(draft_view: dd.DataFrame, cards_per_pack: int):
    pattern = f'^{PACK_CARD_PREFIX}'

    pack_card_cols = [c for c in draft_view.columns if c.startswith(PACK_CARD_PREFIX)]
    pick_num_seen_cols = [f"num_{c}" for c in pack_card_cols]
    is_seen_cols = [f"count_{c}" for c in pack_card_cols]
    names = [re.split(pattern, col)[1] for col in pack_card_cols]
        
    def alsa_lambda(df):
        is_seen_df = numpy.minimum(df[pack_card_cols], 1)
        pick_num_seen_df = is_seen_df.mul(df['pick_number'] + 1, axis=0)

        data_df = pandas.concat([
            df[['draft_id', 'pack_number']],
            is_seen_df.rename(columns=dict(zip(pack_card_cols, is_seen_cols))),
            pick_num_seen_df.rename(columns=dict(zip(pack_card_cols, pick_num_seen_cols)))
        ], axis=1)

        grouped_df = pandas.concat(
            [
                data_df.groupby(['draft_id', 'pack_number']).max(), 
                pandas.DataFrame({'pick_count': data_df.groupby(['draft_id', 'pack_number']).draft_id.count()})
            ], axis=1
        )

        return grouped_df[grouped_df['pick_count'] == cards_per_pack]

    last_seen_agg = draft_view \
        .map_partitions(alsa_lambda) \
        .sum().compute()

    num_seen_sum = draft_view[pack_card_cols].sum().compute()
    
    return pandas.DataFrame(
        {
            'alsa': last_seen_agg[pick_num_seen_cols].values / last_seen_agg[is_seen_cols].values,
            'num_packs_seen': last_seen_agg[is_seen_cols].values,
            'num_seen': num_seen_sum.values,
        },
        index=pandas.Index(names, name='name')
    )


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

    def set_index(self):
        self._draft_df.set_index('draft_id_idx', sorted=True)

    def seen_stats(self):
        return seen_stats(self.draft_view, CARDS_PER_PACK_MAP[self.set_code])
    
    def picked_stats(self):
        return picked_stats(self.draft_view)
 
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