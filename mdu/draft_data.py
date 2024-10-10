import os
import re
import datetime

import dask.dataframe as dd
import numpy
import pandas

from mdu.cache_17l import data_dir_path, data_file_path
import mdu.filter as filter

PACK_CARD_PREFIX = 'pack_card_'
POOL_PREFIX = 'pool_'
NUM_CARDS_IN_PACK = 13

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


def extend_draft_columns(draft_df: dd.DataFrame):
    draft_df['player_cohort'] = draft_df.apply(player_cohort, axis=1, meta=pandas.Series(dtype="object"))
    draft_df['date'] = draft_df['draft_time'].apply(lambda t: str(t[0:10]), meta=pandas.Series(dtype="object"))
    draft_df['week'] = draft_df['date'].apply(week_from_date, meta=pandas.Series(dtype="object"))
    draft_df['event_matches'] = draft_df['event_match_wins'] + draft_df['event_match_losses']

    return draft_df


def ata(draft_df: dd.DataFrame, groupby='name'):
    return draft_df.groupby('picked').pick_number.mean().compute().rename(columns={'picked': 'name'})


def draft_df_basic_aggs(draft_df: dd.DataFrame):
    return df.groupby('name')[['event_matches', 'event_match_wins']].sum() 


def alsa(draft_df: dd.DataFrame, groupby='name'):
    if groupby != 'name':
        raise NotImplementedError(f'groupby {groupby} not implemented yet')
    pattern = f'^{PACK_CARD_PREFIX}'

    pack_card_cols = [c for c in draft_df.columns if c.startswith(PACK_CARD_PREFIX)]
    pack_seen_cols = [f"num_{c}" for c in pack_card_cols]
    is_seen_cols = [f"count_{c}" for c in pack_card_cols]
    names = [re.split(pattern, col)[1] for col in pack_card_cols]
        
    def alsa_lambda(df):
        is_seen_df = numpy.minimum(df[pack_card_cols], 1)
        pack_seen_df = is_seen_df.mul(df['pick_number'] + 1, axis=0)

        data_df = pandas.concat([
            df[['draft_id', 'pack_number']],
            is_seen_df.rename(columns=dict(zip(pack_card_cols, is_seen_cols))),
            pack_seen_df.rename(columns=dict(zip(pack_card_cols, pack_seen_cols)))
        ], axis=1)

        grouped_df = pandas.concat(
            [
                data_df.groupby(['draft_id', 'pack_number']).max(), 
                pandas.DataFrame({'pick_count': data_df.groupby(['draft_id', 'pack_number']).draft_id.count()})
            ], axis=1
        )

        return grouped_df[grouped_df['pick_count'] == NUM_CARDS_IN_PACK]

    last_seen_agg = draft_df \
        .map_partitions(alsa_lambda) \
        .sum().compute()
    
    return pandas.DataFrame(
        {
            'alsa': last_seen_agg[pack_seen_cols].values / last_seen_agg[is_seen_cols].values,
            'num_seen': last_seen_agg[is_seen_cols].values
        },
        index=names
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
        self._card_df = dd.read_csv(data_file_path(set_code, 'card'))
        self._game_df = dd.read_csv(data_file_path(set_code, 'game'))
        self._dv = None

    @property
    def draft_view(self):
        if self._dv is None:
            dv = self._draft_df.copy()
            dv.set_index('draft_id_idx', sorted=True)
            dv = extend_draft_columns(dv)
            if self._filter is not None:
                dv = dv.loc[self._filter]
            self._dv = dv
        return self._dv
    
    def set_filter(self, filter_spec: any):
        if type(filter_spec) == dict:
            filter_spec = filter.from_spec(filter_spec)
        self._filter = filter_spec

    def set_index(self):
        self._draft_df.set_index('draft_id_idx', sorted=True)

    def alsa(self):
        return alsa(self.draft_view)
