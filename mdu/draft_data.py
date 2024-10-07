import os
import re

import dask.dataframe as dd
import numpy
import pandas

from mdu.cache_17l import data_dir_path, data_file_path
import mdu.filter as filter

PACK_CARD_PREFIX = 'pack_card_'
POOL_PREFIX = 'pool_'

def _draft_meta_view(draft_df: dd.DataFrame):
    meta_view = draft_df[['draft_id', 'draft_time', 'rank', 'event_match_wins', 'event_match_losses', 'user_n_games_bucket', 'user_game_win_rate_bucket']]
    meta_view = meta_view.drop_duplicates()
    return meta_view

def _draft_pick_view(draft_df: dd.DataFrame):
    pattern = f'^{PACK_CARD_PREFIX}' 

    unpivot_cols = tuple(name for name in draft_df.columns if re.search(pattern, name) is not None)

    melted_df = dd.reshape.melt(draft_df, ['draft_id_idx', 'draft_id', 'pack_number', 'pick_number', 'pick'], unpivot_cols, 'name', 'num_in_pack')

    melted_df['name'] = melted_df['name'].map(lambda name: re.split(pattern, name)[1], meta=('name', 'object'))
    melted_df['is_pick'] = (melted_df['name'] == melted_df['pick'])
    melted_df = melted_df.drop('pick', axis=1)
    melted_df = melted_df[melted_df['num_in_pack'] > 0]

    return melted_df

def alsa(draft_pick_view: dd.DataFrame, groupby='name'):
    """
    map_partitions to group by draft_id

    ungodly slow, try something else
    """
    if groupby != 'name':
        raise NotImplementedError(f'groupby {groupby} not implemented yet')
    
    last_seen = draft_pick_view \
        .set_index('draft_id_idx', sorted=True) \
        .map_partitions(lambda df: df.groupby(['draft_id', 'pack_number', 'name']).pick_number.max() + 1) \
    
    return last_seen.groupby('name').mean().compute()

def ata(draft_df: dd.DataFrame, groupby='name'):
    return draft_df.groupby('picked').pick_number.mean().compute().rename(columns={'picked': 'name'})

def apwr(draft_df: dd.DataFrame, groupby='name'):
    df = draft_df.copy()
    df['event_matches'] = df['event_match_wins'] + df['event_match_losses']
    return df.groupby('name')[['event_matches', 'event_match_wins']].sum() 

def alsa2(draft_df: dd.DataFrame, groupby='name'):
    if groupby != 'name':
        raise NotImplementedError(f'groupby {groupby} not implemented yet')

    pack_card_cols = [c for c in draft_df.columns if c.startswith('pack_card_')]
    draft_df.loc[pack_card_cols] = dd.min(draft_df[pack_card_cols], 1)

    def alsa_lambda(df):
        is_seen_df = numpy.minimum(df[pack_card_cols], 1)
        pack_seen_df = is_seen_df.mul(df['pick_number'] + 1, axis=0)

        pack_seen_cols = [f"num_{c}" for c in pack_card_cols]
        is_seen_cols = [f"count_{c}" for c in pack_card_cols]
        
        data_df = pandas.concat([
            df[['draft_id']],
            is_seen_df.rename(columns=dict(zip(pack_card_cols, is_seen_cols))),
            pack_seen_df.rename(columns=dict(zip(pack_card_cols, pack_seen_cols)))
        ], axis=1)

        pack_max_df = data_df.groupby(['draft_id', 'pack_number']).max()
        pack_count_df = data_df.groupby(['draft_id', 'pack_number']).count()

        grouped_df = pack_seen_df.groupby(['draft_id', 'pack_number']).agg({''})
    
    last_seen = draft_df \
        .set_index('draft_id_idx', sorted - True) \
        .map_partitions(lambda df: )
        

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

        # self._draft_pick_view = _draft_pick_view(self._draft_df)
    
    def set_filter(self, filter_spec: any):
        if type(filter_spec) == dict:
            filter_spec = filter.from_spec(filter_spec)
        self._filter = filter_spec
