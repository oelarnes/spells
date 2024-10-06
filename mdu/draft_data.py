import os
import re
from enum import Enum, auto

import dask.dataframe as dd

from mdu.config.cache_17l import FILES
from mdu.cache_17l import data_dir_path, card_file_name
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

    melted_df = dd.reshape.melt(draft_df, ['draft_id', 'pack_number', 'pick_number', 'pick'], unpivot_cols, 'name', 'num_in_pack')

    melted_df['name'] = melted_df['name'].map(lambda name: re.split(pattern, name)[1], meta=('name', 'object'))
    melted_df['is_pick'] = (melted_df['name'] == melted_df['pick'])
    melted_df = melted_df.drop('pick', axis=1)
    melted_df = melted_df[melted_df['num_in_pack'] > 0]

    return melted_df

class DraftData:
    def __init__(
        self,
        set_code: str, 
        filter_spec: any = None
    ):
        data_dir = data_dir_path()

        self.set_code = set_code.upper()
        
        self.set_filter(filter_spec)

        self._draft_df = dd.read_csv(os.path.join(data_dir, FILES[set_code]['draft']['target'])).set_index('draft_id_idx', sorted=True)
        self._card_df = dd.read_csv(os.path.join(data_dir, card_file_name(set_code)))
        self._game_df = dd.read_csv(os.path.join(data_dir, FILES[set_code]['game']['target']))

        # self._draft_pick_view = _draft_pick_view(self._draft_df)
    
    def set_filter(self, filter_spec: any):
        if type(filter_spec) == dict:
            filter_spec = filter.from_spec(filter_spec)
        self._filter = filter_spec
