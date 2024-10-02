import os
from enum import Enum, auto

import dask.dataframe as dd

from mdu.config.cache_17l import FILES
from mdu.cache_17l import data_dir_path, card_file_name
import mdu.filter as filter

class DataSetType(Enum):
    PANDAS_DF = auto()

class SeriesType(Enum):
    PANDAS_SERIES = auto()

class GroupBys(Enum):
    EVENT_TYPE = 'event_type'
    RANK = 'rank'
    DRAFT_DATE = 'draft_date'
    PICK_NUM = 'pick_num'
    PACK_NUM = 'pack_num'
    USER_N_GAMES_BUCKET = 'user_n_games_bucket'
    USER_GAME_WIN_RATE_BUCKET = 'user_game_win_rate_bucket'
    NAME = 'name'
    CARD_INDEX = 'card_index'
    COLOR = 'color'
    RARITY = 'rarity'
    TYPE = 'type'
    SUBTYPE = 'subtype'
    CMC = 'cmc'

class AGG_METRICS(Enum):
    COUNT = 'count'
    APWR = 'apwr' # as-picked winrate
    ATA = 'ata' # average taken at
    ALSA = 'alsa' # average last seen at
    ASA = 'asa' # average seen at
    GP_PCT = 'gp%' # play rate
    PICK_RATE = 'pick%' # the times a card matching the group by condition was chosen divided by the total matches on that condition


class DraftData:
    def __init__(
        self,
        set_code: str, 
        start_date: str = '0000', 
        end_date: str = '9999',
        additional_filter: any = None
    ):
        data_dir = data_dir_path()

        self.set_code = set_code.upper()
        
        date_filter = filter.allof([
            filter.base('draft_time', start_date, '>='),
            filter.base('draft_time', end_date, '<=')
        ])

        if additional_filter:
            self.filter = filter.all_of(additional_filter, date_filter)
        else:
            self.filter = date_filter

        self._draft_df = dd.read_csv(os.path.join(data_dir, FILES[set_code]['draft']))
        self._card_df = dd.read_csv(os.path.join(data_dir, card_file_name(set_code)))
        self._game_df = dd.read_csv(os.path.join(data_dir, FILES[set_code]['game']))
    
    def set_filter(self, filter_spec: any):
        if type(filter_spec) == dict:
            filter_spec = filter.from_spec(filter_spec)
        self._filter = filter_spec
