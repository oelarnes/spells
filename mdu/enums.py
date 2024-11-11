"""
enums
"""

from enum import StrEnum, Enum, auto


class View(StrEnum):
    GAME = "game"
    DRAFT = "draft"


class ColType(Enum):
    FILTER_ONLY = auto()
    GROUPBY = auto()
    PICK_SUM = auto()
    NAME_SUM = auto()
    AGG = auto()


class ColName(StrEnum):
    """
    A list of all available columns, including built-in extensions.

    "Name-mapped" columns like "deck_<card name>" are identified by the prefix only.

    New columns may be registered interactively via DraftData().register_column(), named as strings.

    The definitions of the columns and how they may be used is defined in `column_defs`
    """

    # shared
    #    EXPANSION = 'expansion'
    #    EVENT_TYPE = 'event_type'
    DRAFT_ID = "draft_id"
    DRAFT_TIME = "draft_time"  # modified, cast to time
    RANK = "rank"
    # draft
    EVENT_MATCH_WINS = "event_match_wins"
    EVENT_MATCH_LOSSES = "event_match_losses"
    EVENT_MATCHES = "event_matches"
    IS_TROPHY = "is_trophy"
    PACK_NUMBER = "pack_number"
    PICK_NUMBER = "pick_number"
    PICK = "pick"
    PICK_MAINDECK_RATE = "pick_maindeck_rate"
    PICK_SIDEBOARD_IN_RATE = "pick_sideboard_in_rate"
    PACK_CARD = "pack_card"
    POOL = "pool"
    USER_N_GAMES_BUCKET = "user_n_games_bucket"
    USER_GAME_WIN_RATE_BUCKET = "user_game_win_rate_bucket"
    # game
    GAME_TIME = "game_time"
    BUILD_INDEX = "build_index"
    MATCH_NUMBER = "match_number"
    GAME_NUMBER = "game_number"
    OPP_RANK = "opp_rank"  # not populated for recent sets
    MAIN_COLORS = "main_colors"
    SPLASH_COLORS = "splash_colors"
    # extensions
    DRAFT_DATE = "draft_date"
    DRAFT_DAY_OF_WEEK = "draft_day_of_week"
    DRAFT_HOUR = "draft_hour"
    DRAFT_WEEK = "draft_week"
    EVENT_MATCH_WINS_SUM = "event_match_wins_sum"
    EVENT_MATCH_LOSSES_SUM = "event_match_losses_sum"
    EVENT_MATCHES_SUM = "event_matches_sum"
    IS_TROPHY_SUM = "is_trophy_sum"
    TAKEN_AT = "taken_at"
    NUM_TAKEN = "num_taken"
    PACK_NUM = "pack_num"  # pack_number plus 1
    PICK_NUM = "pick_num"  # pick_number plus 1
    PACK_NUM_CARD = "pack_num_card"
    PICK_NUM_CARD = "pick_num_card"
    LAST_SEEN = "last_seen"
    NAME = "name"  # special column for card name index
    PLAYER_COHORT = "player_cohort"
    GAME_DATE = "game_date"
    GAME_DAY_OF_WEEK = "game_day_of_week"
    GAME_HOUR = "game_hour"
    GAME_WEEK = "game_week"

    NUM_SEEN = "num_seen"
    ALSA = "alsa"
    NUM_PICKED = "num_picked"
    ATA = "ata"
    NUM_GP = "num_gp"
    GP_PCT = "gp_pct"
    GP_WR = "gp_wr"
