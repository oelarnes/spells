"""
enums
"""

from enum import StrEnum


class View(StrEnum):
    GAME = "game_view"
    DRAFT = "draft_view"
    AGG = "agg"  # post-sum calculations such as averages use this regardless of dependencies


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
    PICKED = "picked"
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
    PACK_NUM = "pack_num"  # pack_number plus 1
    PICK_NUM = "pick_num"  # pick_number plus 1
    PACK_NUM_CARD = "pack_num_card"
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
