"""
enums
"""

from enum import StrEnum


class View(StrEnum):
    GAME = "game"
    DRAFT = "draft"
    CARD = "card"


class ColType(StrEnum):
    FILTER_ONLY = "filter_only"
    GROUP_BY = "group_by"
    PICK_SUM = "pick_sum"
    GAME_SUM = "game_sum"
    NAME_SUM = "name_sum"
    AGG = "agg"
    CARD_ATTR = "card_attr"


class ColName(StrEnum):
    """
    A list of all available columns, including built-in extensions.

    "Name-mapped" columns like "deck_<card name>" are identified by the prefix only.
    Those columns can be referenced simply as e.g. "deck" in formulas for the post-agg stage.

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
    ON_PLAY = "on_play"
    NUM_MULLIGANS = "num_mulligans"
    OPP_NUM_MULLIGANS = "opp_num_mulligans"
    OPP_COLORS = "opp_colors"
    NUM_TURNS = "num_turns"
    WON = "won"
    OPENING_HAND = "opening_hand"
    DRAWN = "drawn"
    TUTORED = "tutored"
    DECK = "deck"
    SIDEBOARD = "sideboard"
    # card
    SET_CODE = "set_code"
    COLOR = "color"
    RARITY = "rarity"
    COLOR_IDENTITY = "color_identity"
    CARD_TYPE = "card_type"
    SUBTYPE = "subtype"
    MANA_VALUE = "mana_value"
    MANA_COST = "mana_cost"
    POWER = "power"
    TOUGHNESS = "toughness"
    IS_BONUS_SHEET = "is_bonus_sheet"
    IS_DFC = "is_dfc"

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
    LAST_SEEN = "last_seen"
    NUM_SEEN = "num_seen"
    NAME = "name"  # special column for card name index
    PLAYER_COHORT = "player_cohort"
    GAME_DATE = "game_date"
    GAME_DAY_OF_WEEK = "game_day_of_week"
    GAME_HOUR = "game_hour"
    GAME_WEEK = "game_week"
    NUM_COLORS = "num_colors"
    HAS_SPLASH = "has_splash"
    ON_PLAY_SUM = "on_play_sum"
    NUM_WON = "num_won"
    NUM_GAMES = "num_games"
    NUM_MATCHES = "num_matches"
    NUM_EVENTS = "num_events"
    WON_OPENING_HAND = "won_opening_hand"
    WON_DRAWN = "won_drawn"
    WON_TUTORED = "won_tutored"
    WON_DECK = "won_deck"
    WON_SIDEBOARD = "won_sideboard"

    # agg extensions
    ALSA = "alsa"
    ATA = "ata"
    NUM_GP = "num_gp"
    PCT_GP = "pct_gp"
    GP_WR = "gp_wr"
    NUM_OH = "num_oh"
    OH_WR = "oh_wr"
    NUM_GIH = "num_gih"
    NUM_GIH_WON = "num_gih_won"
    GIH_WR = "gih_wr"
    NUM_IN_POOL = "num_in_pool"
    IN_POOL_WR = "in_pool_wr"
    IHD = "ihd"
    NUM_DECK_GIH = "num_deck_gih"
    WON_NUM_DECK_GIH = "won_num_deck_gih"
    DECK_GIH_WR = "deck_gih_wr"
    TROPHY_RATE = "trophy_rate"
    GAME_WR = "game_wr"
    WON_DECK_TOTAL = "won_deck_total"
    DECK_TOTAL = "deck_total"
    GP_WR_MEAN = "gp_wr_mean"
