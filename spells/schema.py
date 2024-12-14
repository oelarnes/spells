# Downloaded from 17Lands.com on 2024-10-17 under below license
# https://www.17lands.com/public_datasets
# https://creativecommons.org/licenses/by/4.0/
#
# Modified by :s%/float16/float64/g
# converted to use by polars

import csv
import re
from typing import Dict

import polars as pl

COLUMN_TYPES = (
    # Metadata
    (re.compile(r"^draft_id_idx$"), pl.Int64),
    (re.compile(r"^user_n_games_bucket$"), pl.Int16),
    (re.compile(r"^user_game_win_rate_bucket$"), pl.Float64),
    (re.compile(r"^expansion$"), pl.String),
    (re.compile(r"^event_type$"), pl.String),
    (re.compile(r"^draft_id$"), pl.String),
    (re.compile(r"^draft_time$"), pl.String),
    (re.compile(r"^rank$"), pl.String),
    # Draft
    (re.compile(r"^event_match_wins$"), pl.Int8),
    (re.compile(r"^event_match_losses$"), pl.Int8),
    (re.compile(r"^pack_number$"), pl.Int8),
    (re.compile(r"^pick_number$"), pl.Int8),
    (re.compile(r"^pick$"), pl.String),
    (re.compile(r"^pick_maindeck_rate$"), pl.Float64),
    (re.compile(r"^pick_sideboard_in_rate$"), pl.Float64),
    (re.compile(r"^pool_.*"), pl.Int8),
    (re.compile(r"^pack_card_.*"), pl.Int8),
    # Game + Replay
    (re.compile(r"^game_time$"), pl.String),
    (re.compile(r"^build_index$"), pl.Int8),
    (re.compile(r"^match_number$"), pl.Int8),
    (re.compile(r"^game_number$"), pl.Int8),
    (re.compile(r"^opp_rank$"), pl.String),
    (re.compile(r"^main_colors$"), pl.String),
    (re.compile(r"^splash_colors$"), pl.String),
    (re.compile(r"^on_play$"), pl.Boolean),
    (re.compile(r"^num_mulligans$"), pl.Int8),
    (re.compile(r"^opp_num_mulligans$"), pl.Int8),
    (re.compile(r"^opp_colors$"), pl.String),
    (re.compile(r"^num_turns$"), pl.Int8),
    (re.compile(r"^won$"), pl.Boolean),
    (re.compile(r"^deck_.*"), pl.Int8),
    (re.compile(r"^sideboard_.*"), pl.Int8),
    # Game
    (re.compile(r"^drawn_.*"), pl.Int8),
    (re.compile(r"^tutored_.*"), pl.Int8),
    (re.compile(r"^opening_hand_.*"), pl.Int8),
    # Replay
    (re.compile(r"^candidate_hand_\d$"), pl.String),
    (re.compile(r"^opening_hand$"), pl.String),
    (re.compile(r"^user_turn_\d+_cards_drawn$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_cards_discarded$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_lands_played$"), pl.String),
    (re.compile(r"^user_turn_\d+_cards_foretold$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_cast$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_non_creatures_cast$"), pl.String),
    (
        re.compile(
            r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_instants_sorceries_cast$"
        ),
        pl.String,
    ),
    (re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_abilities$"), pl.String),
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_cards_learned$"),
        pl.String,
    ),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_attacked$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blocked$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_unblocked$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blocking$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blitzed$"), pl.Int8),
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_player_combat_damage_dealt$"),
        pl.String,
    ),  # DEPRECATED
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_combat_damage_taken$"),
        pl.String,
    ),
    (
        re.compile(
            r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_creatures_killed_combat$"
        ),
        pl.String,
    ),
    (
        re.compile(
            r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_creatures_killed_non_combat$"
        ),
        pl.String,
    ),
    (re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_mana_spent$"), pl.Float64),
    (re.compile(r"^((user)|(oppo))_turn_\d+_eot_user_cards_in_hand$"), pl.String),
    (re.compile(r"^((user)|(oppo))_turn_\d+_eot_oppo_cards_in_hand$"), pl.Float64),
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_lands_in_play$"),
        pl.String,
    ),
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_creatures_in_play$"),
        pl.String,
    ),
    (
        re.compile(
            r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_non_creatures_in_play$"
        ),
        pl.String,
    ),
    (re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_life$"), pl.Float64),
    (
        re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_poison_counters$"),
        pl.Float64,
    ),
    (re.compile(r"^user_turn_\d+_cards_tutored$"), pl.String),
    (re.compile(r"^oppo_turn_\d+_cards_tutored$"), pl.Int8),
    (re.compile(r"^oppo_turn_\d+_cards_drawn_or_tutored$"), pl.Int8),
    (re.compile(r"^oppo_turn_\d+_cards_drawn$"), pl.Int8),
    (re.compile(r"^oppo_turn_\d+_cards_foretold$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_cards_drawn$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_cards_discarded$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_lands_played$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_cards_foretold$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_creatures_cast$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_creatures_blitzed$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_non_creatures_cast$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_instants_sorceries_cast$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_cards_learned$"), pl.Int8),
    (re.compile(r"^((user)|(oppo))_total_mana_spent$"), pl.Int16),
    (re.compile(r"^oppo_total_cards_drawn_or_tutored$"), pl.Int8),
)


def schema(
    filename: str, print_missing: bool = False
) -> Dict[str, pl.datatypes.DataType]:
    dtypes: Dict[str, pl.datatypes.DataType] = {}
    with open(filename, encoding="utf-8") as f:
        columns = csv.DictReader(f).fieldnames
    if columns is None:
        raise ValueError(f"Could not read fieldnames from {filename}")
    for column in columns:
        for regex, column_type in COLUMN_TYPES:
            if regex.match(column):
                dtypes[column] = column_type
                break
            else:
                if print_missing:
                    print(f"Could not find an appropriate type for {column}")
    return dtypes
