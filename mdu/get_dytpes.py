# Downloaded from 17Lands.com on 2024-10-17 under below license
# https://www.17lands.com/public_datasets
# https://creativecommons.org/licenses/by/4.0/
#
# Modified by :s%/float16/float64/g

import re
import pandas as pd
from typing import Dict

COLUMN_TYPES = (
	# Metadata
	(re.compile(r"^user_n_games_bucket$"), "int16"),
	(re.compile(r"^user_game_win_rate_bucket$"), "float64"),
	(re.compile(r"^expansion$"), "str"),
	(re.compile(r"^event_type$"), "str"),
	(re.compile(r"^draft_id$"), "str"),
	(re.compile(r"^draft_time$"), "str"),
	(re.compile(r"^rank$"), "str"),
	# Draft
	(re.compile(r"^event_match_wins$"), "int8"),
	(re.compile(r"^event_match_losses$"), "int8"),
	(re.compile(r"^pack_number$"), "int8"),
	(re.compile(r"^pick_number$"), "int8"),
	(re.compile(r"^pick$"), "str"),
	(re.compile(r"^pick_maindeck_rate$"), "float64"),
	(re.compile(r"^pick_sideboard_in_rate$"), "float64"),
	(re.compile(r"^pool_.*"), "int8"),
	(re.compile(r"^pack_card_.*"), "int8"),
	# Game + Replay
	(re.compile(r"^game_time$"), "str"),
	(re.compile(r"^build_index$"), "int8"),
	(re.compile(r"^match_number$"), "int8"),
	(re.compile(r"^game_number$"), "int8"),
	(re.compile(r"^opp_rank$"), "str"),
	(re.compile(r"^main_colors$"), "str"),
	(re.compile(r"^splash_colors$"), "str"),
	(re.compile(r"^on_play$"), "bool"),
	(re.compile(r"^num_mulligans$"), "int8"),
	(re.compile(r"^opp_num_mulligans$"), "int8"),
	(re.compile(r"^opp_colors$"), "str"),
	(re.compile(r"^num_turns$"), "int8"),
	(re.compile(r"^won$"), "bool"),
	(re.compile(r"^deck_.*"), "int8"),
	(re.compile(r"^sideboard_.*"), "int8"),
	# Game
	(re.compile(r"^drawn_.*"), "int8"),
	(re.compile(r"^tutored_.*"), "int8"),
	(re.compile(r"^opening_hand_.*"), "int8"),
	# Replay
	(re.compile(r"^candidate_hand_\d$"), "str"),
	(re.compile(r"^opening_hand$"), "str"),
	(re.compile(r"^user_turn_\d+_cards_drawn$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_cards_discarded$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_lands_played$"), "str"),
	(re.compile(r"^user_turn_\d+_cards_foretold$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_cast$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_non_creatures_cast$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_instants_sorceries_cast$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_abilities$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_cards_learned$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_attacked$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blocked$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_unblocked$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blocking$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_creatures_blitzed$"), "int8"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_player_combat_damage_dealt$"), "str"),  # DEPRECATED
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_combat_damage_taken$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_creatures_killed_combat$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_creatures_killed_non_combat$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_((user)|(oppo))_mana_spent$"), "float64"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_user_cards_in_hand$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_oppo_cards_in_hand$"), "float64"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_lands_in_play$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_creatures_in_play$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_non_creatures_in_play$"), "str"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_life$"), "float64"),
	(re.compile(r"^((user)|(oppo))_turn_\d+_eot_((user)|(oppo))_poison_counters$"), "float64"),
	(re.compile(r"^user_turn_\d+_cards_tutored$"), "str"),
	(re.compile(r"^oppo_turn_\d+_cards_tutored$"), "int8"),
	(re.compile(r"^oppo_turn_\d+_cards_drawn_or_tutored$"), "int8"),
	(re.compile(r"^oppo_turn_\d+_cards_drawn$"), "int8"),
	(re.compile(r"^oppo_turn_\d+_cards_foretold$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_cards_drawn$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_cards_discarded$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_lands_played$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_cards_foretold$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_creatures_cast$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_creatures_blitzed$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_non_creatures_cast$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_instants_sorceries_cast$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_cards_learned$"), "int8"),
	(re.compile(r"^((user)|(oppo))_total_mana_spent$"), "int16"),
	(re.compile(r"^oppo_total_cards_drawn_or_tutored$"), "int8"),
)


def get_dtypes(filename: str, print_missing: bool = False) -> Dict[str, str]:
	dtypes: Dict[str, str] = {}
	for column in pd.read_csv(filename, nrows=0).columns:
		for regex, column_type in COLUMN_TYPES:
			if regex.match(column):
				dtypes[column] = column_type
				break
		else:
			if print_missing:
				print(f"Could not find an appropriate type for {column}")

	return dtypes