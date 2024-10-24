"""
extension configuration for custom columns
"""

from mdu import calcs

game_counts_extensions = (
    {"prefix": "mull", "calc": calcs.game_counts_mull},
    {"prefix": "turn", "calc": calcs.game_counts_turns},
    {"prefix": "numoh", "calc": calcs.game_counts_numoh},
    {"prefix": "numdr", "calc": calcs.game_counts_numdr},
)
