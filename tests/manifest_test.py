"""
test manifest creation and validation
"""
import pytest

import polars as pl

from spells.columns import ColumnDefinition
from spells.enums import ColType, View
import spells.manifest

@pytest.mark.parametrize(
    "columns, group_by, filter_spec, extensions, expected",
    [
        (None, None, None, None, """{
  columns:
    alsa
    ata
    color
    gih_wr
    gp_wr
    num_gih
    num_gp
    num_oh
    num_seen
    num_taken
    oh_wr
    pct_gp
    rarity
  base_view_group_by:
    name
  view_cols:
    card:
      color
      rarity
    draft:
      last_seen
      num_seen
      num_taken
      pick
      taken_at
    game:
      deck
      drawn
      opening_hand
      sideboard
      won_deck
      won_drawn
      won_opening_hand
  group_by:
    name
}
"""),
        (None, ["player_cohort"], None, None, """{
  columns:
    alsa
    ata
    gih_wr
    gp_wr
    num_gih
    num_gp
    num_oh
    num_seen
    num_taken
    oh_wr
    pct_gp
  base_view_group_by:
    player_cohort
  view_cols:
    draft:
      last_seen
      num_seen
      num_taken
      pick
      player_cohort
      taken_at
    game:
      deck
      drawn
      opening_hand
      player_cohort
      sideboard
      won_deck
      won_drawn
      won_opening_hand
  group_by:
    player_cohort
}
"""),
        (["ata"], ["draft_week", "name"], None, None, """{
  columns:
    ata
  base_view_group_by:
    draft_week
    name
  view_cols:
    draft:
      draft_week
      num_taken
      pick
      taken_at
  group_by:
    draft_week
    name
}
"""),
        (["alsa"], None, {"rank": "gold"}, None, """{
  columns:
    alsa
  base_view_group_by:
    name
  view_cols:
    draft:
      last_seen
      num_seen
      rank
  group_by:
    name
}
"""),
        (["alsa_plus_one"], None, None, [ColumnDefinition('alsa_plus_one', ColType.AGG, pl.col('alsa') + 1, (), ['alsa'])], """{
  columns:
    alsa_plus_one
  base_view_group_by:
    name
  view_cols:
    draft:
      last_seen
      num_seen
  group_by:
    name
}
"""),
        (["gp_wr", "num_gp", "pct_gp"], ["color"], None, None, """{
  columns:
    gp_wr
    num_gp
    pct_gp
  base_view_group_by:
    name
  view_cols:
    card:
      color
    game:
      deck
      sideboard
      won_deck
  group_by:
    color
}
"""),
        (["event_match_wins_sum"], ["is_winner"], None, [ColumnDefinition('is_winner', ColType.GROUP_BY, pl.col('user_game_win_rate_bucket') > 0.55, (View.DRAFT, View.GAME))], """{
  columns:
    event_match_wins_sum
  base_view_group_by:
    is_winner
  view_cols:
    draft:
      event_match_wins_sum
      is_winner
      pick
  group_by:
    is_winner
}
"""),
    ]
)
def test_create_manifest(
    columns: list[str] | None,
    group_by: list[str] | None,
    filter_spec: dict | None,
    extensions: list[ColumnDefinition] | None,
    expected: str,
):
    m = spells.manifest.create(columns, group_by, filter_spec, extensions)

    print(m.test_str())
    assert m.test_str() == expected




