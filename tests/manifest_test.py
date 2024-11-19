"""
test manifest creation and validation
"""
import pytest

import polars as pl

from spells.columns import ColumnDefinition
from spells.enums import ColType
import spells.manifest

@pytest.mark.parametrize(
    "columns, groupbys, filter_spec, extensions, expected",
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
  base_view_groupbys:
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
  groupbys:
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
  base_view_groupbys:
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
  groupbys:
    player_cohort
}
"""),
        (["ata"], ["draft_week", "name"], None, None, """{
  columns:
    ata
  base_view_groupbys:
    draft_week
    name
  view_cols:
    draft:
      draft_week
      num_taken
      pick
      taken_at
  groupbys:
    draft_week
    name
}
"""),
        (["alsa"], None, {"rank": "gold"}, None, """{
  columns:
    alsa
  base_view_groupbys:
    name
  view_cols:
    draft:
      last_seen
      num_seen
      rank
  groupbys:
    name
}
"""),
        (["alsa_plus_one"], None, None, [ColumnDefinition('alsa_plus_one', ColType.AGG, pl.col('alsa') + 1, (), ['alsa'])], """{
  columns:
    alsa_plus_one
  base_view_groupbys:
    name
  view_cols:
    draft:
      last_seen
      num_seen
  groupbys:
    name
}
"""),
        (["gp_wr", "num_gp", "pct_gp"], ["color"], None, None, """{
  columns:
    gp_wr
    num_gp
    pct_gp
  base_view_groupbys:
    name
  view_cols:
    card:
      color
    game:
      deck
      sideboard
      won_deck
  groupbys:
    color
}
"""),
    ]
)
def test_create_manifest(
    columns: list[str] | None,
    groupbys: list[str] | None,
    filter_spec: dict | None,
    extensions: list[ColumnDefinition] | None,
    expected: str,
):
    m = spells.manifest.create(columns, groupbys, filter_spec, extensions)

    print(m.test_str())
    assert m.test_str() == expected




