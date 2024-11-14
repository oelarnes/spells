"""
test manifest creation and validation
"""
import pytest

import polars as pl

from mdu.columns import ColumnDefinition
from mdu.enums import ColType
import mdu.manifest

@pytest.mark.parametrize(
    "columns, groupbys, filter_spec, extensions, expected",
    [
        (None, None, None, None, """{
  columns:
    alsa
    ata
    num_seen
    num_taken
  base_view_groupbys:
    name
  view_cols:
    draft:
      last_seen
      num_seen
      num_taken
      pick
      taken_at
}
"""),
        (None, ["player_cohort"], None, None, """{
  columns:
    alsa
    ata
    num_seen
    num_taken
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
}
""")
    ]
)
def test_create_manifest(
    columns: list[str] | None,
    groupbys: list[str] | None,
    filter_spec: dict | None,
    extensions: list[ColumnDefinition] | None,
    expected: str,
):
    m = mdu.manifest.create(columns, groupbys, filter_spec, extensions)

    print(m.test_str())
    assert m.test_str() == expected




