"""
test manifest creation and validation
"""
import pytest

from mdu.columns import ColumnDefinition
import mdu.manifest

@pytest.mark.parametrize(
    "columns, groupbys, filter_spec, extensions, expected",
    [
        (None, None, None, None, """""")
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

    assert str(m) == expected



