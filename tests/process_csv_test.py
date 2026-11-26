import gzip

import polars as pl
import pytest

from spells import external
from spells.schema import schema

QUOTED = 'pack_card_Dr. Leonard "Bones" McCoy'

# The header escapes the quotes the way 17Lands publishes it
CSV = (
    "draft_id,pick,pick_2_maindeck_rate,brand_new_column,"
    '"pack_card_Dr. Leonard ""Bones"" McCoy",pack_card_Opt\n'
    "a,Opt,0.5,x,1,0\n"
    'b,"Dr. Leonard ""Bones"" McCoy",,y,0,{opt}\n'
)


def write_gz(path, opt="1"):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(CSV.format(opt=opt))


def test_schema_reads_names_with_their_quoting_undone(tmp_path):
    path = tmp_path / "draft.csv"
    path.write_text(CSV.format(opt="1"))
    assert QUOTED in schema(str(path))


def test_schema_keeps_columns_no_pattern_knows_as_strings(tmp_path):
    path = tmp_path / "draft.csv"
    path.write_text(CSV.format(opt="1"))
    dtypes = schema(str(path))
    assert len(dtypes) == 6
    assert dtypes["brand_new_column"] == pl.String
    assert dtypes["pick_2_maindeck_rate"] == pl.Float64


@pytest.mark.parametrize("opt", ["1", "1.0"])
def test_conversion_keeps_every_column_under_its_real_name(tmp_path, opt):
    """A strict Int8 read can't parse 1.0, which forces the in-memory fallback."""
    gz = tmp_path / "draft.csv.gz"
    target = tmp_path / "draft.parquet"
    write_gz(gz, opt)

    external._process_zipped_file(str(gz), str(target))

    df = pl.read_parquet(target)
    assert QUOTED in df.columns
    assert df[QUOTED].to_list() == [1, 0]
    assert "brand_new_column" in df.columns
    assert sorted(p.name for p in tmp_path.iterdir()) == ["draft.parquet"]


def test_a_failed_conversion_leaves_nothing_behind(tmp_path):
    gz = tmp_path / "draft.csv.gz"
    gz.write_bytes(b"not gzip")

    with pytest.raises(OSError):
        external._process_zipped_file(str(gz), str(tmp_path / "draft.parquet"))

    assert list(tmp_path.iterdir()) == []
