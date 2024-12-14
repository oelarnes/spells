# ruff: noqa

import os

import polars as pl

from spells import summon, ColName, ColType, ColSpec

import spells.external as external
import spells.cache as cache
import spells.manifest as manifest
from spells.enums import View
from spells.schema import schema
from spells.extension import stat_cols, context_cols

code = "LCI"
csv_path = os.path.expanduser(
    "~/.local/share/spells/external/LCI/game_data_public.LCI.PremierDraft.csv"
)

game_df = pl.scan_csv(csv_path)

my_schema = game_df.collect_schema()

want_schema = schema(csv_path)
