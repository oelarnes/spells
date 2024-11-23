"launch `pdm run ispells [test]` or use `from scripts.interactive import *` in Jupyter"

import os, sys, functools, re
import importlib

import polars as pl

import spells
import spells.external as external
import spells.columns as mcol
import spells.manifest
from spells.enums import *
from spells.schema import schema

if len(sys.argv) > 1 and sys.argv[1] == "test":
    os.environ["SPELLS_PROJECT_DIR"] = "/Users/Joel/Projects/spells/tests"

myn = ["Cache Grab", "Carrot Cake", "Savor", "Take Out the Trash", "Shore Up"]
m = spells.manifest.create(None, None, None, None)

# pl.Config.set_tbl_cols(50)
# pl.Config.set_tbl_rows(500)

print(
    "%===============================================================================================%"
)
print(
    f"""
    $SPELLS_PROJECT_DIR    = {os.environ['SPELLS_PROJECT_DIR']}
    set_code            = {(set_code := "BLB")}
    group_by            = {(group_by := ['name'])}
    myn                 = {(myn := myn)}
    mcol                = {(mcol := mcol)}
    external            = {(external := external)}
    df_path             = {(df_path := external.data_file_path(set_code, View.DRAFT))}
    base_view_df        = {(base_view_df := pl.scan_csv(df_path, schema=schema(df_path)))}
    m                   = {type(m)}
"""
)
print(
    "%===============================================================================================%"
)
