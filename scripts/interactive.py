"launch `pdm run ispells [test]` or use `from scripts.interactive import *` in Jupyter"

import os, sys, functools, re
import importlib

import polars as pl

import spells.draft_data as draft_data
from spells.filter import from_spec 
import spells.cache_17l as c17
import spells.columns as mcol
import spells.manifest
from spells.enums import *
from spells.get_schema import schema

if len(sys.argv) > 1 and sys.argv[1] == "test":
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils/tests"
else:
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils"

myn = ["Cache Grab", "Carrot Cake", "Savor", "Take Out the Trash", "Shore Up"]
m = spells.manifest.create(None, None, None, None)

pl.Config.set_tbl_cols(50)
pl.Config.set_tbl_rows(500)

print(
    "%===============================================================================================%"
)
print(
    f"""
    $MDU_PROJECT_DIR    = {os.environ['MDU_PROJECT_DIR']}
    set_code            = {(set_code := "BLB")}
    group_by            = {(group_by := ['name'])}
    myn                 = {(myn := myn)}
    mcol                = {(mcol := mcol)}
    c17                 = {(c17 := c17)}
    draft_data          = {(draft_data := draft_data)}
    df_path             = {(df_path := c17.data_file_path(set_code, View.DRAFT))}
    base_view_df        = {(base_view_df := pl.scan_csv(df_path, schema=schema(df_path)))}
    m                   = {type(m)}
"""
)
print(
    "%===============================================================================================%"
)
