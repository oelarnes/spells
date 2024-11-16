"launch `pdm run imdu [test]` or use `from scripts.interactive import *` in Jupyter"

import os, sys, functools, re
import importlib

import pandas as pd
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

import mdu.draft_data as draft_data
from mdu.filter import from_spec 
import mdu.cache_17l as c17
import mdu.columns as mcol
import mdu.manifest
from mdu.enums import *
from mdu.get_schema import schema

if len(sys.argv) > 1 and sys.argv[1] == "test":
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils/tests"
else:
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils"

myn = ["Cache Grab", "Carrot Cake", "Savor", "Take Out the Trash", "Shore Up"]
m = mdu.manifest.create(None, None, None, None)
print(
    "%===============================================================================================%"
)
print(
    f"""
    $MDU_PROJECT_DIR    = {os.environ['MDU_PROJECT_DIR']}
    set_code            = {(set_code := "BLB")}
    groupbys            = {(groupbys := ['name'])}
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
