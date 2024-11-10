"launch `ipython -i scripts/interactive.py [test]` or use `from scripts.interactive import *` in Jupyter"

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
from mdu.enums import *
from mdu.get_schema import schema

if len(sys.argv) > 1 and sys.argv[1] == "test":
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils/tests"
else:
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils"

pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", 100)
myn = ["Cache Grab", "Carrot Cake", "Savor", "Take Out the Trash", "Shore Up"]

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
    defs                = {(defs := mcol.column_def_map)}
    df_path             = {(df_path := c17.data_file_path(set_code, View.DRAFT))}
    df                  = {(df := pl.scan_csv(df_path, schema=schema(df_path)))}
"""
)
print(
    "%===============================================================================================%"
)
