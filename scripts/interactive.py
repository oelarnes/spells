"launch `ipython -i scripts/interactive.py [test]` or use `from scripts.interactive import *` in Jupyter"

import os, sys, functools, re
import importlib

import dask.dataframe as dd
import pandas
import numpy
import matplotlib.pyplot as plt

import mdu.draft_data as draft_data
import mdu.filter
import mdu.cache_17l as c17
import mdu.columns as mcol
from mdu.enums import *

if len(sys.argv) > 1 and sys.argv[1] == "test":
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils/tests"
else:
    os.environ["MDU_PROJECT_DIR"] = "/Users/Joel/Projects/magic-data-utils"

pandas.set_option("display.max_rows", 1000)
pandas.set_option("display.max_columns", 100)
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
"""
)
print(
    "%===============================================================================================%"
)
