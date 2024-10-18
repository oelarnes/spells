# launch `ipython -i scripts/interactive.py` or use `from scripts.interactive import *` in Jupyter

import os, sys, functools, re

import dask.dataframe as dd
import pandas
import numpy
import matplotlib.pyplot as plt

import mdu.draft_data as draft_data
import mdu.filter
import mdu.cache_17l as c17

os.environ['MDU_PROJECT_DIR'] = '/Users/Joel/Projects/magic-data-utils/tests'
print("%============================================================%")
print(f"""
    set_code            = {(set_code := "BLB")}
    ddo                 = {(ddo := draft_data.DraftData(set_code))}
    $MDU_PROJECT_DIR    = {os.environ['MDU_PROJECT_DIR']}
""")
print("%============================================================%")
self = ddo