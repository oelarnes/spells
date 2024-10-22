# launch `ipython -i scripts/interactive.py [test]` or use `from scripts.interactive import *` in Jupyter

import os, sys, functools, re
import importlib

import dask.dataframe as dd
import pandas
import numpy
import matplotlib.pyplot as plt

import mdu.draft_data as draft_data
import mdu.filter
import mdu.cache_17l as c17

if len(sys.argv) > 1 and sys.argv[1] == 'test':
    os.environ['MDU_PROJECT_DIR'] = '/Users/Joel/Projects/magic-data-utils/tests'
else:
    os.environ['MDU_PROJECT_DIR'] = '/Users/Joel/Projects/magic-data-utils'

print("%====================================================================================================%")
print(f"""
    $MDU_PROJECT_DIR    = {os.environ['MDU_PROJECT_DIR']}
    set_code            = {(set_code := "BLB")}
    ddo, self           = {(ddo := draft_data.DraftData(set_code))}
    groupbys            = {(groupbys := ['name'])}
    myn                 = {(myn := ['Cache Grab', 'Carrot Cake', 'Savor', 'Take Out the Trash', 'Shore Up'])}
""")
print("%====================================================================================================%")
self = ddo
