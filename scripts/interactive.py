# launch `ipython -i scripts/interactive.py` or use `from scripts.interactive import *` in Jupyter

import os, sys, functools, re

import dask.dataframe as dd
import pandas
import numpy
import matplotlib.pyplot as plt

import mdu.draft_data as draft_data
import mdu.filter
import mdu.cache_17l as c17