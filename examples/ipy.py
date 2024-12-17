# ruff: noqa

import os
import json

import polars as pl

from spells import *

import spells.external as external
import spells.cache as cache
import spells.manifest as manifest
from spells.enums import View
from spells.schema import schema
from spells.extension import stat_cols, context_cols

pl.Config.set_tbl_rows(1000)
pl.Config.set_tbl_cols(100)
