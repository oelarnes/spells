"""
Tests that the tables keyed by an enum cover it.

Each is indexed with `[]` rather than `.get`, deliberately — a missing entry is
an authoring omission, and a blank cell would hide it. These make the omission
surface here instead of on a user's terminal.
"""

import pytest

from spells import render
from spells.catalog import Freshness
from spells.inventory import REMEDIES, AnomalyKind


@pytest.mark.parametrize("kind", list(AnomalyKind))
def test_every_anomaly_kind_has_help_text(kind):
    assert kind in render.ANOMALY_HELP


@pytest.mark.parametrize("kind", list(AnomalyKind))
def test_every_anomaly_kind_has_a_remedy(kind):
    assert kind in REMEDIES


@pytest.mark.parametrize("freshness", list(Freshness))
def test_every_freshness_has_a_style(freshness):
    assert freshness in render.FRESHNESS_STYLE
