import os

import pandas
import numpy

from pandas.testing import assert_frame_equal

import mdu.cache
import mdu.draft_data as draft_data

os.environ['MDU_PROJECT_DIR'] = 'tests' # will only work from project directory

def test_game_counts():
    ddo = draft_data.DraftData('BLB')

    assert_frame_equal(
        ddo.game_counts(read_cache=False, write_cache=False).head(),
        pandas.DataFrame(
            {
                'deck_all': [6, 0, 0, 0, 0],
                'deck_win': [3, 0, 0, 0, 0],
                'sb_all': [0, 0, 5, 0, 0],
                'sb_win': [0, 0, 2, 0, 0],
                'oh_all': [0, 0, 0, 0, 0],
                'oh_win': [0, 0, 0, 0, 0],
                'drawn_all': [3, 0, 0, 0, 0],
                'drawn_win': [1, 0, 0, 0, 0],
                'tutored_all': [0, 0, 0, 0, 0],
                'tutored_win': [0, 0, 0, 0, 0],
                'mull_all': [1, 0, 0, 0, 0],
                'mull_win': [1, 0, 0, 0, 0],
                'turn_all': [56, 0, 0, 0, 0],
                'turn_win': [26, 0, 0, 0, 0],
                'oh_totals_all': [41, 0, 0, 0, 0],
                'oh_totals_win': [20, 0, 0, 0, 0],
                'drawn_totals_all': [57, 0, 0, 0, 0],
                'drawn_totals_win': [27, 0, 0, 0, 0],
            },
            index = pandas.Index(
                ['Agate Assault', 'Agate-Blade Assassin', "Alania's Pathmaker", 'Alania, Divergent Storm', "Artist's Talent"], 
                name='name'
            )
        )
    )

