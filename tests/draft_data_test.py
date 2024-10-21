import os

import pandas
import numpy

from pandas.testing import assert_frame_equal

import mdu.draft_data as draft_data

os.environ['MDU_PROJECT_DIR'] = 'tests' # will only work from project directory

def test_seen_stats():
    ddo = draft_data.DraftData('BLB')

    assert_frame_equal(
        ddo.seen_stats().head(), 
        pandas.DataFrame(
            {
                'alsa':             [8/3, 22/5, 52/7, 1, numpy.nan],
                'num_packs_seen':   [3, 5, 7, 1, 0],
                'num_seen':         [3, 7, 12, 1, 0],
            }, 
            index = pandas.Index(
                ['Agate Assault', 'Agate-Blade Assassin', "Alania's Pathmaker", 'Alania, Divergent Storm', "Artist's Talent"], 
                name='name'
            )
        )
    )

    ddo = draft_data.DraftData('BLB', filter_spec={
        'rank': 'gold'
    })

    assert_frame_equal(
        ddo.seen_stats().head(), 
        pandas.DataFrame(
            {
                'alsa':             [3.5, 4, 15/2, 1, numpy.nan],
                'num_packs_seen':   [2, 4, 4, 1, 0],
                'num_seen':         [2, 5, 6, 1, 0],
            }, 
            index = pandas.Index(
                ['Agate Assault', 'Agate-Blade Assassin', "Alania's Pathmaker", 'Alania, Divergent Storm', "Artist's Talent"], 
                name='name'
            )
        )
    )

def test_picked_stats():
    ddo = draft_data.DraftData('BLB')

    assert_frame_equal(
        ddo.picked_stats().head(),
        pandas.DataFrame(
            {
                'num_picked':   [3, 3, 1, 1, 2],
                'ata':          [22/3, 22/3, 9., 13., 7.5],
                'num_matches':  [14, 13, 6, 2, 8],
                'apmwr':        [0.5, 8/13, 0.5, 0.5, 0.5]
            }, 
            index = pandas.Index(
                ['Agate Assault', 'Agate-Blade Assassin', "Alania's Pathmaker", 'Alania, Divergent Storm', "Artist's Talent"], 
                name='name'
            )
        )
    )


def test_game_counts():
    ddo = draft_data.DraftData('BLB')

    assert_frame_equal(
        ddo.game_counts().head(),
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

