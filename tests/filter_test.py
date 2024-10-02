import mdu.filter as filter

import pytest
import pandas

ROW_0 = {'int': 1,      'float': 2.,    'text': 'hi'    }
ROW_1 = {'int': 0,      'float': -0.4,  'text': 'foo'   }
ROW_2 = {'int': -10,    'float': 3.14,  'text': 'bar'   }

TEST_DF = pandas.DataFrame([ ROW_0, ROW_1, ROW_2 ])


@pytest.mark.parametrize('lhs, rhs, op, expected', [
    ('int', 1, None, (True, False, False)),
    ('int', 1, '<', (False, True, True)),
    ('float', -0.4, '!=', (True, False, True)),
    ('text', ['hi', 'ok', 'bar'], 'nin', (False, True, False))
])
def test_base(lhs, rhs, op, expected):
    if op is None:
        base_filter = filter.base(lhs, rhs)
    else:
        base_filter = filter.base(lhs, rhs, op)

    assert (base_filter(TEST_DF) == pandas.Series(expected)).all()
    assert (TEST_DF.loc[base_filter] == TEST_DF[pandas.Series(expected)]).all(None)
    assert base_filter.lhs == lhs
    assert base_filter.rhs == rhs
    assert base_filter.op == (op if op else "=")


@pytest.mark.parametrize('filters, expected', [
    ((filter.base('int', 2, '<'), filter.base('float', 3.14)), (False, False, True)),
    ((filter.base('int', 2, '<'), filter.base('int', 0, '>=')), (True, True, False)),
    ((filter.base('text', ['bar'], 'in'), filter.base('int', 0, '!='), filter.base('float', 3.14, '<')), (False, False, False)),
    ((filter.base('text', ['bar'], 'in'), filter.base('int', 0, '!='), filter.base('float', 3.14, '>=')), (False, False, True))
])
def test_all_of(filters, expected):
    and_filter = filter.all_of(filters)

    assert (and_filter(TEST_DF) == pandas.Series(expected)).all()
    assert (TEST_DF.loc[and_filter] == TEST_DF[pandas.Series(expected)]).all(None)
    assert and_filter.children == filters
    assert and_filter.op == "$and"


@pytest.mark.parametrize('filters, expected', [
    ((filter.base('int', 2, '<'), filter.base('float', 3.14)), (True, True, True)),
    ((filter.base('int', 2, '>'), filter.base('int', 0, '<=')), (False, True, True)),
    ((filter.base('text', ['bar'], 'in'), filter.base('int', 0, '!='), filter.base('float', 3.14, '<')), (True, True, True)),
    ((filter.base('text', ['bar'], 'in'), filter.base('int', 0, '!='), filter.base('float', 3.14, '>=')), (True, False, True))
])
def test_any_of(filters, expected):
    and_filter = filter.any_of(filters)

    assert (and_filter(TEST_DF) == pandas.Series(expected)).all()
    assert and_filter.children == filters
    assert and_filter.op == "$or"


@pytest.mark.parametrize('test_filter, expected', [
    (filter.all_of((filter.base('int', 2, '<'), filter.base('float', 3.14))), (True, True, False)),
    (filter.any_of((filter.base('text', ['bar'], 'in'), filter.base('int', 0, '!='), filter.base('float', 3.14, '>='))), (False, True, False))
])
def test_not(test_filter, expected):
    not_filter = filter.negate(test_filter)

    assert (not_filter(TEST_DF) == pandas.Series(expected)).all()
    assert not_filter.children == [test_filter]
    assert not_filter.op == "$not"


@pytest.mark.parametrize('filter_spec, expected', [
    ({'int': 1}, (True, False, False)),
    ({'lhs': 'float', 'rhs': 3, 'op': '<'}, (True, True, False)),
    ({
        '$not': {
            '$or': [
                {'text': 'foo'},
                {'int': 1}
            ]
        }
    }, (False, False, True)),
    ({'$and': [
        {'lhs': 'text', 'rhs': ['foo', 'bar', 'hi'], 'op': 'in'},
        {'lhs': 'int', 'rhs': [1, 2], 'op': 'nin'},
        {'lhs': 'float', 'rhs': 2.4, 'op': '<'}
    ]}, (False, True, False))
])
def test_from_spec(filter_spec, expected):
    test_filter = filter.from_spec(filter_spec)

    assert (test_filter(TEST_DF) == pandas.Series(expected)).all()


@pytest.mark.parametrize('test_filter, expected', [
    (filter.from_spec({'int': 1}), {'int'}),
    (filter.from_spec({
        '$not': {
            '$or': [
                {'text': 'foo'},
                {'int': 1}
            ]
        }
    }), {'text', 'int'}),
    (filter.from_spec({'$and': [
        {'lhs': 'text', 'rhs': ['foo', 'bar', 'hi'], 'op': 'in'},
        {'lhs': 'int', 'rhs': [1, 2], 'op': 'nin'},
        {'lhs': 'float', 'rhs': 2.4, 'op': '<'}
    ]}), {'int', 'float', 'text'})
])
def test_get_leaf_lhs(test_filter, expected):
    assert filter.get_leaf_lhs(test_filter) == expected
    