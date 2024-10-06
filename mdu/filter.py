from functools import reduce

def _negate(bool_fn):
    def negated(df):
        return ~bool_fn(df)
    return negated

def _or(bool_fn1, bool_fn2):
    def ored(df):
        return bool_fn1(df) | bool_fn2(df)
    return ored

def _and(bool_fn1, bool_fn2):
    def anded(df):
        return bool_fn1(df) & bool_fn2(df)
    return anded

def base(lhs, rhs, op='='):
    """
    recommended to import as "filter.base" for clarity

    usage:
    df.loc[filter.base('A', 5)] == df[df['A'] == 5]
    """
    def _filter_eq(ds):
        return ds[lhs] == rhs

    def _filter_leq(ds):
        return ds[lhs] <= rhs

    def _filter_geq(ds):
        return ds[lhs] >= rhs
        
    def _filter_in(ds):
        return ds[lhs].isin(rhs)

    fn_map = {
        '=': _filter_eq,
        '<=': _filter_leq,
        '>=': _filter_geq,
        'in': _filter_in,
        '>': _negate(_filter_leq),
        'nin': _negate(_filter_in),
        '!=': _negate(_filter_eq),
        '<': _negate(_filter_geq)
    }

    filter_fn = fn_map[op]

    filter_fn.lhs = lhs
    filter_fn.rhs = rhs
    filter_fn.op = op

    return filter_fn

def all_of(filters):
    filter_fn = reduce(_and, filters)
    filter_fn.op = "$and"
    filter_fn.children = filters
    return filter_fn

def any_of(filters):
    filter_fn = reduce(_or, filters)
    filter_fn.op = "$or"
    filter_fn.children = filters
    return filter_fn

def negate(fil):
    filter_fn = _negate(fil)
    filter_fn.op = "$not"
    filter_fn.children = [fil]
    return filter_fn

BUILDER_MAP = {
    '$and': all_of,
    '$or': any_of,
    '$not': negate
}

def from_spec(filter_spec):
    """
    filter_spec is a nested dictionary with the leaf-level consisting of specs of the form
    {'lhs': 'a', 'rhs': [1,2,3], 'op': 'in'}
    or
    {'a': 5}

    higher level keys can be `all_of | any_of | not`

    e.g.

    {
        '$and': [
            {
                '$not': {'A' : 5}
            },
            {
                {'lhs': 'B', 'rhs': [1,2], 'op': 'in'}
            }
        ]
    }
    """
    for filter_type in BUILDER_MAP:
        if filter_value := filter_spec.get(filter_type):
            assert len(filter_spec) == 1, f"Operator {filter_type} incompatible with additional keys."
            if type(filter_value) == list:
                arg = tuple(map(from_spec, filter_value))
            else:
                arg = from_spec(filter_value)
            return BUILDER_MAP[filter_type](arg)
    
    if len(filter_spec) == 1:
        for lhs, rhs in filter_spec.items():
            return base(lhs, rhs)
    
    assert 'lhs' in filter_spec and 'rhs' in filter_spec
    return base(filter_spec['lhs'], filter_spec['rhs'], filter_spec.get('op', '='))
    

def get_leaf_lhs(filter):
    if hasattr(filter, 'lhs'):
        return {filter.lhs}

    lhs = set()
    for child in filter.children:
        lhs = lhs.union(get_leaf_lhs(child))
    
    return lhs
