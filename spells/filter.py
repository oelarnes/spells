"""
spells.filter (don't import as builtin filter) returns a function from_spec
that takes a dict-specified filter and returns a Filter object that records
the dependent column names and contains a filter expression for use in polars.
"""

from dataclasses import dataclass
import functools

import polars as pl


@dataclass(frozen=True)
class Filter:
    expr: pl.Expr
    lhs: frozenset[str]


def __negate(f: Filter) -> Filter:
    return Filter(expr=~f.expr, lhs=f.lhs)


def _or(f1: Filter, f2: Filter) -> Filter:
    return Filter(expr=f1.expr | f2.expr, lhs=f1.lhs.union(f2.lhs))


def _and(f1: Filter, f2: Filter) -> Filter:
    return Filter(expr=f1.expr & f2.expr, lhs=f1.lhs.union(f2.lhs))


def _filter_eq(lhs: str, rhs: str) -> Filter:
    return Filter(expr=pl.col(lhs) == rhs, lhs=frozenset({lhs}))


def _filter_leq(lhs: str, rhs: str) -> Filter:
    return Filter(expr=pl.col(lhs) <= rhs, lhs=frozenset({lhs}))


def _filter_geq(lhs: str, rhs: str) -> Filter:
    return Filter(expr=pl.col(lhs) >= rhs, lhs=frozenset({lhs}))


def _filter_in(lhs: str, rhs: str) -> Filter:
    return Filter(expr=pl.col(lhs).is_in(rhs), lhs=frozenset({lhs}))


def _filter_gt(lhs: str, rhs: str) -> Filter:
    return __negate(_filter_leq(lhs, rhs))


def _filter_nin(lhs: str, rhs: str) -> Filter:
    return __negate(_filter_in(lhs, rhs))


def _filter_neq(lhs: str, rhs: str) -> Filter:
    return __negate(_filter_eq(lhs, rhs))


def _filter_lt(lhs: str, rhs: str) -> Filter:
    return __negate(_filter_geq(lhs, rhs))


filter_fn_map = {
    "=": _filter_eq,
    "<=": _filter_leq,
    ">=": _filter_geq,
    "in": _filter_in,
    ">": _filter_gt,
    "nin": _filter_nin,
    "!=": _filter_neq,
    "<": _filter_lt,
}


def _base(lhs, rhs, op="=") -> Filter:
    return filter_fn_map[op](lhs, rhs)


def _all_of(filters) -> Filter:
    return functools.reduce(_and, filters)


def _any_of(filters) -> Filter:
    return functools.reduce(_or, filters)


def _negate(fil) -> Filter:
    return __negate(fil)


BUILDER_MAP = {"$and": _all_of, "$or": _any_of, "$not": _negate}


def from_spec(filter_spec: dict | None) -> Filter | None:
    """
    filter_spec is a nested dictionary with the leaf-level consisting of specs of the form
    {'lhs': 'a', 'rhs': [1,2,3], 'op': 'in'}
    or
    {'a': 5}

    higher level keys can be `_all_of | _any_of | not`

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

    an empty input returns None, which represents a trivial filter
    """
    if not filter_spec:
        return None

    for filter_type, filter_fn in BUILDER_MAP.items():
        if filter_value := filter_spec.get(filter_type):
            assert (
                len(filter_spec) == 1
            ), f"Operator {filter_type} incompatible with additional keys."
            if isinstance(filter_value, list):
                arg = tuple(map(from_spec, filter_value))
            else:
                arg = from_spec(filter_value)
            return filter_fn(arg)

    if len(filter_spec) == 1:
        for lhs, rhs in filter_spec.items():
            return _base(lhs, rhs)

    assert "lhs" in filter_spec and "rhs" in filter_spec
    return _base(filter_spec["lhs"], filter_spec["rhs"], filter_spec.get("op", "="))
