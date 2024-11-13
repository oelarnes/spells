from dataclasses import dataclass

import mdu.columns
import mdu.filter
from mdu.enums import View, ColName, ColType
from mdu.columns import ColumnDefinition


@dataclass(frozen=True)
class Manifest():
    columns: tuple[str, ...]
    col_def_map: dict[str, ColumnDefinition]
    base_view_groupbys: frozenset[str]
    view_cols: dict[View, frozenset[str]]
    card_attr_groupbys: frozenset[str]
    dd_filter: mdu.filter.Filter | None

    def __post_init__(self):
        for col in self.columns:
            assert col in self.col_def_map, f"Undefined column {col}!"

        for col in self.base_view_groupbys:
            assert self.col_def_map[col].col_type == ColType.GROUPBY, f"Invalid groupby {col}!"

        for view, view_cols in self.view_cols.items():
            for col in view_cols:
                assert view in self.col_def_map[col].views, f"View cols generated incorrectly, {col} not in view {view}"
            if view != View.CARD:
                for col in self.base_view_groupbys:
                    assert view in self.col_def_map[col].views, f"Groupby {col} not in view {view}!"
                if self.dd_filter is not None:
                    for col in self.dd_filter.lhs:
                        assert col in view_cols, f"filter col {col} not found in base view"

        if len(self.card_attr_groupbys):
            assert ColName.NAME in self.base_view_groupbys, "Must groupby name to group by card attributes"

    def test_print(self):
        print('{') #} bad autoindent
        print(2*' ' + "columns:")
        for c in self.columns:
            print(4*' ' + c)
        print(2*' ' + "base_view_groupbys:")
        for c in self.base_view_groupbys:
            print(4*' ' + str(c))
              
        

def _resolve_view_cols(
    col_set: frozenset[str],
    col_def_map: dict[str, ColumnDefinition],
) -> dict[View, frozenset[str]]:
    """
    For each view ('game', 'draft', and 'card'), return the columns
    that must be present at the aggregation step. 'name' need not be
    included, and 'pick' will be added if needed.

    Dependencies within base views will be resolved by `col_df`.
    """
    unresolved_cols = col_set
    view_resolution = {}

    iter = 0
    while unresolved_cols and iter<100:
        iter += 1
        next_cols = frozenset()
        for col in unresolved_cols:
            cdef = col_def_map[col]
            if cdef.col_type == ColType.PICK_SUM:
                view_resolution[View.DRAFT] = view_resolution.get(View.DRAFT, frozenset()).union({ColName.PICK})
            if cdef.views:
                for view in cdef.views:
                    view_resolution[view] = view_resolution.get(view, frozenset()).union({col})
            else:
                if cdef.dependencies is None:
                    raise ValueError(
                        f"Invalid column def: {col} has neither views nor dependencies!"
                    )
                for dep in cdef.dependencies:
                    next_cols = next_cols.union({dep})
        unresolved_cols = next_cols

    if iter >= 100:
        raise ValueError("broken dependency chain in column spec, loop probable")
        
    return view_resolution


def create(
    columns: list[str] | None = None,
    groupbys: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: list[ColumnDefinition] | None = None,
):
    cols = tuple(mdu.columns.default_columns) if columns is None else tuple(columns)
    gbs = frozenset({ColName.NAME}) if groupbys is None else frozenset(groupbys)

    col_def_map = dict(mdu.columns.col_def_map)
    if extensions is not None:
        for cdef in extensions:
            col_def_map[cdef.name] = cdef

    base_view_groupbys = frozenset()
    card_attr_groupbys = frozenset()
    for col in gbs:
        cdef = col_def_map[col]
        if cdef.col_type == ColType.GROUPBY:
            base_view_groupbys = base_view_groupbys.union({col})
        elif cdef.col_type == ColType.CARD_ATTR:
            base_view_groupbys = base_view_groupbys.union({ColName.NAME})
            card_attr_groupbys = card_attr_groupbys.union({col})

    dd_filter = mdu.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(gbs)
    if dd_filter is not None:
        col_set = col_set.union(dd_filter.lhs)

    view_cols = _resolve_view_cols(col_set, col_def_map)

    needed_views = frozenset()
    for view in [View.DRAFT, View.GAME, View.CARD]:
        for col in view_cols[view]:
            if col_def_map[col].views == (view,):  # only found in this view
                needed_views = needed_views.union({view})

    view_cols = {v: view_cols[v] for v in needed_views}

    return Manifest(
        columns=cols,
        col_def_map=col_def_map,
        base_view_groupbys=gbs,
        view_cols=view_cols,
        card_attr_groupbys=card_attr_groupbys,
        dd_filter = dd_filter
    )




