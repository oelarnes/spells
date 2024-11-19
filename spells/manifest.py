from dataclasses import dataclass

import spells.columns
import spells.filter
from spells.enums import View, ColName, ColType
from spells.columns import ColumnDefinition


@dataclass(frozen=True)
class Manifest:
    columns: tuple[str, ...]
    col_def_map: dict[str, ColumnDefinition]
    base_view_groupbys: frozenset[str]
    view_cols: dict[View, frozenset[str]]
    groupbys: tuple[str,...]
    dd_filter: spells.filter.Filter | None

    def __post_init__(self):
        # No name filter check
        if self.dd_filter is not None:
            assert (
                "name" not in self.dd_filter.lhs
            ), "Don't filter on 'name', include 'name' in groupbys and filter the final result instead"

        # Col in col_def_map check
        for col in self.columns:
            assert col in self.col_def_map, f"Undefined column {col}!"

        # base_view_groupbys have col_type GROUPBY check
        for col in self.base_view_groupbys:
            assert self.col_def_map[col].col_type == ColType.GROUPBY, f"Invalid groupby {col}!"

        for view, cols_for_view in self.view_cols.items():
            # cols_for_view are actually in view check
            for col in cols_for_view:
                assert (
                    view in self.col_def_map[col].views
                ), f"View cols generated incorrectly, {col} not in view {view}"
            if view != View.CARD:
                for col in self.base_view_groupbys:
                    # base_view_groupbys in view check
                    assert (
                        col == ColName.NAME or view in self.col_def_map[col].views
                    ), f"Groupby {col} not in view {view}!"
                    # base_view_groupbys in view_cols for view
                    assert (
                        col == ColName.NAME or col in cols_for_view
                    ), f"Groupby {col} not in view_cols[view]"
                # filter cols are in both base_views check
                if self.dd_filter is not None:
                    for col in self.dd_filter.lhs:
                        assert col in cols_for_view, f"filter col {col} not found in base view"
            if view == View.CARD:
                # name in groupbys check
                assert ColName.NAME in self.base_view_groupbys, f"base views must groupby by name to join card attrs"

        for col, cdef in self.col_def_map.items():
            # name_sum extension cols have name_sum first dependency for renaming
            if cdef.col_type == ColType.NAME_SUM and cdef.dependencies:
                assert (
                    self.col_def_map[cdef.dependencies[0]].col_type == ColType.NAME_SUM
                ), "dependency 0 of a name_sum column with dependencies must be a name_sum column to derive names"

    def test_str(self):
        result = "{\n" + 2 * " " + "columns:\n"
        for c in sorted(self.columns):
            result += 4 * " " + c + "\n"
        result += 2 * " " + "base_view_groupbys:\n"
        for c in sorted(self.base_view_groupbys):
            result += 4 * " " + c + "\n"
        result += 2 * " " + "view_cols:\n"
        for v, view_cols in sorted(self.view_cols.items()):
            result += 4 * " " + v + ":\n"
            for c in sorted(view_cols):
                result += 6 * " " + c + "\n"
        result += 2 * " " + "groupbys:\n"
        for c in sorted(self.groupbys):
            result += 4 * " " + c + "\n"
        result += "}\n"

        return result


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

    iter_num = 0
    while unresolved_cols and iter_num < 100:
        iter_num += 1
        next_cols = frozenset()
        for col in unresolved_cols:
            cdef = col_def_map[col]
            if cdef.col_type == ColType.PICK_SUM:
                view_resolution[View.DRAFT] = view_resolution.get(View.DRAFT, frozenset()).union(
                    {ColName.PICK}
                )
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

    if iter_num >= 100:
        raise ValueError("broken dependency chain in column spec, loop probable")

    return view_resolution


def create(
    columns: list[str] | None = None,
    groupbys: list[str] | None = None,
    filter_spec: dict | None = None,
    extensions: list[ColumnDefinition] | None = None,
):
    col_def_map = dict(spells.columns.col_def_map)
    if extensions is not None:
        for cdef in extensions:
            col_def_map[cdef.name] = cdef

    gbs = (ColName.NAME,) if groupbys is None else tuple(groupbys)
    if columns is None:
        cols = tuple(spells.columns.default_columns)
        if ColName.NAME not in gbs:
            cols = tuple(c for c in cols if c not in [ColName.COLOR, ColName.RARITY])
    else:
        cols = tuple(columns)

    base_view_groupbys = frozenset()
    for col in gbs:
        cdef = col_def_map[col]
        if cdef.col_type == ColType.GROUPBY:
            base_view_groupbys = base_view_groupbys.union({col})
        elif cdef.col_type == ColType.CARD_ATTR:
            base_view_groupbys = base_view_groupbys.union({ColName.NAME})

    dd_filter = spells.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(frozenset(gbs) - {ColName.NAME})
    if dd_filter is not None:
        col_set = col_set.union(dd_filter.lhs)

    view_cols = _resolve_view_cols(col_set, col_def_map)

    needed_views = frozenset()
    for view, cols_for_view in view_cols.items():
        for col in cols_for_view:
            if col_def_map[col].views == (view,):  # only found in this view
                needed_views = needed_views.union({view})

    view_cols = {v: view_cols[v] for v in needed_views}

    return Manifest(
        columns=cols,
        col_def_map=col_def_map,
        base_view_groupbys=base_view_groupbys,
        view_cols=view_cols,
        groupbys=gbs,
        dd_filter=dd_filter,
    )
