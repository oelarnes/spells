from dataclasses import dataclass

import spells.columns
import spells.filter
from spells.enums import View, ColName, ColType
from spells.columns import ColumnDefinition


@dataclass(frozen=True)
class Manifest:
    columns: tuple[str, ...]
    col_def_map: dict[str, ColumnDefinition]
    base_view_group_by: frozenset[str]
    view_cols: dict[View, frozenset[str]]
    group_by: tuple[str, ...]
    filter: spells.filter.Filter | None
    card_sum: frozenset[str]

    def __post_init__(self):
        # No name filter check
        if self.filter is not None:
            assert (
                "name" not in self.filter.lhs
            ), "Don't filter on 'name', include 'name' in groupbys and filter the final result instead"

        # Col in col_def_map check
        for col in self.columns:
            assert col in self.col_def_map, f"Undefined column {col}!"
            assert (
                self.col_def_map[col].col_type != ColType.GROUP_BY
            ), f"group_by column {col} must be passed as group_by"
            assert (
                self.col_def_map[col].col_type != ColType.FILTER_ONLY
            ), f"filter_only column {col} cannot be summoned"

        # base_view_groupbys have col_type GROUP_BY check
        for col in self.base_view_group_by:
            assert (
                self.col_def_map[col].col_type == ColType.GROUP_BY
            ), f"Invalid groupby {col}!"

        for view, cols_for_view in self.view_cols.items():
            # cols_for_view are actually in view check
            for col in cols_for_view:
                assert (
                    view in self.col_def_map[col].views
                ), f"View cols generated incorrectly, {col} not in view {view}"
                # game sum cols on in game, and no NAME groupby
                assert self.col_def_map[col].col_type != ColType.GAME_SUM or (
                    view == View.GAME and ColName.NAME not in self.base_view_group_by
                ), f"Invalid manifest for GAME_SUM column {col}"
            if view != View.CARD:
                for col in self.base_view_group_by:
                    # base_view_groupbys in view check
                    assert (
                        col == ColName.NAME or view in self.col_def_map[col].views
                    ), f"Groupby {col} not in view {view}!"
                    # base_view_groupbys in view_cols for view
                    assert (
                        col == ColName.NAME or col in cols_for_view
                    ), f"Groupby {col} not in view_cols[view]"
                # filter cols are in both base_views check
                if self.filter is not None:
                    for col in self.filter.lhs:
                        assert (
                            col in cols_for_view
                        ), f"filter col {col} not found in base view"

            if view == View.CARD:
                # name in groupbys check
                assert (
                    ColName.NAME in self.base_view_group_by
                ), "base views must groupby by name to join card attrs"

    def test_str(self):
        result = "{\n" + 2 * " " + "columns:\n"
        for c in sorted(self.columns):
            result += 4 * " " + c + "\n"
        result += 2 * " " + "base_view_group_by:\n"
        for c in sorted(self.base_view_group_by):
            result += 4 * " " + c + "\n"
        result += 2 * " " + "view_cols:\n"
        for v, view_cols in sorted(self.view_cols.items()):
            result += 4 * " " + v + ":\n"
            for c in sorted(view_cols):
                result += 6 * " " + c + "\n"
        result += 2 * " " + "group_by:\n"
        for c in sorted(self.group_by):
            result += 4 * " " + c + "\n"
        result += "}\n"

        return result


def _resolve_view_cols(
    col_set: frozenset[str],
    col_def_map: dict[str, ColumnDefinition],
) -> tuple[dict[View, frozenset[str]], frozenset[str]]:
    """
    For each view ('game', 'draft', and 'card'), return the columns
    that must be present at the aggregation step. 'name' need not be
    included, and 'pick' will be added if needed.
    """
    MAX_DEPTH = 1000
    unresolved_cols = col_set
    view_resolution = {}
    card_sum = frozenset()

    iter_num = 0
    while unresolved_cols and iter_num < MAX_DEPTH:
        iter_num += 1
        next_cols = frozenset()
        for col in unresolved_cols:
            cdef = col_def_map[col]
            if cdef.col_type == ColType.PICK_SUM:
                view_resolution[View.DRAFT] = view_resolution.get(
                    View.DRAFT, frozenset()
                ).union({ColName.PICK})
            if cdef.col_type == ColType.CARD_SUM:
                card_sum = card_sum.union({col})
            if cdef.views:
                for view in cdef.views:
                    view_resolution[view] = view_resolution.get(
                        view, frozenset()
                    ).union({col})
            else:
                if cdef.dependencies is None:
                    raise ValueError(
                        f"Invalid column def: {col} has neither views nor dependencies!"
                    )
                for dep in cdef.dependencies:
                    next_cols = next_cols.union({dep})
        unresolved_cols = next_cols

    if iter_num >= MAX_DEPTH:
        raise ValueError("broken dependency chain in column spec, loop probable")

    return view_resolution, card_sum


def create(
    col_def_map: dict[str, ColumnDefinition],
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    filter_spec: dict | None = None,
):
    gbs = (ColName.NAME,) if group_by is None else tuple(group_by)
    if columns is None:
        cols = tuple(spells.columns.default_columns)
        if ColName.NAME not in gbs:
            cols = tuple(c for c in cols if c not in [ColName.COLOR, ColName.RARITY])
    else:
        cols = tuple(columns)

    m_filter = spells.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(frozenset(gbs) - {ColName.NAME})
    if m_filter is not None:
        col_set = col_set.union(m_filter.lhs)

    view_cols, card_sum = _resolve_view_cols(col_set, col_def_map)
    base_view_group_by = frozenset()

    if card_sum:
        base_view_group_by = base_view_group_by.union({ColName.NAME})

    for col in gbs:
        cdef = col_def_map[col]
        if cdef.col_type == ColType.GROUP_BY:
            base_view_group_by = base_view_group_by.union({col})
        elif cdef.col_type == ColType.CARD_ATTR:
            base_view_group_by = base_view_group_by.union({ColName.NAME})

    needed_views = frozenset()
    for view, cols_for_view in view_cols.items():
        for col in cols_for_view:
            if col_def_map[col].views == {view}:  # only found in this view
                needed_views = needed_views.union({view})

    if not needed_views:
        needed_views = {View.DRAFT}

    view_cols = {v: view_cols[v] for v in needed_views}

    return Manifest(
        columns=cols,
        col_def_map=col_def_map,
        base_view_group_by=base_view_group_by,
        view_cols=view_cols,
        group_by=gbs,
        filter=m_filter,
        card_sum=card_sum,
    )
