from dataclasses import dataclass

import spells.columns
import spells.filter
from spells.enums import View, ColName, ColType
from spells.columns import ColDef


@dataclass(frozen=True)
class Manifest:
    columns: tuple[str, ...]
    col_def_map: dict[str, ColDef]
    base_view_group_by: frozenset[str]
    view_cols: dict[View, frozenset[str]]
    group_by: tuple[str, ...]
    filter: spells.filter.Filter | None

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
            for col in cols_for_view:
                # game sum cols on in game, and no NAME groupby
                assert self.col_def_map[col].col_type != ColType.GAME_SUM or (
                    view == View.GAME and ColName.NAME not in self.base_view_group_by
                ), f"Invalid manifest for GAME_SUM column {col}"
            if view != View.CARD:
                for col in self.base_view_group_by:
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
    col_def_map: dict[str, ColDef],
) -> dict[View, frozenset[str]]:
    """
    For each view ('game', 'draft', and 'card'), return the columns
    that must be present at the aggregation step. 'name' need not be
    included, and 'pick' will be added if needed.
    """
    MAX_DEPTH = 1000
    unresolved_cols = col_set
    view_resolution = {}

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
            # now determine views and deps
            if cdef.col_type == ColType.CARD_ATTR:
                view_resolution[View.CARD] = view_resolution.get(
                    View.CARD, frozenset()
                ).union({col})
            elif cdef.views:
                for view in cdef.views:
                    view_resolution[view] = view_resolution.get(
                        view, frozenset()
                    ).union({col})
            else:
                if cdef.dependencies is None:
                    raise ValueError(
                        f"Invalid column def: {col} has neither views nor dependencies!"
                    )
                if cdef.col_type != ColType.AGG:
                    fully_resolved = True
                    col_views = frozenset({View.GAME, View.DRAFT, View.CARD})
                    for dep in cdef.dependencies:
                        dep_views = frozenset()
                        for view, view_cols in view_resolution.items():
                            if dep in view_cols:
                                dep_views = dep_views.union({view})
                        if not dep_views:
                            fully_resolved = False
                            next_cols = next_cols.union({dep})
                        else:
                            col_views = col_views.intersection(dep_views)
                    if fully_resolved:
                        assert len(
                            col_views
                        ), f"Column {col} can't be defined in any views!"
                        for view in col_views:
                            if view not in view_resolution:
                                print(cdef)
                                assert False, f"Something went wrong with col {col}"

                            view_resolution[view] = view_resolution[view].union({col})
                    else:
                        next_cols = next_cols.union({col})
                else:
                    for dep in cdef.dependencies:
                        next_cols = next_cols.union({dep})
        unresolved_cols = next_cols

    if iter_num >= MAX_DEPTH:
        raise ValueError("broken dependency chain in column spec, loop probable")

    return view_resolution


def create(
    col_def_map: dict[str, ColDef],
    columns: list[str] | None = None,
    group_by: list[str] | None = None,
    filter_spec: dict | None = None,
):
    gbs = (ColName.NAME,) if group_by is None else tuple(group_by)

    if columns is None:
        cols = tuple(spells.columns.default_columns)
        if ColName.NAME not in gbs:
            cols = tuple(
                col for col in cols if col not in (ColName.COLOR, ColName.RARITY)
            )
    else:
        cols = tuple(columns)

    m_filter = spells.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(frozenset(gbs) - {ColName.NAME})
    if m_filter is not None:
        col_set = col_set.union(m_filter.lhs)

    base_view_group_by = frozenset()

    for col in gbs:
        cdef = col_def_map[col]
        if cdef.col_type == ColType.GROUP_BY:
            base_view_group_by = base_view_group_by.union({col})
        elif cdef.col_type == ColType.CARD_ATTR:
            base_view_group_by = base_view_group_by.union({ColName.NAME})

    view_cols = _resolve_view_cols(col_set, col_def_map)

    needed_views = frozenset()
    if View.CARD in view_cols:
        needed_views = needed_views.union({View.CARD})

    draft_view_cols = view_cols.get(View.DRAFT, frozenset())
    game_view_cols = view_cols.get(View.GAME, frozenset())

    base_cols = draft_view_cols.union(game_view_cols)

    if base_cols == draft_view_cols:
        needed_views = needed_views.union({View.DRAFT})
    elif base_cols == game_view_cols:
        needed_views = needed_views.union({View.GAME})
    else:
        needed_views = needed_views.union({View.GAME, View.DRAFT})

    view_cols = {v: view_cols.get(v, frozenset({ColName.PICK})) for v in needed_views}

    return Manifest(
        columns=cols,
        col_def_map=col_def_map,
        base_view_group_by=base_view_group_by,
        view_cols=view_cols,
        group_by=gbs,
        filter=m_filter,
    )
