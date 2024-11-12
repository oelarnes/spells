from dataclasses import dataclass

import mdu.columns
import mdu.filter
from mdu.enums import View, ColName, ColType
from mdu.columns import ColumnDefinition


def _resolve_base_view_cols(
    col_set: frozenset[str],
    col_def_map: dict[str, ColumnDefinition],
) -> dict[View, frozenset[str]]:
    """
    For each base view ('game' and 'draft'), return the columns
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
            if cdef.base_views:
                for view in cdef.base_views:
                    view_resolution[view] = view_resolution.get(view, frozenset()).union({col})
            else:
                if cdef.dependencies is None:
                    raise ValueError(
                        f"Invalid column def: {col} has neither base views nor dependencies!"
                    )
                for dep in cdef.dependencies:
                    next_cols = next_cols.union({dep})
        unresolved_cols = next_cols

    if iter >= 100:
        raise ValueError("broken dependency chain in column spec, loop probable")
        
    return view_resolution


def create(
    columns: list[str] | None,
    groupbys: list[str] | None,
    filter_spec: dict | None,
    extensions: list[ColumnDefinition] | None
):
    cols = tuple(mdu.columns.default_columns) if columns is None else tuple(columns)
    gbs = frozenset({ColName.NAME}) if groupbys is None else frozenset(groupbys)

    col_def_map = dict(mdu.columns.col_def_map)
    if extensions is not None:
        for cdef in extensions:
            col_def_map[cdef.name] = cdef

    dd_filter = mdu.filter.from_spec(filter_spec)

    col_set = frozenset(cols)
    col_set = col_set.union(gbs)
    if dd_filter is not None:
        col_set = col_set.union(dd_filter.lhs)

    base_view_cols = _resolve_base_view_cols(col_set, col_def_map)

    base_views = frozenset()
    for view in [View.DRAFT, View.GAME]:
        for col in base_view_cols[view]:
            if col_def_map[col].base_views == (view,):  # only found in this view
                base_views = base_views.union({view})

    base_view_cols = {v: base_view_cols[v] for v in base_views}

    return Manifest(
        columns=cols,
        col_def_map=col_def_map,
        base_view_groupbys=gbs,
        base_view_cols=base_view_cols,
        card_attr_groupbys=frozenset(),
        card_attr_cols=frozenset(),
        dd_filter = dd_filter
    )


@dataclass
class Manifest():
    columns: tuple[str, ...]
    col_def_map: dict[str, ColumnDefinition]
    base_view_groupbys: frozenset[str]
    base_view_cols: dict[View, frozenset[str]]
    card_attr_groupbys: frozenset[str]
    card_attr_cols: frozenset[str]
    dd_filter: mdu.filter.Filter | None

    def __post_init__(self):
        for col in self.columns:
            if col not in self.col_def_map:
                raise ValueError(f"Undefined column {col}!")

        for col in self.base_view_groupbys:
            if self.col_def_map[col].col_type != ColType.GROUPBY:
                raise ValueError(f"Invalid groupby {col}!")

        for view, view_cols in self.base_view_cols.items():
            for col in self.base_view_groupbys:
                if view not in self.col_def_map[col].base_views:
                    raise ValueError(f"Groupby {col} not in view {view}!")

            if self.dd_filter is not None:
                for col in self.dd_filter.lhs:
                    if col not in view_cols:
                        raise ValueError(f"filter col {col} not found in base view")


