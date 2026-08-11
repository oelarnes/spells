"""What spells knows about the data you have.

Run it with `python first_summon.py`, then change something and run it again.
`columns` are what you want measured, `group_by` is what you want them measured
across; every name comes from `spells.enums.ColName`.
"""

from collections import defaultdict

import polars as pl

from spells import inventory, summon
from spells.enums import ColName, EventType


def by_event_types(inv: inventory.Inventory) -> dict[tuple[EventType, ...], list[str]]:
    """Downloaded sets, grouped by exactly which event types they have.

    `summon` takes the product of the sets and the event types it is given, so
    a single call naming every event type fails the read for any set missing
    one rather than skipping it. Grouping first means each call asks only for
    cells that exist — one call per distinct combination instead of one per
    set.
    """
    groups = defaultdict(list)
    for set_inv in inv.sets.values():
        if set_inv.has_external:
            groups[tuple(sorted(set_inv.events, key=str))].append(set_inv.set_code)
    return dict(groups)


def main() -> None:
    groups = by_event_types(inventory.scan())
    if not groups:
        print("No sets downloaded yet. Run `spells` to get some.")
        return

    frames = [
        summon(
            sorted(set_codes),
            columns=[ColName.NUM_TAKEN, ColName.NUM_GAMES],
            group_by=[ColName.EXPANSION, ColName.EVENT_TYPE],
            event_type=list(event_types),
        )
        for event_types, set_codes in groups.items()
    ]

    print(pl.concat(frames).sort([ColName.EXPANSION, ColName.EVENT_TYPE]))


if __name__ == "__main__":
    main()
