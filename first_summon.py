"""What spells knows about the data you have.

Run it with `python first_summon.py`, then change something and run it again.
`columns` are what you want measured, `group_by` is what you want them measured
across; every name comes from `spells.enums.ColName`.
"""

from spells import inventory, summon
from spells.enums import ColName, EventType


def event_types_by_set(inv: inventory.Inventory) -> dict[str, list[EventType]]:
    """Each downloaded set, mapped to the event types it actually has.

    Passing one list instead would ask every set for every event type, and
    asking for one a set does not have fails the read rather than returning
    nothing for it.
    """
    return {
        set_inv.set_code: sorted(set_inv.events, key=str)
        for set_inv in inv.sets.values()
        if set_inv.has_external
    }


def main() -> None:
    by_set = event_types_by_set(inventory.scan())
    if not by_set:
        print("No sets downloaded yet. Run `spells` to get some.")
        return

    df = summon(
        sorted(by_set),
        columns=[ColName.NUM_TAKEN, ColName.NUM_GAMES],
        group_by=[ColName.EXPANSION, ColName.EVENT_TYPE],
        event_type=by_set,
    )
    print(df.sort([ColName.EXPANSION, ColName.EVENT_TYPE]))


if __name__ == "__main__":
    main()
