"""What spells knows about the data you have.

Run it with `python first_summon.py`, then change something and run it again.
`columns` are what you want measured, `group_by` is what you want them measured
across; every name comes from `spells.enums.ColName`.
"""

from spells import inventory, summon
from spells.enums import ColName


def installed() -> tuple[list[str], list[str]]:
    """Only what is downloaded.

    Naming an event type with no parquet on disk fails the read rather than
    returning nothing for it, so the query is built from the inventory instead
    of from everything 17Lands publishes.
    """
    inv = inventory.scan()
    sets = sorted(s.set_code for s in inv.sets.values() if s.has_external)
    event_types = sorted({e for s in inv.sets.values() for e in s.events}, key=str)
    return sets, event_types


def main() -> None:
    sets, event_types = installed()
    if not sets:
        print("No sets downloaded yet. Run `spells` to get some.")
        return

    df = summon(
        sets,
        columns=[ColName.NUM_TAKEN, ColName.NUM_GAMES],
        group_by=[ColName.EXPANSION, ColName.EVENT_TYPE],
        event_type=event_types,
    )
    print(df)


if __name__ == "__main__":
    main()
