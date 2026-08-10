"""The interactive walkthrough behind a bare `spells`.

Everything here is a front end over `inventory`, `catalog`, `repair`, and
`external`; nothing does work the flat commands cannot. What it adds is not
having to know the commands exist, which is the whole problem for a first-time
user staring at an empty data home.

Two rules shape it. The menu offers only what the data home actually needs, so
a set that is already current is never presented as a chore. And every action
prints the command that would have done the same thing, so the wizard teaches
its way out of being needed.
"""

from dataclasses import dataclass

import questionary

from spells import catalog, console, external, inventory, repair
from spells.catalog import Freshness, Target
from spells.enums import EventType, View

CANCEL = "cancel"

# Long enough to scroll, short enough that a new user is not reading a wall of
# expansions they have never heard of.
BROWSE_LIMIT = 12


@dataclass(frozen=True)
class Action:
    key: str
    label: str
    detail: str


def _ask(prompt) -> object | None:
    """questionary returns None when interrupted; treat that as backing out."""
    try:
        return prompt.ask()
    except KeyboardInterrupt:
        return None


def _echo_command(command: str) -> None:
    console.info(f"[dim]$[/dim] [bold]{command}[/bold]")


def _held(inv: inventory.Inventory) -> set[str]:
    return {s.set_code for s in inv.sets.values() if s.has_external}


def _stale_targets(inv: inventory.Inventory, cat: catalog.Catalog) -> list[Target]:
    targets = []
    for set_inv in inv.sets.values():
        if not set_inv.has_external:
            continue
        for event_type, files in set_inv.events.items():
            for view in (View.DRAFT, View.GAME):
                local = getattr(files, view, None)
                targets.append(
                    Target(
                        set_inv.set_code,
                        event_type,
                        view,
                        local.mtime if local else None,
                    )
                )
    return targets


def _sets_needing_update(inv: inventory.Inventory, cat: catalog.Catalog) -> list[str]:
    rows = catalog.resolve(_stale_targets(inv, cat), cat)
    return sorted(
        {
            r.target.expansion
            for r in rows
            if r.freshness in (Freshness.STALE, Freshness.ABSENT)
        }
    )


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def _choose_expansion(cat: catalog.Catalog, held: set[str]) -> str | None:
    candidates = [
        e
        for e in catalog.in_release_order(cat, cat.expansions)
        if e not in held and cat.is_addable(e)
    ]
    if not candidates:
        console.info("Every published set is already downloaded.")
        return None

    def label(expansion: str) -> str:
        when = cat.updated(expansion)
        return f"{expansion:<16}{when.isoformat() if when else ''}"

    choices = [questionary.Choice(label(e), value=e) for e in candidates[:BROWSE_LIMIT]]
    if len(candidates) > BROWSE_LIMIT:
        choices.append(
            questionary.Choice(
                f"... {len(candidates) - BROWSE_LIMIT} older sets", value="__more__"
            )
        )
    choices.append(questionary.Choice("Back", value=CANCEL))

    picked = _ask(questionary.select("Which set?", choices=choices, qmark="  "))
    if picked == "__more__":
        picked = _ask(
            questionary.select(
                "Which set?",
                choices=[questionary.Choice(label(e), value=e) for e in candidates]
                + [questionary.Choice("Back", value=CANCEL)],
                qmark="  ",
            )
        )
    return None if picked in (None, CANCEL) else str(picked)


def _choose_event_type(cat: catalog.Catalog, expansion: str) -> EventType | None:
    published = [
        d.event_type
        for d in cat.for_expansion(expansion)
        if d.is_supported and d.url(View.DRAFT)
    ]
    if len(published) == 1:
        return published[0]

    choices = [questionary.Choice(str(e), value=e) for e in published]
    choices.append(questionary.Choice("Back", value=CANCEL))
    picked = _ask(
        questionary.select(
            f"Which event type for {expansion}?", choices=choices, qmark="  "
        )
    )
    return None if picked in (None, CANCEL) else picked


def _download_size(cat: catalog.Catalog, expansion: str, event_type: EventType) -> int:
    dataset = cat.get(expansion, event_type)
    if dataset is None:
        return 0
    total = 0
    for view in (View.DRAFT, View.GAME):
        url = dataset.url(view)
        remote = catalog.head(url) if url else None
        total += remote.size if remote and remote.size else 0
    return total


def add_set(inv: inventory.Inventory, cat: catalog.Catalog) -> None:
    expansion = _choose_expansion(cat, _held(inv))
    if expansion is None:
        return

    event_type = _choose_event_type(cat, expansion)
    if event_type is None:
        return

    size = _download_size(cat, expansion, event_type)
    size_note = f" (~{console.sizeof_fmt(size)} compressed)" if size else ""
    if not _ask(
        questionary.confirm(
            f"Download {expansion} {event_type}{size_note}?", default=True, qmark="  "
        )
    ):
        return

    _echo_command(f"spells add {expansion} {event_type}")
    external._add(expansion, event_type=event_type)


def update_sets(inv: inventory.Inventory, cat: catalog.Catalog) -> None:
    stale = _sets_needing_update(inv, cat)
    if not stale:
        console.info("Everything you have is current.")
        return

    picked = _ask(
        questionary.checkbox(
            "Which sets should be brought up to date?",
            choices=[questionary.Choice(s, value=s, checked=True) for s in stale],
            qmark="  ",
        )
    )
    if not picked:
        return

    for expansion in picked:
        set_inv = inv.sets.get(expansion)
        events = list(set_inv.events) if set_inv else [EventType.PREMIER]
        for event_type in events:
            _echo_command(f"spells add {expansion} {event_type}")
            external._add(expansion, event_type=event_type)


def free_space(inv: inventory.Inventory, cat: catalog.Catalog) -> None:
    repairs = repair.plan(inv)
    if not repairs:
        console.info("Nothing to clean up.")
        return

    files = sum(r.files for r in repairs)
    freed = sum(r.size for r in repairs)
    console.info(f"{files} unusable file(s) taking {console.sizeof_fmt(freed)}.")

    if not _ask(
        questionary.confirm(f"Delete {files} file(s)?", default=False, qmark="  ")
    ):
        return

    _echo_command("spells doctor --yes")
    outcome = repair.apply(repairs)
    console.info(
        f"Removed {outcome.removed} file(s), freed {console.sizeof_fmt(outcome.freed)}."
    )
    for path, error in outcome.failures:
        console.error(f"could not remove {path}: {error}")


def show_summary(inv: inventory.Inventory, cat: catalog.Catalog) -> None:
    _echo_command("spells status")
    sets = [s for s in inv.sets.values() if s.has_external]
    console.info(
        f"{len(sets)} set(s), {console.sizeof_fmt(inv.total_bytes)} in {inv.data_home}"
    )
    for set_inv in sorted(sets, key=lambda s: s.set_code):
        events = ", ".join(str(e) for e in set_inv.events)
        console.detail(f"{set_inv.set_code:<16}{events}")


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


def _bootstrap(cat: catalog.Catalog) -> None:
    """First run: there is nothing on disk, so there is exactly one useful
    thing to do and no menu is worth showing."""
    console.info("No 17Lands data here yet.")
    console.detail("spells keeps draft and game datasets under this directory,")
    console.detail("then answers questions about them with `summon`.\n")

    add_set(inventory.scan(), cat)

    inv = inventory.scan()
    if any(s.has_external for s in inv.sets.values()):
        console.info("\nReady. From here:")
        console.detail("spells status        what you have")
        console.detail("spells check         whether 17Lands has anything newer")
        console.detail("spells              this walkthrough again")


def _menu(inv: inventory.Inventory, cat: catalog.Catalog) -> list[Action]:
    """Only what this data home actually needs, so nothing already fine is
    offered as a task."""
    actions = []

    unadded = catalog.unadded(cat, _held(inv))
    if unadded:
        actions.append(
            Action("add", "Add a set", f"{len(unadded)} published, not downloaded")
        )
    else:
        actions.append(Action("add", "Add a set", ""))

    stale = _sets_needing_update(inv, cat)
    if stale:
        actions.append(
            Action("update", "Update out-of-date data", f"{len(stale)} set(s)")
        )

    repairs = repair.plan(inv)
    if repairs:
        freed = sum(r.size for r in repairs)
        actions.append(
            Action("clean", "Free up space", f"{console.sizeof_fmt(freed)} unusable")
        )

    actions.append(Action("summary", "Show what I have", ""))
    actions.append(Action("quit", "Quit", ""))
    return actions


HANDLERS = {
    "add": add_set,
    "update": update_sets,
    "clean": free_space,
    "summary": show_summary,
}


def run() -> None:
    """Entry point for a bare `spells` at a terminal."""
    cat = catalog.fetch()
    if cat.is_fallback:
        console.error("Could not reach 17Lands; only local actions are available.")

    inv = inventory.scan()
    if not any(s.has_external for s in inv.sets.values()):
        _bootstrap(cat)
        return

    while True:
        inv = inventory.scan()
        actions = _menu(inv, cat)
        choice = _ask(
            questionary.select(
                "What would you like to do?",
                choices=[
                    questionary.Choice(
                        f"{a.label:<26}{a.detail}" if a.detail else a.label, value=a.key
                    )
                    for a in actions
                ],
                qmark="  ",
            )
        )
        if choice in (None, "quit"):
            return
        HANDLERS[str(choice)](inv, cat)
        console.info("")
