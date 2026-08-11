"""The interactive walkthrough behind a bare `spells`.

Everything here is a front end over `inventory`, `catalog`, `repair`, and
`external`; nothing does work the flat commands cannot. What it adds is not
having to know the commands exist, which is the whole problem for a first-time
user staring at an empty data home.

Two rules shape it. The opening screen states everything the flat commands
would have told you — what is on disk, and what 17Lands has that you do not —
so the menu is a decision rather than an investigation. And every action prints
the command that would have done the same thing, so the walkthrough teaches its
way out of being needed.
"""

from dataclasses import dataclass, replace
from pathlib import Path
import runpy
import shutil

import questionary
from prompt_toolkit.key_binding import KeyBindings, merge_key_bindings

from spells import cache, catalog, console, external, inventory, render, repair
from spells.catalog import CheckRow, Freshness, Target
from spells.enums import EventType, View

# how many set codes to name before the list stops fitting a narrow terminal
NAMED = 4

BACK_KEY = "b"
QUIT_KEY = "q"

# questionary writes this line itself but cannot mention keys it does not know
# about, so the whole thing is replaced rather than added to
PICK_KEYS = "(arrows move, <space> select, <a> all, <i> invert, <b> back, <q> quit)"

# the current format and the one before it, ticked on a first run
FIRST_RUN_SETS = 2


@dataclass(frozen=True)
class Candidate:
    """One downloadable (set, event type), and why it is being offered."""

    expansion: str
    event_type: EventType
    reason: str
    wanted: bool


@dataclass(frozen=True)
class Action:
    key: str
    label: str
    detail: str


class Quit(Exception):
    """Raised out of a prompt to leave the walkthrough entirely.

    An exception rather than a sentinel return, so quitting from three prompts
    deep does not need every step in between to recognize and forward it — the
    same way questionary itself handles ctrl-c.
    """


def _bind_keys(prompt) -> None:
    """Add `b` and `q`, which questionary has no keys for.

    Merged rather than added to: `confirm` is built on a PromptSession, which
    combines its own bindings with questionary's and hands the application an
    already-merged set that cannot be appended to.

    Eager, like questionary's own `a` and `i`: an ordinary letter needs no
    disambiguation. Escape would read more naturally but is the first byte of
    every arrow-key sequence, so it cannot be answered until prompt_toolkit has
    waited to see whether more follows.
    """
    app = getattr(prompt, "application", None)
    if app is None or getattr(app, "key_bindings", None) is None:
        return

    extra = KeyBindings()

    @extra.add(BACK_KEY, eager=True)
    def _back(event):
        event.app.exit(result=None)

    @extra.add(QUIT_KEY, eager=True)
    def _quit(event):
        event.app.exit(exception=Quit)

    app.key_bindings = merge_key_bindings([app.key_bindings, extra])


def _ask(prompt) -> object | None:
    """None means the user backed out — `b`, ctrl-c, or an empty selection."""
    _bind_keys(prompt)
    try:
        return prompt.ask()
    except KeyboardInterrupt:
        return None


def _echo_command(command: str) -> None:
    console.info(f"[dim]$[/dim] [bold]{command}[/bold]")


def _cache_totals(inv: inventory.Inventory) -> tuple[int, int]:
    return (
        sum(s.cache_files for s in inv.sets.values()),
        sum(s.cache_bytes for s in inv.sets.values()),
    )


def _held(inv: inventory.Inventory) -> set[str]:
    return {s.set_code for s in inv.sets.values() if s.has_external}


def _targets(inv: inventory.Inventory, cat: catalog.Catalog) -> list[Target]:
    """Every published dataset for a set on disk, alongside what is there."""
    targets = []
    for set_inv in inv.sets.values():
        if not set_inv.has_external:
            continue
        published = {
            d.event_type for d in cat.for_expansion(set_inv.set_code) if d.is_supported
        }
        for event_type in sorted(published | set(set_inv.events)):
            files = set_inv.events.get(event_type)
            for view in render.CHECKED_VIEWS:
                local = getattr(files, view, None) if files else None
                targets.append(
                    Target(
                        set_inv.set_code,
                        event_type,
                        view,
                        local.mtime if local else None,
                    )
                )
    return targets


def _check(inv: inventory.Inventory, cat: catalog.Catalog) -> list[CheckRow]:
    if cat.is_fallback:
        return []
    return catalog.resolve(_targets(inv, cat), cat)


def candidates(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> list[Candidate]:
    """What is worth downloading, newest set first.

    Pre-selected when it is an update to something tracked, a gap in a set
    partly downloaded, or a set never added; left unticked when it is an event
    type published for a set you have but evidently did not want.
    """
    tracked = {
        (r.target.expansion, r.target.event_type)
        for r in rows
        if r.target.local_mtime is not None
    }

    offers: dict[tuple[str, EventType], tuple[str, bool]] = {}
    for row in rows:
        key = (row.target.expansion, row.target.event_type)
        if row.freshness == Freshness.STALE:
            offers[key] = ("update available", True)
        elif row.freshness == Freshness.ABSENT and key not in offers:
            offers[key] = (
                ("incomplete", True) if key in tracked else ("not downloaded", False)
            )

    # "new" only means anything relative to what is already here, so on a first
    # run nothing is new and everything is merely available.
    started = bool(_held(inv))
    for expansion in catalog.unadded(cat, _held(inv)):
        for dataset in cat.for_expansion(expansion):
            if dataset.is_supported and dataset.draft_url:
                offers.setdefault(
                    (expansion, dataset.event_type),
                    (
                        "new set" if started else "available",
                        started and dataset.event_type == EventType.PREMIER,
                    ),
                )

    order = {e: i for i, e in enumerate(catalog.in_release_order(cat, cat.expansions))}
    offered = sorted(
        (
            Candidate(expansion, event_type, reason, wanted)
            for (expansion, event_type), (reason, wanted) in offers.items()
        ),
        key=lambda c: (order.get(c.expansion, len(order)), str(c.event_type)),
    )
    return offered if started else _first_run_defaults(offered)


def _first_run_defaults(offered: list[Candidate]) -> list[Candidate]:
    """Tick the current format and the one before it.

    Every published set is unticked at this point, since none of them is new to
    a data home with nothing in it. Leaving it there means a new user has to
    know which codes are current before they can start, while ticking all of
    them would put tens of gigabytes one keypress away.
    """
    newest = list(dict.fromkeys(c.expansion for c in offered))[:FIRST_RUN_SETS]
    return [
        replace(c, wanted=True)
        if c.expansion in newest and c.event_type == EventType.PREMIER
        else c
        for c in offered
    ]


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


def _reasons_for(offers: list[Candidate], expansion: str) -> str:
    """Why a set is listed.

    The reasons that pre-selected it, if any, since those are what the user is
    being asked to agree with. Otherwise the event types on offer — saying "not
    downloaded" next to a set you plainly have reads as though the whole thing
    were missing, when only an event type is.
    """
    same_set = [c for c in offers if c.expansion == expansion]
    if wanted := [c.reason for c in same_set if c.wanted]:
        return ", ".join(dict.fromkeys(wanted))
    return ", ".join(dict.fromkeys(str(c.event_type) for c in same_set))


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


def download(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    offers = candidates(inv, cat, rows)
    if not offers:
        console.info("Nothing to download; everything published is already here.")
        return

    picked_sets = _ask(
        questionary.checkbox(
            "Which sets to download?",
            instruction=PICK_KEYS,
            choices=[
                questionary.Choice(
                    f"{expansion:<16}{_reasons_for(offers, expansion)}",
                    value=expansion,
                    checked=any(c.wanted for c in offers if c.expansion == expansion),
                )
                for expansion in dict.fromkeys(c.expansion for c in offers)
            ],
            qmark="  ",
        )
    )
    if not picked_sets:
        return

    scoped = [c for c in offers if c.expansion in picked_sets]
    event_types = sorted({c.event_type for c in scoped}, key=str)
    if len(event_types) > 1:
        picked_events = _ask(
            questionary.checkbox(
                "Which event types?",
                instruction=PICK_KEYS,
                choices=[
                    questionary.Choice(
                        str(e),
                        value=e,
                        checked=any(c.wanted for c in scoped if c.event_type == e),
                    )
                    for e in event_types
                ],
                qmark="  ",
            )
        )
        if not picked_events:
            return
        scoped = [c for c in scoped if c.event_type in picked_events]

    size = sum(_download_size(cat, c.expansion, c.event_type) for c in scoped)
    note = f", ~{console.sizeof_fmt(size)} compressed" if size else ""
    if not _ask(
        questionary.confirm(
            f"Download {len(scoped)} dataset(s){note}?", default=True, qmark="  "
        )
    ):
        return

    for candidate in scoped:
        _echo_command(f"spells add {candidate.expansion} {candidate.event_type}")
        external._add(candidate.expansion, event_type=candidate.event_type)


def _removal_size(
    inv: inventory.Inventory, code: str, event_types: list[EventType]
) -> int:
    """Bytes freed by dropping these event types from one set.

    The card file and derived cache only count when the last event type goes,
    since those survive a partial removal.
    """
    set_inv = inv.sets[code]
    wanted = [e for e in set_inv.events if e in event_types]
    if set(wanted) == set(set_inv.events):
        return set_inv.total_bytes

    return sum(
        info.size
        for event_type in wanted
        for info in (
            getattr(set_inv.events[event_type], view)
            for view in inventory.DATASET_VIEWS
        )
        if info is not None
    )


def remove(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    held = sorted(_held(inv))
    if not held:
        console.info("Nothing downloaded.")
        return

    picked = _ask(
        questionary.checkbox(
            "Which sets to remove?",
            instruction=PICK_KEYS,
            choices=[
                questionary.Choice(
                    f"{code:<16}"
                    f"{console.sizeof_fmt(inv.sets[code].external_bytes):>10}  "
                    f"{', '.join(str(e) for e in inv.sets[code].events)}",
                    value=code,
                )
                for code in held
            ],
            qmark="  ",
        )
    )
    if not picked:
        return

    held_events = sorted({e for c in picked for e in inv.sets[c].events}, key=str)
    if len(held_events) > 1:
        picked_events = _ask(
            questionary.checkbox(
                "Which event types?",
                instruction=PICK_KEYS,
                choices=[
                    questionary.Choice(str(e), value=e, checked=True)
                    for e in held_events
                ],
                qmark="  ",
            )
        )
        if not picked_events:
            return
        held_events = list(picked_events)

    targets = [
        (code, event_type)
        for code in picked
        for event_type in inv.sets[code].events
        if event_type in held_events
    ]
    if not targets:
        console.info("Nothing selected on disk.")
        return

    freed = sum(_removal_size(inv, code, held_events) for code in picked)
    if not _ask(
        questionary.confirm(
            f"Delete {len(targets)} dataset(s), freeing "
            f"{console.sizeof_fmt(freed)}?",
            default=False,
            qmark="  ",
        )
    ):
        return

    for code in picked:
        wanted = [e for e in inv.sets[code].events if e in held_events]
        # taking every event type takes the card file and directory with it,
        # which the whole-set path already does
        if set(wanted) == set(inv.sets[code].events):
            _echo_command(f"spells remove {code} --yes")
            external._remove(code)
        else:
            for event_type in wanted:
                _echo_command(f"spells remove {code} {event_type}")
                external._remove_event_type(code, event_type)


def free_space(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    repairs = repair.plan(inv)
    if not repairs:
        console.info("Nothing to clean up.")
        return

    render._render_repairs(repairs)
    files = sum(r.files for r in repairs)
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


EXAMPLE = Path(__file__).parent / "examples" / "first_summon.py"


def try_summon(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    """Run the example query, then offer to hand over the script.

    Running it proves the install works on real data; keeping it is the point,
    since the next thing anyone wants is the same query with different columns.
    """
    if not _held(inv):
        console.info("Download a set first — there is nothing to summon yet.")
        return

    console.info("Aggregating every downloaded set. The first run is the slow one.")
    _echo_command(f"python {EXAMPLE.name}")
    console.info("")

    try:
        runpy.run_path(str(EXAMPLE), run_name="__main__")
    except Exception as e:  # a broken query should not take the walkthrough down
        console.error(f"{type(e).__name__}: {e}")
        return

    destination = Path.cwd() / EXAMPLE.name
    question = (
        f"Overwrite {destination}?"
        if destination.exists()
        else f"Copy the script to {destination}?"
    )
    if not _ask(questionary.confirm(question, default=False, qmark="  ")):
        return

    shutil.copy(EXAMPLE, destination)
    console.info(f"Wrote {destination}")


def clear_cache(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    """Derived query results only. They rebuild on demand, so this needs no
    per-set choice — the only question is whether to reclaim the space."""
    files, size = _cache_totals(inv)
    if not files:
        console.info("No derived cache to clear.")
        return

    if not _ask(
        questionary.confirm(
            f"Clear {files} cached result(s), {console.sizeof_fmt(size)}? "
            "They rebuild on demand.",
            default=True,
            qmark="  ",
        )
    ):
        return

    _echo_command("spells clean all")
    cache.clean("all")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


def _menu(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> list[Action]:
    """Only what this data home actually offers, so nothing already fine is
    presented as a chore."""
    actions = []

    offers = candidates(inv, cat, rows)
    if offers:
        wanted = sum(1 for c in offers if c.wanted)
        detail = f"{wanted} suggested" if wanted else f"{len(offers)} available"
        actions.append(Action("download", "Download datasets", detail))

    if _held(inv):
        actions.append(Action("remove", "Remove datasets", ""))
        actions.append(
            Action("summon", "Run an example query", "check the install works")
        )

    repairs = repair.plan(inv)
    if repairs:
        freed = console.sizeof_fmt(sum(r.size for r in repairs))
        actions.append(Action("repair", "Remove unusable files", freed))

    files, size = _cache_totals(inv)
    if files:
        actions.append(
            Action(
                "cache",
                "Clear derived cache",
                f"{console.sizeof_fmt(size)}, rebuilds on demand",
            )
        )

    actions.append(Action("status", "Show status", ""))
    actions.append(Action("quit", "Quit", ""))
    return actions


def show_status(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    """Everything the flat commands would have told you.

    Drawn once on the way in, and again on request: a download scrolls it off
    the screen, and the menu alone does not say what is already here.
    """
    render.banner()
    _echo_command("spells status")

    if _held(inv):
        render._render_status(
            inv, list(inv.sets.values()), inv.all_anomalies, detailed=False
        )
    else:
        console.info(f"No 17Lands data yet in {inv.data_home}")
        console.detail("spells keeps draft and game datasets there,")
        console.detail("then answers questions about them with `summon`.")

    if cat.is_fallback:
        console.error("Could not reach 17Lands; only local actions are available.")
        return

    stale = sorted({r.target.expansion for r in rows if r.freshness == Freshness.STALE})
    unadded = catalog.unadded(cat, _held(inv))

    if stale:
        console.info("")
        console.info(f"{len(stale)} set(s) have newer data:")
        console.detail(", ".join(stale))
    if unadded:
        more = f", +{len(unadded) - NAMED} more" if len(unadded) > NAMED else ""
        console.info("")
        console.info(f"{len(unadded)} set(s) published, not downloaded:")
        console.detail(", ".join(unadded[:NAMED]) + more)


def _opening(inv: inventory.Inventory, cat: catalog.Catalog) -> list[CheckRow]:
    rows = _check(inv, cat)
    show_status(inv, cat, rows)
    return rows


HANDLERS = {
    "download": download,
    "remove": remove,
    "summon": try_summon,
    "repair": free_space,
    "cache": clear_cache,
    "status": show_status,
}


def run() -> None:
    """Entry point for a bare `spells` at a terminal."""
    cat = catalog.fetch()
    inv = inventory.scan()

    try:
        rows = _opening(inv, cat)
        while True:
            console.info("")
            choice = _ask(
                questionary.select(
                    "What would you like to do?",
                    choices=[
                        questionary.Choice(
                            f"{a.label:<22}{a.detail}" if a.detail else a.label,
                            value=a.key,
                        )
                        for a in _menu(inv, cat, rows)
                    ],
                    qmark="  ",
                )
            )
            if choice in (None, "quit"):
                return

            HANDLERS[str(choice)](inv, cat, rows)
            inv = inventory.scan()
            rows = _check(inv, cat)
    except Quit:
        return
