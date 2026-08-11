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

import questionary

from spells import cache, catalog, console, external, inventory, render, repair
from spells.catalog import CheckRow, Freshness, Target
from spells.enums import EventType, View

# how many set codes to name before the list stops fitting a narrow terminal
NAMED = 4

# an explicit way out of a multi-select, since "tick nothing and press enter"
# is a thing you have to already know
BACK = "__back__"

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


def _ask(prompt) -> object | None:
    """questionary returns None when interrupted; treat that as backing out."""
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


def _back_choice() -> questionary.Choice:
    return questionary.Choice("← back, change nothing", value=BACK)


def _backed_out(picked) -> bool:
    """Backing out wins over anything else ticked: someone who selected it
    meant to leave, whatever else the cursor passed over."""
    return not picked or BACK in picked


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
            "Which sets? (space toggles, enter confirms)",
            choices=[_back_choice()]
            + [
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
    if _backed_out(picked_sets):
        return

    scoped = [c for c in offers if c.expansion in picked_sets]
    event_types = sorted({c.event_type for c in scoped}, key=str)
    if len(event_types) > 1:
        picked_events = _ask(
            questionary.checkbox(
                "Which event types?",
                choices=[_back_choice()]
                + [
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
        if _backed_out(picked_events):
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


def remove(
    inv: inventory.Inventory, cat: catalog.Catalog, rows: list[CheckRow]
) -> None:
    held = sorted(_held(inv))
    if not held:
        console.info("Nothing downloaded.")
        return

    picked = _ask(
        questionary.checkbox(
            "Remove which sets? (space toggles, enter confirms)",
            choices=[_back_choice()]
            + [
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
    if _backed_out(picked):
        return

    freed = sum(inv.sets[c].total_bytes for c in picked)
    if not _ask(
        questionary.confirm(
            f"Delete {len(picked)} set(s), freeing {console.sizeof_fmt(freed)}?",
            default=False,
            qmark="  ",
        )
    ):
        return

    for code in picked:
        _echo_command(f"spells remove {code} --yes")
        external._remove(code)


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


HANDLERS = {
    "download": download,
    "remove": remove,
    "repair": free_space,
    "cache": clear_cache,
}


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

    actions.append(Action("quit", "Quit", ""))
    return actions


def _opening(inv: inventory.Inventory, cat: catalog.Catalog) -> list[CheckRow]:
    """Everything the flat commands would have told you, before being asked."""
    render.banner()

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
        return []

    rows = _check(inv, cat)
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
    return rows


def run() -> None:
    """Entry point for a bare `spells` at a terminal."""
    cat = catalog.fetch()
    inv = inventory.scan()
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
