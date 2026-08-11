"""Rendering the inventory, the catalog comparison, and planned repairs.

The flat commands and the walkthrough show the same things, so the tables live
here rather than in either of them — `cli` would otherwise have to import
`wizard` and be imported back.

Nothing here decides anything. Deciding what is stale, what is repairable, and
what a set even is belongs to `inventory`, `catalog`, and `repair`.
"""

from rich.console import Console
from rich.padding import Padding
from rich.table import Table

from spells import catalog, repair
from spells.catalog import Freshness
from spells.console import sizeof_fmt
from spells.enums import EventType, View
from spells.inventory import Anomaly, AnomalyKind, Inventory

console = Console()
err_console = Console(stderr=True)

BANNER = r"""[bold magenta]                _ _
 ___ _ __   ___| | |___
/ __| '_ \ / _ \ | / __|
\__ \ |_) |  __/ | \__ \
|___/ .__/ \___|_|_|___/
    |_|[/bold magenta]"""


def banner() -> None:
    """Art only. Where the data lives is the status header's job, and the
    walkthrough prints that immediately after."""
    console.print(BANNER)


ANOMALY_HELP = {
    AnomalyKind.STRAY_DOWNLOAD: "leftover download artifacts",
    AnomalyKind.LEGACY_CONTEXT: "pre-event_type context files",
    AnomalyKind.LEGACY_SNAPSHOT: "pre-0.14 snapshots, unreadable today",
    AnomalyKind.ORPHAN_CACHE: "cache with no data to rebuild from",
    AnomalyKind.ORPHAN_DIR: "directories spells does not write",
    AnomalyKind.UNKNOWN_FILE: "unrecognized files",
    AnomalyKind.INCOMPLETE_SET: "sets missing dataset files",
    AnomalyKind.EMPTY_SET: "set directories with nothing in them",
}


def _event_summary(set_inv) -> str:
    if not set_inv.events:
        return "[dim]—[/dim]"
    parts = []
    for event, files in set_inv.events.items():
        if files.is_complete:
            parts.append(str(event))
        else:
            parts.append(f"[yellow]{event} (no {', '.join(files.missing)})[/yellow]")
    return "\n".join(parts)


def _snapshot_summary(set_inv) -> str:
    valid = set_inv.ratings.valid + set_inv.deck_color.valid
    legacy = set_inv.ratings.legacy + set_inv.deck_color.legacy
    if not valid and not legacy:
        return "[dim]—[/dim]"
    text = f"{valid}"
    if legacy:
        text += f" [yellow]+{legacy} old[/yellow]"
    return text


def _render_status(
    inv: Inventory, sets: list, anomalies: list[Anomaly], detailed: bool
) -> None:
    console.print(
        f"  🪄 [bold]spells[/bold] ✨ {inv.data_home} "
        f"[dim]({sizeof_fmt(inv.total_bytes)})[/dim]\n"
    )

    if not sets:
        console.print("  [dim]No data found. Try `spells add DSK`.[/dim]")
        return

    # snapshots only exist for callers of the private ratings API, so for most
    # users the column would be a header over a column of dashes
    show_snapshots = any(s.ratings.total or s.deck_color.total for s in sets)

    table = Table(box=None, pad_edge=False, header_style="bold")
    table.add_column("set")
    table.add_column("external", justify="right")
    table.add_column("cards", justify="center")
    table.add_column("cache", justify="right")
    if show_snapshots:
        table.add_column("snapshots", justify="right")
    table.add_column("event types")

    for s in sets:
        row = [
            s.set_code,
            sizeof_fmt(s.external_bytes) if s.external_bytes else "[dim]—[/dim]",
            "✓" if s.card_file else "[dim]—[/dim]",
            str(s.cache_files) if s.cache_files else "[dim]—[/dim]",
        ]
        if show_snapshots:
            row.append(_snapshot_summary(s))
        row.append(_event_summary(s))
        table.add_row(*row)

    console.print(Padding(table, (0, 0, 0, 2)))

    cache_files = sum(s.cache_files for s in sets)
    if cache_files:
        cache_bytes = sum(s.cache_bytes for s in sets)
        console.print(
            f"\n  {cache_files} cached query results "
            f"[dim]({sizeof_fmt(cache_bytes)}, rebuild on demand)[/dim]"
        )

    # cached draft logs are keyed by draft id alone, so they cannot be
    # attributed to the set being reported on
    if inv.draft_logs and not detailed:
        console.print(
            f"  {inv.draft_logs} cached draft logs "
            f"[dim]({sizeof_fmt(inv.draft_log_bytes)})[/dim]"
        )

    if not anomalies:
        return

    console.print(f"\n[bold]issues[/bold] ({len(anomalies)})")

    if detailed:
        for a in anomalies:
            size = f" [dim]{sizeof_fmt(a.size)}[/dim]" if a.size else ""
            # soft_wrap keeps long paths on one line for copy-paste
            console.print(
                f"  [yellow]{a.kind}[/yellow]{size} — {a.detail}", soft_wrap=True
            )
            console.print(f"    [dim]{a.path}[/dim]", soft_wrap=True)
        return

    summary = Table(box=None, pad_edge=False, show_header=False)
    summary.add_column(style="yellow")
    summary.add_column(justify="right")
    summary.add_column(justify="right")
    summary.add_column(style="dim")

    for kind in AnomalyKind:
        matching = [a for a in anomalies if a.kind == kind]
        if not matching:
            continue
        size = sum(a.size for a in matching)
        summary.add_row(
            str(kind),
            str(len(matching)),
            sizeof_fmt(size) if size else "—",
            ANOMALY_HELP[kind],
        )

    console.print(Padding(summary, (0, 0, 0, 2)))
    console.print("\n  [dim]`spells status <SET>` for detail[/dim]")


FRESHNESS_STYLE = {
    Freshness.CURRENT: ("[green]current[/green]", "up to date"),
    Freshness.STALE: ("[yellow]stale[/yellow]", "17Lands has newer data"),
    Freshness.ABSENT: ("[cyan]absent[/cyan]", "published, not downloaded"),
    Freshness.UNPUBLISHED: ("[dim]unpublished[/dim]", "17Lands does not publish it"),
    Freshness.UNKNOWN: ("[dim]unknown[/dim]", "could not determine"),
}

CHECKED_VIEWS = (View.DRAFT, View.GAME)

UNADDED_SHOWN = 5


def _held_events(inv: Inventory) -> set[tuple[str, EventType]]:
    return {(s.set_code, event) for s in inv.sets.values() for event in s.events}


def _is_actionable(row: catalog.CheckRow, held: set[tuple[str, EventType]]) -> bool:
    """Whether a row represents something the user actually needs to do.

    An event type they have never added is a suggestion, not a defect: nearly
    every set publishes TradDraft, so counting those as work would make the
    exit-3 contract fire permanently and be useless to cron.
    """
    if row.freshness == Freshness.STALE:
        return True
    key = (row.target.expansion, row.target.event_type)
    return row.freshness == Freshness.ABSENT and key in held


def _render_check(
    rows: list[catalog.CheckRow],
    cat: catalog.Catalog,
    held: set[tuple[str, EventType]],
    detailed: bool,
    unadded: list[str],
) -> None:
    if cat.is_fallback:
        err_console.print(
            "[yellow]Could not reach the 17Lands catalog; "
            "reporting local state only.[/yellow]\n"
        )

    if not rows:
        console.print("  [dim]Nothing to check. Try `spells add DSK`.[/dim]")
        return

    tracked = [r for r in rows if (r.target.expansion, r.target.event_type) in held]
    untracked = [r for r in rows if r not in tracked]

    if tracked:
        table = Table(box=None, pad_edge=False, header_style="bold")
        table.add_column("set")
        table.add_column("event type")
        table.add_column("file")
        table.add_column("status")
        table.add_column("updated", justify="right")

        for row in tracked:
            remote = row.remote
            when = "[dim]—[/dim]"
            if remote is not None and remote.last_modified is not None:
                when = remote.last_modified.date().isoformat()
            table.add_row(
                row.target.expansion,
                str(row.target.event_type),
                str(row.target.view),
                FRESHNESS_STYLE[row.freshness][0],
                when,
            )

        console.print(Padding(table, (0, 0, 0, 2)))

    if untracked:
        available: dict[str, set[EventType]] = {}
        for row in untracked:
            if row.freshness == Freshness.ABSENT:
                available.setdefault(row.target.expansion, set()).add(
                    row.target.event_type
                )

        if available and detailed:
            console.print("\n  [bold]also published[/bold] [dim](not added)[/dim]")
            for expansion, events in sorted(available.items()):
                names = ", ".join(sorted(str(e) for e in events))
                console.print(f"    {expansion}: [cyan]{names}[/cyan]")
        elif available:
            events = sorted({str(e) for evs in available.values() for e in evs})
            console.print(
                f"\n  [dim]{len(available)} set(s) also publish "
                f"{', '.join(events)}[/dim]"
            )
            console.print("  [dim]`spells check <SET>` for detail[/dim]")

    if unadded:
        shown = unadded[:UNADDED_SHOWN]
        console.print(f"\n  [bold]published, not added[/bold] ({len(unadded)})")
        for expansion in shown:
            when = cat.updated(expansion)
            console.print(
                f"    [cyan]{expansion:<14}[/cyan] "
                f"[dim]{when.isoformat() if when else '—'}[/dim]"
            )
        if len(unadded) > len(shown):
            console.print(f"    [dim]+{len(unadded) - len(shown)} older[/dim]")

    work = sorted({r.target.expansion for r in tracked if _is_actionable(r, held)})
    if work:
        console.print(
            f"\n  [bold]{len(work)} set(s) incomplete or out of date:[/bold] "
            f"{', '.join(work)}"
        )
        console.print(
            "  [dim]`spells add <SET>` fills gaps, `refresh` re-downloads[/dim]"
        )


def _render_repairs(repairs: list[repair.Repair]) -> None:
    total_files = sum(r.files for r in repairs)
    total_size = sum(r.size for r in repairs)

    table = Table(box=None, pad_edge=False, header_style="bold")
    table.add_column("issue")
    table.add_column("set")
    table.add_column("files", justify="right")
    table.add_column("size", justify="right")

    for r in repairs:
        table.add_row(
            str(r.anomaly.kind),
            r.anomaly.set_code or "[dim]—[/dim]",
            str(r.files),
            sizeof_fmt(r.size),
        )

    console.print(Padding(table, (0, 0, 0, 2)))
    console.print(
        f"\n  [bold]would remove {total_files} file(s)[/bold], "
        f"{sizeof_fmt(total_size)}"
    )


def _render_advisories(inv: Inventory, set_code: str | None) -> None:
    advisories = [
        a
        for a in inv.all_anomalies
        if not a.is_repairable and (set_code is None or a.set_code == set_code)
    ]
    if not advisories:
        return

    console.print(f"\n  [bold]needs your judgement[/bold] ({len(advisories)})")
    for a in advisories:
        console.print(f"    [yellow]{a.kind}[/yellow] {a.detail}", soft_wrap=True)
        console.print(f"      [dim]{a.path}[/dim]", soft_wrap=True)
