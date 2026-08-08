"""cli tool `spells`"""

import json
import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.padding import Padding
from rich.table import Table

from spells import cache, catalog, external, inventory
from spells.cache import DataDir
from spells.catalog import Freshness, Target
from spells import repair
from spells.enums import EventType, View
from spells.inventory import Anomaly, AnomalyKind, Inventory

ANOMALY_HELP = {
    AnomalyKind.STRAY_DOWNLOAD: "leftover download artifacts",
    AnomalyKind.LEGACY_CONTEXT: "pre-event_type context files",
    AnomalyKind.LEGACY_SNAPSHOT: "pre-0.14 snapshots, unreadable today",
    AnomalyKind.ORPHAN_CACHE: "cache with no data to rebuild from",
    AnomalyKind.ORPHAN_DIR: "directories spells does not write",
    AnomalyKind.UNKNOWN_FILE: "unrecognized files",
    AnomalyKind.INCOMPLETE_SET: "sets missing dataset files",
}

app = typer.Typer(
    help="Manage 17Lands public datasets, card files, and local caches.",
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

console = Console()
err_console = Console(stderr=True)


# Fred Cirera via https://stackoverflow.com/questions/1094841/get-a-human-readable-version-of-a-file-size
def sizeof_fmt(num: float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def _confirm(action: str, yes: bool) -> None:
    """Destructive operations confirm on a terminal and refuse when piped, so
    an unattended caller can never be left hanging on a prompt."""
    if yes:
        return
    if not sys.stdin.isatty():
        err_console.print(
            f"Refusing to {action} without confirmation. Re-run with --yes."
        )
        raise typer.Exit(2)
    if not typer.confirm(f"{action[0].upper()}{action[1:]}?"):
        raise typer.Exit(2)


def _anomaly_dict(anomaly: Anomaly) -> dict:
    return {
        "kind": str(anomaly.kind),
        "path": str(anomaly.path),
        "detail": anomaly.detail,
        "set_code": anomaly.set_code,
        "bytes": anomaly.size,
    }


def _inventory_dict(inv: Inventory, sets: list) -> dict:
    """Explicit rather than dataclasses.asdict: this is the `--json` contract,
    and it should not drift every time the dataclasses are refactored."""
    return {
        "data_home": str(inv.data_home),
        "total_bytes": inv.total_bytes,
        "draft_logs": inv.draft_logs,
        "sets": [
            {
                "set_code": s.set_code,
                "event_types": {
                    str(event): {
                        "complete": files.is_complete,
                        "missing": list(files.missing),
                    }
                    for event, files in s.events.items()
                },
                "card_file": s.card_file is not None,
                "external_bytes": s.external_bytes,
                "cache_files": s.cache_files,
                "cache_bytes": s.cache_bytes,
                "ratings": {
                    "valid": s.ratings.valid,
                    "legacy": s.ratings.legacy,
                    "bytes": s.ratings.total_bytes,
                },
                "deck_color": {
                    "valid": s.deck_color.valid,
                    "legacy": s.deck_color.legacy,
                    "bytes": s.deck_color.total_bytes,
                },
                "anomalies": [_anomaly_dict(a) for a in s.anomalies],
            }
            for s in sets
        ],
        "anomalies": [_anomaly_dict(a) for a in inv.anomalies],
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

    table = Table(box=None, pad_edge=False, header_style="bold")
    table.add_column("set")
    table.add_column("external", justify="right")
    table.add_column("cards", justify="center")
    table.add_column("cache", justify="right")
    table.add_column("snapshots", justify="right")
    table.add_column("event types")

    for s in sets:
        table.add_row(
            s.set_code,
            sizeof_fmt(s.external_bytes) if s.external_bytes else "[dim]—[/dim]",
            "✓" if s.card_file else "[dim]—[/dim]",
            str(s.cache_files) if s.cache_files else "[dim]—[/dim]",
            _snapshot_summary(s),
            _event_summary(s),
        )

    console.print(Padding(table, (0, 0, 0, 2)))

    # cached draft logs are keyed by draft id alone, so they cannot be
    # attributed to the set being reported on
    if inv.draft_logs and not detailed:
        console.print(
            f"\n  {inv.draft_logs} cached draft logs "
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


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        status()


@app.command()
def status(
    set_code: Annotated[
        str | None, typer.Argument(help="Limit to a single set.")
    ] = None,
    json_out: Annotated[
        bool, typer.Option("--json", help="Machine-readable output.")
    ] = False,
) -> None:
    """Show what is on disk and anything that looks wrong with it."""
    inv = inventory.scan()

    sets = list(inv.sets.values())
    anomalies = inv.all_anomalies

    if set_code is not None:
        sets = [s for s in sets if s.set_code == set_code]
        if not sets:
            err_console.print(f"No data found for set {set_code}")
            raise typer.Exit(1)
        anomalies = sets[0].anomalies

    if json_out:
        print(json.dumps(_inventory_dict(inv, sets), indent=2))
        return

    _render_status(inv, sets, anomalies, detailed=set_code is not None)


@app.command(hidden=True)
def info() -> None:
    """Deprecated alias for `status`."""
    status()


@app.command()
def add(
    set_code: str,
    event_type: Annotated[EventType, typer.Argument()] = EventType.PREMIER,
    card_only: Annotated[
        bool,
        typer.Option(
            "--card-only",
            help="Spot-check the card file against draft data already on disk.",
        ),
    ] = False,
) -> None:
    """Download draft, game, and card files, skipping any already present."""
    if card_only:
        raise typer.Exit(external._add_card_only(set_code, event_type=event_type))
    raise typer.Exit(external._add(set_code, event_type=event_type))


@app.command()
def refresh(
    set_code: str,
    event_type: Annotated[EventType, typer.Argument()] = EventType.PREMIER,
    card_only: Annotated[
        bool,
        typer.Option(
            "--card-only",
            help="Rebuild only the card file, from draft data already on disk.",
        ),
    ] = False,
) -> None:
    """Re-download and overwrite existing files. Use sparingly."""
    if card_only:
        raise typer.Exit(external._refresh_card_only(set_code, event_type=event_type))
    raise typer.Exit(external._refresh(set_code, event_type=event_type))


@app.command()
def remove(
    set_code: str,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Skip confirmation.")
    ] = False,
) -> None:
    """Delete a set's downloaded data and its derived cache."""
    _confirm(f"delete all downloaded data for {set_code}", yes)
    raise typer.Exit(external._remove(set_code))


@app.command()
def clean(
    set_code: Annotated[str, typer.Argument(help='A set code, or "all".')],
) -> None:
    """Delete derived cache files. Always safe: they rebuild on demand."""
    raise typer.Exit(cache.clean(set_code))


FRESHNESS_STYLE = {
    Freshness.CURRENT: ("[green]current[/green]", "up to date"),
    Freshness.STALE: ("[yellow]stale[/yellow]", "17Lands has newer data"),
    Freshness.ABSENT: ("[cyan]absent[/cyan]", "published, not downloaded"),
    Freshness.UNPUBLISHED: ("[dim]unpublished[/dim]", "17Lands does not publish it"),
    Freshness.UNKNOWN: ("[dim]unknown[/dim]", "could not determine"),
}

CHECKED_VIEWS = (View.DRAFT, View.GAME)

UNADDED_SHOWN = 5


def _targets(
    inv: Inventory, set_code: str | None, cat: catalog.Catalog
) -> list[Target]:
    """Every downloadable file worth asking about.

    Sets already on disk are checked for staleness *and* for event types the
    catalog publishes but we never added; a set with nothing on disk is only
    reachable by naming it explicitly.
    """
    if set_code is not None:
        expansions = [set_code]
    else:
        expansions = catalog.in_release_order(
            cat, (s.set_code for s in inv.sets.values() if s.has_external)
        )

    targets = []
    for expansion in expansions:
        set_inv = inv.sets.get(expansion)
        published = {
            d.event_type for d in cat.for_expansion(expansion) if d.is_supported
        }
        held = set(set_inv.events) if set_inv else set()
        for event_type in sorted(published | held):
            files = set_inv.events.get(event_type) if set_inv else None
            for view in CHECKED_VIEWS:
                local = getattr(files, view, None) if files else None
                targets.append(
                    Target(expansion, event_type, view, local.mtime if local else None)
                )
    return targets


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


@app.command()
def check(
    set_code: Annotated[
        str | None, typer.Argument(help="Limit to a single set.")
    ] = None,
    json_out: Annotated[
        bool, typer.Option("--json", help="Machine-readable output.")
    ] = False,
) -> None:
    """Compare local data against what 17Lands publishes.

    Exits 3 when data you already track is out of date, so cron can chain on
    it. Event types you have never added are reported but do not affect the
    exit code.
    """
    inv = inventory.scan()
    cat = catalog.fetch()
    rows = catalog.resolve(_targets(inv, set_code, cat), cat)
    held = _held_events(inv)

    # only meaningful for the whole-data-home view; naming a set already says
    # which expansion you care about
    unadded = (
        []
        if set_code is not None
        else catalog.unadded(
            cat, {s.set_code for s in inv.sets.values() if s.has_external}
        )
    )

    if set_code is not None and not rows:
        err_console.print(f"17Lands publishes no draft data for set {set_code}")
        raise typer.Exit(1)

    if json_out:
        print(
            json.dumps(
                {
                    "catalog_reachable": not cat.is_fallback,
                    "unadded_expansions": [
                        {
                            "set_code": expansion,
                            "last_updated": (
                                cat.updated(expansion).isoformat()
                                if cat.updated(expansion)
                                else None
                            ),
                        }
                        for expansion in unadded
                    ],
                    "datasets": [
                        {
                            "set_code": r.target.expansion,
                            "event_type": str(r.target.event_type),
                            "view": str(r.target.view),
                            "status": str(r.freshness),
                            "tracked": (r.target.expansion, r.target.event_type)
                            in held,
                            "actionable": _is_actionable(r, held),
                            "url": r.remote.url if r.remote else None,
                            "remote_last_modified": (
                                r.remote.last_modified.isoformat()
                                if r.remote and r.remote.last_modified
                                else None
                            ),
                            "remote_bytes": r.remote.size if r.remote else None,
                        }
                        for r in rows
                    ],
                },
                indent=2,
            )
        )
    else:
        _render_check(rows, cat, held, detailed=set_code is not None, unadded=unadded)

    if any(_is_actionable(r, held) for r in rows):
        raise typer.Exit(3)


@app.command()
def path(
    set_code: Annotated[str | None, typer.Argument()] = None,
    kind: Annotated[
        DataDir | None, typer.Option("--kind", help="Which store to report.")
    ] = None,
) -> None:
    """Print a data path, for scripting."""
    if kind is None:
        target = cache.external_set_path(set_code) if set_code else cache.data_home()
    elif set_code:
        target = f"{cache.data_dir_path(kind)}/{set_code}"
    else:
        target = cache.data_dir_path(kind)

    print(target)


def _render_repairs(repairs: list[repair.Repair], executed: bool) -> None:
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
    verb = "removed" if executed else "would remove"
    console.print(
        f"\n  [bold]{verb} {total_files} file(s)[/bold], " f"{sizeof_fmt(total_size)}"
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


def _repair_dict(r: repair.Repair) -> dict:
    return {
        "kind": str(r.anomaly.kind),
        "set_code": r.anomaly.set_code,
        "detail": r.anomaly.detail,
        "files": r.files,
        "bytes": r.size,
        "paths": [str(p) for p in r.paths],
    }


def _run_repairs(
    repairs: list[repair.Repair], execute: bool, yes: bool, action: str
) -> None:
    """Dry run unless told otherwise: every path here is irreversible, and the
    snapshot ones cannot be refetched."""
    if not execute:
        console.print(f"\n  [dim]dry run — re-run with --execute to {action}[/dim]")
        return

    total = sum(r.files for r in repairs)
    _confirm(f"permanently delete {total} file(s)", yes)

    outcome = repair.apply(repairs)
    console.print(
        f"\n  removed {outcome.removed} file(s), freed {sizeof_fmt(outcome.freed)}"
    )
    for path, error in outcome.failures:
        err_console.print(f"  [red]could not remove[/red] {path}: {error}")
    if outcome.failures:
        raise typer.Exit(1)


@app.command()
def doctor(
    set_code: Annotated[
        str | None, typer.Argument(help="Limit to a single set.")
    ] = None,
    execute: Annotated[
        bool, typer.Option("--execute", help="Actually delete. Off by default.")
    ] = False,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Skip confirmation.")
    ] = False,
    json_out: Annotated[
        bool, typer.Option("--json", help="Machine-readable output.")
    ] = False,
) -> None:
    """Find files spells can no longer use, and optionally remove them."""
    inv = inventory.scan()
    repairs = repair.plan(inv, set_code)

    if json_out:
        print(json.dumps({"repairs": [_repair_dict(r) for r in repairs]}, indent=2))
        if execute:
            _run_repairs(repairs, execute, yes, "delete them")
        return

    if not repairs:
        console.print("  [green]Nothing to repair.[/green]")
        _render_advisories(inv, set_code)
        return

    _render_repairs(repairs, executed=False)
    _run_repairs(repairs, execute, yes, "delete them")
    _render_advisories(inv, set_code)


snapshots_app = typer.Typer(
    help="Inspect and prune cached 17Lands API responses.",
    no_args_is_help=True,
)
app.add_typer(snapshots_app, name="snapshots")


@snapshots_app.command("list")
def snapshots_list(
    set_code: Annotated[
        str | None, typer.Argument(help="Limit to a single set.")
    ] = None,
    json_out: Annotated[
        bool, typer.Option("--json", help="Machine-readable output.")
    ] = False,
) -> None:
    """Show cached responses per set, split into readable and dead."""
    inv = inventory.scan()
    sets = [
        s
        for s in inv.sets.values()
        if (set_code is None or s.set_code == set_code)
        and (s.ratings.total or s.deck_color.total)
    ]

    if json_out:
        print(
            json.dumps(
                {
                    "sets": [
                        {
                            "set_code": s.set_code,
                            "ratings": {
                                "valid": s.ratings.valid,
                                "legacy": s.ratings.legacy,
                                "bytes": s.ratings.total_bytes,
                            },
                            "deck_color": {
                                "valid": s.deck_color.valid,
                                "legacy": s.deck_color.legacy,
                                "bytes": s.deck_color.total_bytes,
                            },
                        }
                        for s in sets
                    ]
                },
                indent=2,
            )
        )
        return

    if not sets:
        console.print("  [dim]No cached responses.[/dim]")
        return

    table = Table(box=None, pad_edge=False, header_style="bold")
    table.add_column("set")
    table.add_column("keep", justify="right")
    table.add_column("dead", justify="right")
    table.add_column("size", justify="right")

    for s in sets:
        dead = s.ratings.legacy + s.deck_color.legacy
        table.add_row(
            s.set_code,
            str(s.ratings.valid + s.deck_color.valid),
            f"[yellow]{dead}[/yellow]" if dead else "[dim]—[/dim]",
            sizeof_fmt(s.snapshot_bytes),
        )

    console.print(Padding(table, (0, 0, 0, 2)))
    console.print(
        "\n  [dim]`keep` cannot be refetched: 17Lands resolves a time period"
        "\n  against its own today, so a past window is gone once deleted.[/dim]"
    )


@snapshots_app.command("prune")
def snapshots_prune(
    set_code: Annotated[
        str | None, typer.Argument(help="Limit to a single set.")
    ] = None,
    execute: Annotated[
        bool, typer.Option("--execute", help="Actually delete. Off by default.")
    ] = False,
    yes: Annotated[
        bool, typer.Option("--yes", "-y", help="Skip confirmation.")
    ] = False,
) -> None:
    """Delete pre-0.14 cached responses, which nothing can read."""
    inv = inventory.scan()
    repairs = repair.prune_snapshots(inv, set_code)

    if not repairs:
        console.print("  [green]No dead snapshots.[/green]")
        return

    _render_repairs(repairs, executed=False)
    _run_repairs(repairs, execute, yes, "delete them")


def cli() -> None:
    app()
