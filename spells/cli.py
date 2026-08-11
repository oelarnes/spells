"""cli tool `spells`"""

import json
import sys
from typing import Annotated

import typer
from rich.padding import Padding
from rich.table import Table

from spells import cache, cards as cards_module, catalog, external, inventory, render
from spells import console as spells_console
from spells.console import sizeof_fmt
from spells.render import console, err_console
from spells.cache import DataDir
from spells.catalog import Target
from spells import repair
from spells.enums import EventType
from spells.inventory import Anomaly, Inventory


app = typer.Typer(
    help="Manage 17Lands public datasets, card files, and local caches.",
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


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


NAMES_SHOWN = 6


def _name_sample(names: list[str]) -> str:
    shown = ", ".join(names[:NAMES_SHOWN])
    rest = len(names) - NAMES_SHOWN
    return f"{shown}, +{rest} more" if rest > 0 else shown


def _guard_card_file(action, *args, **kwargs) -> int:
    """Present a card-file mismatch as an error rather than a traceback.

    Comparing the wrong set puts hundreds of names on each side, so the counts
    lead and only a sample of each is printed.
    """
    try:
        return action(*args, **kwargs)
    except cards_module.CardFileMismatch as e:
        err_console.print(
            f"[red]Card file for {e.set_code} does not match the draft data.[/red]"
        )
        if e.only_in_data:
            err_console.print(
                f"  {len(e.only_in_data)} only in draft data: "
                f"{_name_sample(e.only_in_data)}"
            )
        if e.only_in_file:
            err_console.print(
                f"  {len(e.only_in_file)} only in card file: "
                f"{_name_sample(e.only_in_file)}"
            )
        err_console.print(
            f"\n  Run `spells cards {e.set_code} --rebuild` to regenerate it."
        )
        raise typer.Exit(1)


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


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Suppress progress. Errors still print."),
    ] = False,
) -> None:
    spells_console.set_quiet(quiet)
    if ctx.invoked_subcommand is not None:
        return

    # the walkthrough needs a terminal to prompt into; anything else — a pipe,
    # cron, `spells | less` — gets the report it would have got before
    if _is_interactive() and not quiet:
        from spells import wizard

        wizard.run()
    else:
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
    """Show what is on disk and anything that looks wrong with it.

    Sets are listed alphabetically. `check` lists them newest first, but only because it already holds the catalog; reading it here would make a purely local report depend on the network.
    """
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

    render._render_status(inv, sets, anomalies, detailed=set_code is not None)


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
        raise typer.Exit(
            _guard_card_file(external._add_card_only, set_code, event_type=event_type)
        )
    raise typer.Exit(_guard_card_file(external._add, set_code, event_type=event_type))


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
        raise typer.Exit(
            _guard_card_file(
                external._refresh_card_only, set_code, event_type=event_type
            )
        )
    raise typer.Exit(
        _guard_card_file(external._refresh, set_code, event_type=event_type)
    )


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
            for view in render.CHECKED_VIEWS:
                local = getattr(files, view, None) if files else None
                targets.append(
                    Target(expansion, event_type, view, local.mtime if local else None)
                )
    return targets


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
    held = render._held_events(inv)

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
                            "actionable": render._is_actionable(r, held),
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
        render._render_check(
            rows, cat, held, detailed=set_code is not None, unadded=unadded
        )

    if any(render._is_actionable(r, held) for r in rows):
        raise typer.Exit(3)


@app.command()
def cards(
    set_code: str,
    event_type: Annotated[EventType, typer.Argument()] = EventType.PREMIER,
    rebuild: Annotated[
        bool,
        typer.Option("--rebuild", help="Regenerate from MTGJSON, discarding the file."),
    ] = False,
) -> None:
    """Check the card file against downloaded draft data, or rebuild it."""
    if rebuild:
        raise typer.Exit(
            _guard_card_file(
                external._refresh_card_only, set_code, event_type=event_type
            )
        )
    raise typer.Exit(
        _guard_card_file(external._add_card_only, set_code, event_type=event_type)
    )


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


def _repair_dict(r: repair.Repair) -> dict:
    return {
        "kind": str(r.anomaly.kind),
        "set_code": r.anomaly.set_code,
        "detail": r.anomaly.detail,
        "files": r.files,
        "bytes": r.size,
        "paths": [str(p) for p in r.paths],
    }


def _is_interactive() -> bool:
    """Both ends must be a terminal: piping stdout would post the prompt into
    the pipe, where nobody sees it."""
    return sys.stdin.isatty() and console.is_terminal


def _run_repairs(repairs: list[repair.Repair], yes: bool, action: str) -> None:
    """Ask at a terminal, act on `--yes`, and otherwise only report.

    Every path here is irreversible and the snapshot ones cannot be refetched,
    so the one thing this must never do is delete without having been told to.
    """
    total = sum(r.files for r in repairs)

    if not yes:
        if not _is_interactive():
            console.print(f"\n  [dim]dry run — pass --yes to {action}[/dim]")
            return
        if not typer.confirm(f"\n  Permanently delete {total} file(s)?", default=False):
            console.print("  [dim]nothing deleted[/dim]")
            return

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
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Delete without asking. Required headless."),
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
        if yes:
            _run_repairs(repairs, yes, "delete them")
        return

    if not repairs:
        console.print("  [green]Nothing to repair.[/green]")
        render._render_advisories(inv, set_code)
        return

    render._render_repairs(repairs)
    _run_repairs(repairs, yes, "delete them")
    render._render_advisories(inv, set_code)


@app.command()
def snapshots(
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


def cli() -> None:
    app()
