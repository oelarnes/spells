"""cli tool `spells`"""

import json
import sys
from typing import Annotated

import typer
from rich.console import Console
from rich.padding import Padding
from rich.table import Table

from spells import cache, external, inventory
from spells.cache import DataDir
from spells.enums import EventType
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


def cli() -> None:
    app()
