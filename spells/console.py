"""The one place spells writes to a terminal.

Bare `print` is block-buffered when stdout is a pipe, so progress written that
way arrived after errors written to stderr, interleaving the two out of order.
Routing both through rich also gets `NO_COLOR` and non-terminal detection for
free.

Library diagnostics do not belong here — those go to `logging`, which already
has a rotating file handler, so that a warning meant for whoever is writing an
extension is not mixed into a user's progress output.
"""

from rich.console import Console

_out = Console()
_err = Console(stderr=True)

_quiet = False


def set_quiet(quiet: bool) -> None:
    global _quiet
    _quiet = quiet


def is_quiet() -> bool:
    return _quiet


def info(content: str) -> None:
    """Progress, suppressed by --quiet.

    No prefix: the command the user typed already says what is happening, and
    the old one named the internal function rather than the command, so
    `spells cards` announced itself as `add`.
    """
    if not _quiet:
        _out.print(f"  {content}", soft_wrap=True)


def detail(content: str) -> None:
    """A continuation line under the preceding `info`."""
    if not _quiet:
        _out.print(f"      {content}", soft_wrap=True)


def error(content: str) -> None:
    """Failures, on stderr and never suppressed."""
    _err.print(f"[red]{content}[/red]", soft_wrap=True)
