"""
Tests for the single output path.

The ordering test is the reason this module exists: bare `print` is
block-buffered when stdout is a pipe, so progress written that way arrived
*after* errors written to stderr.
"""

import subprocess
import sys

import pytest

from spells import console


@pytest.fixture(autouse=True)
def loud():
    console.set_quiet(False)
    yield
    console.set_quiet(False)


def test_info_writes_progress(capsys):
    console.info("add", "downloading")
    assert "downloading" in capsys.readouterr().out


def test_quiet_suppresses_progress(capsys):
    console.set_quiet(True)
    console.info("add", "downloading")
    console.detail("a detail")
    assert capsys.readouterr().out == ""


def test_quiet_never_suppresses_errors(capsys):
    console.set_quiet(True)
    console.error("it broke")
    captured = capsys.readouterr()
    assert "it broke" in captured.err
    assert captured.out == ""


def test_errors_go_to_stderr_so_stdout_stays_pipeable(capsys):
    console.info("add", "progress")
    console.error("failure")
    captured = capsys.readouterr()
    assert "progress" in captured.out and "progress" not in captured.err
    assert "failure" in captured.err and "failure" not in captured.out


def test_progress_and_errors_stay_in_order_when_piped(tmp_path):
    """The regression this module was written for. Needs a real subprocess:
    capsys does not reproduce the block buffering that caused it."""
    script = tmp_path / "prog.py"
    script.write_text(
        "from spells import console\n"
        "console.info('add', 'FIRST')\n"
        "console.error('SECOND')\n"
    )
    out = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    merged = out.stdout + out.stderr
    combined = subprocess.run(
        f"{sys.executable} {script} 2>&1", shell=True, capture_output=True, text=True
    ).stdout

    assert "FIRST" in merged and "SECOND" in merged
    assert combined.index("FIRST") < combined.index("SECOND")
