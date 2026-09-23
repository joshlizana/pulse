import subprocess
import sys
from pathlib import Path

import pytest

from conftest import created
from pulse.cli import ExitCode, main
from pulse.config import Config

CONSOLE_SCRIPT = Path(sys.executable).parent / "pulse"
ENTRY_POINTS = {
    "console script": [str(CONSOLE_SCRIPT)],
    "python -m pulse": [sys.executable, "-m", "pulse"],
}

HOLD_LOCK = """
import sys
from pulse.config import Config
from pulse.lock import FileLock

with FileLock(Config().data_dir):
    print("locked", flush=True)
    sys.stdin.read()
"""


@pytest.fixture
def lock_held_elsewhere(home):
    """A separate process holding the instance lock until the test ends."""
    holder = subprocess.Popen(
        [sys.executable, "-c", HOLD_LOCK],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert holder.stdout.readline().strip() == "locked"
    yield
    holder.stdin.close()
    holder.wait(timeout=10)


def test_help_exits_success_and_shows_the_data_directory(home, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--help"])
    assert exit_info.value.code == ExitCode.SUCCESS
    out = capsys.readouterr().out
    assert out.startswith("Usage: pulse")
    assert str(Config().data_dir) in out


def test_help_creates_nothing(home):
    with pytest.raises(SystemExit):
        main(["--help"])
    assert created(home) == []


def test_unknown_argument_is_a_usage_error(home, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--bogus"])
    assert exit_info.value.code == ExitCode.USAGE_ERROR
    assert "--bogus" in capsys.readouterr().err
    assert created(home) == []


def test_run_creates_only_the_data_directory_and_lockfile(home):
    assert main([]) == ExitCode.SUCCESS
    assert created(home) == [
        ".local",
        ".local/share",
        ".local/share/pulse",
        ".local/share/pulse/pulse.lock",
    ]


def test_empty_argv_ignores_the_real_command_line(home, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["pytest", "--not-a-pulse-option"])
    assert main([]) == ExitCode.SUCCESS


def test_second_instance_is_refused(lock_held_elsewhere, capsys):
    assert main([]) == ExitCode.ALREADY_RUNNING
    assert "already running" in capsys.readouterr().err


@pytest.mark.parametrize("command", ENTRY_POINTS.values(), ids=ENTRY_POINTS.keys())
def test_entry_point_help(home, command):
    result = subprocess.run([*command, "--help"], capture_output=True, text=True)
    assert result.returncode == ExitCode.SUCCESS
    assert result.stdout.startswith("Usage: pulse")


@pytest.mark.parametrize("command", ENTRY_POINTS.values(), ids=ENTRY_POINTS.keys())
def test_entry_point_refused_while_locked(lock_held_elsewhere, command):
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == ExitCode.ALREADY_RUNNING
    assert "already running" in result.stderr


def test_cli_import_stays_light():
    # Every spawned child imports pulse.cli (TDD §4.6), so heavy libraries must
    # load only where a process uses them.
    heavy = ("textual", "atproto", "duckdb", "streamlit")
    probe = (
        "import sys, pulse.cli; "
        f"print([m for m in {heavy!r} if m in sys.modules])"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"
