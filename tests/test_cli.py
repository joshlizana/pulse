import dataclasses
import queue
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import created, is_shared, lock_is_free
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


@pytest.fixture
def app_runs(monkeypatch):
    """Replace the TUI's run with a stand-in that records what it was given.

    The startup path runs for real: config, lock, channels and the app's
    construction. The stand-in also records whether the lock was held while the
    app ran.
    """
    from pulse.app import Pulse

    runs = []

    def run(app):
        held = not lock_is_free(Config().data_dir)
        runs.append({"app": app, "lock_held": held})

    monkeypatch.setattr(Pulse, "run", run)
    return runs


def test_run_creates_only_the_data_directory_and_lockfile(home, app_runs):
    assert main([]) == ExitCode.SUCCESS
    assert created(home) == [
        ".local",
        ".local/share",
        ".local/share/pulse",
        ".local/share/pulse/pulse.lock",
    ]


def test_app_runs_once_under_the_lock(home, app_runs):
    main([])
    assert len(app_runs) == 1
    assert app_runs[0]["lock_held"]


def test_app_receives_shared_channels(home, app_runs):
    main([])
    channels = app_runs[0]["app"].channels
    for field in dataclasses.fields(channels):
        assert is_shared(getattr(channels, field.name)), field.name


CAPACITIES = {"logs": 2, "feed": 3, "raw": 4, "rows": 5}


def test_queues_hold_exactly_their_configured_capacity(home, app_runs, monkeypatch):
    """Each queue refuses the item after its capacity, so none is unbounded.

    A queue built with no size, or a size of zero or less, holds any number of
    items and fails quietly: raw and rows would never block, and backpressure
    would never reach extract.
    """
    import pulse.cli

    small = Config(
        log_queue_maxsize=CAPACITIES["logs"],
        feed_queue_maxsize=CAPACITIES["feed"],
        raw_queue_maxsize=CAPACITIES["raw"],
        rows_queue_maxsize=CAPACITIES["rows"],
    )
    monkeypatch.setattr(pulse.cli, "Config", lambda: small)
    main([])
    channels = app_runs[0]["app"].channels
    for name, capacity in CAPACITIES.items():
        q = getattr(channels, name)
        for i in range(capacity):
            q.put_nowait(i)
        with pytest.raises(queue.Full):
            q.put_nowait(capacity)


def test_empty_argv_ignores_the_real_command_line(home, app_runs, monkeypatch):
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
