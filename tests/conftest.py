import fcntl
import os
import signal

import pytest

TEST_DEADLINE_SECONDS = 10


@pytest.fixture(autouse=True)
def deadline():
    """Fail any test that runs past the deadline.

    A regression to a blocking lock waits forever, and a hang reports nothing.
    SIGALRM interrupts even a blocked system call, and the handler raising turns
    the hang into a failure with a traceback.
    """

    def expire(signum, frame):
        raise TimeoutError(f"test ran past {TEST_DEADLINE_SECONDS}s")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, TEST_DEADLINE_SECONDS)
    yield
    signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, previous)


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Point HOME at an empty directory, so every platformdirs path lands there.

    The XDG variables take precedence over HOME when set, so they are cleared.
    Subprocesses inherit the redirected environment.
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    for name in ("XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_STATE_HOME"):
        monkeypatch.delenv(name, raising=False)
    return tmp_path


def created(root):
    """Every path created under root, relative to it."""
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*"))


def lock_is_free(data_dir):
    """Whether a fresh open of the data directory's lockfile can take the lock."""
    fd = os.open(data_dir / "pulse.lock", os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


def is_shared(obj):
    """Whether a ctypes object lives in multiprocessing shared memory.

    `multiprocessing.sharedctypes` attaches the shared block as `_wrapper`, and
    pickling an object for a spawned child relies on it.
    """
    return hasattr(obj, "_wrapper")
