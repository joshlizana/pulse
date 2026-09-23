import fcntl
import os

import pytest

from conftest import created
from pulse.lock import AlreadyRunningError, FileLock


def lock_is_free(data_dir):
    fd = os.open(data_dir / "pulse.lock", os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    finally:
        os.close(fd)


def test_creates_the_data_directory_and_lockfile(tmp_path):
    data_dir = tmp_path / "data"
    with FileLock(data_dir):
        assert created(tmp_path) == ["data", "data/pulse.lock"]


def test_second_lock_is_refused_while_held(tmp_path):
    with FileLock(tmp_path):
        with pytest.raises(AlreadyRunningError):
            with FileLock(tmp_path):
                pass


def test_lock_is_free_after_the_block(tmp_path):
    with FileLock(tmp_path):
        assert not lock_is_free(tmp_path)
    assert lock_is_free(tmp_path)


def test_refused_acquire_closes_its_file(tmp_path):
    with FileLock(tmp_path):
        second = FileLock(tmp_path)
        with pytest.raises(AlreadyRunningError):
            second.acquire()
        assert second.file.closed


def test_release_leaves_a_duplicate_holding_the_lock(tmp_path):
    # A child keeps a duplicate of the descriptor (TDD §4.3). Releasing closes
    # this process's copy, so the lock holds for as long as the duplicate is open.
    lock = FileLock(tmp_path)
    lock.acquire()
    duplicate = os.dup(lock.file.fileno())
    lock.release()
    try:
        assert not lock_is_free(tmp_path)
    finally:
        os.close(duplicate)
    assert lock_is_free(tmp_path)
