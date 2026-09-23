import fcntl
from pathlib import Path


class AlreadyRunningError(Exception):
    pass


class FileLock:
    def __init__(self, data_dir: Path):
        data_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = data_dir / "pulse.lock"
        self.file = open(self.lock_file, "a")

    def acquire(self):
        try:
            fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.file.close()
            raise AlreadyRunningError(
                "Another instance of Pulse is already running. "
                "Please close it before starting a new one."
            ) from None

    def release(self):
        self.file.close()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
