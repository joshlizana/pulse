import ctypes
import enum
import multiprocessing
import sys

from pulse.channels import Channels, Extract, Transform, Load, Dashboard
from pulse.config import Config
from pulse.lock import AlreadyRunningError, FileLock


class ExitCode(enum.IntEnum):
    SUCCESS = 0
    USAGE_ERROR = 2
    ALREADY_RUNNING = 3


def parse_args(argv: list[str], config: Config) -> None:
    if len(argv) > 0:
        if argv[0] in ("-h", "--help"):
            print(
                "Usage: pulse [options]\n\n"
                "Options:\n"
                "  -h, --help    Show this help message and exit\n\n"
                f"Data directory: {config.data_dir}"
            )
            sys.exit(ExitCode.SUCCESS)
        else:
            print(f"Unknown argument: {argv[0]}", file=sys.stderr)
            sys.exit(ExitCode.USAGE_ERROR)


def main(argv: list[str] | None = None) -> int:
    ctx = multiprocessing.get_context("spawn")
    config = Config()

    parse_args(argv if argv is not None else sys.argv[1:], config=config)

    try:
        with FileLock(config.data_dir):
            channels = Channels(
                extract=ctx.RawValue(Extract),
                transform=ctx.RawValue(Transform),
                load=ctx.RawValue(Load),
                dashboard=ctx.RawValue(Dashboard),
                stop_event=ctx.RawValue(ctypes.c_bool),
                stop_time=ctx.RawValue(ctypes.c_double),
                logs=ctx.Queue(config.log_queue_maxsize),
                feed=ctx.Queue(config.feed_queue_maxsize),
                raw=ctx.Queue(config.raw_queue_maxsize),
                rows=ctx.Queue(config.rows_queue_maxsize),
            )
            from pulse.app import Pulse
            app = Pulse(channels=channels)
            app.run()
    except AlreadyRunningError as e:
        print(e, file=sys.stderr)
        return ExitCode.ALREADY_RUNNING
    return ExitCode.SUCCESS
