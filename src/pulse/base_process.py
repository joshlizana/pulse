import logging
import multiprocessing
import signal
import sys
import time
from multiprocessing.connection import Connection
from threading import Thread

import structlog

from pulse.channels import Channels
from pulse.logging import configure_logging
from pulse.config import Config

logging.raiseExceptions = False
logging.basicConfig(level=logging.INFO)


class PulseProcess(multiprocessing.Process):
    def __init__(
            self,
            config: Config,
            channels: Channels,
            pipe: Connection,
            *args,
            **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.config: Config = config
        self.channels: Channels = channels
        self.pipe: Connection = pipe
        self.logger: structlog.BoundLogger | None = None
        self.running: bool = True

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGHUP, self.handle_shutdown)

        out = open(self.config.log_file / "process.log", "a")
        sys.stdout = out
        sys.stderr = out

        self.logger = configure_logging(
            name=self.__class__.__name__,
            queue=self.channels.logs
        )
        try:
            poll_thread = Thread(target=self.poll, args=(self.pipe,))
            poll_thread.start()

            self.logger.info(self.__class__.__name__ + " started.")
            self.work()
            self.logger.info(self.__class__.__name__ + " stopped.")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            self.logger.info(self.__class__.__name__ + " exiting.")
            sys.stdout.close()
            sys.stderr.close()
            out.close()

    def work(self) -> None:
        raise NotImplementedError()

    def handle_shutdown(self, signum, frame) -> None:
        self.running = False
        self.logger.info(f"Received shutdown signal: {signum}")
        self.channels.stop_event = True
        self.channels.stop_time = time.monotonic()

    def poll(self, pipe: Connection) -> None:
        while self.running:
            try:
                pipe.poll(0.1)
            except EOFError:
                self.running = False
