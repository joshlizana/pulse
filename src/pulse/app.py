import multiprocessing

from multiprocessing.connection import Connection

import structlog

from textual import work
from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Log

from pulse.channels import Channels
from pulse.config import Config
from pulse.dashboard import Dashboard
from pulse.extract import Extract
from pulse.transform import Transform
from pulse.load import Load
from pulse.logging import configure_logging


class Pulse(App):
    def __init__(self, channels: Channels, config: Config) -> None:
        super().__init__()
        self.config: Config = config
        self.channels: Channels = channels
        self.logger: structlog.BoundLogger = configure_logging(
            log_file_path=config.log_dir,
            name="app"
        )
        self.process_pipes: list[Connection] = []

    BINDINGS = [("q", "quit", "Quit")]

    async def on_mount(self) -> None:
        pass

    async def on_load(self) -> None:
        pass

    def compose(self) -> ComposeResult:
        yield Header()
        yield Log(id="live_pipeline", auto_scroll=True)
        yield Footer()

    @work(exclusive=True, thread=True)
    async def pipeline_output(self) -> None:
        log_widget = self.query_one("#live_pipeline", Log)

        while self.channels.stop_event.value is False:

            if not self.channels.logs.empty():
                try:
                    log_data = self.channels.logs.get_nowait()

                    timestamp = log_data.get(
                        "timestamp",
                        "1970-01-01T00:00:00"
                    ).replace("T", " ")
                    level = log_data.get("level", "info").upper()
                    process_name = log_data.get("name", "PROCESS")
                    cid = log_data.get("cid", "NO-CID")
                    event = log_data.get("event", "NO-EVENT")
                    display_line = f"[{timestamp}] [{level}] [{process_name}] [{cid}] {event}"

                    self.call_from_thread(log_widget.write_line, display_line)
                except Exception:
                    pass
            elif not self.channels.feed.empty():
                try:
                    feed_data = self.channels.feed.get_nowait()

                    self.call_from_thread(log_widget.write_line, f"Feed: {feed_data}")
                except Exception:
                    pass
            else:
                await self.sleep(0.1)

    @work(thread=True)
    def pipeline(self) -> None:
        ctx = multiprocessing.get_context("spawn")

        for process in self.config.processes:
            parent, child = multiprocessing.Pipe()
            if process.name == "EXTRACT":
                ctx.Process(target=Extract, args=(self.config, self.channels, child)).start()
            elif process.name == "TRANSFORM":
                ctx.Process(target=Transform, args=(self.config, self.channels, child)).start()
            elif process.name == "LOAD":
                ctx.Process(target=Load, args=(self.config, self.channels, child)).start()
            elif process.name == "DASHBOARD":
                ctx.Process(target=Dashboard, args=(self.config, self.channels, child)).start()

            child.close()
