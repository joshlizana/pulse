from textual import work
from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Static

from pulse.channels import Channels


class Pulse(App):
    def __init__(self, channels: Channels) -> None:
        super().__init__()
        self.channels = channels

    BINDINGS = [("q", "quit", "Quit")]

    async def on_mount(self) -> None:
        pass

    async def on_load(self) -> None:
        pass

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Hello, Pulse!")
        yield Footer()

    @work(thread=True)
    def spawn_children(self) -> None:
        pass
