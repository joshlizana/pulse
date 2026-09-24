import time

from pulse.base_process import PulseProcess


class Transform(PulseProcess):
    def work(self) -> None:
        while self.channels.stop_event.value is False:
            time.sleep(0.1)
