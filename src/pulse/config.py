from dataclasses import dataclass, field
from pathlib import Path

import platformdirs


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    def __post_init__(self):
        if self.log_queue_maxsize <= 0:
            raise ValueError("log_queue_maxsize must be greater than 0")
        if self.feed_queue_maxsize <= 0:
            raise ValueError("feed_queue_maxsize must be greater than 0")
        if self.raw_queue_maxsize <= 0:
            raise ValueError("raw_queue_maxsize must be greater than 0")
        if self.rows_queue_maxsize <= 0:
            raise ValueError("rows_queue_maxsize must be greater than 0")

    data_dir: Path = field(
        default_factory=lambda: platformdirs.user_data_path(
            "pulse", ensure_exists=False
        )
    )
    log_queue_maxsize: int = 1000
    feed_queue_maxsize: int = 100
    raw_queue_maxsize: int = 100000
    rows_queue_maxsize: int = 100000
