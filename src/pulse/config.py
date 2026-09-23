from dataclasses import dataclass, field
from pathlib import Path

import platformdirs


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    data_dir: Path = field(
        default_factory=lambda: platformdirs.user_data_path(
            "pulse", ensure_exists=False
        )
    )
