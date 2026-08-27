"""Runtime configuration, sourced from environment variables (and .env)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    data_dir: Path
    tiingo_token: str | None

    @property
    def meta_path(self) -> Path:
        return self.data_dir / "meta.db"

    @property
    def eod_dir(self) -> Path:
        return self.data_dir / "eod"

    def intraday_dir(self, freq: str = "1hour") -> Path:
        return self.data_dir / "intraday" / freq

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.eod_dir.mkdir(parents=True, exist_ok=True)


def load_config(data_dir: str | Path | None = None) -> Config:
    load_dotenv()
    resolved = Path(data_dir or os.environ.get("MARKET_DATA_DIR", "data")).resolve()
    return Config(data_dir=resolved, tiingo_token=os.environ.get("TIINGO_API_TOKEN"))
