"""Local market data warehouse and backtesting toolkit.

Storage layout (under the configured data directory):

    data/
      meta.db                          SQLite: ticker universe, coverage intervals, metadata
      eod/{TICKER}.parquet             one file per ticker, full daily history
      intraday/{freq}/{TICKER}/{year}.parquet
"""

from marketdata.config import Config, load_config

__all__ = ["Config", "load_config"]
