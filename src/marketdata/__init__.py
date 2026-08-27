"""Local market data warehouse and backtesting toolkit.

Storage layout (under the configured data directory):

    data/
      meta.db                          SQLite: identity, universe, and metadata
      eod/{TICKER}.parquet             one file per ticker, full daily history
      intraday/{freq}/{TICKER}/{year}.parquet

The ticker-keyed Parquet paths are the transitional v1 substrate. M1 moves
active bars to instrument-keyed hash buckets before production ingestion
resumes.
"""

from marketdata.config import Config, load_config

__all__ = ["Config", "load_config"]
