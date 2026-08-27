"""Local market data warehouse and backtesting toolkit.

Storage layout (under the configured data directory):

    data/
      meta.db                          SQLite: identity, universe, and metadata
      bars/eod/bucket={00..ff}/bars.parquet
      bars/intraday/{freq}/year={YYYY}/bucket={00..ff}/bars.parquet
      quarantine/v1-ticker-bars/       retained migration sources

Active bars are keyed by opaque ``instrument_id`` values. Production
ingestion remains paused until M1 completes identity-validated request
orchestration.
"""

from marketdata.config import Config, load_config

__all__ = ["Config", "load_config"]
