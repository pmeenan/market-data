"""Shared Tiingo bar-field contracts for transport and normalization."""

from typing import Final

TIINGO_EOD_FIELD_MAP: Final = {
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "adjOpen": "adj_open",
    "adjHigh": "adj_high",
    "adjLow": "adj_low",
    "adjClose": "adj_close",
    "adjVolume": "adj_volume",
    "divCash": "div_cash",
    "splitFactor": "split_factor",
}

TIINGO_INTRADAY_FIELD_MAP: Final = {
    "date": "ts",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}
