"""Thin Tiingo REST client with throttling and retries.

Covers the three endpoints this project needs:
  - daily (EOD) prices with split/dividend adjustments
  - IEX intraday bars
  - the supported-tickers dump (for seeding universe candidates)
"""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from datetime import date
from typing import Any

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com"
SUPPORTED_TICKERS_URL = (
    "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
)


class TiingoError(RuntimeError):
    pass


class TiingoClient:
    def __init__(
        self,
        token: str,
        *,
        min_request_interval: float = 0.1,
        max_retries: int = 5,
        timeout: float = 30.0,
    ):
        if not token:
            raise TiingoError("A Tiingo API token is required (set TIINGO_API_TOKEN)")
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Token {token}", "Content-Type": "application/json"}
        )
        self._min_interval = min_request_interval
        self._max_retries = max_retries
        self._timeout = timeout
        self._last_request = 0.0

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{BASE_URL}{path}"
        backoff = 1.0
        for attempt in range(self._max_retries + 1):
            wait = self._min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

            resp = self._session.get(url, params=params, timeout=self._timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                raise TiingoError(f"Not found: {path}")
            if (
                resp.status_code in (429, 500, 502, 503, 504)
                and attempt < self._max_retries
            ):
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else backoff
                log.warning(
                    "Tiingo %s on %s, retrying in %.1fs", resp.status_code, path, delay
                )
                time.sleep(delay)
                backoff = min(backoff * 2, 60.0)
                continue
            raise TiingoError(
                f"Tiingo request failed ({resp.status_code}): {resp.text[:500]}"
            )
        raise TiingoError(
            f"Tiingo request failed after {self._max_retries} retries: {path}"
        )

    def eod(
        self,
        ticker: str,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> list[dict[str, Any]]:
        """Daily bars including adjusted OHLCV, dividends, and split factors."""
        params: dict[str, Any] = {"format": "json"}
        if start:
            params["startDate"] = str(start)
        if end:
            params["endDate"] = str(end)
        return self._get(f"/tiingo/daily/{ticker.lower()}/prices", params)

    def intraday(
        self,
        ticker: str,
        start: date | str,
        end: date | str | None = None,
        freq: str = "1min",
    ) -> list[dict[str, Any]]:
        """Intraday bars from Tiingo's IEX feed (unadjusted)."""
        params: dict[str, Any] = {
            "startDate": str(start),
            "resampleFreq": freq,
            "columns": "open,high,low,close,volume",
        }
        if end:
            params["endDate"] = str(end)
        return self._get(f"/iex/{ticker.lower()}/prices", params)

    def ticker_metadata(self, ticker: str) -> dict[str, Any]:
        return self._get(f"/tiingo/daily/{ticker.lower()}")

    def supported_tickers(self) -> list[dict[str, str]]:
        """Download and parse Tiingo's full supported-tickers list.

        Returns rows with keys: ticker, exchange, assetType, priceCurrency,
        startDate, endDate. No API token quota is consumed.
        """
        resp = requests.get(SUPPORTED_TICKERS_URL, timeout=120)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                return list(csv.DictReader(text))
