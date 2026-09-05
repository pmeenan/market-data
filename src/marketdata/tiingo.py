"""Thin Tiingo REST client with throttling and retries.

Covers the three endpoints this project needs:
  - daily (EOD) prices with split/dividend adjustments
  - IEX intraday bars
  - the supported-tickers dump (for seeding universe candidates)
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
import zipfile
from collections.abc import Collection
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Protocol

import requests

from marketdata.bar_fields import TIINGO_EOD_FIELD_MAP, TIINGO_INTRADAY_FIELD_MAP

log = logging.getLogger(__name__)

BASE_URL = "https://api.tiingo.com"
SUPPORTED_TICKERS_URL = (
    "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
)

_EOD_CSV_FIELDS = frozenset(TIINGO_EOD_FIELD_MAP)
_INTRADAY_CSV_FIELDS = frozenset(TIINGO_INTRADAY_FIELD_MAP)
_INTRADAY_COLUMNS = ",".join(
    field for field in TIINGO_INTRADAY_FIELD_MAP if field != "date"
)


class TiingoError(RuntimeError):
    pass


class TiingoNotFoundError(TiingoError):
    """A non-retryable HTTP 404 for one Tiingo resource."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Not found: {path}")


class ResponseReservationExceeded(TiingoError):
    """The response body exceeded the bytes reserved before transport."""


class RequestAttemptObserver(Protocol):
    """Durably account for one authenticated transport attempt.

    ``before_attempt`` runs before the request is sent and may raise to stop on
    quota.  ``after_attempt`` receives the encoded bytes observed for that
    attempt.  An incomplete attempt is intentionally distinguishable so a
    conservative ledger can retain its pre-request byte reservation.
    """

    def before_attempt(
        self, path: str = "", params: dict[str, Any] | None = None
    ) -> Any: ...

    def after_attempt(
        self,
        reservation: Any,
        observed_bytes: int,
        *,
        complete: bool,
        bytes_known: bool,
    ) -> None: ...

    def response_byte_limit(self, reservation: Any) -> int | None: ...


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
        self._session.headers.update({"Authorization": f"Token {token}"})
        self._min_interval = min_request_interval
        self._max_retries = max_retries
        self._timeout = timeout
        self._last_request = 0.0
        self._request_count = 0
        self._response_bytes = 0
        self._attempt_observer: RequestAttemptObserver | None = None

    @property
    def request_count(self) -> int:
        """Authenticated HTTP attempts, including retries and failed attempts."""
        return self._request_count

    @property
    def response_bytes(self) -> int:
        """Cumulative encoded response-body bytes observed on the transport."""
        return self._response_bytes

    @property
    def max_attempts(self) -> int:
        """Maximum authenticated attempts made for one logical request."""
        return self._max_retries + 1

    def set_attempt_observer(
        self, observer: RequestAttemptObserver | None
    ) -> RequestAttemptObserver | None:
        """Attach durable quota/accounting hooks for subsequent attempts."""
        previous = self._attempt_observer
        self._attempt_observer = observer
        return previous

    def _request(
        self, path: str, params: dict[str, Any] | None = None
    ) -> requests.Response:
        url = f"{BASE_URL}{path}"
        backoff = 1.0
        for attempt in range(self._max_retries + 1):
            wait = self._min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

            observer = self._attempt_observer
            reservation = (
                observer.before_attempt(path, params) if observer is not None else None
            )
            self._request_count += 1
            bytes_before = self._response_bytes
            body_complete = False
            bytes_known = False
            resp: requests.Response | None = None
            try:
                resp = self._session.get(
                    url, params=params, timeout=self._timeout, stream=True
                )
                byte_limit = (
                    getattr(observer, "response_byte_limit", lambda _: None)(
                        reservation
                    )
                    if observer is not None
                    else None
                )
                content_length = _content_length(resp.headers.get("Content-Length"))
                if (
                    byte_limit is not None
                    and content_length is not None
                    and content_length > byte_limit
                ):
                    raise ResponseReservationExceeded(
                        "Tiingo response Content-Length exceeds the reserved "
                        "budget allowance"
                    )
                self._read_and_meter(resp, byte_limit=byte_limit)
                body_complete = True
                bytes_known = True
            except TiingoError as exc:
                bytes_known = bytes_known or bool(
                    getattr(exc, "response_bytes_known", False)
                )
                if resp is not None:
                    resp.close()
                raise
            except requests.RequestException as exc:
                # No response means the authenticated attempt transferred no
                # response body. Partial bodies retain their reservation even
                # when the transport exposes a measured prefix.
                bytes_known = resp is None
                if resp is not None:
                    resp.close()
                if attempt < self._max_retries:
                    log.warning(
                        "Tiingo transport error on %s, retrying in %.1fs",
                        path,
                        backoff,
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
                    continue
                raise TiingoError(
                    f"Tiingo transport failed after {attempt + 1} attempts: {path}"
                ) from exc
            finally:
                if observer is not None:
                    observer.after_attempt(
                        reservation,
                        self._response_bytes - bytes_before,
                        complete=body_complete,
                        bytes_known=bytes_known,
                    )
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                resp.close()
                raise TiingoNotFoundError(path)
            if (
                resp.status_code in (429, 500, 502, 503, 504)
                and attempt < self._max_retries
            ):
                retry_after = resp.headers.get("Retry-After")
                delay = _retry_delay(retry_after, backoff)
                log.warning(
                    "Tiingo %s on %s, retrying in %.1fs", resp.status_code, path, delay
                )
                resp.close()
                time.sleep(delay)
                backoff = min(backoff * 2, 60.0)
                continue
            detail = resp.text[:500]
            resp.close()
            raise TiingoError(f"Tiingo request failed ({resp.status_code}): {detail}")
        raise TiingoError(
            f"Tiingo request failed after {self._max_retries} retries: {path}"
        )

    def _read_and_meter(
        self, resp: requests.Response, *, byte_limit: int | None = None
    ) -> None:
        """Consume one body, enforcing its reservation while it streams."""
        chunks: list[bytes] = []
        try:
            iterator = (
                resp.iter_content(chunk_size=64 * 1024)
                if hasattr(resp, "iter_content")
                else (resp.content,)
            )
            for chunk in iterator:
                if chunk:
                    chunks.append(chunk)
                transferred = _transferred_bytes(resp, complete=False)
                measured = (
                    transferred
                    if transferred is not None
                    else sum(len(item) for item in chunks)
                )
                if byte_limit is not None and measured > byte_limit:
                    self._response_bytes += measured
                    exc = ResponseReservationExceeded(
                        "Tiingo response exceeded its conservative byte reservation"
                    )
                    exc.response_bytes_known = True  # type: ignore[attr-defined]
                    raise exc
        except requests.RequestException as exc:
            transferred = _transferred_bytes(resp, complete=False)
            if transferred is None:
                raise TiingoError(
                    "Tiingo response failed before its transferred bytes could "
                    "be measured"
                ) from exc
            self._response_bytes += transferred
            raise
        resp._content = b"".join(chunks)
        resp._content_consumed = True
        transferred = _transferred_bytes(resp, complete=True)
        if transferred is None:
            raise TiingoError("Tiingo response bytes could not be measured")
        self._response_bytes += transferred

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        resp = self._request(path, params)
        try:
            return resp.json()
        except requests.exceptions.JSONDecodeError as exc:
            raise TiingoError(f"Tiingo returned invalid JSON for {path}") from exc

    def _get_csv(
        self,
        path: str,
        params: dict[str, Any],
        required_fields: frozenset[str],
        *,
        allow_header_only_missing_date: bool = False,
    ) -> list[dict[str, Any]]:
        resp = self._request(path, params)
        try:
            text = resp.content.decode("utf-8-sig")
            stripped = text.strip()
            if not stripped:
                return []
            if stripped.startswith(("[", "{")):
                try:
                    json_payload = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise TiingoError(
                        f"Tiingo returned invalid JSON instead of CSV for {path}"
                    ) from exc
                if json_payload == []:
                    return []
                raise TiingoError(f"Tiingo returned JSON instead of CSV for {path}")
            reader = csv.DictReader(io.StringIO(text), strict=True)
            ordered_fieldnames = reader.fieldnames or []
            fieldnames = set(ordered_fieldnames)
            if len(fieldnames) != len(ordered_fieldnames):
                raise TiingoError(f"Tiingo CSV for {path} has duplicate columns")
            raw_rows = list(reader)
            missing = required_fields - fieldnames
            if missing:
                # IEX omits its implicit date column from a header-only empty
                # response even though populated responses include it. Accept
                # only that exact no-row shape; a populated response missing
                # date remains malformed and fail-closed.
                if (
                    allow_header_only_missing_date
                    and not raw_rows
                    and missing == {"date"}
                ):
                    return []
                raise TiingoError(
                    f"Tiingo CSV for {path} is missing columns: {sorted(missing)}"
                )
        except (UnicodeDecodeError, csv.Error) as exc:
            raise TiingoError(f"Tiingo returned invalid CSV for {path}") from exc
        if any(
            None in row or any(value is None for value in row.values())
            for row in raw_rows
        ):
            raise TiingoError(f"Tiingo returned malformed CSV rows for {path}")
        return [
            {key: None if value == "" else value for key, value in row.items()}
            for row in raw_rows
        ]

    def eod(
        self,
        ticker: str,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> list[dict[str, Any]]:
        """Daily bars including adjusted OHLCV, dividends, and split factors."""
        params: dict[str, Any] = {"format": "csv"}
        if start:
            params["startDate"] = str(start)
        if end:
            params["endDate"] = str(end)
        return self._get_csv(
            f"/tiingo/daily/{ticker.lower()}/prices", params, _EOD_CSV_FIELDS
        )

    def intraday(
        self,
        ticker: str,
        start: date | str,
        end: date | str | None = None,
        freq: str = "1hour",
    ) -> list[dict[str, Any]]:
        """Intraday bars from Tiingo's IEX feed (unadjusted)."""
        params: dict[str, Any] = {
            "startDate": str(start),
            "resampleFreq": freq,
            "columns": _INTRADAY_COLUMNS,
            "format": "csv",
        }
        if end:
            params["endDate"] = str(end)
        return self._get_csv(
            f"/iex/{ticker.lower()}/prices",
            params,
            _INTRADAY_CSV_FIELDS,
            allow_header_only_missing_date=True,
        )

    def ticker_metadata(self, ticker: str) -> dict[str, Any]:
        # Tiingo routes a lowercase ``/tiingo/daily/meta`` to a different
        # handler and returns 404, while ``/tiingo/daily/META`` resolves the
        # listing (RE-013). The price endpoints accept either case.
        return self._get_json(f"/tiingo/daily/{ticker.strip().upper()}")

    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]:
        """Download and parse Tiingo's full supported-tickers list.

        Returns rows with keys: ticker, exchange, assetType, priceCurrency,
        startDate, endDate. When ``tickers`` is provided, unrelated rows are
        discarded while streaming the archive instead of being materialized.
        No API token quota is consumed.
        """
        resp = requests.get(SUPPORTED_TICKERS_URL, timeout=120)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.DictReader(text)
                if tickers is None:
                    return list(reader)
                requested = {ticker.strip().upper() for ticker in tickers}
                return [
                    row
                    for row in reader
                    if (row.get("ticker") or "").strip().upper() in requested
                ]


def _transferred_bytes(resp: requests.Response, *, complete: bool) -> int | None:
    """Return encoded body bytes without trusting decoded ``resp.content``."""
    try:
        transferred = int(resp.raw.tell())
    except (AttributeError, OSError, TypeError, ValueError):
        transferred = -1
    if transferred >= 0:
        return transferred
    content_length = resp.headers.get("Content-Length") if complete else None
    if content_length is not None:
        try:
            length = int(content_length)
        except ValueError:
            pass
        else:
            if length >= 0:
                return length
    if (
        complete
        and not resp.headers.get("Content-Encoding")
        and isinstance(resp._content, bytes)
    ):
        return len(resp._content)
    return None


def _retry_delay(retry_after: str | None, default: float) -> float:
    if retry_after is None:
        return default
    try:
        return max(0.0, float(retry_after))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(retry_after)
        except (TypeError, ValueError, OverflowError):
            return default
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None
