"""Conservative operator bootstrap for Tiingo EOD identity evidence.

The public supported-tickers archive is useful alias evidence, but it is not a
stable security master.  This module therefore admits only ticker strings with
exactly one in-scope archive record and requires the authenticated EOD metadata
endpoint to agree with that record before recording a validated request key.
Reused, absent, or mismatching symbols remain explicit blockers.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from marketdata.errors import BudgetExhausted
from marketdata.locking import data_directory_locked
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    BudgetPolicy,
    PersistentAttemptObserver,
)
from marketdata.store import MetaStore
from marketdata.tiingo import TiingoClient, TiingoError

US_EXCHANGES = frozenset({"NYSE", "NASDAQ", "NYSE ARCA", "AMEX", "BATS"})
US_ASSET_TYPES = frozenset({"Stock", "ETF"})


class IdentityBootstrapClient(Protocol):
    def supported_tickers(self) -> list[dict[str, str]]: ...

    def ticker_metadata(self, ticker: str) -> dict[str, Any]: ...


@dataclass
class IdentityBootstrapResult:
    requested: int
    validated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    blocked: dict[str, str] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    stop_reason: str | None = None

    @property
    def ok(self) -> bool:
        return not self.blocked and not self.failed and self.stop_reason is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "validated": sorted(self.validated),
            "skipped": sorted(self.skipped),
            "blocked": dict(sorted(self.blocked.items())),
            "failed": dict(sorted(self.failed.items())),
            "stop_reason": self.stop_reason,
            "ok": self.ok,
        }


@data_directory_locked("identity:bootstrap-eod")
def bootstrap_eod_identities(
    client: IdentityBootstrapClient,
    meta: MetaStore,
    tickers: Sequence[str],
    *,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
    clock: Callable[[], datetime] | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> IdentityBootstrapResult:
    """Validate safe bare-ticker EOD identities for a frozen ticker cohort.

    Authenticated metadata attempts use the same durable request/byte ledger as
    bar ingestion.  Each accepted record is committed independently, so an
    interrupted run can skip already validated evidence on restart.
    """
    normalized = sorted({ticker.strip().upper() for ticker in tickers})
    if any(not ticker for ticker in normalized):
        raise ValueError("identity bootstrap ticker must not be blank")
    result = IdentityBootstrapResult(requested=len(normalized))
    requested = set(normalized)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in client.supported_tickers():
        ticker = (row.get("ticker") or "").strip().upper()
        if ticker not in requested or not _in_scope(row):
            continue
        grouped[ticker].append(row)

    candidates: list[tuple[str, dict[str, str]]] = []
    for ticker in normalized:
        records = grouped.get(ticker, [])
        if not records:
            result.blocked[ticker] = "no in-scope Tiingo supported-tickers record"
        elif len(records) > 1:
            result.blocked[ticker] = (
                f"{len(records)} in-scope Tiingo records reuse this ticker"
            )
        else:
            candidates.append((ticker, records[0]))

    observer_kwargs: dict[str, Any] = {
        "work_kind": "current",
        "operation": "identity-bootstrap:eod",
        "policy": policy,
    }
    if clock is not None:
        observer_kwargs["clock"] = clock
    observer = PersistentAttemptObserver(meta, **observer_kwargs)
    with _observed_metadata_client(client, observer) as metered:
        for position, (ticker, archive) in enumerate(candidates, start=1):
            try:
                start = _required_date(archive, "startDate")
                end = _required_date(archive, "endDate")
                instrument_id = _instrument_id(archive)
                if _already_validated(meta, instrument_id, ticker, start, end):
                    result.skipped.append(ticker)
                    continue
                metadata = metered.ticker_metadata(ticker)
                _validate_metadata(ticker, archive, metadata)
                meta.upsert_instrument(
                    instrument_id,
                    lifecycle_status="unknown",
                    description=str(metadata.get("description") or "") or None,
                )
                evidence = {
                    "source": "tiingo-supported-tickers+eod-metadata",
                    "archive": {key: archive.get(key) for key in sorted(archive)},
                    "metadata": {
                        key: metadata.get(key)
                        for key in (
                            "ticker",
                            "exchangeCode",
                            "startDate",
                            "endDate",
                            "name",
                        )
                    },
                }
                meta.add_instrument_alias(
                    instrument_id,
                    ticker,
                    start,
                    end,
                    exchange=archive.get("exchange"),
                    asset_type=archive.get("assetType"),
                    evidence=evidence,
                )
                meta.add_vendor_identifier(
                    instrument_id,
                    "eod",
                    "ticker",
                    ticker,
                    start,
                    end,
                    validation_state="validated",
                    evidence=evidence,
                )
                result.validated.append(ticker)
            except BudgetExhausted as exc:
                result.stop_reason = exc.reason
                break
            except (TiingoError, ValueError) as exc:
                result.failed[ticker] = str(exc)
            finally:
                if progress is not None:
                    progress(position, len(candidates))
    for year in meta.universe_years():
        meta.resolve_universe(year)
    return result


class _ObservedMetadataClient:
    def __init__(self, client: IdentityBootstrapClient, observer: Any):
        self._client = client
        self._observer = observer

    def supported_tickers(self) -> list[dict[str, str]]:
        return self._client.supported_tickers()

    def ticker_metadata(self, ticker: str) -> dict[str, Any]:
        reservation = self._observer.before_attempt(f"/tiingo/daily/{ticker.lower()}")
        before = int(getattr(self._client, "response_bytes", 0))
        complete = False
        try:
            result = self._client.ticker_metadata(ticker)
            complete = True
            return result
        finally:
            after = int(getattr(self._client, "response_bytes", before))
            self._observer.after_attempt(
                reservation,
                max(0, after - before),
                complete=complete,
                bytes_known=complete,
            )


@contextmanager
def _observed_metadata_client(
    client: IdentityBootstrapClient, observer: Any
) -> Iterator[IdentityBootstrapClient]:
    if isinstance(client, TiingoClient):
        previous = client.set_attempt_observer(observer)
        try:
            yield client
        finally:
            client.set_attempt_observer(previous)
    else:
        yield _ObservedMetadataClient(client, observer)


def _in_scope(row: Mapping[str, str]) -> bool:
    return (
        row.get("exchange") in US_EXCHANGES
        and row.get("assetType") in US_ASSET_TYPES
        and row.get("priceCurrency") in (None, "", "USD")
        and bool(row.get("startDate"))
        and bool(row.get("endDate"))
    )


def _required_date(row: Mapping[str, Any], field_name: str) -> date:
    value = row.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"Tiingo {field_name} is missing")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Tiingo {field_name} is not an ISO date: {value!r}") from exc


def _instrument_id(archive: Mapping[str, str]) -> str:
    """Return an opaque, rerun-stable id for one immutable listing anchor."""
    anchor = "|".join(
        (
            "tiingo-supported-listing-v1",
            str(archive.get("ticker", "")).upper(),
            str(archive.get("exchange", "")).upper(),
            str(archive.get("assetType", "")),
            str(archive.get("startDate", "")),
        )
    )
    return uuid5(NAMESPACE_URL, anchor).hex


def _already_validated(
    meta: MetaStore,
    instrument_id: str,
    ticker: str,
    start: date,
    end: date,
) -> bool:
    alias = meta.resolve_alias_range(ticker, start, end)
    if not alias.resolved or any(
        segment.instrument_id != instrument_id for segment in alias.segments
    ):
        return False
    identifier = meta.resolve_vendor_identifier(instrument_id, "eod", start, end)
    return (
        identifier.status == "resolved"
        and identifier.identifier_type == "ticker"
        and identifier.identifier_value == ticker
    )


def _validate_metadata(
    ticker: str, archive: Mapping[str, str], metadata: Mapping[str, Any]
) -> None:
    expected = {
        "ticker": ticker,
        "exchangeCode": archive.get("exchange"),
        "startDate": archive.get("startDate"),
        "endDate": archive.get("endDate"),
    }
    actual = {
        "ticker": str(metadata.get("ticker") or "").upper(),
        "exchangeCode": metadata.get("exchangeCode"),
        "startDate": metadata.get("startDate"),
        "endDate": metadata.get("endDate"),
    }
    mismatches = [key for key in expected if actual[key] != expected[key]]
    if mismatches:
        fields = ", ".join(mismatches)
        raise ValueError(f"Tiingo EOD metadata disagrees with archive fields: {fields}")
