"""Conservative operator bootstrap for Tiingo EOD identity evidence.

The public supported-tickers archive is useful alias evidence, but it is not a
stable security master. Unique listing anchors require matching authenticated
EOD metadata. Reused tickers receive separate archive-bounded episodes;
overlapping spans remain explicit downstream blockers rather than being
silently assigned to either instrument.
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
    overlaps: dict[str, list[str]] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    registered_episodes: int = 0
    stop_reason: str | None = None

    @property
    def ok(self) -> bool:
        return (
            not self.blocked
            and not self.overlaps
            and not self.failed
            and self.stop_reason is None
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "validated": sorted(self.validated),
            "skipped": sorted(self.skipped),
            "blocked": dict(sorted(self.blocked.items())),
            "overlaps": dict(sorted(self.overlaps.items())),
            "failed": dict(sorted(self.failed.items())),
            "registered_episodes": self.registered_episodes,
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

    candidates: list[tuple[str, list[dict[str, str]]]] = []
    for ticker in normalized:
        records = sorted(
            grouped.get(ticker, []),
            key=lambda row: (
                row.get("startDate", ""),
                row.get("endDate", ""),
                row.get("exchange", ""),
                row.get("assetType", ""),
            ),
        )
        if not records:
            result.blocked[ticker] = "no in-scope Tiingo supported-tickers record"
        else:
            candidates.append((ticker, records))

    episode_rows = {str(row["instrument_id"]): row for row in meta.identity_episodes()}
    superseded = {
        str(row["source_instrument_id"])
        for row in episode_rows.values()
        if row["source_instrument_id"] is not None and row["dataset_key"] == "eod"
    }

    observer_kwargs: dict[str, Any] = {
        "work_kind": "current",
        "operation": "identity-bootstrap:eod",
        "policy": policy,
    }
    if clock is not None:
        observer_kwargs["clock"] = clock
    observer = PersistentAttemptObserver(meta, **observer_kwargs)
    with _observed_metadata_client(client, observer) as metered:
        for position, (ticker, archives) in enumerate(candidates, start=1):
            try:
                parsed = _effective_archive_records(archives)
                overlap_ranges = _archive_overlap_ranges(parsed)
                if overlap_ranges:
                    result.overlaps[ticker] = overlap_ranges
                requires_metadata_upgrade = (
                    len(parsed) == 1
                    and parsed[0][3] in episode_rows
                    and episode_rows[parsed[0][3]]["confidence"] == "archive_bound"
                )
                if (
                    all(
                        instrument_id in superseded
                        or _already_validated(meta, instrument_id, ticker, start, end)
                        for _, start, end, instrument_id in parsed
                    )
                    and not requires_metadata_upgrade
                ):
                    for ordinal, (_, start, end, instrument_id) in enumerate(
                        parsed, start=1
                    ):
                        if instrument_id not in superseded:
                            meta.prune_archive_episode_envelope(
                                instrument_id, ticker, start, end
                            )
                            episode = episode_rows.get(instrument_id)
                            if (
                                episode is not None
                                and episode["basis"] == "archive_record"
                            ):
                                meta.set_identity_episode_ordinal(
                                    instrument_id, ordinal
                                )
                    result.skipped.append(ticker)
                    continue
                if len(parsed) == 1:
                    archive, start, end, instrument_id = parsed[0]
                    if instrument_id in superseded:
                        result.skipped.append(ticker)
                        continue
                    if requires_metadata_upgrade:
                        meta.remove_uncovered_archive_episode(instrument_id)
                    metadata = metered.ticker_metadata(ticker)
                    _validate_metadata(ticker, archive, metadata)
                    evidence = _metadata_evidence(archive, metadata)
                    _register_episode(
                        meta,
                        instrument_id=instrument_id,
                        ticker=ticker,
                        start=start,
                        end=end,
                        archive=archive,
                        evidence=evidence,
                        description=str(metadata.get("description") or "") or None,
                        ordinal=1,
                        confidence="metadata_validated",
                    )
                    meta.prune_archive_episode_envelope(
                        instrument_id, ticker, start, end
                    )
                    result.registered_episodes += 1
                else:
                    for ordinal, (archive, start, end, instrument_id) in enumerate(
                        parsed, start=1
                    ):
                        if instrument_id in superseded:
                            continue
                        evidence = {
                            "source": "tiingo-supported-tickers-episode",
                            "archive_record_count": len(parsed),
                            "archive_snapshot_count": len(archives),
                            "archive": {
                                key: archive.get(key) for key in sorted(archive)
                            },
                            "boundary_policy": (
                                "archive rows create separate listing episodes; "
                                "overlapping dates remain fail-closed"
                            ),
                        }
                        _register_episode(
                            meta,
                            instrument_id=instrument_id,
                            ticker=ticker,
                            start=start,
                            end=end,
                            archive=archive,
                            evidence=evidence,
                            description=None,
                            ordinal=ordinal,
                            confidence="archive_bound",
                        )
                        meta.prune_archive_episode_envelope(
                            instrument_id, ticker, start, end
                        )
                        result.registered_episodes += 1
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


def _effective_archive_records(
    archives: Sequence[dict[str, str]],
) -> list[tuple[dict[str, str], date, date, str]]:
    """Collapse stale snapshots that share the same immutable listing anchor."""
    effective: dict[str, tuple[dict[str, str], date, date, str]] = {}
    for archive in archives:
        start = _required_date(archive, "startDate")
        end = _required_date(archive, "endDate")
        instrument_id = _instrument_id(archive)
        candidate = (archive, start, end, instrument_id)
        current = effective.get(instrument_id)
        if current is None or end > current[2]:
            effective[instrument_id] = candidate
    return sorted(
        effective.values(),
        key=lambda item: (
            item[1],
            item[2],
            item[0].get("exchange", ""),
            item[0].get("assetType", ""),
        ),
    )


def _archive_overlap_ranges(
    records: Sequence[tuple[dict[str, str], date, date, str]],
) -> list[str]:
    """Report every pairwise archive overlap without choosing an owner."""
    overlaps: set[tuple[date, date]] = set()
    for index, (_, first_start, first_end, _) in enumerate(records):
        for _, second_start, second_end, _ in records[index + 1 :]:
            overlap_start = max(first_start, second_start)
            overlap_end = min(first_end, second_end)
            if overlap_start <= overlap_end:
                overlaps.add((overlap_start, overlap_end))
    return [f"{start}..{end}" for start, end in sorted(overlaps)]


def _already_validated(
    meta: MetaStore,
    instrument_id: str,
    ticker: str,
    start: date,
    end: date,
) -> bool:
    return meta.has_exact_identity_evidence(instrument_id, "eod", ticker, start, end)


def _metadata_evidence(
    archive: Mapping[str, str], metadata: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "source": "tiingo-supported-tickers+eod-metadata",
        "archive": {key: archive.get(key) for key in sorted(archive)},
        "metadata": {
            key: metadata.get(key)
            for key in ("ticker", "exchangeCode", "startDate", "endDate", "name")
        },
    }


def _register_episode(
    meta: MetaStore,
    *,
    instrument_id: str,
    ticker: str,
    start: date,
    end: date,
    archive: Mapping[str, str],
    evidence: Mapping[str, Any],
    description: str | None,
    ordinal: int,
    confidence: str,
) -> None:
    meta.upsert_instrument(
        instrument_id,
        lifecycle_status="unknown",
        description=description,
    )
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
    meta.record_identity_episode(
        instrument_id,
        source_instrument_id=None,
        dataset_key="eod",
        ticker=ticker,
        display_label=(
            ticker if confidence == "metadata_validated" else f"{ticker}@{start}"
        ),
        episode_ordinal=ordinal,
        basis="archive_record",
        confidence=confidence,
        observed_first=None,
        observed_last=None,
        evidence=evidence,
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
