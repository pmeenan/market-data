"""Conservative operator bootstrap for Tiingo dataset identity evidence.

The public supported-tickers archive is useful alias evidence, but it is not a
stable security master. Unique listing anchors require matching authenticated
EOD metadata. Reused tickers receive separate archive-bounded episodes;
overlapping spans remain explicit downstream blockers rather than being
silently assigned to either instrument. IEX identifiers are established
separately for each exact frequency by a bounded response probe inside one
conflict-free stable alias envelope.
"""

from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from marketdata.calendar import (
    IntradayRequestChunk,
    max_intraday_probe_sessions,
    next_session_after,
    plan_intraday_requests,
    session_schedule,
)
from marketdata.errors import QUOTA_STOP_REASONS, BudgetExhausted
from marketdata.identity import merge_closed_date_ranges, require_dataset_key
from marketdata.ingest import intraday_target_rows
from marketdata.locking import data_directory_locked
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    BudgetPolicy,
    PersistentAttemptObserver,
    observed_client,
)
from marketdata.store import MetaStore
from marketdata.store.bars import require_intraday_freq
from marketdata.tiingo import TiingoClient, TiingoError, TiingoNotFoundError

US_EXCHANGES = frozenset({"NYSE", "NASDAQ", "NYSE ARCA", "AMEX", "BATS"})
US_ASSET_TYPES = frozenset({"Stock", "ETF"})


def supported_us_stock_etf_records(
    rows: Sequence[Mapping[str, str]],
) -> list[dict[str, str]]:
    """Return a deterministic all-history US stock/ETF archive snapshot."""
    normalized: dict[tuple[str, str, str, str, str, str], dict[str, str]] = {}
    for row in rows:
        if not _in_scope(row):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        start = str(row.get("startDate") or "").strip()
        end = str(row.get("endDate") or "").strip()
        try:
            start_date = date.fromisoformat(start)
            end_date = date.fromisoformat(end)
        except ValueError:
            continue
        if not ticker or start_date > end_date:
            continue
        record = {
            "ticker": ticker,
            "exchange": str(row.get("exchange") or "").strip(),
            "assetType": str(row.get("assetType") or "").strip(),
            "priceCurrency": str(row.get("priceCurrency") or "").strip(),
            "startDate": start,
            "endDate": end,
        }
        key = (
            record["ticker"],
            record["exchange"],
            record["assetType"],
            record["priceCurrency"],
            record["startDate"],
            record["endDate"],
        )
        normalized[key] = record
    return [normalized[key] for key in sorted(normalized)]


class IdentityBootstrapClient(Protocol):
    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]: ...

    def ticker_metadata(self, ticker: str) -> dict[str, Any]: ...


class IntradayIdentityBootstrapClient(Protocol):
    def intraday(
        self,
        ticker: str,
        start: date,
        end: date,
        freq: str = "1hour",
    ) -> list[dict[str, Any]]: ...


@dataclass
class IdentityBootstrapResult:
    requested: int
    validated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    blocked: dict[str, str] = field(default_factory=dict)
    overlaps: dict[str, list[str]] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    registered_episodes: int = 0
    extended_episodes: int = 0
    stop_reason: str | None = None

    @property
    def quota_stopped(self) -> bool:
        return self.stop_reason in QUOTA_STOP_REASONS

    @property
    def partial(self) -> bool:
        return bool(self.blocked or self.overlaps or self.failed)

    @property
    def operational_failure(self) -> bool:
        return self.stop_reason is not None and not self.quota_stopped

    @property
    def ok(self) -> bool:
        return not self.partial and not self.operational_failure

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "validated": sorted(self.validated),
            "skipped": sorted(self.skipped),
            "blocked": dict(sorted(self.blocked.items())),
            "overlaps": dict(sorted(self.overlaps.items())),
            "failed": dict(sorted(self.failed.items())),
            "registered_episodes": self.registered_episodes,
            "extended_episodes": self.extended_episodes,
            "stop_reason": self.stop_reason,
            "quota_stopped": self.quota_stopped,
            "partial": self.partial,
            "operational_failure": self.operational_failure,
            "ok": self.ok,
        }


@dataclass
class IntradayIdentityBootstrapResult:
    requested: int
    dataset_key: str
    start: date
    end: date
    candidate_segments: int = 0
    probe_attempts: int = 0
    probe_rows: int = 0
    validated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    out_of_range: list[str] = field(default_factory=list)
    blocked: dict[str, str] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    stop_reason: str | None = None

    @property
    def quota_stopped(self) -> bool:
        return self.stop_reason in QUOTA_STOP_REASONS

    @property
    def partial(self) -> bool:
        return bool(self.blocked or self.failed)

    @property
    def operational_failure(self) -> bool:
        return self.stop_reason is not None and not self.quota_stopped

    @property
    def ok(self) -> bool:
        return not self.partial and not self.operational_failure

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "dataset_key": self.dataset_key,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "candidate_segments": self.candidate_segments,
            "probe_attempts": self.probe_attempts,
            "probe_rows": self.probe_rows,
            "validated": sorted(self.validated),
            "skipped": sorted(self.skipped),
            "out_of_range": sorted(self.out_of_range),
            "blocked": dict(sorted(self.blocked.items())),
            "failed": dict(sorted(self.failed.items())),
            "stop_reason": self.stop_reason,
            "quota_stopped": self.quota_stopped,
            "partial": self.partial,
            "operational_failure": self.operational_failure,
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
    for row in client.supported_tickers(requested):
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
    identity_changed = False
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
                continuation = None
                if len(parsed) == 1 and not requires_metadata_upgrade:
                    archive, start, end, instrument_id = parsed[0]
                    if not _already_validated(meta, instrument_id, ticker, start, end):
                        continuation = _validated_eod_continuation(
                            meta,
                            episode_rows.get(instrument_id),
                            instrument_id=instrument_id,
                            ticker=ticker,
                            start=start,
                            end=end,
                            archive=archive,
                        )
                if continuation is not None:
                    evidence, description = continuation
                    _register_episode(
                        meta,
                        instrument_id=instrument_id,
                        ticker=ticker,
                        start=start,
                        end=end,
                        archive=archive,
                        evidence=evidence,
                        description=description,
                        ordinal=1,
                        confidence="metadata_validated",
                    )
                    identity_changed = True
                    meta.prune_archive_episode_envelope(
                        instrument_id, ticker, start, end
                    )
                    result.validated.append(ticker)
                    result.extended_episodes += 1
                    continue
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
                            identity_changed = (
                                meta.prune_archive_episode_envelope(
                                    instrument_id, ticker, start, end
                                )
                                or identity_changed
                            )
                            episode = episode_rows.get(instrument_id)
                            if (
                                episode is not None
                                and episode["basis"] == "archive_record"
                                and episode["episode_ordinal"] != ordinal
                            ):
                                meta.set_identity_episode_ordinal(
                                    instrument_id, ordinal
                                )
                                identity_changed = True
                    result.skipped.append(ticker)
                    continue
                if len(parsed) == 1:
                    archive, start, end, instrument_id = parsed[0]
                    if instrument_id in superseded:
                        result.skipped.append(ticker)
                        continue
                    if requires_metadata_upgrade:
                        meta.remove_uncovered_archive_episode(instrument_id)
                        identity_changed = True
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
                    identity_changed = True
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
                        identity_changed = True
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
    if identity_changed:
        for year in meta.universe_years():
            meta.resolve_universe(year)
    return result


@dataclass(frozen=True)
class _IntradayIdentityCandidate:
    instrument_id: str
    ticker: str
    start: date
    end: date
    evidence_state: str | None = None

    @property
    def key(self) -> str:
        return f"{self.ticker}:{self.start}..{self.end}:{self.instrument_id}"


class _ProbeRejected(ValueError):
    """A bounded response supplied no affirmative identity evidence."""


class _ProbeConflict(ValueError):
    """A bounded response contradicted the requested identity envelope."""


@data_directory_locked("identity:bootstrap-intraday")
def bootstrap_intraday_identities(
    client: IntradayIdentityBootstrapClient,
    meta: MetaStore,
    tickers: Sequence[str],
    *,
    start: date,
    end: date,
    freq: str = "1hour",
    probe_sessions: int = 20,
    retry_blocked: bool = False,
    policy: BudgetPolicy = DEFAULT_BUDGET_POLICY,
    clock: Callable[[], datetime] | None = None,
    progress: Callable[[int, int], None] | None = None,
) -> IntradayIdentityBootstrapResult:
    """Establish exact-frequency IEX ticker evidence with bounded probes.

    The stable alias registry is used only to enumerate candidates. Each
    conflict-free alias segment receives its own IEX request, and only a
    non-empty response inside the requested target envelope validates that
    ticker for the exact intraday frequency. Empty and contradictory probes
    are persisted as terminal fail-closed evidence so restarts can advance.
    """
    require_intraday_freq(freq)
    if start > end:
        raise ValueError("intraday identity start must not be after end")
    if probe_sessions <= 0:
        raise ValueError("probe_sessions must be positive")
    max_probe_sessions = max_intraday_probe_sessions(freq)
    if probe_sessions > max_probe_sessions:
        raise ValueError(
            f"probe_sessions must not exceed {max_probe_sessions} for {freq}"
        )
    dataset_key = require_dataset_key(f"intraday_{freq}")
    normalized = sorted({ticker.strip().upper() for ticker in tickers})
    if any(not ticker for ticker in normalized):
        raise ValueError("identity bootstrap ticker must not be blank")
    result = IntradayIdentityBootstrapResult(
        requested=len(normalized),
        dataset_key=dataset_key,
        start=start,
        end=end,
    )
    requested = set(normalized)
    sources_by_ticker: dict[str, list[Any]] = defaultdict(list)
    for source in meta.identity_aliases(requested):
        ticker = str(source["ticker"]).upper()
        if ticker in requested:
            sources_by_ticker[ticker].append(source)

    candidate_ranges: dict[tuple[str, str], list[tuple[date, date]]] = defaultdict(list)
    for ticker in normalized:
        sources = sources_by_ticker.get(ticker, [])
        if not sources:
            result.blocked[f"{ticker}:{start}..{end}"] = (
                "no stable identity alias envelope is available to probe"
            )
            continue
        intersects = False
        for source in sources:
            source_start = date.fromisoformat(str(source["start_date"]))
            source_end = date.fromisoformat(str(source["end_date"]))
            candidate_start = max(start, source_start)
            candidate_end = min(end, source_end)
            if candidate_start > candidate_end:
                continue
            intersects = True
            instrument_id = str(source["instrument_id"])
            report = meta.resolve_alias_range(ticker, candidate_start, candidate_end)
            for segment in report.segments:
                segment_key = f"{ticker}:{segment.start}..{segment.end}"
                if segment.status != "resolved":
                    result.blocked[segment_key] = (
                        "stable alias evidence does not uniquely resolve this "
                        f"IEX probe segment ({segment.status})"
                    )
                    continue
                if segment.instrument_id != instrument_id:
                    continue
                candidate_ranges[(instrument_id, ticker)].append(
                    (segment.start, segment.end)
                )
        if not intersects:
            result.out_of_range.append(ticker)

    base_candidates: list[_IntradayIdentityCandidate] = []
    for (instrument_id, ticker), ranges in candidate_ranges.items():
        base_candidates.extend(
            _IntradayIdentityCandidate(instrument_id, ticker, range_start, range_end)
            for range_start, range_end in merge_closed_date_ranges(ranges)
        )
    candidates = [
        _IntradayIdentityCandidate(
            candidate.instrument_id,
            candidate.ticker,
            evidence.start,
            evidence.end,
            evidence.validation_state,
        )
        for candidate in base_candidates
        for evidence in meta.vendor_identifier_evidence_segments(
            candidate.instrument_id,
            dataset_key,
            "ticker",
            candidate.ticker,
            candidate.start,
            candidate.end,
        )
    ]
    ordered = sorted(
        candidates,
        key=lambda item: (item.ticker, item.start, item.end, item.instrument_id),
    )
    result.candidate_segments = len(ordered)

    overall_sessions = session_schedule(start, end)["session_date"].to_list()
    probe_chunks: dict[_IntradayIdentityCandidate, IntradayRequestChunk | None] = {}
    for candidate in ordered:
        if candidate.evidence_state == "validated" or (
            candidate.evidence_state in {"rejected", "conflict"} and not retry_blocked
        ):
            continue
        first = bisect_left(overall_sessions, candidate.start)
        last = bisect_right(overall_sessions, candidate.end)
        sessions = overall_sessions[first:last]
        if not sessions:
            probe_chunks[candidate] = None
            continue
        probe_dates = sessions[-probe_sessions:]
        probe_start = probe_dates[0]
        probe_end = probe_dates[-1]
        chunks = plan_intraday_requests(probe_start, probe_end, freq=freq)
        fetch_end = next_session_after(probe_end)
        if (
            len(chunks) != 1
            or chunks[0].end != probe_end
            or chunks[0].fetch_end != fetch_end
        ):
            raise ValueError(
                f"{probe_sessions} probe sessions do not fit one safe {freq} "
                f"chunk for {candidate.key}"
            )
        probe_chunks[candidate] = chunks[0]

    observer_kwargs: dict[str, Any] = {
        "work_kind": "current",
        "operation": f"identity-bootstrap:{dataset_key}",
        "policy": policy,
    }
    if clock is not None:
        observer_kwargs["clock"] = clock
    observer = PersistentAttemptObserver(meta, **observer_kwargs)

    with observed_client(client, observer) as metered:
        for position, candidate in enumerate(ordered, start=1):
            probe_evidence: dict[str, Any] = {}
            try:
                if candidate.evidence_state == "validated":
                    resolution = meta.resolve_vendor_identifier(
                        candidate.instrument_id,
                        dataset_key,
                        candidate.start,
                        candidate.end,
                    )
                    if (
                        resolution.status != "resolved"
                        or (resolution.identifier_type or "").casefold() != "ticker"
                        or (resolution.identifier_value or "").upper()
                        != candidate.ticker
                    ):
                        result.blocked[candidate.key] = (
                            "recorded IEX evidence conflicts with another stable "
                            "instrument"
                        )
                    else:
                        result.skipped.append(candidate.key)
                    continue
                if (
                    candidate.evidence_state in {"rejected", "conflict"}
                    and not retry_blocked
                ):
                    result.blocked[candidate.key] = (
                        "previous IEX probe recorded "
                        f"{candidate.evidence_state} evidence; "
                        "retry explicitly after reviewing the report"
                    )
                    continue

                chunk = probe_chunks[candidate]
                if chunk is None:
                    detail = "identity envelope contains no XNYS session to probe"
                    _record_intraday_probe_evidence(
                        meta,
                        candidate,
                        dataset_key,
                        validation_state="rejected",
                        detail=detail,
                    )
                    result.blocked[candidate.key] = detail
                    continue
                probe_evidence = {
                    "probe_start": chunk.start.isoformat(),
                    "probe_end": chunk.end.isoformat(),
                    "fetch_end": chunk.fetch_end.isoformat(),
                }

                rows = metered.intraday(
                    candidate.ticker,
                    chunk.start,
                    chunk.fetch_end,
                    freq=freq,
                )
                result.probe_attempts += 1
                result.probe_rows += len(rows)
                probe_evidence["response_rows"] = len(rows)
                try:
                    target_rows = intraday_target_rows(rows, chunk)
                except ValueError as exc:
                    raise _ProbeConflict(str(exc)) from exc
                if not target_rows:
                    raise _ProbeRejected(
                        "IEX probe returned no rows inside the stable target envelope"
                    )
                target_dates = [
                    date.fromisoformat(str(row["date"])[:10]) for row in target_rows
                ]
                probe_evidence.update(
                    {
                        "target_rows": len(target_dates),
                        "observed_first_date": min(target_dates).isoformat(),
                        "observed_last_date": max(target_dates).isoformat(),
                        "boundary_policy": (
                            "the identity registry supplies only the stable alias "
                            "envelope; "
                            "this exact-frequency IEX response supplies identifier "
                            "validation; every later response remains range-validated"
                        ),
                    }
                )
                _record_intraday_probe_evidence(
                    meta,
                    candidate,
                    dataset_key,
                    validation_state="validated",
                    detail="IEX probe returned target rows inside the stable envelope",
                    probe_evidence=probe_evidence,
                )
                result.validated.append(candidate.key)
            except BudgetExhausted as exc:
                result.stop_reason = exc.reason
                break
            except _ProbeRejected as exc:
                detail = str(exc)
                _record_intraday_probe_evidence(
                    meta,
                    candidate,
                    dataset_key,
                    validation_state="rejected",
                    detail=detail,
                    probe_evidence=probe_evidence,
                )
                result.blocked[candidate.key] = detail
            except _ProbeConflict as exc:
                detail = str(exc)
                _record_intraday_probe_evidence(
                    meta,
                    candidate,
                    dataset_key,
                    validation_state="conflict",
                    detail=detail,
                    probe_evidence=probe_evidence,
                )
                result.blocked[candidate.key] = detail
            except TiingoNotFoundError as exc:
                result.probe_attempts += 1
                detail = str(exc)
                _record_intraday_probe_evidence(
                    meta,
                    candidate,
                    dataset_key,
                    validation_state="rejected",
                    detail=detail,
                    probe_evidence=probe_evidence,
                )
                result.blocked[candidate.key] = detail
            except TiingoError as exc:
                result.probe_attempts += 1
                result.failed[candidate.key] = str(exc)
            finally:
                if progress is not None:
                    progress(position, len(ordered))
    return result


def _record_intraday_probe_evidence(
    meta: MetaStore,
    candidate: _IntradayIdentityCandidate,
    dataset_key: str,
    *,
    validation_state: str,
    detail: str,
    probe_evidence: Mapping[str, Any] | None = None,
) -> None:
    evidence = {
        "source": "tiingo-iex-probe+stable-alias-envelope",
        "dataset_key": dataset_key,
        "instrument_id": candidate.instrument_id,
        "ticker": candidate.ticker,
        "valid_from": candidate.start.isoformat(),
        "valid_to": candidate.end.isoformat(),
        "outcome": validation_state,
        "detail": detail,
    }
    evidence.update(probe_evidence or {})
    meta.add_vendor_identifier(
        candidate.instrument_id,
        dataset_key,
        "ticker",
        candidate.ticker,
        candidate.start,
        candidate.end,
        validation_state=validation_state,
        evidence=evidence,
    )


class _ObservedMetadataClient:
    def __init__(self, client: IdentityBootstrapClient, observer: Any):
        self._client = client
        self._observer = observer

    def supported_tickers(
        self, tickers: Collection[str] | None = None
    ) -> list[dict[str, str]]:
        return self._client.supported_tickers(tickers)

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


def _validated_eod_continuation(
    meta: MetaStore,
    episode: Any,
    *,
    instrument_id: str,
    ticker: str,
    start: date,
    end: date,
    archive: Mapping[str, str],
) -> tuple[dict[str, Any], str | None] | None:
    """Reuse an authenticated listing anchor when only its current end advances.

    Tiingo's active supported-list record advances ``endDate`` each session.
    Requiring a fresh metadata request for that mechanical continuation would
    consume one request per active listing.  Continuation is admitted only for
    an already metadata-validated singleton whose immutable archive fields are
    unchanged and whose new tail has no competing alias owner.
    """
    if (
        episode is None
        or str(episode["basis"]) != "archive_record"
        or str(episode["confidence"]) != "metadata_validated"
        or str(episode["ticker"]) != ticker
    ):
        return None
    try:
        prior_evidence = json.loads(str(episode["evidence"]))
    except (json.JSONDecodeError, TypeError):
        return None
    prior_archive = prior_evidence.get("archive")
    metadata = prior_evidence.get("metadata")
    if not isinstance(prior_archive, dict) or not isinstance(metadata, dict):
        return None
    immutable_fields = (
        "ticker",
        "exchange",
        "assetType",
        "priceCurrency",
        "startDate",
    )
    if any(
        prior_archive.get(field) != archive.get(field) for field in immutable_fields
    ):
        return None
    if (
        str(metadata.get("ticker") or "").upper() != ticker
        or metadata.get("exchangeCode") != archive.get("exchange")
        or metadata.get("startDate") != archive.get("startDate")
    ):
        return None

    aliases = [
        row
        for row in meta.instrument_alias_records(instrument_id)
        if str(row["ticker"]) == ticker
        and date.fromisoformat(str(row["start_date"])) == start
        and str(row["exchange"]) == str(archive.get("exchange") or "")
        and str(row["asset_type"]) == str(archive.get("assetType") or "")
    ]
    prior_ends = sorted(
        (
            date.fromisoformat(str(row["end_date"]))
            for row in aliases
            if date.fromisoformat(str(row["end_date"])) < end
        ),
        reverse=True,
    )
    if not prior_ends:
        return None
    prior_end = prior_ends[0]
    if not meta.has_exact_identity_evidence(
        instrument_id, "eod", ticker, start, prior_end
    ):
        return None
    added_tail = meta.resolve_alias_range(ticker, prior_end + timedelta(days=1), end)
    if any(segment.status != "zero_matches" for segment in added_tail.segments):
        return None

    evidence = {
        "source": "tiingo-supported-tickers-current-continuation",
        "archive": {key: archive.get(key) for key in sorted(archive)},
        "metadata": metadata,
        "continuation": {
            "authenticated_anchor_valid_to": metadata.get("endDate"),
            "previous_valid_to": prior_end.isoformat(),
            "policy": "unchanged unique listing anchor; endDate advanced only",
        },
    }
    description = str(metadata.get("name") or "") or None
    return evidence, description


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
