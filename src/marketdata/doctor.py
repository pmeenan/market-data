"""Bounded operator health checks for the warehouse and its collectors.

Every check reads durable state only (SQLite metadata and the request ledger)
so the command is cheap enough to run after each timer cycle. Findings name the
condition, its severity, and a bounded sample; nothing here mutates the
warehouse. The checks exist because the first live ongoing cycle failed in ways
that were visible only by hand-reading status JSON and SQLite (D-037).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

from marketdata.calendar import latest_completed_session
from marketdata.config import Config
from marketdata.identity import DATASET_KEYS
from marketdata.scheduler import CURRENT_RETRY_ATTEMPTS, DEFAULT_BUDGET_POLICY
from marketdata.store.meta import MetaStore

HealthSeverity = Literal["ok", "warning", "error"]
HealthCheck = Literal[
    "request_rate",
    "ongoing_progress",
    "ongoing_exclusions",
    "coverage_freshness",
    "unresolved_listings",
]

# Retrying a current target this many turns without one successful depth is
# the signature of a planner or identity defect rather than vendor lag.
STUCK_RETRY_TURNS = 5
# A cohort whose top ranks are stale is the failure the scanner cares about.
TOP_RANK_WATCH = 100
_SAMPLE_LIMIT = 10
_RATE_WARNING_FRACTION = 0.8
_STALE_ERROR_FRACTION = 0.25
_STUCK_ERROR_FRACTION = 0.10
_CYCLE_JOBS = (
    ("eod", "eod_job_id"),
    ("intraday_1hour", "hourly_job_id"),
    ("intraday_5min", "five_min_job_id"),
)


@dataclass(frozen=True)
class HealthFinding:
    """One bounded, structured health observation."""

    check: HealthCheck
    severity: HealthSeverity
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "severity": self.severity,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class HealthReport:
    generated_at: datetime
    target_session: date | None
    findings: tuple[HealthFinding, ...]

    @property
    def ok(self) -> bool:
        return all(finding.severity != "error" for finding in self.findings)

    def counts(self) -> dict[str, int]:
        counts = {"ok": 0, "warning": 0, "error": 0}
        for finding in self.findings:
            counts[finding.severity] += 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "target_session": (
                self.target_session.isoformat() if self.target_session else None
            ),
            "ok": self.ok,
            "counts": self.counts(),
            "findings": [finding.to_dict() for finding in self.findings],
        }


def check_health(
    config: Config,
    *,
    now: datetime | None = None,
    stuck_retry_turns: int = STUCK_RETRY_TURNS,
    top_rank_watch: int = TOP_RANK_WATCH,
) -> HealthReport:
    """Run every bounded health check against durable warehouse state."""
    if now is None:
        now = datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("health check timestamp must be timezone-aware")
    if stuck_retry_turns < 1 or top_rank_watch < 1:
        raise ValueError("health thresholds must be positive")
    if not config.meta_path.exists():
        raise RuntimeError("health checks require meta.db")
    findings: list[HealthFinding] = []
    target_session: date | None = None
    with MetaStore(config.meta_path) as meta:
        findings.append(_request_rate_finding(meta, now))
        programs = meta.ongoing_programs()
        cycles = [
            cycle
            for cycle in (
                meta.latest_ongoing_cycle(str(program["program_id"]))
                for program in programs
            )
            if cycle is not None
        ]
        if cycles:
            target_session = max(
                date.fromisoformat(str(cycle["session_date"])) for cycle in cycles
            )
        else:
            try:
                target_session = latest_completed_session(now)
            except ValueError:
                target_session = None
        for cycle in cycles:
            findings.extend(_ongoing_progress_findings(meta, cycle, stuck_retry_turns))
            findings.extend(_ongoing_exclusion_findings(meta, cycle))
            findings.extend(
                _coverage_freshness_findings(
                    meta, cycle, target_session, top_rank_watch
                )
            )
            findings.extend(_unresolved_listing_findings(meta, cycle))
        if not cycles:
            findings.append(
                HealthFinding(
                    "ongoing_progress",
                    "warning",
                    "no ongoing collection program has run a cycle yet",
                )
            )
    return HealthReport(now, target_session, tuple(findings))


def _request_rate_finding(meta: MetaStore, now: datetime) -> HealthFinding:
    usage = meta.request_rate_usage(now=now)
    hourly_limit = DEFAULT_BUDGET_POLICY.hourly_request_limit
    daily_limit = DEFAULT_BUDGET_POLICY.daily_request_limit
    hourly_fraction = usage["hourly_requests"] / hourly_limit
    daily_fraction = usage["daily_requests"] / daily_limit
    worst = max(hourly_fraction, daily_fraction)
    severity: HealthSeverity = "ok"
    if worst >= 1.0:
        severity = "error"
    elif worst >= _RATE_WARNING_FRACTION:
        severity = "warning"
    return HealthFinding(
        "request_rate",
        severity,
        (
            f"{usage['daily_requests']:,} Tiingo requests in the last 24h "
            f"({daily_fraction:.0%} of {daily_limit:,}); "
            f"{usage['hourly_requests']:,} in the last hour "
            f"({hourly_fraction:.0%} of {hourly_limit:,})"
        ),
        {
            "hourly_requests": usage["hourly_requests"],
            "hourly_limit": hourly_limit,
            "daily_requests": usage["daily_requests"],
            "daily_limit": daily_limit,
        },
    )


def _ongoing_progress_findings(
    meta: MetaStore, cycle: Any, stuck_retry_turns: int
) -> list[HealthFinding]:
    findings: list[HealthFinding] = []
    for dataset_key, column in _CYCLE_JOBS:
        job_id = str(cycle[column])
        job = meta.history_job(job_id)
        if job is None:
            continue
        targets = meta.history_targets(job_id)
        stuck = [
            target
            for target in targets
            if str(target["last_attempt_status"] or "") == "current_retry_pending"
            and int(target["successful_depth"]) == 0
            and int(target["attempted_turns"]) >= stuck_retry_turns
        ]
        exhausted = [
            target
            for target in targets
            if str(target["last_attempt_status"] or "") == "current_retry_exhausted"
        ]
        total = len(targets)
        affected = len(stuck) + len(exhausted)
        if total == 0 or affected == 0:
            continue
        severity: HealthSeverity = (
            "error" if affected / total >= _STUCK_ERROR_FRACTION else "warning"
        )
        tickers = _target_tickers(meta, job_id, [*stuck, *exhausted])
        findings.append(
            HealthFinding(
                "ongoing_progress",
                severity,
                (
                    f"{dataset_key} cycle {cycle['session_date']}: {len(stuck)} of "
                    f"{total} targets retried at least {stuck_retry_turns} turns "
                    f"without advancing coverage and {len(exhausted)} exhausted all "
                    f"{CURRENT_RETRY_ATTEMPTS}; every retry spends requests"
                ),
                {
                    "program_id": str(cycle["program_id"]),
                    "job_id": job_id,
                    "job_status": str(job["status"]),
                    "targets": total,
                    "stuck": len(stuck),
                    "exhausted": len(exhausted),
                    "sample": tickers[:_SAMPLE_LIMIT],
                },
            )
        )
    return findings


def _ongoing_exclusion_findings(meta: MetaStore, cycle: Any) -> list[HealthFinding]:
    findings: list[HealthFinding] = []
    for dataset_key, column in _CYCLE_JOBS:
        job_id = str(cycle[column])
        if meta.history_job(job_id) is None:
            continue
        blocked = meta.history_blocked_ranges(job_id)
        if not blocked:
            continue
        by_status: dict[str, int] = {}
        for row in blocked:
            by_status[str(row["status"])] = by_status.get(str(row["status"]), 0) + 1
        sample = sorted({str(row["ticker"]) for row in blocked})[:_SAMPLE_LIMIT]
        findings.append(
            HealthFinding(
                "ongoing_exclusions",
                "warning",
                (
                    f"{dataset_key} cycle {cycle['session_date']}: {len(blocked)} "
                    "frozen targets were excluded before any request "
                    f"({', '.join(f'{k}={v}' for k, v in sorted(by_status.items()))})"
                ),
                {
                    "job_id": job_id,
                    "excluded": len(blocked),
                    "by_status": by_status,
                    "sample": sample,
                },
            )
        )
    return findings


def _coverage_freshness_findings(
    meta: MetaStore, cycle: Any, target_session: date | None, top_rank_watch: int
) -> list[HealthFinding]:
    snapshot_id = cycle["cohort_snapshot_id"]
    if snapshot_id is None or target_session is None:
        return []
    members = meta.ongoing_cohort_members(str(snapshot_id))
    if not members:
        return []
    findings: list[HealthFinding] = []
    for dataset_key in DATASET_KEYS:
        coverage = meta.coverage(dataset_key)
        stale = [
            member
            for member in members
            if (
                coverage.get(str(member["instrument_id"])) is None
                or coverage[str(member["instrument_id"])][1] < target_session
            )
        ]
        if not stale:
            findings.append(
                HealthFinding(
                    "coverage_freshness",
                    "ok",
                    f"{dataset_key}: all {len(members)} cohort members are covered "
                    f"through {target_session}",
                    {"dataset_key": dataset_key, "members": len(members), "stale": 0},
                )
            )
            continue
        top_stale = [m for m in stale if int(m["rank"]) <= top_rank_watch]
        fraction = len(stale) / len(members)
        severity: HealthSeverity = "warning"
        if top_stale or fraction >= _STALE_ERROR_FRACTION:
            severity = "error"
        findings.append(
            HealthFinding(
                "coverage_freshness",
                severity,
                (
                    f"{dataset_key}: {len(stale)} of {len(members)} cohort members "
                    f"({fraction:.0%}) lack coverage through {target_session}; "
                    f"{len(top_stale)} are in the top {top_rank_watch} by dollar volume"
                ),
                {
                    "dataset_key": dataset_key,
                    "members": len(members),
                    "stale": len(stale),
                    "stale_in_top_ranks": len(top_stale),
                    "sample": [
                        {
                            "rank": int(m["rank"]),
                            "ticker": str(m["ticker"]),
                            "coverage_end": (
                                coverage[str(m["instrument_id"])][1].isoformat()
                                if str(m["instrument_id"]) in coverage
                                else None
                            ),
                        }
                        for m in stale[:_SAMPLE_LIMIT]
                    ],
                },
            )
        )
    return findings


def _unresolved_listing_findings(meta: MetaStore, cycle: Any) -> list[HealthFinding]:
    session = date.fromisoformat(str(cycle["session_date"]))
    tickers = meta.ongoing_supported_tickers(str(cycle["supported_snapshot_id"]))
    if not tickers:
        return []
    resolving = meta.uniquely_resolving_tickers(session)
    unresolved = [ticker for ticker in tickers if ticker not in resolving]
    if not unresolved:
        return [
            HealthFinding(
                "unresolved_listings",
                "ok",
                f"every active supported listing resolves to one instrument on {session}",
                {"listings": len(tickers), "unresolved": 0},
            )
        ]
    return [
        HealthFinding(
            "unresolved_listings",
            "warning",
            (
                f"{len(unresolved)} of {len(tickers)} active supported listings do "
                f"not resolve to exactly one instrument on {session}; they receive "
                "no EOD updates and cannot enter the intraday cohort"
            ),
            {
                "listings": len(tickers),
                "unresolved": len(unresolved),
                "sample": unresolved[:_SAMPLE_LIMIT],
            },
        )
    ]


def _target_tickers(meta: MetaStore, job_id: str, targets: Sequence[Any]) -> list[str]:
    tickers: list[str] = []
    for target in targets:
        ranges = meta.history_ranges(job_id, int(target["target_ordinal"]))
        tickers.extend(str(row["ticker"]) for row in ranges[:1])
    return sorted(dict.fromkeys(tickers))
