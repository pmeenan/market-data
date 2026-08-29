"""Safe Tiingo identity-bootstrap tests (offline)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

import marketdata.identity_bootstrap as identity_bootstrap_mod
from marketdata.identity_bootstrap import (
    _instrument_id,
    bootstrap_eod_identities,
    bootstrap_intraday_identities,
)
from marketdata.scheduler import BudgetPolicy
from marketdata.store import MetaStore
from marketdata.tiingo import TiingoError


def _archive(
    ticker: str,
    *,
    exchange: str = "NASDAQ",
    start: str = "2020-01-02",
    end: str = "2026-08-27",
) -> dict[str, str]:
    return {
        "ticker": ticker,
        "exchange": exchange,
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": start,
        "endDate": end,
    }


class FakeIdentityClient:
    def __init__(self, archive, metadata):
        self.archive = archive
        self.metadata = metadata
        self.metadata_calls: list[str] = []

    def supported_tickers(self, tickers=None):
        if tickers is None:
            return self.archive
        return [row for row in self.archive if row["ticker"] in tickers]

    def ticker_metadata(self, ticker):
        self.metadata_calls.append(ticker)
        return self.metadata[ticker]


class FailingMetadataClient(FakeIdentityClient):
    def __init__(self, archive, exc):
        super().__init__(archive, {})
        self.exc = exc

    def ticker_metadata(self, ticker):
        self.metadata_calls.append(ticker)
        raise self.exc


class FakeIntradayIdentityClient:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.calls: list[tuple[str, date, date, str]] = []

    def intraday(self, ticker, start, end, freq="1hour"):
        self.calls.append((ticker, start, end, freq))
        if ticker not in self.responses:
            return [_intraday_row(start)]
        response = self.responses[ticker]
        if callable(response):
            return response(start, end)
        return list(response)


def _intraday_row(day: date) -> dict:
    return {
        "date": f"{day.isoformat()}T15:00:00.000Z",
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.2,
        "volume": 100,
    }


def _eod_identity(meta, instrument_id, ticker, start, end):
    meta.upsert_instrument(instrument_id)
    meta.add_instrument_alias(instrument_id, ticker, start, end)
    meta.add_vendor_identifier(
        instrument_id,
        "eod",
        "ticker",
        ticker,
        start,
        end,
        validation_state="validated",
    )


def _metadata(row, **overrides):
    result = {
        "ticker": row["ticker"],
        "exchangeCode": row["exchange"],
        "startDate": row["startDate"],
        "endDate": row["endDate"],
        "name": f"{row['ticker']} Incorporated",
        "description": "fixture",
    }
    result.update(overrides)
    return result


def test_bootstrap_validates_metadata_and_archive_bounded_reused_episodes(tmp_path):
    safe = _archive("SAFE")
    bad = _archive("BAD")
    reused_old = _archive("REUSE", start="2010-01-01", end="2015-01-01")
    reused_new = _archive("REUSE", start="2020-01-01")
    overlap_old = _archive("OVER", start="2010-01-01", end="2020-01-01")
    overlap_new = _archive("OVER", start="2015-01-01")
    client = FakeIdentityClient(
        [safe, bad, reused_old, reused_new, overlap_old, overlap_new],
        {
            "SAFE": _metadata(safe),
            "BAD": _metadata(bad, exchangeCode="NYSE"),
        },
    )

    def now():
        return datetime(2026, 8, 28, tzinfo=UTC)

    with MetaStore(tmp_path / "meta.db") as meta:
        result = bootstrap_eod_identities(
            client,
            meta,
            ["SAFE", "BAD", "REUSE", "OVER", "MISSING"],
            clock=now,
        )

        assert result.validated == ["OVER", "REUSE", "SAFE"]
        assert result.registered_episodes == 5
        assert result.overlaps == {"OVER": ["2015-01-01..2020-01-01"]}
        assert result.blocked == {
            "MISSING": "no in-scope Tiingo supported-tickers record",
        }
        assert "exchangeCode" in result.failed["BAD"]
        assert client.metadata_calls == ["BAD", "SAFE"]
        assert meta.request_usage(now=now(), rolling_days=32)["requests"] == 2

        report = meta.resolve_alias_range(
            "SAFE",
            datetime.fromisoformat(safe["startDate"]).date(),
            datetime.fromisoformat(safe["endDate"]).date(),
        )
        assert report.resolved
        instrument_id = report.segments[0].instrument_id
        assert instrument_id is not None
        identifier = meta.resolve_vendor_identifier(
            instrument_id,
            "eod",
            datetime.fromisoformat(safe["startDate"]).date(),
            datetime.fromisoformat(safe["endDate"]).date(),
        )
        assert (identifier.status, identifier.identifier_value) == (
            "resolved",
            "SAFE",
        )

        resumed = bootstrap_eod_identities(client, meta, ["SAFE"], clock=now)
        assert resumed.skipped == ["SAFE"]
        assert client.metadata_calls == ["BAD", "SAFE"]
        assert len(meta.instrument_ids()) == 5
        reuse = meta.resolve_alias_range(
            "REUSE",
            datetime.fromisoformat(reused_old["startDate"]).date(),
            datetime.fromisoformat(reused_new["endDate"]).date(),
        )
        assert [segment.status for segment in reuse.segments] == [
            "resolved",
            "zero_matches",
            "resolved",
        ]
        assert len(meta.identity_episodes()) == 5


def test_bootstrap_collapses_stale_snapshots_and_upgrades_singleton(tmp_path):
    stale = _archive("SNAP", end="2024-01-02")
    current = _archive("SNAP", end="2026-08-27")
    client = FakeIdentityClient([current, stale], {"SNAP": _metadata(current)})
    instrument_id = _instrument_id(current)

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.upsert_instrument(instrument_id)
        for record in (stale, current):
            start = date.fromisoformat(record["startDate"])
            end = date.fromisoformat(record["endDate"])
            meta.add_instrument_alias(instrument_id, "SNAP", start, end)
            meta.add_vendor_identifier(
                instrument_id,
                "eod",
                "ticker",
                "SNAP",
                start,
                end,
                validation_state="validated",
            )
        meta.record_identity_episode(
            instrument_id,
            source_instrument_id=None,
            dataset_key="eod",
            ticker="SNAP",
            display_label="SNAP@20200102",
            episode_ordinal=2,
            basis="archive_record",
            confidence="archive_bound",
            observed_first=None,
            observed_last=None,
        )

        result = bootstrap_eod_identities(client, meta, ["SNAP"])

        assert result.validated == ["SNAP"]
        assert result.registered_episodes == 1
        assert client.metadata_calls == ["SNAP"]
        aliases = meta.instrument_alias_records(instrument_id)
        assert [(row["start_date"], row["end_date"]) for row in aliases] == [
            ("2020-01-02", "2026-08-27")
        ]
        episode = meta.identity_episodes()[0]
        assert episode["confidence"] == "metadata_validated"
        assert episode["episode_ordinal"] == 1


def test_bootstrap_records_per_ticker_vendor_errors_as_partial(tmp_path):
    archive = _archive("SAFE")
    cases = ("Not found", "transport failed")

    for index, message in enumerate(cases):
        with MetaStore(tmp_path / str(index) / "meta.db") as meta:
            result = bootstrap_eod_identities(
                FailingMetadataClient([archive], TiingoError(message)),
                meta,
                ["SAFE"],
            )

        assert result.failed == {"SAFE": message}
        assert result.partial is True
        assert result.operational_failure is False


def test_bootstrap_skips_universe_resolution_when_identity_is_unchanged(
    tmp_path, monkeypatch
):
    archive = _archive("SAFE")
    client = FakeIdentityClient([archive], {"SAFE": _metadata(archive)})

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.set_universe(2026, [{"ticker": "SAFE", "rank": 1}])
        first = bootstrap_eod_identities(client, meta, ["SAFE"])
        assert first.validated == ["SAFE"]

        resolutions = []
        original = MetaStore.resolve_universe

        def track_resolution(store, year):
            resolutions.append(year)
            return original(store, year)

        monkeypatch.setattr(MetaStore, "resolve_universe", track_resolution)
        second = bootstrap_eod_identities(client, meta, ["SAFE"])

    assert second.skipped == ["SAFE"]
    assert resolutions == []


def test_intraday_bootstrap_probes_exact_frequency_and_persists_fail_closed_outcomes(
    tmp_path,
):
    request_start = date(2018, 1, 1)
    request_end = date(2026, 8, 27)

    def outside_request(start, end):
        return [_intraday_row(start.replace(year=start.year - 1))]

    client = FakeIntradayIdentityClient(
        {
            "EMPTY": [],
            "BAD": outside_request,
        }
    )
    now = datetime(2026, 8, 28, tzinfo=UTC)
    with MetaStore(tmp_path / "meta.db") as meta:
        _eod_identity(
            meta,
            "safe-id",
            "SAFE",
            date(2020, 1, 2),
            request_end,
        )
        _eod_identity(
            meta,
            "empty-id",
            "EMPTY",
            date(2020, 1, 2),
            request_end,
        )
        _eod_identity(
            meta,
            "bad-id",
            "BAD",
            date(2020, 1, 2),
            request_end,
        )
        _eod_identity(
            meta,
            "over-old",
            "OVER",
            date(2017, 1, 3),
            date(2020, 12, 31),
        )
        _eod_identity(
            meta,
            "over-new",
            "OVER",
            date(2019, 1, 2),
            request_end,
        )
        _eod_identity(
            meta,
            "old-id",
            "OLD",
            date(2010, 1, 4),
            date(2015, 12, 31),
        )

        first = bootstrap_intraday_identities(
            client,
            meta,
            ["SAFE", "EMPTY", "BAD", "OVER", "OLD", "MISSING"],
            start=request_start,
            end=request_end,
            freq="1hour",
            probe_sessions=5,
            clock=lambda: now,
        )

        assert len(first.validated) == 3
        assert any(value.startswith("SAFE:") for value in first.validated)
        assert sum(value.startswith("OVER:") for value in first.validated) == 2
        assert first.out_of_range == ["OLD"]
        assert any("multiple_matches" in detail for detail in first.blocked.values())
        assert any(value.startswith("EMPTY:") for value in first.blocked)
        assert any(value.startswith("BAD:") for value in first.blocked)
        assert any(value.startswith("MISSING:") for value in first.blocked)
        assert first.probe_attempts == 5
        assert first.probe_rows == 4
        assert (
            meta.resolve_vendor_identifier(
                "safe-id",
                "intraday_1hour",
                date(2020, 1, 2),
                request_end,
            ).status
            == "resolved"
        )
        assert (
            meta.resolve_vendor_identifier(
                "safe-id",
                "intraday_5min",
                date(2020, 1, 2),
                request_end,
            ).status
            == "zero_matches"
        )
        assert (
            meta.vendor_identifier_evidence_segments(
                "empty-id",
                "intraday_1hour",
                "ticker",
                "EMPTY",
                date(2020, 1, 2),
                request_end,
            )[0].validation_state
            == "rejected"
        )
        empty_evidence = json.loads(
            meta._con.execute(
                """SELECT evidence FROM vendor_identifiers
                   WHERE instrument_id = 'empty-id'
                     AND dataset_key = 'intraday_1hour'"""
            ).fetchone()["evidence"]
        )
        assert empty_evidence["response_rows"] == 0
        assert empty_evidence["probe_start"] <= empty_evidence["probe_end"]
        assert empty_evidence["probe_end"] < empty_evidence["fetch_end"]
        assert (
            meta.vendor_identifier_evidence_segments(
                "bad-id",
                "intraday_1hour",
                "ticker",
                "BAD",
                date(2020, 1, 2),
                request_end,
            )[0].validation_state
            == "conflict"
        )

        calls = list(client.calls)
        usage = meta.request_usage(now=now, rolling_days=32)
        resumed = bootstrap_intraday_identities(
            client,
            meta,
            ["SAFE", "EMPTY", "BAD", "OVER", "OLD", "MISSING"],
            start=request_start,
            end=request_end,
            freq="1hour",
            probe_sessions=5,
            clock=lambda: now,
        )

        assert client.calls == calls
        assert len(resumed.skipped) == 3
        assert resumed.probe_attempts == 0
        assert (
            meta.request_usage(now=now, rolling_days=32)["requests"]
            == usage["requests"]
        )

        client.responses["EMPTY"] = lambda start, end: [_intraday_row(start)]
        retried = bootstrap_intraday_identities(
            client,
            meta,
            ["EMPTY"],
            start=request_start,
            end=request_end,
            freq="1hour",
            probe_sessions=5,
            retry_blocked=True,
            clock=lambda: now,
        )

        assert len(retried.validated) == 1
        assert (
            meta.resolve_vendor_identifier(
                "empty-id",
                "intraday_1hour",
                date(2020, 1, 2),
                request_end,
            ).status
            == "resolved"
        )


def test_intraday_bootstrap_reuses_evidence_across_range_changes(tmp_path, monkeypatch):
    start = date(2024, 1, 2)
    old_end = date(2024, 1, 31)
    new_end = date(2024, 2, 2)
    client = FakeIntradayIdentityClient({"EMPTY": []})
    schedule_calls: list[tuple[date, date]] = []
    real_session_schedule = identity_bootstrap_mod.session_schedule

    def counted_session_schedule(schedule_start, schedule_end):
        schedule_calls.append((schedule_start, schedule_end))
        return real_session_schedule(schedule_start, schedule_end)

    monkeypatch.setattr(
        identity_bootstrap_mod, "session_schedule", counted_session_schedule
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        _eod_identity(meta, "safe-id", "SAFE", start, new_end)
        _eod_identity(meta, "empty-id", "EMPTY", start, new_end)

        first = bootstrap_intraday_identities(
            client,
            meta,
            ["SAFE", "EMPTY"],
            start=start,
            end=old_end,
        )
        calls_after_first = len(client.calls)
        extended = bootstrap_intraday_identities(
            client,
            meta,
            ["SAFE", "EMPTY"],
            start=start,
            end=new_end,
        )
        calls_after_extension = len(client.calls)
        narrowed = bootstrap_intraday_identities(
            client,
            meta,
            ["SAFE", "EMPTY"],
            start=date(2024, 1, 10),
            end=date(2024, 1, 25),
        )

        assert first.probe_attempts == 2
        assert calls_after_first == 2
        assert extended.probe_attempts == 2
        assert calls_after_extension == 4
        assert len(extended.skipped) == 1
        assert len(extended.validated) == 1
        assert len(extended.blocked) == 2
        assert narrowed.probe_attempts == 0
        assert len(client.calls) == calls_after_extension
        assert len(narrowed.skipped) == 1
        assert len(narrowed.blocked) == 1
        assert schedule_calls == [
            (start, old_end),
            (start, new_end),
            (date(2024, 1, 10), date(2024, 1, 25)),
        ]
        assert (
            meta.resolve_vendor_identifier(
                "safe-id", "intraday_1hour", start, new_end
            ).status
            == "resolved"
        )
        evidence_ranges = {
            row["instrument_id"]: []
            for row in meta._con.execute(
                """SELECT instrument_id FROM vendor_identifiers
                   WHERE dataset_key = 'intraday_1hour'"""
            ).fetchall()
        }
        for row in meta._con.execute(
            """SELECT instrument_id, valid_from, valid_to
               FROM vendor_identifiers
               WHERE dataset_key = 'intraday_1hour'
               ORDER BY instrument_id, valid_from"""
        ).fetchall():
            evidence_ranges[row["instrument_id"]].append(
                (row["valid_from"], row["valid_to"])
            )
        assert evidence_ranges == {
            "empty-id": [(str(start), str(old_end)), ("2024-02-01", str(new_end))],
            "safe-id": [(str(start), str(old_end)), ("2024-02-01", str(new_end))],
        }


def test_intraday_bootstrap_rejects_impossible_probe_count_before_transport(
    tmp_path,
):
    client = FakeIntradayIdentityClient()
    with MetaStore(tmp_path / "meta.db") as meta:
        with pytest.raises(ValueError, match="must not exceed 127 for 5min"):
            bootstrap_intraday_identities(
                client,
                meta,
                ["AAPL"],
                start=date(2020, 1, 2),
                end=date(2026, 8, 27),
                freq="5min",
                probe_sessions=128,
            )

    assert client.calls == []


def test_intraday_bootstrap_quota_stop_resumes_after_validated_prefix(tmp_path):
    start, end = date(2024, 1, 2), date(2024, 1, 31)
    now = datetime(2026, 8, 28, tzinfo=UTC)
    policy = BudgetPolicy(
        hourly_request_limit=1,
        daily_request_limit=10,
        total_byte_limit=1_000_000_000,
        historical_byte_limit=1_000_000_000,
        historical_byte_limit_max=1_000_000_000,
    )
    client = FakeIntradayIdentityClient()
    with MetaStore(tmp_path / "meta.db") as meta:
        _eod_identity(meta, "a-id", "AAA", start, end)
        _eod_identity(meta, "b-id", "BBB", start, end)

        stopped = bootstrap_intraday_identities(
            client,
            meta,
            ["AAA", "BBB"],
            start=start,
            end=end,
            policy=policy,
            clock=lambda: now,
        )

        assert stopped.quota_stopped is True
        assert stopped.stop_reason == "hourly_request_limit"
        assert len(stopped.validated) == 1
        assert stopped.probe_attempts == 1
        assert len(client.calls) == 1
