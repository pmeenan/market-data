"""Durable request-budget and breadth-first scheduler tests (offline)."""

from datetime import UTC, date, datetime, timedelta

import pytest

from marketdata.scheduler import (
    BudgetExhausted,
    BudgetPolicy,
    PersistentAttemptObserver,
    history_job_id,
    initialize_history_job,
    resolve_history_job,
    run_history_sweep,
    run_ingestion_cycle,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import instrument_bucket
from marketdata.tiingo import ResponseReservationExceeded, TiingoError


class FakeIntraday:
    def __init__(self, *, fail: set[str] = frozenset()):
        self.calls: list[tuple[str, date, date, str]] = []
        self.eod_calls: list[tuple[str, date, date]] = []
        self.events: list[str] = []
        self.fail = fail

    def intraday(self, ticker, start, end, freq="1hour"):
        start = date.fromisoformat(str(start))
        end = date.fromisoformat(str(end))
        self.calls.append((ticker, start, end, freq))
        self.events.append(f"intraday:{freq}")
        if ticker in self.fail:
            raise TiingoError("simulated transport failure")
        rows = []
        cursor = start
        while cursor <= end:
            if cursor.weekday() < 5:
                rows.append(
                    {
                        "date": f"{cursor.isoformat()}T15:00:00.000Z",
                        "open": 10.0,
                        "high": 10.5,
                        "low": 9.5,
                        "close": 10.2,
                        "volume": 100,
                    }
                )
            cursor += timedelta(days=1)
        return rows

    def eod(self, ticker, start, end):
        self.events.append("eod")
        self.eod_calls.append((ticker, start, end))
        day = date.fromisoformat(str(end))
        return [
            {
                "date": f"{day.isoformat()}T00:00:00.000Z",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000,
                "adjOpen": 100.0,
                "adjHigh": 101.0,
                "adjLow": 99.0,
                "adjClose": 100.0,
                "adjVolume": 1000,
                "divCash": 0.0,
                "splitFactor": 1.0,
            }
        ]


def _scheduler_store(tmp_path, tickers=("A", "B", "C")):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        for ticker in tickers:
            instrument_id = f"instrument-{ticker.lower()}"
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(instrument_id, ticker, start, end)
            meta.add_vendor_identifier(
                instrument_id,
                "intraday_5min",
                "ticker",
                ticker,
                start,
                end,
                validation_state="validated",
            )
        job_id = history_job_id("intraday_5min", tickers, start, end)
        initialize_history_job(
            meta,
            job_id=job_id,
            dataset_key="intraday_5min",
            tickers=tickers,
            start=start,
            end=end,
        )
    return data_dir, job_id


def _same_bucket_ids() -> tuple[str, str]:
    seen: dict[str, str] = {}
    for number in range(10_000):
        instrument_id = f"batched-{number}"
        bucket = instrument_bucket(instrument_id)
        if bucket in seen:
            return seen[bucket], instrument_id
        seen[bucket] = instrument_id
    raise AssertionError("could not find a deterministic bucket collision")


def test_scheduler_resumes_unvisited_remainder_before_any_target_deepens(tmp_path):
    data_dir, job_id = _scheduler_store(tmp_path)
    client = FakeIntraday()

    with MetaStore(data_dir / "meta.db") as meta:
        cohort_order = [
            row["instrument_id"].removeprefix("instrument-").upper()
            for row in meta.history_targets(job_id)
        ]
        first = run_history_sweep(client, BarStore(data_dir), meta, job_id, max_units=2)
        assert first.stop_reason == "max_units"
        assert (first.sweep_ended, meta.history_job(job_id)["cursor"]) == (0, 2)

    # Reopening meta.db proves the cursor, cohort, and per-target frontier are
    # process-restart state rather than incidental in-memory ordering.
    with MetaStore(data_dir / "meta.db") as meta:
        remainder = run_history_sweep(client, BarStore(data_dir), meta, job_id)
        assert remainder.sweep_ended == 1
        assert [call[0] for call in client.calls] == cohort_order
        assert [row["successful_depth"] for row in meta.history_targets(job_id)] == [
            1,
            1,
            1,
        ]

        run_history_sweep(client, BarStore(data_dir), meta, job_id)
        assert [call[0] for call in client.calls[:6]] == cohort_order * 2


def test_ready_units_in_one_bucket_publish_as_one_batch(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    instrument_ids = _same_bucket_ids()
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        for instrument_id, ticker in zip(instrument_ids, ("A", "B"), strict=True):
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(instrument_id, ticker, start, end)
            meta.add_vendor_identifier(
                instrument_id,
                "intraday_5min",
                "ticker",
                ticker,
                start,
                end,
                validation_state="validated",
            )
        initialize_history_job(
            meta,
            job_id="bucket-batch",
            dataset_key="intraday_5min",
            tickers=["A", "B"],
            start=start,
            end=end,
        )
        bars = BarStore(data_dir)
        published = []
        original_publish = bars.publish_intraday

        def record_publish(frames, *, freq):
            published.append(sorted(frames))
            return original_publish(frames, freq=freq)

        monkeypatch.setattr(bars, "publish_intraday", record_publish)

        result = run_history_sweep(FakeIntraday(), bars, meta, "bucket-batch")

        assert result.successful_units == 2
        assert published == [sorted(instrument_ids)]


def test_quota_stop_does_not_consume_target_turn_or_move_cursor(tmp_path):
    data_dir, job_id = _scheduler_store(tmp_path)
    client = FakeIntraday()
    now = datetime(2026, 8, 28, 12, tzinfo=UTC)
    policy = BudgetPolicy(
        hourly_request_limit=2,
        daily_request_limit=100,
        total_byte_limit=1_000_000,
        historical_byte_limit=1_000_000,
        rolling_days=32,
        response_reservation_bytes=100,
    )
    with MetaStore(data_dir / "meta.db") as meta:
        cohort_order = [
            row["instrument_id"].removeprefix("instrument-").upper()
            for row in meta.history_targets(job_id)
        ]
        stopped = run_history_sweep(
            client,
            BarStore(data_dir),
            meta,
            job_id,
            policy=policy,
            clock=lambda: now,
        )
        assert stopped.stop_reason == "hourly_request_limit"
        assert meta.history_job(job_id)["cursor"] == 2
        assert [row["attempted_turns"] for row in meta.history_targets(job_id)] == [
            1,
            1,
            0,
        ]

        resumed = run_history_sweep(
            client,
            BarStore(data_dir),
            meta,
            job_id,
            policy=policy,
            clock=lambda: now + timedelta(hours=2),
        )
        assert resumed.stop_reason is None
        assert meta.history_job(job_id)["sweep"] == 1
        assert [call[0] for call in client.calls] == cohort_order


def test_failed_and_identity_blocked_turns_retain_frontiers_but_not_peers(tmp_path):
    data_dir, job_id = _scheduler_store(tmp_path)
    with MetaStore(data_dir / "meta.db") as meta:
        # Remove B's exact-frequency evidence to make it identity-blocked.
        meta._con.execute(
            """DELETE FROM vendor_identifiers
               WHERE instrument_id = 'instrument-b'"""
        )
        meta._con.commit()
        original_frontiers = {
            row["target_ordinal"]: row["frontier_end"]
            for row in meta.history_ranges(job_id)
        }
        client = FakeIntraday(fail={"A"})
        result = run_history_sweep(client, BarStore(data_dir), meta, job_id)

        assert result.ingest.failed
        assert result.ingest.blocked
        targets = meta.history_targets(job_id)
        assert [row["attempted_turns"] for row in targets] == [1, 1, 1]
        ranges = {row["target_ordinal"]: row for row in meta.history_ranges(job_id)}
        ordinals = {row["instrument_id"]: row["target_ordinal"] for row in targets}
        failed = ordinals["instrument-a"]
        blocked = ordinals["instrument-b"]
        safe = ordinals["instrument-c"]
        assert ranges[failed]["frontier_end"] == original_frontiers[failed]
        assert ranges[blocked]["frontier_end"] == original_frontiers[blocked]
        assert ranges[safe]["frontier_end"] < original_frontiers[safe]
        assert meta.get_coverage("instrument-c", "intraday_5min") is not None


def test_disconnected_older_coverage_is_bridged_from_its_trailing_edge(tmp_path):
    data_dir = tmp_path / "data"
    evidence_start = date(2023, 12, 1)
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", evidence_start, end)
        meta.add_vendor_identifier(
            "instrument-a",
            "eod",
            "ticker",
            "A",
            evidence_start,
            end,
            validation_state="validated",
        )
        meta.set_coverage("instrument-a", "eod", evidence_start, date(2023, 12, 15))
        initialize_history_job(
            meta,
            job_id="trailing-bridge",
            dataset_key="eod",
            tickers=["A"],
            start=start,
            end=end,
        )
        client = FakeIntraday()

        result = run_history_sweep(
            client, BarStore(data_dir), meta, "trailing-bridge", max_units=1
        )

        assert client.eod_calls[0][1:] == (date(2023, 12, 16), end)
        assert result.successful_units == 1
        assert meta.history_targets("trailing-bridge")[0]["successful_depth"] == 1


def test_partial_trailing_turn_counts_as_successful_request_depth(tmp_path):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", start, end)
        meta.add_vendor_identifier(
            "instrument-a",
            "intraday_5min",
            "ticker",
            "A",
            start,
            end,
            validation_state="validated",
        )
        meta.set_coverage("instrument-a", "intraday_5min", start, date(2024, 1, 15))
        initialize_history_job(
            meta,
            job_id="partial-trailing",
            dataset_key="intraday_5min",
            tickers=["A"],
            start=start,
            end=end,
        )

        result = run_history_sweep(
            FakeIntraday(), BarStore(data_dir), meta, "partial-trailing", max_units=1
        )

        target = meta.history_targets("partial-trailing")[0]
        assert meta.history_ranges("partial-trailing")[0]["status"] == "active"
        assert target["last_attempt_status"] == "advanced"
        assert target["successful_depth"] == result.successful_units == 1


def test_terminal_identity_blocker_does_not_hold_later_phase_forever(tmp_path):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", start, end)
        initialize_history_job(
            meta,
            job_id="blocked-phase-1",
            phase=1,
            dataset_key="intraday_1hour",
            tickers=["A"],
            start=start,
            end=end,
        )
        initialize_history_job(
            meta,
            job_id="phase-2",
            phase=2,
            dataset_key="eod",
            tickers=["A"],
            start=start,
            end=end,
        )

        blocked = run_history_sweep(
            FakeIntraday(), BarStore(data_dir), meta, "blocked-phase-1"
        )
        later = run_history_sweep(FakeIntraday(), BarStore(data_dir), meta, "phase-2")

        assert blocked.job_status == "blocked"
        assert later.stop_reason is None

        meta.add_vendor_identifier(
            "instrument-a",
            "intraday_1hour",
            "ticker",
            "A",
            start,
            end,
            validation_state="validated",
        )
        initialize_history_job(
            meta,
            job_id="blocked-phase-1",
            phase=1,
            dataset_key="intraday_1hour",
            tickers=["A"],
            start=start,
            end=end,
        )
        assert meta.history_job("blocked-phase-1")["status"] == "active"


def test_force_requests_get_new_jobs_unless_explicit_id_is_supplied(tmp_path):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", start, end)
        first = resolve_history_job(
            meta,
            dataset_key="eod",
            tickers=["A"],
            start=start,
            end=end,
            force=True,
        )
        second = resolve_history_job(
            meta,
            dataset_key="eod",
            tickers=["A"],
            start=start,
            end=end,
            force=True,
        )

        assert first != second
        assert meta.history_job(first) is not None
        assert meta.history_job(second) is not None


def test_current_cycle_validates_every_dataset_before_transport(tmp_path):
    client = FakeIntraday()
    with MetaStore(tmp_path / "meta.db") as meta:
        with pytest.raises(ValueError, match="invalid current dataset"):
            run_ingestion_cycle(
                client,
                BarStore(tmp_path),
                meta,
                current_tickers=["A"],
                current_datasets=["eod", "invalid"],
                history_job_id=None,
            )
    assert client.events == []


def test_oversized_response_is_checkpointed_as_terminal_range_blocker(tmp_path):
    data_dir, job_id = _scheduler_store(tmp_path, tickers=("A",))

    class Oversized(FakeIntraday):
        def intraday(self, ticker, start, end, freq="1hour"):
            self.calls.append((ticker, start, end, freq))
            raise ResponseReservationExceeded("response exceeded reservation")

    client = Oversized()
    with MetaStore(data_dir / "meta.db") as meta:
        first = run_history_sweep(client, BarStore(data_dir), meta, job_id)
        second = run_history_sweep(client, BarStore(data_dir), meta, job_id)

        assert first.stop_reason == "response_reservation_exceeded"
        assert first.job_status == "blocked"
        assert meta.history_ranges(job_id)[0]["terminal_blocked"] == 1
        assert second.attempted_units == 0
        assert len(client.calls) == 1


def test_budget_reserves_requests_and_bytes_before_transport(tmp_path):
    now = datetime(2026, 8, 28, 12, tzinfo=UTC)
    policy = BudgetPolicy(
        hourly_request_limit=10,
        daily_request_limit=10,
        total_byte_limit=400,
        historical_byte_limit=300,
        rolling_days=32,
        response_reservation_bytes=100,
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        historical = PersistentAttemptObserver(
            meta,
            work_kind="historical",
            operation="test-history",
            policy=policy,
            clock=lambda: now,
        )
        for observed in (60, 60, 70):
            attempt = historical.before_attempt()
            historical.after_attempt(attempt, observed, complete=True)
        # An incomplete response keeps its 100-byte reservation charged.
        attempt = historical.before_attempt()
        historical.after_attempt(attempt, 7, complete=False, bytes_known=False)
        usage = meta.request_usage(now=now, rolling_days=32)
        assert usage == {
            "requests": 4,
            "observed_bytes": 197,
            "charged_bytes": 290,
            "historical_charged_bytes": 290,
            "incomplete_attempts": 1,
        }
        with pytest.raises(BudgetExhausted, match="historical_byte_limit"):
            historical.before_attempt()

        # Current collection owns the separate 10 GB-equivalent reserve and
        # can proceed even after history is stopped by its lower ceiling.
        current = PersistentAttemptObserver(
            meta,
            work_kind="current",
            operation="test-current",
            policy=policy,
            clock=lambda: now,
        )
        current_attempt = current.before_attempt()
        current.after_attempt(current_attempt, 25, complete=True)
        with pytest.raises(BudgetExhausted, match="total_byte_limit"):
            current.before_attempt()


def test_retry_attempts_are_individually_reserved_and_settled(tmp_path):
    now = datetime(2026, 8, 28, 12, tzinfo=UTC)
    policy = BudgetPolicy(
        hourly_request_limit=1,
        daily_request_limit=10,
        total_byte_limit=1_000,
        historical_byte_limit=1_000,
        rolling_days=32,
        response_reservation_bytes=100,
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        observer = PersistentAttemptObserver(
            meta,
            work_kind="historical",
            operation="retry-test",
            policy=policy,
            clock=lambda: now,
        )
        first = observer.before_attempt()
        observer.after_attempt(first, 17, complete=True)
        with pytest.raises(BudgetExhausted, match="hourly_request_limit"):
            observer.before_attempt()
        rows = meta.request_attempts()
        assert len(rows) == 1
        assert (rows[0]["observed_bytes"], rows[0]["complete"]) == (17, 1)


def test_daily_request_limit_uses_a_conservative_rolling_window(tmp_path):
    moments = [datetime(2026, 8, 28, 10, tzinfo=UTC)]
    policy = BudgetPolicy(
        hourly_request_limit=10,
        daily_request_limit=1,
        total_byte_limit=1_000,
        historical_byte_limit=1_000,
        rolling_days=32,
        response_reservation_bytes=100,
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        observer = PersistentAttemptObserver(
            meta,
            work_kind="current",
            operation="daily-limit",
            policy=policy,
            clock=lambda: moments[0],
        )
        attempt = observer.before_attempt()
        observer.after_attempt(attempt, 10, complete=True)
        moments[0] += timedelta(hours=2)

        with pytest.raises(BudgetExhausted, match="daily_request_limit"):
            observer.before_attempt()


def test_later_phase_cannot_run_while_an_earlier_phase_is_active(tmp_path):
    data_dir, _ = _scheduler_store(tmp_path, tickers=("A",))
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        initialize_history_job(
            meta,
            job_id="phase-1-hourly",
            phase=1,
            dataset_key="intraday_1hour",
            tickers=["A"],
            start=start,
            end=end,
        )
        initialize_history_job(
            meta,
            job_id="phase-2-eod",
            phase=2,
            dataset_key="eod",
            tickers=["A"],
            start=start,
            end=end,
        )
        client = FakeIntraday()
        result = run_history_sweep(client, BarStore(data_dir), meta, "phase-2-eod")

        assert result.stop_reason == "phase_predecessor_active"
        assert client.calls == []
        assert meta.history_job("phase-2-eod")["cursor"] == 0


def test_ingestion_cycle_finishes_current_work_before_history(tmp_path):
    data_dir = tmp_path / "data"
    history_start, history_end = date(2024, 1, 1), date(2024, 12, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", history_start)
        for dataset_key in ("eod", "intraday_5min"):
            meta.add_vendor_identifier(
                "instrument-a",
                dataset_key,
                "ticker",
                "A",
                history_start,
                date.max,
                validation_state="validated",
            )
        initialize_history_job(
            meta,
            job_id="history-after-current",
            dataset_key="intraday_5min",
            tickers=["A"],
            start=history_start,
            end=history_end,
        )
        client = FakeIntraday()

        cycle = run_ingestion_cycle(
            client,
            BarStore(data_dir),
            meta,
            current_tickers=["A"],
            current_datasets=["eod"],
            history_job_id="history-after-current",
        )

        assert cycle.stop_reason is None
        assert cycle.current.ok
        assert cycle.history is not None
        assert client.events[:2] == ["eod", "intraday:5min"]
        assert [row["work_kind"] for row in meta.request_attempts()] == [
            "current",
            "historical",
        ]
