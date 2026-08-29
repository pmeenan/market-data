"""Durable request-budget and breadth-first scheduler tests (offline)."""

import json
import threading
from datetime import UTC, date, datetime, timedelta

import pytest

import marketdata.scheduler as scheduler_mod
from marketdata.backfill_program import sync_backfill_program
from marketdata.locking import LOCK_FILE_NAME
from marketdata.scheduler import (
    BudgetExhausted,
    BudgetPolicy,
    PersistentAttemptObserver,
    cancel_history_job,
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


def _register_two_phase_program(meta, start, end, phase1_job, phase2_job):
    meta.create_backfill_program(
        program_id="test-program",
        definition_hash="a" * 64,
        components=[
            {
                "component_key": "phase1",
                "component_ordinal": 10,
                "phase": 1,
                "dataset_key": "intraday_1hour",
                "scope_key": "seed",
                "start": start,
                "end": end,
                "job_id": phase1_job,
            },
            {
                "component_key": "phase2",
                "component_ordinal": 20,
                "phase": 2,
                "dataset_key": "eod",
                "scope_key": "all",
                "start": start,
                "end": end,
                "job_id": phase2_job,
            },
        ],
    )
    for scope_key in ("seed", "all"):
        meta.freeze_backfill_program_scope(
            program_id="test-program",
            scope_key=scope_key,
            source_kind="seed_universes",
            tickers=["A"],
        )
    meta.advance_backfill_program_identity(
        program_id="test-program",
        component_key="phase2",
        cursor=1,
        prepared=True,
        stop_reason=None,
    )


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


def test_history_sweep_holds_shared_lock_during_transport(tmp_path):
    data_dir, job_id = _scheduler_store(tmp_path, tickers=("A",))

    class LockInspectingClient(FakeIntraday):
        def intraday(self, ticker, start, end, freq="1hour"):
            holder = json.loads((data_dir / LOCK_FILE_NAME).read_text())
            assert holder["operation"].startswith("ingest:history-turn:")
            return super().intraday(ticker, start, end, freq)

    with MetaStore(data_dir / "meta.db") as meta:
        result = run_history_sweep(
            LockInspectingClient(), BarStore(data_dir), meta, job_id
        )

    assert result.successful_units == 1


def test_cancel_stops_a_running_sweep_after_its_current_turn(tmp_path):
    assert instrument_bucket("instrument-a") != instrument_bucket("instrument-b")
    data_dir, job_id = _scheduler_store(tmp_path, tickers=("A", "B"))
    started = threading.Event()
    release = threading.Event()
    outcome = []

    class PausedClient(FakeIntraday):
        def intraday(self, ticker, start, end, freq="1hour"):
            started.set()
            assert release.wait(timeout=5)
            return super().intraday(ticker, start, end, freq)

    client = PausedClient()

    def run() -> None:
        with MetaStore(data_dir / "meta.db") as meta:
            outcome.append(run_history_sweep(client, BarStore(data_dir), meta, job_id))

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(timeout=5)
    with MetaStore(data_dir / "meta.db") as meta:
        cancel_history_job(meta, job_id)
    release.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(client.calls) == 1
    assert outcome[0].job_status == "cancelled"
    with MetaStore(data_dir / "meta.db") as meta:
        job = meta.history_job(job_id)
        assert (job["status"], job["cancelled"]) == ("blocked", 1)
        with pytest.raises(ValueError, match="job is cancelled"):
            initialize_history_job(
                meta,
                job_id=job_id,
                dataset_key="intraday_5min",
                tickers=["A", "B"],
                start=date(2024, 1, 1),
                end=date(2024, 12, 31),
                retry_blocked=True,
            )


def test_history_sweep_yields_lock_between_turns_for_current_work(
    tmp_path, monkeypatch
):
    assert instrument_bucket("instrument-a") != instrument_bucket("instrument-b")
    data_dir, job_id = _scheduler_store(tmp_path, tickers=("A", "B"))
    turn_released = threading.Event()
    current_finished = threading.Event()
    history_outcome = []
    original_exit = scheduler_mod.DataDirectoryLock.__exit__
    yielded = False

    def yield_after_first_turn(lock, exc_type, exc, traceback):
        nonlocal yielded
        original_exit(lock, exc_type, exc, traceback)
        if lock.operation.startswith("ingest:history-turn:") and not yielded:
            yielded = True
            turn_released.set()
            assert current_finished.wait(timeout=5)

    monkeypatch.setattr(
        scheduler_mod.DataDirectoryLock, "__exit__", yield_after_first_turn
    )

    def run_history() -> None:
        with MetaStore(data_dir / "meta.db") as meta:
            history_outcome.append(
                run_history_sweep(FakeIntraday(), BarStore(data_dir), meta, job_id)
            )

    thread = threading.Thread(target=run_history)
    thread.start()
    assert turn_released.wait(timeout=5)
    try:
        with MetaStore(data_dir / "meta.db") as meta:
            current = run_ingestion_cycle(
                FakeIntraday(),
                BarStore(data_dir),
                meta,
                current_tickers=["A"],
                current_datasets=["intraday_5min"],
                history_job_id=None,
            )
    finally:
        current_finished.set()
    thread.join(timeout=5)

    assert current.ok
    assert not thread.is_alive()
    assert history_outcome[0].sweep_ended == 1


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


def test_disconnected_validated_range_is_a_terminal_job_blocker(tmp_path):
    data_dir = tmp_path / "data"
    alias_start, alias_end = date(2024, 1, 1), date(2024, 12, 31)
    range_end = date(2024, 6, 30)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias("instrument-a", "A", alias_start, alias_end)
        meta.add_vendor_identifier(
            "instrument-a",
            "eod",
            "ticker",
            "A",
            alias_start,
            alias_end,
            validation_state="validated",
        )
        # This later coverage cannot be bridged by a request confined to the
        # job range. Validated ingestion therefore rejects it before transport.
        meta.set_coverage("instrument-a", "eod", date(2024, 12, 1), alias_end)
        initialize_history_job(
            meta,
            job_id="disconnected-blocker",
            dataset_key="eod",
            tickers=["A"],
            start=alias_start,
            end=range_end,
        )
        client = FakeIntraday()

        first = run_history_sweep(
            client, BarStore(data_dir), meta, "disconnected-blocker"
        )
        second = run_history_sweep(
            client, BarStore(data_dir), meta, "disconnected-blocker"
        )

        history_range = meta.history_ranges("disconnected-blocker")[0]
        assert first.job_status == "blocked"
        assert first.ingest.blocked
        assert history_range["terminal_blocked"] == 1
        assert second.attempted_units == 0
        assert client.eod_calls == []


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
        _register_two_phase_program(meta, start, end, "blocked-phase-1", "phase-2")

        blocked = run_history_sweep(
            FakeIntraday(), BarStore(data_dir), meta, "blocked-phase-1"
        )
        sync_backfill_program(meta, "test-program")
        later = run_history_sweep(FakeIntraday(), BarStore(data_dir), meta, "phase-2")

        assert blocked.job_status == "blocked"
        assert later.stop_reason is None

        # Routine scheduler/timer invocations leave terminal exclusions
        # dormant instead of spending quota on the same evidence again.
        initialize_history_job(
            meta,
            job_id="blocked-phase-1",
            phase=1,
            dataset_key="intraday_1hour",
            tickers=["A"],
            start=start,
            end=end,
        )
        assert meta.history_job("blocked-phase-1")["status"] == "blocked"
        assert meta.history_ranges("blocked-phase-1")[0]["terminal_blocked"] == 1

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
            retry_blocked=True,
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
    now = datetime(2026, 8, 20, 12, tzinfo=UTC)
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


def test_historical_limit_ramps_over_final_seven_calendar_days():
    policy = scheduler_mod.DEFAULT_BUDGET_POLICY

    expected = {
        24: 30_000_000_000,
        25: 30_000_000_000,
        26: 31_500_000_000,
        27: 33_000_000_000,
        28: 34_500_000_000,
        29: 36_000_000_000,
        30: 37_500_000_000,
        31: 39_000_000_000,
    }
    assert {
        day: policy.historical_total_byte_limit(datetime(2026, 8, day, 12, tzinfo=UTC))
        for day in expected
    } == expected
    assert (
        policy.historical_total_byte_limit(datetime(2027, 2, 21, 12, tzinfo=UTC))
        == 30_000_000_000
    )
    assert (
        policy.historical_total_byte_limit(datetime(2027, 2, 22, 12, tzinfo=UTC))
        == 30_000_000_000
    )
    assert (
        policy.historical_total_byte_limit(datetime(2027, 2, 28, 12, tzinfo=UTC))
        == 39_000_000_000
    )

    fixture_policy = BudgetPolicy(
        total_byte_limit=40,
        historical_byte_limit=30,
        response_reservation_bytes=1,
    )
    assert (
        fixture_policy.historical_total_byte_limit(
            datetime(2026, 8, 31, 12, tzinfo=UTC)
        )
        == 30
    )


def test_late_month_history_uses_only_unused_total_headroom(tmp_path):
    moments = [datetime(2026, 8, 24, 12, tzinfo=UTC)]
    policy = BudgetPolicy(
        hourly_request_limit=100,
        daily_request_limit=100,
        total_byte_limit=40,
        historical_byte_limit=30,
        historical_byte_limit_max=39,
        rolling_days=32,
        response_reservation_bytes=1,
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        current = PersistentAttemptObserver(
            meta,
            work_kind="current",
            operation="current-reserve",
            policy=policy,
            clock=lambda: moments[0],
        )
        historical = PersistentAttemptObserver(
            meta,
            work_kind="historical",
            operation="history-ramp",
            policy=policy,
            clock=lambda: moments[0],
        )
        for _ in range(5):
            attempt = current.before_attempt()
            current.after_attempt(attempt, 1, complete=True)
        for _ in range(25):
            attempt = historical.before_attempt()
            historical.after_attempt(attempt, 1, complete=True)
        with pytest.raises(BudgetExhausted, match="historical_byte_limit"):
            historical.before_attempt()

        moments[0] = datetime(2026, 8, 31, 12, tzinfo=UTC)
        for _ in range(9):
            attempt = historical.before_attempt()
            historical.after_attempt(attempt, 1, complete=True)
        with pytest.raises(BudgetExhausted, match="historical_byte_limit"):
            historical.before_attempt()

        final_current = current.before_attempt()
        current.after_attempt(final_current, 1, complete=True)
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
        _register_two_phase_program(meta, start, end, "phase-1-hourly", "phase-2-eod")
        sync_backfill_program(meta, "test-program")
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
