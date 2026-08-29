"""Ordered backfill-program orchestration tests (offline)."""

from datetime import date, timedelta

import pytest

from marketdata.backfill_program import (
    initialize_default_backfill_program,
    run_backfill_program_step,
)
from marketdata.identity_bootstrap import bootstrap_eod_identities
from marketdata.scheduler import (
    initialize_history_job,
    resolve_history_job,
    run_history_sweep,
)
from marketdata.store import BarStore, MetaStore


class FakeProgramClient:
    def __init__(self, records):
        self.records = records
        self.response_bytes = 0
        self.request_count = 0
        self.supported_calls = 0
        self.eod_calls = []
        self.intraday_calls = []

    def supported_tickers(self, tickers=None):
        self.supported_calls += 1
        if tickers is None:
            return list(self.records)
        requested = {ticker.upper() for ticker in tickers}
        return [row for row in self.records if row["ticker"] in requested]

    def ticker_metadata(self, ticker):
        row = next(row for row in self.records if row["ticker"] == ticker.upper())
        return {
            "ticker": ticker.upper(),
            "exchangeCode": row["exchange"],
            "startDate": row["startDate"],
            "endDate": row["endDate"],
            "name": f"{ticker.upper()} fixture",
        }

    def eod(self, ticker, start, end):
        self.eod_calls.append((ticker, start, end))
        return [
            {
                "date": f"{end.isoformat()}T00:00:00.000Z",
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

    def intraday(self, ticker, start, end, freq="1hour"):
        self.intraday_calls.append((ticker, start, end, freq))
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


def _archive(ticker, start, end):
    return {
        "ticker": ticker,
        "exchange": "NYSE",
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }


def _terminal_phase1_fixture(tmp_path):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    client = FakeProgramClient([_archive("A", start, end), _archive("B", start, end)])
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(2024, [{"ticker": "A", "rank": 1}])
        bootstrap_eod_identities(client, meta, ["A"])
        alias = meta.identity_aliases(["A"])[0]
        instrument_id = str(alias["instrument_id"])
        meta.add_vendor_identifier(
            instrument_id,
            "intraday_1hour",
            "ticker",
            "A",
            start,
            end,
            validation_state="validated",
        )
        meta.set_coverage(instrument_id, "eod", start, end)
        meta.set_coverage(instrument_id, "intraday_1hour", start, end)
        for job_id, dataset_key in (
            ("phase1-eod", "eod"),
            ("phase1-hourly", "intraday_1hour"),
        ):
            initialize_history_job(
                meta,
                job_id=job_id,
                phase=1,
                dataset_key=dataset_key,
                tickers=["A"],
                start=start,
                end=end,
            )
            result = run_history_sweep(client, BarStore(data_dir), meta, job_id)
            assert result.job_status == "complete"
        initialize_default_backfill_program(
            meta,
            program_id="test-program",
            phase1_eod_job_id="phase1-eod",
            phase1_hourly_job_id="phase1-hourly",
        )
    return data_dir, client


def test_default_program_rejects_a_phase1_canary_as_the_seed_predecessor(tmp_path):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(
            2024,
            [{"ticker": "A", "rank": 1}, {"ticker": "B", "rank": 2}],
        )
        meta.upsert_instrument("a")
        meta.add_instrument_alias("a", "A", start, end)
        for job_id, dataset_key in (
            ("canary-eod", "eod"),
            ("canary-hourly", "intraday_1hour"),
        ):
            initialize_history_job(
                meta,
                job_id=job_id,
                phase=1,
                dataset_key=dataset_key,
                tickers=["A"],
                start=start,
                end=end,
            )

        with pytest.raises(ValueError, match="different request"):
            initialize_default_backfill_program(
                meta,
                program_id="test-program",
                phase1_eod_job_id="canary-eod",
                phase1_hourly_job_id="canary-hourly",
            )


def test_program_freezes_supported_scope_and_advances_through_phase3(tmp_path):
    data_dir, client = _terminal_phase1_fixture(tmp_path)
    bars = BarStore(data_dir)

    with MetaStore(data_dir / "meta.db") as meta:
        frozen = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert (frozen.action, frozen.cohort_count) == ("scope_frozen", 2)
        assert (
            client.supported_calls == 2
        )  # Initial A bootstrap plus one full snapshot.

    # Reopening the database proves later identity batches use the frozen
    # supported archive rather than downloading a changed vendor scope.
    client.records.append(_archive("C", date(2024, 1, 1), date(2024, 1, 31)))
    with MetaStore(data_dir / "meta.db") as meta:
        first_identity = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        second_identity = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert (first_identity.identity_cursor, second_identity.identity_cursor) == (
            1,
            2,
        )
        assert first_identity.component_state == "preparing"
        assert second_identity.component_state == "pending"
        assert second_identity.action == "identity_prepared"
        assert client.supported_calls == 2
        assert meta.backfill_program_tickers(
            "test-program", "tiingo-supported-us-v1"
        ) == ["A", "B"]
        phase2_component = meta.backfill_program_component(
            "test-program", "phase2_all_eod"
        )
        assert phase2_component is not None
        with pytest.raises(ValueError, match="phase_program_request_mismatch"):
            resolve_history_job(
                meta,
                job_id=str(phase2_component["job_id"]),
                phase=2,
                dataset_key="eod",
                tickers=["A"],
                start=date.fromisoformat(str(phase2_component["range_start"])),
                end=date.fromisoformat(str(phase2_component["range_end"])),
            )

        phase2 = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert phase2.component_key == "phase2_all_eod"
        assert phase2.history is not None
        assert phase2.history.job_status == "complete"

        phase3_identity = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert phase3_identity.component_key == "phase3_seed_5min"
        assert phase3_identity.action == "identity_prepared"

        phase3 = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert phase3.history is not None
        assert phase3.history.job_status == "complete"
        assert phase3.program_status == "complete"

        complete = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=1
        )
        assert complete.action == "program_complete"


def test_unregistered_later_phase_cannot_treat_missing_predecessor_as_complete(
    tmp_path,
):
    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 1), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("a")
        meta.add_instrument_alias("a", "A", start, end)
        initialize_history_job(
            meta,
            job_id="orphan-phase3",
            phase=3,
            dataset_key="intraday_5min",
            tickers=["A"],
            start=start,
            end=end,
        )

        result = run_history_sweep(
            FakeProgramClient([]), BarStore(data_dir), meta, "orphan-phase3"
        )

        assert result.stop_reason == "phase_program_required"
        assert result.attempted_units == 0


def test_registered_later_phase_rejects_direct_wrong_cohort_job(tmp_path):
    data_dir, client = _terminal_phase1_fixture(tmp_path)
    bars = BarStore(data_dir)
    with MetaStore(data_dir / "meta.db") as meta:
        run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=10
        )
        prepared = run_backfill_program_step(
            client, bars, meta, program_id="test-program", identity_batch_size=10
        )
        assert prepared.action == "identity_prepared"
        component = meta.backfill_program_component("test-program", "phase2_all_eod")
        assert component is not None
        job_id = str(component["job_id"])
        initialize_history_job(
            meta,
            job_id=job_id,
            phase=2,
            dataset_key="eod",
            tickers=["A"],
            start=date.fromisoformat(str(component["range_start"])),
            end=date.fromisoformat(str(component["range_end"])),
        )

        result = run_history_sweep(client, bars, meta, job_id)

        assert result.stop_reason == "phase_program_request_mismatch"
        assert result.attempted_units == 0
