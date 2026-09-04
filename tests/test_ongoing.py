"""Durable overnight ongoing-collection tests (offline)."""

from datetime import date, timedelta

from marketdata.calendar import session_schedule
from marketdata.identity_bootstrap import bootstrap_eod_identities
from marketdata.ongoing import (
    _run_dataset_step,
    initialize_ongoing_program,
    run_ongoing_program_step,
)
from marketdata.scheduler import (
    DEFAULT_BUDGET_POLICY,
    CurrentJobMember,
    initialize_current_job,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import eod_frame
from marketdata.tiingo import TiingoError


class FakeOngoingClient:
    def __init__(self, records):
        self.records = records
        self.response_bytes = 0
        self.request_count = 0
        self.eod_calls = []
        self.intraday_calls = []

    def supported_tickers(self, tickers=None):
        if tickers is None:
            return list(self.records)
        requested = {ticker.upper() for ticker in tickers}
        return [row for row in self.records if row["ticker"] in requested]

    def ticker_metadata(self, ticker):
        row = next(row for row in self.records if row["ticker"] == ticker.upper())
        self.request_count += 1
        return {
            "ticker": ticker.upper(),
            "exchangeCode": row["exchange"],
            "startDate": row["startDate"],
            "endDate": row["endDate"],
            "name": f"{ticker.upper()} fixture",
        }

    def eod(self, ticker, start, end):
        self.request_count += 1
        self.eod_calls.append((ticker, start, end))
        sessions = session_schedule(start, end)["session_date"].to_list()
        return [_eod_row(day, 1_000 if ticker == "A" else 100) for day in sessions]

    def intraday(self, ticker, start, end, freq="1hour"):
        self.request_count += 1
        self.intraday_calls.append((ticker, start, end, freq))
        return [
            {
                "date": f"{day.isoformat()}T15:00:00.000Z",
                "open": 10.0,
                "high": 10.5,
                "low": 9.5,
                "close": 10.2,
                "volume": 100,
            }
            for day in session_schedule(start, end)["session_date"].to_list()
        ]


def _archive(ticker):
    return {
        "ticker": ticker,
        "exchange": "NYSE",
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": "2020-01-01",
        "endDate": "2099-12-31",
    }


def _eod_row(day, volume):
    return {
        "date": f"{day.isoformat()}T00:00:00.000Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": volume,
        "adjOpen": 100.0,
        "adjHigh": 101.0,
        "adjLow": 99.0,
        "adjClose": 100.0,
        "adjVolume": volume,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def _run_cycle(client, bars, meta, session):
    results = []
    for _ in range(100):
        result = run_ongoing_program_step(
            client,
            bars,
            meta,
            program_id="test-ongoing",
            session_date=session,
            identity_batch_size=10,
            max_units=10,
        )
        results.append(result)
        if result.terminal and result.session_date == session:
            return results
    raise AssertionError("ongoing cycle did not reach a terminal state")


def test_ongoing_program_bridges_gaps_ranks_monthly_and_fills_both_intraday(tmp_path):
    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    client = FakeOngoingClient([_archive("A"), _archive("B")])
    first_session = date(2024, 1, 31)
    ranking_sessions = session_schedule(
        first_session - timedelta(days=45), first_session
    )["session_date"].to_list()[-20:]

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        bootstrap_eod_identities(client, meta, ["A", "B"])
        identities = {
            str(row["ticker"]): str(row["instrument_id"])
            for row in meta.identity_aliases(["A", "B"])
        }
        for ticker, instrument_id in identities.items():
            for dataset_key in ("intraday_1hour", "intraday_5min"):
                meta.add_vendor_identifier(
                    instrument_id,
                    dataset_key,
                    "ticker",
                    ticker,
                    date(2020, 1, 1),
                    date(2099, 12, 31),
                    validation_state="validated",
                )
            initial_rows = [
                _eod_row(day, 1_000 if ticker == "A" else 100)
                for day in ranking_sessions[:-1]
            ]
            bars.publish_eod({instrument_id: eod_frame(ticker, initial_rows)})
            meta.set_coverage(
                instrument_id, "eod", ranking_sessions[0], ranking_sessions[-2]
            )
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=first_session,
            cohort_size=1,
            lookback_sessions=20,
            min_observations=15,
        )

        first_results = _run_cycle(client, bars, meta, first_session)

        cycle = meta.ongoing_cycle("test-ongoing", first_session)
        assert cycle["state"] == "complete"
        snapshot_id = str(cycle["cohort_snapshot_id"])
        members = meta.ongoing_cohort_members(snapshot_id)
        assert [(row["ticker"], row["rank"]) for row in members] == [("A", 1)]
        assert meta.get_coverage(identities["A"], "eod")[1] == first_session
        assert any(
            call[0] == "A"
            and call[1] <= ranking_sessions[-2]
            and call[2] == first_session
            for call in client.eod_calls
        )
        assert {(call[0], call[3]) for call in client.intraday_calls} == {
            ("A", "1hour"),
            ("A", "5min"),
        }
        assert any(result.action == "cohort_selected" for result in first_results)

        second_session = date(2024, 2, 1)
        second_results = _run_cycle(client, bars, meta, second_session)
        second_cycle = meta.ongoing_cycle("test-ongoing", second_session)
        assert second_cycle["state"] == "complete"
        assert second_cycle["cohort_snapshot_id"] != snapshot_id
        assert any(result.action == "cohort_selected" for result in second_results)


def test_same_month_cycles_reuse_the_accepted_cohort_snapshot(tmp_path):
    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    client = FakeOngoingClient([_archive("A")])
    first_session = date(2024, 1, 30)
    sessions = session_schedule(first_session - timedelta(days=45), first_session)[
        "session_date"
    ].to_list()[-20:]

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        bootstrap_eod_identities(client, meta, ["A"])
        alias = meta.identity_aliases(["A"])[0]
        instrument_id = str(alias["instrument_id"])
        for dataset_key in ("intraday_1hour", "intraday_5min"):
            meta.add_vendor_identifier(
                instrument_id,
                dataset_key,
                "ticker",
                "A",
                date(2020, 1, 1),
                date(2099, 12, 31),
                validation_state="validated",
            )
        bars.publish_eod(
            {instrument_id: eod_frame("A", [_eod_row(day, 1_000) for day in sessions])}
        )
        meta.set_coverage(instrument_id, "eod", sessions[0], sessions[-1])
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=first_session,
            cohort_size=1,
            lookback_sessions=20,
            min_observations=15,
        )

        _run_cycle(client, bars, meta, first_session)
        first_snapshot = meta.ongoing_cycle("test-ongoing", first_session)[
            "cohort_snapshot_id"
        ]
        second_session = date(2024, 1, 31)
        _run_cycle(client, bars, meta, second_session)
        second_snapshot = meta.ongoing_cycle("test-ongoing", second_session)[
            "cohort_snapshot_id"
        ]

        assert second_snapshot == first_snapshot


def test_terminal_static_blockers_are_reported_as_cycle_exclusions(tmp_path):
    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    client = FakeOngoingClient([_archive("A")])
    session = date(2024, 1, 31)

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("missing-alias-id")
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=session,
            cohort_size=1,
            lookback_sessions=20,
            min_observations=15,
        )
        supported = meta.create_ongoing_supported_snapshot(
            as_of_session=session,
            records=[_archive("A")],
        )
        cycle = meta.create_ongoing_cycle(
            program_id="test-ongoing",
            session_date=session,
            supported_snapshot_id=str(supported["snapshot_id"]),
            eod_job_id="test-eod",
            hourly_job_id="test-hourly",
            five_min_job_id="test-5min",
        )
        for job_id, dataset_key in (
            ("test-eod", "eod"),
            ("test-hourly", "intraday_1hour"),
        ):
            initialize_current_job(
                meta,
                job_id=job_id,
                dataset_key=dataset_key,
                members=[CurrentJobMember("MISSING", "missing-alias-id")],
                end=session,
                default_start=session,
                refresh_overlap_days=7,
            )
        meta.update_ongoing_cycle("test-ongoing", session, state="five_min")
        cycle = meta.ongoing_cycle("test-ongoing", session)
        assert cycle is not None

        result = _run_dataset_step(
            client,
            bars,
            meta,
            cycle,
            dataset_key="intraday_5min",
            members=[CurrentJobMember("MISSING", "missing-alias-id")],
            default_start=session,
            next_state="complete",
            max_units=1,
            policy=DEFAULT_BUDGET_POLICY,
        )

        assert result.cycle_state == "complete_with_exclusions"
        assert result.partial


def test_retrying_eod_target_does_not_hold_healthy_intraday_work(tmp_path):
    class OneFailingEod(FakeOngoingClient):
        def eod(self, ticker, start, end):
            if ticker == "B":
                self.request_count += 1
                self.eod_calls.append((ticker, start, end))
                raise TiingoError("simulated delayed EOD identity")
            return super().eod(ticker, start, end)

    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    client = OneFailingEod([_archive("A"), _archive("B")])
    session = date(2024, 1, 31)
    ranking_sessions = session_schedule(session - timedelta(days=45), session)[
        "session_date"
    ].to_list()[-20:]

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        bootstrap_eod_identities(client, meta, ["A", "B"])
        identities = {
            str(row["ticker"]): str(row["instrument_id"])
            for row in meta.identity_aliases(["A", "B"])
        }
        for ticker, instrument_id in identities.items():
            for dataset_key in ("intraday_1hour", "intraday_5min"):
                meta.add_vendor_identifier(
                    instrument_id,
                    dataset_key,
                    "ticker",
                    ticker,
                    date(2020, 1, 1),
                    date(2099, 12, 31),
                    validation_state="validated",
                )
            initial_rows = [
                _eod_row(day, 1_000 if ticker == "A" else 100)
                for day in ranking_sessions[:-1]
            ]
            bars.publish_eod({instrument_id: eod_frame(ticker, initial_rows)})
            meta.set_coverage(
                instrument_id, "eod", ranking_sessions[0], ranking_sessions[-2]
            )
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=session,
            cohort_size=1,
            lookback_sessions=20,
            min_observations=15,
        )

        results = _run_cycle(client, bars, meta, session)

        cycle = meta.ongoing_cycle("test-ongoing", session)
        assert cycle["state"] == "complete_with_exclusions"
        members = meta.ongoing_cohort_members(str(cycle["cohort_snapshot_id"]))
        assert [row["ticker"] for row in members] == ["A"]
        actions = [result.action for result in results]
        assert actions.index("deferred_retry_sweep") > next(
            index
            for index, result in enumerate(results)
            if result.dataset_key == "intraday_5min"
            and result.action == "dataset_sweep"
        )
        assert {(call[0], call[3]) for call in client.intraday_calls} == {
            ("A", "1hour"),
            ("A", "5min"),
        }
        assert len([call for call in client.eod_calls if call[0] == "B"]) == 40
        terminal_eod = next(
            result
            for result in reversed(results)
            if result.dataset_key == "eod"
            and result.sweep is not None
            and result.sweep.job_status == "blocked"
        )
        assert terminal_eod.sweep is not None
        assert terminal_eod.sweep.ingest.blocked
        assert not terminal_eod.sweep.ingest.failed


def test_cancelled_dataset_is_a_terminal_cycle_exclusion_not_a_permanent_stall(
    tmp_path,
):
    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    client = FakeOngoingClient([_archive("A")])
    session = date(2024, 1, 31)

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("instrument-a")
        meta.add_instrument_alias(
            "instrument-a", "A", date(2020, 1, 1), date(2099, 12, 31)
        )
        meta.add_vendor_identifier(
            "instrument-a",
            "eod",
            "ticker",
            "A",
            date(2020, 1, 1),
            date(2099, 12, 31),
            validation_state="validated",
        )
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=session,
            cohort_size=1,
            lookback_sessions=20,
            min_observations=15,
        )
        supported = meta.create_ongoing_supported_snapshot(
            as_of_session=session,
            records=[_archive("A")],
        )
        cycle = meta.create_ongoing_cycle(
            program_id="test-ongoing",
            session_date=session,
            supported_snapshot_id=str(supported["snapshot_id"]),
            eod_job_id="test-eod",
            hourly_job_id="test-hourly",
            five_min_job_id="test-5min",
        )
        meta.update_ongoing_cycle("test-ongoing", session, state="eod")
        initialize_current_job(
            meta,
            job_id="test-eod",
            dataset_key="eod",
            members=[CurrentJobMember("A", "instrument-a")],
            end=session,
            default_start=session,
            refresh_overlap_days=7,
        )
        meta.cancel_history_job("test-eod")
        cycle = meta.ongoing_cycle("test-ongoing", session)
        assert cycle is not None

        result = _run_dataset_step(
            client,
            bars,
            meta,
            cycle,
            dataset_key="eod",
            members=[CurrentJobMember("A", "instrument-a")],
            default_start=session,
            next_state="cohort",
            max_units=1,
            policy=DEFAULT_BUDGET_POLICY,
        )

        assert result.cycle_state == "cohort"
        assert result.partial
        assert result.jobs["eod"]["status"] == "cancelled"
        assert result.jobs["eod"]["has_exclusions"]
