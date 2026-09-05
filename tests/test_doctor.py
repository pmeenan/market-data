"""Bounded operator health checks (offline)."""

from datetime import UTC, date, datetime

from click.testing import CliRunner

from marketdata.cli import main
from marketdata.config import Config
from marketdata.doctor import check_health
from marketdata.ongoing import initialize_ongoing_program
from marketdata.scheduler import CurrentJobMember, initialize_current_job
from marketdata.store import MetaStore

_SESSION = date(2026, 9, 2)
_NOW = datetime(2026, 9, 5, 14, tzinfo=UTC)


def _archive(ticker: str) -> dict[str, str]:
    return {
        "ticker": ticker,
        "exchange": "NYSE",
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": "2020-01-02",
        "endDate": _SESSION.isoformat(),
    }


def _fixture(tmp_path) -> Config:
    config = Config(tmp_path / "data", None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, ticker in (("a-id", "AAA"), ("b-id", "BBB")):
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(instrument_id, ticker, date(2020, 1, 2), _SESSION)
            for dataset_key in ("eod", "intraday_1hour", "intraday_5min"):
                meta.add_vendor_identifier(
                    instrument_id,
                    dataset_key,
                    "ticker",
                    ticker,
                    date(2020, 1, 2),
                    _SESSION,
                    validation_state="validated",
                )
        # BBB is current everywhere; AAA (the top-ranked name) is stale.
        for dataset_key in ("eod", "intraday_1hour", "intraday_5min"):
            meta.set_coverage("b-id", dataset_key, date(2020, 1, 2), _SESSION)
            meta.set_coverage("a-id", dataset_key, date(2020, 1, 2), date(2026, 8, 27))
        initialize_ongoing_program(
            meta,
            program_id="test-ongoing",
            initial_session=_SESSION,
            cohort_size=2,
            lookback_sessions=20,
            min_observations=15,
        )
        supported = meta.create_ongoing_supported_snapshot(
            as_of_session=_SESSION,
            records=[_archive("AAA"), _archive("BBB"), _archive("META")],
        )
        cohort = meta.create_ongoing_cohort_snapshot(
            program_id="test-ongoing",
            as_of_session=_SESSION,
            lookback_start=date(2026, 8, 6),
            lookback_end=_SESSION,
            cohort_size=2,
            min_observations=15,
            members=[
                {
                    "rank": 1,
                    "instrument_id": "a-id",
                    "ticker": "AAA",
                    "avg_dollar_volume": 2e9,
                    "observation_count": 20,
                },
                {
                    "rank": 2,
                    "instrument_id": "b-id",
                    "ticker": "BBB",
                    "avg_dollar_volume": 1e9,
                    "observation_count": 20,
                },
            ],
        )
        meta.create_ongoing_cycle(
            program_id="test-ongoing",
            session_date=_SESSION,
            supported_snapshot_id=str(supported["snapshot_id"]),
            eod_job_id="test-eod",
            hourly_job_id="test-hourly",
            five_min_job_id="test-5min",
        )
        meta.update_ongoing_cycle(
            "test-ongoing",
            _SESSION,
            state="hourly",
            cohort_snapshot_id=str(cohort["snapshot_id"]),
        )
        members = [CurrentJobMember("AAA", "a-id"), CurrentJobMember("BBB", "b-id")]
        for job_id, dataset_key in (
            ("test-eod", "eod"),
            ("test-hourly", "intraday_1hour"),
        ):
            initialize_current_job(
                meta,
                job_id=job_id,
                dataset_key=dataset_key,
                members=[*members, CurrentJobMember("META", None)],
                end=_SESSION,
                default_start=_SESSION,
                refresh_overlap_days=7,
            )
        # AAA's hourly target has retried repeatedly without any progress.
        target = next(
            row
            for row in meta.history_targets("test-hourly")
            if row["instrument_id"] == "a-id"
        )
        for turn in range(6):
            meta.checkpoint_history_turn(
                job_id="test-hourly",
                target_ordinal=int(target["target_ordinal"]),
                range_ordinal=0,
                frontier_end=_SESSION,
                range_status="active",
                attempt_status="current_retry_pending",
                detail="completed current request did not establish coverage",
                attempted=True,
                successful=False,
                terminal_blocked=False,
                cursor=0,
                sweep=turn,
                job_status="active",
            )
    return config


def test_health_report_flags_stuck_retries_stale_top_ranks_and_unresolved(tmp_path):
    config = _fixture(tmp_path)

    report = check_health(config, now=_NOW)
    by_check = {}
    for finding in report.findings:
        by_check.setdefault(finding.check, []).append(finding)

    assert report.target_session == _SESSION
    assert not report.ok
    assert by_check["request_rate"][0].severity == "ok"

    progress = by_check["ongoing_progress"]
    assert [f.severity for f in progress] == ["error"]  # 1 of 2 targets stuck
    assert progress[0].details["stuck"] == 1
    assert progress[0].details["sample"] == ["AAA"]
    assert "spends requests" in progress[0].message

    exclusions = by_check["ongoing_exclusions"]
    assert {f.details["job_id"] for f in exclusions} == {"test-eod", "test-hourly"}
    assert exclusions[0].details["sample"] == ["META"]

    freshness = {f.details["dataset_key"]: f for f in by_check["coverage_freshness"]}
    assert freshness["eod"].severity == "error"
    assert freshness["eod"].details["stale_in_top_ranks"] == 1
    assert freshness["eod"].details["sample"][0]["ticker"] == "AAA"
    assert freshness["eod"].details["sample"][0]["coverage_end"] == "2026-08-27"

    unresolved = by_check["unresolved_listings"][0]
    assert unresolved.severity == "warning"
    assert unresolved.details["sample"] == ["META"]
    assert unresolved.details["listings"] == 3


def test_health_report_is_clean_when_everything_is_current(tmp_path):
    config = _fixture(tmp_path)
    with MetaStore(config.meta_path) as meta:
        for dataset_key in ("eod", "intraday_1hour", "intraday_5min"):
            meta.set_coverage("a-id", dataset_key, date(2020, 1, 2), _SESSION)
        meta.upsert_instrument("meta-id")
        meta.add_instrument_alias("meta-id", "META", date(2012, 5, 18), _SESSION)

    report = check_health(config, now=_NOW, stuck_retry_turns=7)

    checks = {f.check: f.severity for f in report.findings}
    assert checks["coverage_freshness"] == "ok"
    assert checks["unresolved_listings"] == "ok"
    assert "ongoing_progress" not in checks
    assert report.ok
    assert report.to_dict()["counts"]["error"] == 0


def test_doctor_cli_exits_nonzero_on_errors_and_writes_summary(tmp_path):
    config = _fixture(tmp_path)
    summary = tmp_path / "health.json"

    result = CliRunner().invoke(
        main,
        ["--data-dir", str(config.data_dir), "doctor", "--summary-json", str(summary)],
    )

    assert result.exit_code == 1, result.output
    assert "[ERROR  ] coverage_freshness: eod" in result.output
    assert "sample: #1 AAA" in result.output
    assert "Result: " in result.output
    assert summary.exists()


def test_doctor_cli_is_clean_on_an_empty_warehouse(tmp_path):
    config = Config(tmp_path / "data", None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()

    result = CliRunner().invoke(main, ["--data-dir", str(config.data_dir), "doctor"])

    assert result.exit_code == 0, result.output
    assert "no ongoing collection program" in result.output
