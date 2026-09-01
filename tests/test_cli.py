"""CLI behavior tests (offline; the Tiingo client is faked)."""

import json
import subprocess
import sys
from contextlib import contextmanager
from datetime import date

from click.testing import CliRunner
from test_ingest import FakeTiingo, eod_row, weekdays

import marketdata.cli as cli_mod
from marketdata.cli import main


def _fake_client(history, fail=frozenset()):
    client = FakeTiingo(history, fail=fail)
    return lambda config: client


def test_backfill_help_makes_terminal_retries_explicit():
    runner = CliRunner()

    eod = runner.invoke(main, ["backfill", "eod", "--help"])
    intraday = runner.invoke(main, ["backfill", "intraday", "--help"])

    assert eod.exit_code == intraday.exit_code == 0
    assert "--retry-blocked" in eod.output
    assert "--retry-blocked" in intraday.output


def test_backfill_program_commands_expose_bounded_turn_controls():
    runner = CliRunner()

    step = runner.invoke(main, ["backfill", "program-step", "--help"])
    initialize = runner.invoke(main, ["backfill", "program-init", "--help"])

    assert step.exit_code == initialize.exit_code == 0
    assert "--identity-batch-size" in step.output
    assert "--max-units" in step.output
    assert "--status-json" in step.output
    assert "--phase1-eod-job-id" in initialize.output


def test_backfill_program_status_is_read_only_under_mutation_lock(tmp_path):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.create_backfill_program(
            program_id="status-test",
            definition_hash="a" * 64,
            components=[
                {
                    "component_key": "phase1",
                    "component_ordinal": 10,
                    "phase": 1,
                    "dataset_key": "eod",
                    "scope_key": "seed",
                    "start": date(2024, 1, 1),
                    "end": date(2024, 1, 31),
                    "job_id": "status-phase1",
                }
            ],
        )
        meta.freeze_backfill_program_scope(
            program_id="status-test",
            scope_key="seed",
            source_kind="seed_universes",
            tickers=["AAPL"],
        )
        before = (
            meta.backfill_program("status-test")["updated_at"],
            meta.backfill_program_component("status-test", "phase1")["updated_at"],
        )

    with _external_lock(data_dir):
        result = CliRunner().invoke(
            main,
            [
                "--data-dir",
                str(data_dir),
                "backfill",
                "program-status",
                "--program-id",
                "status-test",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Backfill program status-test: active" in result.output
    assert "identity=pending 0/1" in result.output
    with MetaStore(data_dir / "meta.db") as meta:
        after = (
            meta.backfill_program("status-test")["updated_at"],
            meta.backfill_program_component("status-test", "phase1")["updated_at"],
        )
    assert after == before


def test_status_headroom_includes_next_request_reservation(tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from marketdata.scheduler import BudgetPolicy, PersistentAttemptObserver
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    policy = BudgetPolicy(
        total_byte_limit=400,
        historical_byte_limit=300,
        response_reservation_bytes=100,
    )
    now = datetime.now(UTC)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        observer = PersistentAttemptObserver(
            meta,
            work_kind="current",
            operation="status-fixture",
            policy=policy,
            clock=lambda: now,
        )
        attempt = observer.before_attempt()
        observer.after_attempt(attempt, 250, complete=True)
    monkeypatch.setattr(cli_mod, "DEFAULT_BUDGET_POLICY", policy)

    result = CliRunner().invoke(main, ["--data-dir", str(data_dir), "status"])

    assert result.exit_code == 0, result.output
    assert "0 bytes available after the next 100-byte reservation" in result.output


def test_research_reconcile_cli_is_dry_run_unless_apply_is_explicit(tmp_path):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.create_research_run(
            run_id="abandoned",
            study_name="fixture-study",
            study_schema_version=1,
            parameters={},
        )
    runner = CliRunner()

    dry_run = runner.invoke(main, ["--data-dir", str(data_dir), "research-reconcile"])

    assert dry_run.exit_code == 1
    assert "1 abandoned running rows" in dry_run.output
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.research_run("abandoned")["status"] == "running"

    applied = runner.invoke(
        main, ["--data-dir", str(data_dir), "research-reconcile", "--apply"]
    )

    assert applied.exit_code == 0, applied.output
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.research_run("abandoned")["status"] == "failed"


def test_research_run_cli_dispatches_json_to_the_library_boundary(
    tmp_path, monkeypatch
):
    from marketdata.research import PublishedResearchRun
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
    seen = {}

    def run(config, study_name, parameters):
        seen.update(
            data_dir=config.data_dir,
            study_name=study_name,
            parameters=parameters,
        )
        return PublishedResearchRun(
            run_id="fixture-run",
            study_name=study_name,
            study_schema_version=1,
            input_fingerprint="a" * 64,
            observation_count=7,
            observation_path=data_dir / "observations.parquet",
            manifest_path=data_dir / "input_files.parquet",
        )

    monkeypatch.setattr(cli_mod, "run_registered_event_study", run)

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "research-run",
            "gap-recovery",
            "--parameters-json",
            '{"gap_threshold":-0.05,"enabled":true}',
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen == {
        "data_dir": data_dir,
        "study_name": "gap-recovery",
        "parameters": {"gap_threshold": -0.05, "enabled": True},
    }
    assert "7 observations" in result.output
    assert "no portfolio or order simulation" in result.output


def test_research_run_cli_rejects_non_object_parameters(tmp_path):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "research-run",
            "gap-recovery",
            "--parameters-json",
            "[]",
        ],
    )

    assert result.exit_code == 1
    assert "must decode to a JSON object" in result.output


@contextmanager
def _external_lock(data_dir):
    script = """
import sys
from pathlib import Path
from marketdata.locking import DataDirectoryLock

with DataDirectoryLock(Path(sys.argv[1]), operation="test-external-holder"):
    print("ready", flush=True)
    sys.stdin.read(1)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(data_dir)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "ready"
    try:
        yield
    finally:
        _stdout, stderr = process.communicate("\n", timeout=5)
        assert process.returncode == 0, stderr


def test_v2_backfill_ingests_validated_segment_and_writes_report(tmp_path, monkeypatch):
    from marketdata.store import MetaStore

    start, end = date(2024, 1, 1), date(2024, 1, 5)
    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias("apple-id", "AAPL", start, end)
        meta.add_vendor_identifier(
            "apple-id",
            "eod",
            "ticker",
            "AAPL",
            start,
            end,
            validation_state="validated",
        )
    client = FakeTiingo({"AAPL": [eod_row(date(2024, 1, 2))]})
    monkeypatch.setattr(cli_mod, "_client", lambda config: client)
    summary = tmp_path / "reports" / "summary.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "backfill",
            "eod",
            "-t",
            "AAPL",
            "--start",
            str(start),
            "--end",
            str(end),
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 fetched" in result.output
    assert '"status": "fetched"' in summary.read_text()
    payload = json.loads(summary.read_text())
    assert payload["ok"] is True
    assert payload["scheduler"]["job_status"] == "complete"


def test_invalid_range_is_click_error_and_writes_summary_atomically(tmp_path):
    summary = tmp_path / "missing" / "summary.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(tmp_path / "data"),
            "backfill",
            "eod",
            "-t",
            "AAPL",
            "--start",
            "2024-06-01",
            "--end",
            "2024-01-01",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 1
    assert "Error: --start must not be after --end" in result.output
    assert "Traceback" not in result.output
    assert '"ok": false' in summary.read_text()
    assert not summary.with_suffix(".json.tmp").exists()


def test_identity_bootstrap_command_validates_safe_universe_record(
    tmp_path, monkeypatch
):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    archive = {
        "ticker": "SAFE",
        "exchange": "NASDAQ",
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": "2020-01-02",
        "endDate": "2026-08-27",
    }

    class IdentityClient:
        response_bytes = 0

        def supported_tickers(self, tickers=None):
            holder = json.loads((data_dir / ".market-data.lock").read_text())
            assert holder["operation"] == "identity:bootstrap-eod"
            return [archive]

        def ticker_metadata(self, ticker):
            assert ticker == "SAFE"
            return {
                "ticker": "SAFE",
                "exchangeCode": "NASDAQ",
                "startDate": "2020-01-02",
                "endDate": "2026-08-27",
                "name": "Safe Incorporated",
                "description": "fixture",
            }

    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(2024, [{"ticker": "SAFE", "rank": 1}])
    monkeypatch.setattr(cli_mod, "_client", lambda config: IdentityClient())
    summary = tmp_path / "identity.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "identity",
            "bootstrap-eod",
            "--universe",
            "2024",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 validated" in result.output
    assert json.loads(summary.read_text())["validated"] == ["SAFE"]
    with MetaStore(data_dir / "meta.db") as meta:
        assert len(meta.instrument_ids()) == 1
        assert (
            meta._con.execute(
                "SELECT status FROM universe_resolutions WHERE year = 2024"
            ).fetchone()["status"]
            == "resolved"
        )


def test_intraday_identity_bootstrap_command_records_exact_frequency(
    tmp_path, monkeypatch
):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    start, end = date(2024, 1, 2), date(2024, 1, 31)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias("apple-id", "AAPL", start, end)
        meta.add_vendor_identifier(
            "apple-id",
            "eod",
            "ticker",
            "AAPL",
            start,
            end,
            validation_state="validated",
        )
    client = FakeTiingo({})
    monkeypatch.setattr(cli_mod, "_client", lambda config: client)
    summary = tmp_path / "intraday-identity.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "identity",
            "bootstrap-intraday",
            "-t",
            "AAPL",
            "--start",
            str(start),
            "--end",
            str(end),
            "--freq",
            "1hour",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(summary.read_text())
    assert payload["dataset_key"] == "intraday_1hour"
    assert payload["probe_attempts"] == 1
    assert len(payload["validated"]) == 1
    with MetaStore(data_dir / "meta.db") as meta:
        assert (
            meta.resolve_vendor_identifier(
                "apple-id", "intraday_1hour", start, end
            ).status
            == "resolved"
        )
        assert (
            meta.resolve_vendor_identifier(
                "apple-id", "intraday_5min", start, end
            ).status
            == "zero_matches"
        )


def test_intraday_identity_bootstrap_rejects_oversized_probe_before_work(tmp_path):
    summary = tmp_path / "intraday-identity.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(tmp_path / "missing-data"),
            "identity",
            "bootstrap-intraday",
            "-t",
            "AAPL",
            "--start",
            "2020-01-02",
            "--end",
            "2026-08-27",
            "--freq",
            "5min",
            "--probe-sessions",
            "130",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 1
    assert "must not exceed 127" in result.output
    assert "does not exist" not in result.output
    assert "must not exceed 127" in json.loads(summary.read_text())["error"]


def test_identity_bootstrap_fails_before_mutation_when_data_lock_is_held(
    tmp_path, monkeypatch
):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(2024, [{"ticker": "SAFE", "rank": 1}])

    class IdentityClient:
        def supported_tickers(self, tickers=None):
            raise AssertionError("bootstrap transport started under contention")

    monkeypatch.setattr(cli_mod, "_client", lambda config: IdentityClient())
    summary = tmp_path / "identity-contended.json"

    with _external_lock(data_dir):
        result = CliRunner().invoke(
            main,
            [
                "--data-dir",
                str(data_dir),
                "identity",
                "bootstrap-eod",
                "--universe",
                "2024",
                "--summary-json",
                str(summary),
            ],
        )

    assert result.exit_code == 2
    assert "data directory is busy" in result.output
    assert json.loads(summary.read_text())["ok"] is False
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.instrument_ids() == set()
        assert meta.request_attempts() == []


def test_blank_ticker_is_click_error_before_client_or_write(tmp_path, monkeypatch):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
    monkeypatch.setattr(
        cli_mod,
        "_client",
        lambda config: (_ for _ in ()).throw(AssertionError("client called")),
    )
    summary = tmp_path / "nested" / "summary.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "backfill",
            "eod",
            "-t",
            " ",
            "--start",
            "2024-01-01",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 1
    assert "Error: --tickers values must not be blank" in result.output
    assert "Traceback" not in result.output
    assert summary.exists()


def test_v1_backfill_requires_migration_first(tmp_path, monkeypatch):
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    monkeypatch.setattr(cli_mod, "_client", _fake_client(history))

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--data-dir",
            str(tmp_path / "data"),
            "backfill",
            "eod",
            "-t",
            "AAPL",
            "--start",
            "2024-01-01",
            "--end",
            "2024-06-28",
        ],
    )
    assert result.exit_code == 1
    assert "migrate the warehouse to v2 first" in result.output


def test_intraday_rejects_unsupported_freq(tmp_path, monkeypatch):
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}))
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--data-dir",
            str(tmp_path / "data"),
            "backfill",
            "intraday",
            "-t",
            "AAPL",
            "--start",
            "2024-01-01",
            "--freq",
            "1min",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid value" in result.output


def test_update_defaults_to_latest_universe(tmp_path, monkeypatch):
    from marketdata.store import MetaStore

    history = {
        t: [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
        for t in ("OLD", "NEW")
    }
    client = FakeTiingo(history)
    monkeypatch.setattr(cli_mod, "_client", lambda config: client)
    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.set_universe(2025, [{"ticker": "OLD", "rank": 1}])
        meta.set_universe(2026, [{"ticker": "NEW", "rank": 1}])
        for ticker in ("OLD", "NEW"):
            instrument_id = f"{ticker.lower()}-id"
            valid_to = date(2025, 12, 31) if ticker == "OLD" else date.max
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(instrument_id, ticker, date(2024, 1, 1), valid_to)
            meta.add_vendor_identifier(
                instrument_id,
                "eod",
                "ticker",
                ticker,
                date(2024, 1, 1),
                valid_to,
                validation_state="validated",
            )
        meta.activate_canonical_generation()

    runner = CliRunner()
    result = runner.invoke(main, ["--data-dir", str(data_dir), "update"])
    assert result.exit_code == 0, result.output
    assert {call[0] for call in client.eod_calls} == {"NEW"}

    all_result = runner.invoke(
        main, ["--data-dir", str(data_dir), "update", "--all-universes"]
    )
    assert all_result.exit_code == 0, all_result.output
    assert {call[0] for call in client.eod_calls} == {"NEW"}


def test_scheduled_update_refreshes_latest_identity_and_records_run_telemetry(
    tmp_path, monkeypatch
):
    from marketdata.store import MetaStore

    today = date.today()
    archive = {
        "ticker": "SAFE",
        "exchange": "NASDAQ",
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": "2020-01-02",
        "endDate": today.isoformat(),
    }

    class ScheduledClient(FakeTiingo):
        def __init__(self):
            super().__init__({"SAFE": [eod_row(today)]})
            self.metadata_calls = []
            self.request_count = 0
            self.response_bytes = 0

        def supported_tickers(self, tickers=None):
            return [archive]

        def ticker_metadata(self, ticker):
            self.metadata_calls.append(ticker)
            self.request_count += 1
            self.response_bytes += 7
            return {
                "ticker": ticker,
                "exchangeCode": "NASDAQ",
                "startDate": archive["startDate"],
                "endDate": archive["endDate"],
                "name": "Safe Incorporated",
                "description": "fixture",
            }

        def eod(self, ticker, start=None, end=None):
            self.request_count += 1
            self.response_bytes += 11
            return super().eod(ticker, start, end)

    data_dir = tmp_path / "data"
    summary = tmp_path / "current-status.json"
    client = ScheduledClient()
    monkeypatch.setattr(cli_mod, "_client", lambda config: client)
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(2025, [{"ticker": "OLD", "rank": 1}])
        meta.set_universe(2026, [{"ticker": "SAFE", "rank": 1}])

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "--refresh-identities",
            "--status-json",
            str(summary),
        ],
    )

    assert result.exit_code == 0, result.output
    assert client.metadata_calls == ["SAFE"]
    assert [call[0] for call in client.eod_calls] == ["SAFE"]
    payload = json.loads(summary.read_text())
    assert payload["ok"] is True
    assert payload["identity_bootstrap"]["counts"]["validated"] == 1
    assert payload["current"]["counts"]["fetched"] == 1
    assert "segments" not in payload["current"]
    assert payload["operation"]["kind"] == "current_eod_update"
    assert payload["operation"]["request_attempts"] == 2
    assert payload["operation"]["observed_response_bytes"] == 18
    assert payload["operation"]["started_at"] <= payload["operation"]["finished_at"]


def test_update_per_symbol_vendor_failure_is_partial(tmp_path, monkeypatch):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    summary = tmp_path / "current-status.json"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias("apple-id", "AAPL", date(2024, 1, 1))
        meta.add_vendor_identifier(
            "apple-id",
            "eod",
            "ticker",
            "AAPL",
            date(2024, 1, 1),
            date.max,
            validation_state="validated",
        )
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}, fail={"AAPL"}))

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "-t",
            "AAPL",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(summary.read_text())
    assert payload["ok"] is False
    assert payload["current"]["failed"]
    assert payload["operation"]["finished_at"]


def test_bounded_current_status_caps_diagnostic_details():
    from marketdata.ingest import IngestResult
    from marketdata.scheduler import IngestionCycleResult

    failures = {f"instrument-{index:03d}": "failed" for index in range(105)}
    payload = cli_mod._bounded_current_status(
        IngestionCycleResult(current=IngestResult(failed=failures)),
        operation=None,
        identity_bootstrap=None,
    )

    assert payload["current"]["counts"]["failed"] == 105
    assert len(payload["current"]["failed"]) == 100
    assert payload["current"]["omitted_details"]["failed"] == 5


def test_update_precondition_failure_uses_operational_exit_and_status(tmp_path):
    data_dir = tmp_path / "data"
    status = tmp_path / "current-status.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "--status-json",
            str(status),
        ],
    )

    assert result.exit_code == 2
    assert "migrate the warehouse to v2 first" in result.output
    payload = json.loads(status.read_text())
    assert payload["ok"] is False
    assert payload["operation"]["kind"] == "current_eod_update"


def test_update_missing_universe_uses_operational_exit_and_status(tmp_path):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    status = tmp_path / "current-status.json"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "--status-json",
            str(status),
        ],
    )

    assert result.exit_code == 2
    assert "No tickers specified and no universe exists yet" in result.output
    assert json.loads(status.read_text())["ok"] is False


def test_update_identity_quota_pause_is_clean(tmp_path, monkeypatch):
    from marketdata.identity_bootstrap import IdentityBootstrapResult
    from marketdata.scheduler import IngestionCycleResult
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    status = tmp_path / "current-status.json"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.set_universe(2026, [{"ticker": "SAFE", "rank": 1}])
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}))
    monkeypatch.setattr(
        cli_mod,
        "bootstrap_eod_identities",
        lambda client, meta, tickers: IdentityBootstrapResult(
            requested=1, stop_reason="hourly_request_limit"
        ),
    )
    monkeypatch.setattr(
        cli_mod,
        "run_ingestion_cycle",
        lambda *args, **kwargs: IngestionCycleResult(),
    )

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "--refresh-identities",
            "--status-json",
            str(status),
        ],
    )

    assert result.exit_code == 0, result.output
    identity = json.loads(status.read_text())["identity_bootstrap"]
    assert identity["quota_stopped"] is True
    assert identity["ok"] is True


def test_update_status_write_failure_uses_operational_exit(tmp_path, monkeypatch):
    from marketdata.scheduler import IngestionCycleResult
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}))
    monkeypatch.setattr(
        cli_mod,
        "run_ingestion_cycle",
        lambda *args, **kwargs: IngestionCycleResult(),
    )
    monkeypatch.setattr(
        cli_mod,
        "_write_json_atomic",
        lambda payload, path: (_ for _ in ()).throw(OSError("disk full")),
    )

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "-t",
            "SAFE",
            "--status-json",
            str(tmp_path / "status.json"),
        ],
    )

    assert result.exit_code == 2
    assert "could not write ingestion report" in result.output


def test_update_reports_budget_pause_as_clean_resumable_stop(tmp_path, monkeypatch):
    from marketdata.errors import BudgetExhausted
    from marketdata.store import MetaStore

    class BudgetStopped:
        def eod(self, ticker, start, end):
            raise BudgetExhausted("hourly_request_limit")

    data_dir = tmp_path / "data"
    summary = tmp_path / "summary.json"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias("apple-id", "AAPL", date(2024, 1, 1))
        meta.add_vendor_identifier(
            "apple-id",
            "eod",
            "ticker",
            "AAPL",
            date(2024, 1, 1),
            date.max,
            validation_state="validated",
        )
    monkeypatch.setattr(cli_mod, "_client", lambda config: BudgetStopped())

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "update",
            "-t",
            "AAPL",
            "--summary-json",
            str(summary),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "stopped cleanly: hourly_request_limit" in result.output
    payload = json.loads(summary.read_text())
    assert payload["stop_reason"] == "hourly_request_limit"
    assert payload["current"]["ok"] is True


def test_mutation_lock_contention_is_nonzero_and_machine_readable(
    tmp_path, monkeypatch
):
    from marketdata.scheduler import initialize_history_job
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    summary = tmp_path / "summary.json"
    backfill_summary = tmp_path / "backfill-summary.json"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias("apple-id", "AAPL", date(2024, 1, 1))
        meta.add_vendor_identifier(
            "apple-id",
            "eod",
            "ticker",
            "AAPL",
            date(2024, 1, 1),
            date.max,
            validation_state="validated",
        )
        initialize_history_job(
            meta,
            job_id="cancel-during-lock",
            dataset_key="eod",
            tickers=["AAPL"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 2),
        )
    client = FakeTiingo({})
    monkeypatch.setattr(cli_mod, "_client", lambda config: client)

    with _external_lock(data_dir):
        update_result = CliRunner().invoke(
            main,
            [
                "--data-dir",
                str(data_dir),
                "update",
                "-t",
                "AAPL",
                "--summary-json",
                str(summary),
            ],
        )
        reconcile_result = CliRunner().invoke(
            main, ["--data-dir", str(data_dir), "reconcile"]
        )
        backfill_result = CliRunner().invoke(
            main,
            [
                "--data-dir",
                str(data_dir),
                "backfill",
                "eod",
                "-t",
                "AAPL",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--job-id",
                "lock-contended",
                "--summary-json",
                str(backfill_summary),
            ],
        )
        cancel_result = CliRunner().invoke(
            main,
            [
                "--data-dir",
                str(data_dir),
                "backfill",
                "cancel",
                "cancel-during-lock",
            ],
        )

    assert update_result.exit_code == 2
    assert "data directory is busy" in update_result.output
    payload = json.loads(summary.read_text())
    assert payload["ok"] is False
    assert "operation=test-external-holder" in payload["error"]
    assert len(payload["error"]) < 1_024
    assert not client.eod_calls
    assert reconcile_result.exit_code == 1
    assert "data directory is busy" in reconcile_result.output
    assert backfill_result.exit_code == 2
    assert json.loads(backfill_summary.read_text())["ok"] is False
    assert cancel_result.exit_code == 0, cancel_result.output
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.history_job("lock-contended") is None
        assert meta.history_job("cancel-during-lock")["cancelled"] == 1


def test_backfill_cancel_keeps_audit_state(tmp_path):
    from marketdata.scheduler import initialize_history_job
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
        meta.upsert_instrument("apple-id")
        meta.add_instrument_alias(
            "apple-id", "AAPL", date(2024, 1, 1), date(2024, 1, 31)
        )
        initialize_history_job(
            meta,
            job_id="obsolete-phase-1",
            phase=1,
            dataset_key="eod",
            tickers=["AAPL"],
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
        )

    result = CliRunner().invoke(
        main,
        ["--data-dir", str(data_dir), "backfill", "cancel", "obsolete-phase-1"],
    )

    assert result.exit_code == 0, result.output
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.history_job("obsolete-phase-1")["status"] == "blocked"
        assert meta.history_job("obsolete-phase-1")["cancelled"] == 1


def test_reconcile_command(tmp_path, monkeypatch):
    from marketdata.locking import LOCK_FILE_NAME
    from marketdata.store import BarStore, MetaStore
    from marketdata.store.bars import eod_frame

    runner = CliRunner()
    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.upsert_instrument("AAPL")
        meta.activate_canonical_generation()
    BarStore(data_dir).publish_eod(
        {
            "AAPL": eod_frame(
                "AAPL",
                [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 3, 29))],
            )
        }
    )
    original_storage_generation = MetaStore.storage_generation

    def storage_generation_under_lock(meta):
        holder = json.loads((data_dir / LOCK_FILE_NAME).read_text())
        assert holder["operation"] == "reconcile:active-generation"
        return original_storage_generation(meta)

    monkeypatch.setattr(MetaStore, "storage_generation", storage_generation_under_lock)
    result = runner.invoke(main, ["--data-dir", str(data_dir), "reconcile"])
    assert result.exit_code == 0
    assert "eod: coverage rebuilt for 1 instruments" in result.output


def test_cancel_and_reconcile_do_not_create_a_mistyped_warehouse(tmp_path):
    cancel_dir = tmp_path / "mistyped-cancel"
    reconcile_dir = tmp_path / "mistyped-reconcile"

    cancel = CliRunner().invoke(
        main,
        ["--data-dir", str(cancel_dir), "backfill", "cancel", "missing-job"],
    )
    reconcile = CliRunner().invoke(
        main, ["--data-dir", str(reconcile_dir), "reconcile"]
    )

    assert cancel.exit_code == 1
    assert reconcile.exit_code == 1
    assert "warehouse is not initialized" in cancel.output
    assert "warehouse is not initialized" in reconcile.output
    assert not cancel_dir.exists()
    assert not reconcile_dir.exists()


def test_reconcile_restores_v1_coverage_without_migrating(tmp_path):
    from marketdata.store import BarStore, MetaStore
    from marketdata.store.bars import eod_frame

    data_dir = tmp_path / "data"
    bars = BarStore(data_dir)
    bars.write_eod("AAPL", eod_frame("AAPL", [eod_row(date(2024, 1, 2))]))
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.storage_generation() == "v1"
        meta.set_ticker_coverage_v1("GHOST", "eod", date(2020, 1, 1), date(2020, 1, 2))

    result = CliRunner().invoke(main, ["--data-dir", str(data_dir), "reconcile"])

    assert result.exit_code == 0, result.output
    assert "eod: coverage rebuilt for 1 tickers" in result.output
    with MetaStore(data_dir / "meta.db") as meta:
        assert meta.storage_generation() == "v1"
        assert meta.get_ticker_coverage_v1("AAPL", "eod") is not None
        assert meta.get_ticker_coverage_v1("GHOST", "eod") is None


def test_universe_rank_remains_available_on_v1(tmp_path):
    from test_universe import _synthetic_eod

    from marketdata.store import BarStore

    data_dir = tmp_path / "data"
    BarStore(data_dir).write_eod(
        "BIG", _synthetic_eod("BIG", close=100.0, volume=1_000_000)
    )

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "universe",
            "rank",
            "--year",
            "2024",
            "--top",
            "1",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Universe 2024: 1 tickers stored" in result.output


def test_sql_reads_canonical_instrument_view(tmp_path):
    from marketdata.store import BarStore, MetaStore
    from marketdata.store.bars import eod_frame

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.upsert_instrument("apple-id")
        meta.activate_canonical_generation()
    BarStore(data_dir).publish_eod(
        {"apple-id": eod_frame("AAPL", [eod_row(date(2024, 1, 2))])}
    )

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "sql",
            "SELECT instrument_id, count(*) AS rows FROM eod GROUP BY instrument_id",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "apple-id" in result.output


def test_migrate_v2_bars_command_writes_default_report(tmp_path):
    from marketdata.store import MetaStore

    data_dir = tmp_path / "data"
    MetaStore(data_dir / "meta.db").close()

    result = CliRunner().invoke(main, ["--data-dir", str(data_dir), "migrate-v2-bars"])

    assert result.exit_code == 0, result.output
    assert "Migration pass complete: no source files" in result.output
    assert (
        data_dir / "quarantine" / "v1-ticker-bars" / "migration-report.json"
    ).exists()


def test_identity_blocked_v2_ingestion_writes_no_canonical_bars(tmp_path, monkeypatch):
    from marketdata.config import Config
    from marketdata.store import BarStore, MetaStore

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.set_ticker_coverage_v1("AAPL", "eod", date(2000, 1, 1), date(2025, 1, 1))
        meta.activate_canonical_generation()
        assert meta.ticker_coverage_v1("eod") == {}
    Config(data_dir, None).ensure_dirs()
    assert not BarStore(data_dir).has_canonical_bars()
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}))

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(data_dir),
            "backfill",
            "eod",
            "-t",
            "AAPL",
            "--start",
            "2024-01-01",
        ],
    )

    assert result.exit_code == 1
    assert "Blocked segments" in result.output
    assert "production ingestion remains paused" not in result.output
    assert not BarStore(data_dir).has_canonical_bars()


def test_canonical_reconcile_issues_exit_nonzero(tmp_path):
    from marketdata.store import BarStore, MetaStore
    from marketdata.store.bars import eod_frame

    data_dir = tmp_path / "data"
    with MetaStore(data_dir / "meta.db") as meta:
        meta.activate_canonical_generation()
    BarStore(data_dir).publish_eod(
        {"unknown": eod_frame("UNKNOWN", [eod_row(date(2024, 1, 2))])}
    )

    result = CliRunner().invoke(main, ["--data-dir", str(data_dir), "reconcile"])

    assert result.exit_code == 1
    assert "unknown_instrument" in result.output


def test_migration_os_error_is_reported_as_click_error(tmp_path, monkeypatch):
    def fail_migration(*args, **kwargs):
        raise OSError("simulated permission failure")

    monkeypatch.setattr(cli_mod, "migrate_v1_bars", fail_migration)
    result = CliRunner().invoke(
        main, ["--data-dir", str(tmp_path / "data"), "migrate-v2-bars"]
    )

    assert result.exit_code == 1
    assert "Error: simulated permission failure" in result.output
