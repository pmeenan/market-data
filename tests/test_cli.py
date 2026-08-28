"""CLI behavior tests (offline; the Tiingo client is faked)."""

import json
from datetime import date

from click.testing import CliRunner
from test_ingest import FakeTiingo, eod_row, weekdays

import marketdata.cli as cli_mod
from marketdata.cli import main


def _fake_client(history, fail=frozenset()):
    client = FakeTiingo(history, fail=fail)
    return lambda config: client


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


def test_backfill_cancel_releases_phase_gate_without_deleting_audit_state(tmp_path):
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
        assert meta.active_lower_phase_jobs(2) == []


def test_reconcile_command(tmp_path, monkeypatch):
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
    result = runner.invoke(main, ["--data-dir", str(data_dir), "reconcile"])
    assert result.exit_code == 0
    assert "eod: coverage rebuilt for 1 instruments" in result.output


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
    assert "Identity-blocked segments" in result.output
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
