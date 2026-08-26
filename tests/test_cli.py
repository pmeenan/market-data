"""CLI behavior tests (offline; the Tiingo client is faked)."""

import json
from datetime import date, timedelta

from click.testing import CliRunner

import marketdata.cli as cli_mod
from marketdata.cli import main
from test_ingest import FakeTiingo, eod_row, weekdays


def _fake_client(history, fail=frozenset()):
    client = FakeTiingo(history, fail=fail)
    return lambda config: client


def test_backfill_partial_failure_exits_nonzero(tmp_path, monkeypatch):
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]}
    monkeypatch.setattr(cli_mod, "_client", _fake_client(history, fail={"BADCO"}))
    summary = tmp_path / "summary.json"

    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(tmp_path / "data"),
        "backfill", "eod", "-t", "AAPL", "-t", "BADCO",
        "--start", "2024-01-01", "--end", "2024-06-28",
        "--summary-json", str(summary),
    ])
    assert result.exit_code == 1
    payload = json.loads(summary.read_text())
    assert payload["fetched"] == ["AAPL"]
    assert "BADCO" in payload["failed"]
    assert payload["ok"] is False


def test_backfill_success_exits_zero(tmp_path, monkeypatch):
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]}
    monkeypatch.setattr(cli_mod, "_client", _fake_client(history))

    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(tmp_path / "data"),
        "backfill", "eod", "-t", "AAPL",
        "--start", "2024-01-01", "--end", "2024-06-28",
    ])
    assert result.exit_code == 0, result.output


def test_intraday_rejects_unknown_freq(tmp_path, monkeypatch):
    monkeypatch.setattr(cli_mod, "_client", _fake_client({}))
    runner = CliRunner()
    result = runner.invoke(main, [
        "--data-dir", str(tmp_path / "data"),
        "backfill", "intraday", "-t", "AAPL",
        "--start", "2024-01-01", "--freq", "2min",
    ])
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

    runner = CliRunner()
    assert runner.invoke(main, ["--data-dir", str(data_dir), "update"]).exit_code == 0
    assert {c[0] for c in client.eod_calls} == {"NEW"}

    client.eod_calls.clear()
    assert runner.invoke(
        main, ["--data-dir", str(data_dir), "update", "--all-universes"]
    ).exit_code == 0
    assert {c[0] for c in client.eod_calls} == {"OLD", "NEW"}


def test_reconcile_command(tmp_path, monkeypatch):
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 3, 29))]}
    monkeypatch.setattr(cli_mod, "_client", _fake_client(history))
    runner = CliRunner()
    data_dir = str(tmp_path / "data")
    assert runner.invoke(main, [
        "--data-dir", data_dir, "backfill", "eod", "-t", "AAPL",
        "--start", "2024-01-01", "--end", "2024-03-29",
    ]).exit_code == 0
    result = runner.invoke(main, ["--data-dir", data_dir, "reconcile"])
    assert result.exit_code == 0
    assert "eod: coverage rebuilt for 1 tickers" in result.output
