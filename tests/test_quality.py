"""Structured, read-only quality findings and consumer-defined gates."""

import json
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest
from click.testing import CliRunner

import marketdata.cli as cli_mod
import marketdata.quality as quality_mod
from marketdata.cli import main
from marketdata.config import Config
from marketdata.quality import (
    DEFAULT_ZERO_VOLUME_RUN_LENGTH,
    check_quality,
    evaluate_quality,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import EOD_SCHEMA, INTRADAY_SCHEMA, eod_frame


def _eod_row(day: date) -> dict:
    return {
        "date": f"{day.isoformat()}T00:00:00.000Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 0,
        "adjOpen": 100.0,
        "adjHigh": 101.0,
        "adjLow": 99.0,
        "adjClose": 100.0,
        "adjVolume": 0,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def _setup_eod_quality_fixture(tmp_path) -> tuple[Config, bytes]:
    config = Config(tmp_path, None)
    bars = BarStore(tmp_path)
    days = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        # January 4 is deliberately missing inside durable coverage.
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
        date(2024, 1, 11),
    ]
    frame = eod_frame("BAD", [_eod_row(day) for day in days]).with_columns(
        high=pl.when(pl.col("date") == date(2024, 1, 5))
        .then(98.0)
        .otherwise(pl.col("high")),
        div_cash=pl.when(pl.col("date") == date(2024, 1, 8))
        .then(-1.0)
        .otherwise(pl.col("div_cash")),
        split_factor=pl.when(pl.col("date") == date(2024, 1, 9))
        .then(0.0)
        .otherwise(pl.col("split_factor")),
    )
    canonical = bars.canonicalize_eod("bad-id", frame)
    # Publication normally enforces uniqueness; write a duplicate directly to
    # prove the diagnostic catches corrupt canonical input.
    corrupt = pl.concat(
        [canonical, canonical.filter(pl.col("date") == date(2024, 1, 3))]
    ).cast(
        {"instrument_id": pl.Utf8}
        | {k: v for k, v in EOD_SCHEMA.items() if k != "ticker"}
    )
    path = bars.canonical_eod_path("bad-id")
    path.parent.mkdir(parents=True)
    corrupt.write_parquet(path)
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("bad-id", lifecycle_status="inactive")
        meta.set_coverage("bad-id", "eod", date(2024, 1, 2), date(2024, 1, 11))
        meta.activate_canonical_generation()
    return config, path.read_bytes()


def _setup_intraday_quality_fixture(tmp_path) -> tuple[Config, bytes]:
    config = Config(tmp_path, None)
    bars = BarStore(tmp_path)
    timestamps = [
        datetime(2024, 6, 3, 13, 30, tzinfo=UTC) + timedelta(minutes=5 * offset)
        for offset in range(5)
    ]
    timestamps.append(datetime(2024, 6, 3, 20, 0, tzinfo=UTC))
    frame = pl.DataFrame(
        {
            "ticker": ["BAD"] * len(timestamps),
            "ts": timestamps,
            "open": [100.0] * len(timestamps),
            "high": [101.0] * len(timestamps),
            "low": [99.0] * len(timestamps),
            "close": [100.0] * len(timestamps),
            "volume": [0] * len(timestamps),
        }
    ).cast(INTRADAY_SCHEMA)
    canonical = bars.canonicalize_intraday("bad-id", frame)
    corrupt = pl.concat([canonical, canonical.head(1)])
    path = bars.canonical_intraday_path("bad-id", 2024, "5min")
    path.parent.mkdir(parents=True)
    corrupt.write_parquet(path)
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("bad-id", lifecycle_status="active")
        meta.set_coverage("bad-id", "intraday_5min", date(2024, 6, 3), date(2024, 6, 4))
        meta.activate_canonical_generation()
    return config, path.read_bytes()


def _find(report, check: str):
    return [finding for finding in report.findings if finding.check == check]


def test_eod_minimum_checks_are_structured_and_do_not_repair_bars(tmp_path):
    config, original_bytes = _setup_eod_quality_fixture(tmp_path)

    report = check_quality(config, dataset_keys=["eod"])

    assert set(report.checks_run) == {
        "missing_expected_sessions",
        "duplicate_keys",
        "ohlc_invariants",
        "negative_values",
        "zero_volume_runs",
        "split_sanity",
        "coverage_delisting_summary",
    }
    assert _find(report, "missing_expected_sessions")[0].sample == ("2024-01-04",)
    assert _find(report, "duplicate_keys")[0].details == {"extra_rows": 1}
    assert _find(report, "ohlc_invariants")[0].details == {"raw_rows": 1}
    assert _find(report, "negative_values")[0].details == {"div_cash_rows": 1}
    assert _find(report, "zero_volume_runs")[0].details == {
        "zero_rows_in_runs": 5,
        "longest_run": 5,
    }
    assert _find(report, "split_sanity")[0].sample == ("2024-01-09",)
    coverage = _find(report, "coverage_delisting_summary")[0]
    assert coverage.severity == "info"
    assert coverage.details == {
        "lifecycle_status": "inactive",
        "is_delisted": True,
        "coverage_first": "2024-01-02",
        "coverage_last": "2024-01-11",
        "observed_first": "2024-01-02",
        "observed_last": "2024-01-11",
        "row_count": 8,
        "time_key": "date",
    }
    assert (
        BarStore(tmp_path).canonical_eod_path("bad-id").read_bytes() == original_bytes
    )


def test_intraday_checks_separate_off_session_rows_and_iex_zero_volume(tmp_path):
    config, original_bytes = _setup_intraday_quality_fixture(tmp_path)

    report = check_quality(config, dataset_keys=["intraday_5min"])

    assert _find(report, "missing_expected_sessions")[0].sample == ("2024-06-04",)
    assert _find(report, "duplicate_keys")[0].count == 1
    off_session = _find(report, "off_session_intraday")[0]
    assert off_session.count == 1
    assert off_session.sample == ("2024-06-03T20:00:00+00:00",)
    zero_volume = _find(report, "zero_volume_runs")[0]
    assert zero_volume.details == {"zero_rows_in_runs": 5, "longest_run": 5}
    assert "IEX-only" in zero_volume.message
    assert "split_sanity" not in report.checks_run
    assert (
        BarStore(tmp_path).canonical_intraday_path("bad-id", 2024, "5min").read_bytes()
        == original_bytes
    )


def test_consumer_gate_blocks_declared_findings_and_unrun_checks(tmp_path):
    config, _ = _setup_eod_quality_fixture(tmp_path)
    report = check_quality(config, dataset_keys=["eod"])

    clean_policy = evaluate_quality(report, ["coverage_delisting_summary"])
    finding_policy = evaluate_quality(report, ["ohlc_invariants"])
    unrun_policy = evaluate_quality(report, ["off_session_intraday"])

    assert clean_policy.passed
    assert not finding_policy.passed
    assert [finding.check for finding in finding_policy.blocking_findings] == [
        "ohlc_invariants"
    ]
    assert not unrun_policy.passed
    assert unrun_policy.checks_not_run == ("off_session_intraday",)


def test_explicit_instrument_without_coverage_fails_a_coverage_gate(tmp_path):
    config = Config(tmp_path, None)
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("empty-id")
        meta.activate_canonical_generation()

    report = check_quality(
        config,
        dataset_keys=["eod"],
        instrument_ids=["empty-id"],
    )
    finding = _find(report, "coverage_delisting_summary")[0]

    assert finding.severity == "warning"
    assert not evaluate_quality(report, ["coverage_delisting_summary"]).passed


def test_empty_scope_does_not_claim_checks_ran_or_pass_a_declared_gate(tmp_path):
    config = Config(tmp_path, None)
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()

    filtered = check_quality(
        config,
        dataset_keys=["eod"],
        instrument_ids=[],
    )
    warehouse = check_quality(config, dataset_keys=["eod"])
    gate = evaluate_quality(
        filtered,
        ["ohlc_invariants", "coverage_delisting_summary"],
    )

    assert filtered.checks_run == ()
    assert warehouse.checks_run == ()
    assert not gate.passed
    assert gate.checks_not_run == (
        "ohlc_invariants",
        "coverage_delisting_summary",
    )


def test_check_is_unrun_when_any_requested_applicable_dataset_is_empty(tmp_path):
    config, _ = _setup_eod_quality_fixture(tmp_path)

    report = check_quality(config, dataset_keys=["eod", "intraday_5min"])
    gate = evaluate_quality(report, ["ohlc_invariants"])

    assert "ohlc_invariants" not in report.checks_run
    assert gate.checks_not_run == ("ohlc_invariants",)
    assert not gate.passed


def test_null_volume_is_invalid_and_cannot_bridge_zero_volume_runs(tmp_path):
    config = Config(tmp_path, None)
    bars = BarStore(tmp_path)
    days = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
        date(2024, 1, 9),
        date(2024, 1, 10),
        date(2024, 1, 11),
        date(2024, 1, 12),
    ]
    rows = [_eod_row(day) for day in days]
    rows[4]["volume"] = None
    bars.publish_eod({"bad-id": eod_frame("BAD", rows)})
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("bad-id")
        meta.set_coverage("bad-id", "eod", days[0], days[-1])
        meta.activate_canonical_generation()

    report = check_quality(config, dataset_keys=["eod"])
    gate = evaluate_quality(report, ["negative_values", "zero_volume_runs"])

    assert _find(report, "negative_values")[0].details == {"volume_rows": 1}
    assert not _find(report, "zero_volume_runs")
    assert not gate.passed


def test_intraday_zero_volume_run_resets_at_session_boundary(tmp_path):
    config = Config(tmp_path, None)
    bars = BarStore(tmp_path)
    timestamps = [
        datetime(2024, 6, 7, 19, 45, tzinfo=UTC) + timedelta(minutes=5 * offset)
        for offset in range(3)
    ] + [
        datetime(2024, 6, 10, 13, 30, tzinfo=UTC) + timedelta(minutes=5 * offset)
        for offset in range(2)
    ]
    frame = pl.DataFrame(
        {
            "ticker": ["GAP"] * len(timestamps),
            "ts": timestamps,
            "open": [100.0] * len(timestamps),
            "high": [101.0] * len(timestamps),
            "low": [99.0] * len(timestamps),
            "close": [100.0] * len(timestamps),
            "volume": [0] * len(timestamps),
        }
    ).cast(INTRADAY_SCHEMA)
    bars.publish_intraday({"gap-id": frame}, freq="5min")
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("gap-id")
        meta.set_coverage(
            "gap-id", "intraday_5min", date(2024, 6, 7), date(2024, 6, 10)
        )
        meta.activate_canonical_generation()

    report = check_quality(config, dataset_keys=["intraday_5min"])

    assert not _find(report, "zero_volume_runs")


def test_selected_orphaned_bars_use_the_actionable_coverage_message(tmp_path):
    config = Config(tmp_path, None)
    bars = BarStore(tmp_path)
    bars.publish_eod({"orphan-id": eod_frame("ORPHAN", [_eod_row(date(2024, 1, 2))])})
    with MetaStore(config.meta_path) as meta:
        meta.upsert_instrument("orphan-id")
        meta.activate_canonical_generation()

    report = check_quality(
        config,
        dataset_keys=["eod"],
        instrument_ids=["orphan-id"],
    )

    assert (
        _find(report, "coverage_delisting_summary")[0].message
        == "stored bars exist without durable coverage in this scope"
    )


def test_numeric_schema_drift_fails_loudly_before_checks_are_advertised(
    tmp_path, monkeypatch
):
    config = Config(tmp_path, None)
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
    monkeypatch.setattr(
        quality_mod,
        "CANONICAL_EOD_SCHEMA",
        quality_mod.CANONICAL_EOD_SCHEMA | {"unclassified_numeric": pl.Float64},
    )

    with pytest.raises(RuntimeError, match="do not cover canonical eod"):
        check_quality(config, dataset_keys=["eod"])


def test_quality_cli_writes_full_report_and_uses_explicit_blocking_policy(tmp_path):
    config, _ = _setup_eod_quality_fixture(tmp_path)
    summary_path = tmp_path / "reports" / "quality.json"

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(config.data_dir),
            "quality",
            "--dataset",
            "eod",
            "--block-on",
            "missing_expected_sessions",
            "--summary-json",
            str(summary_path),
        ],
    )

    assert result.exit_code == 1, result.output
    assert "Quality gate: failed" in result.output
    payload = json.loads(summary_path.read_text())
    assert payload["gate"]["passed"] is False
    assert payload["scope"]["dataset_keys"] == ["eod"]
    assert {finding["check"] for finding in payload["findings"]} >= {
        "missing_expected_sessions",
        "duplicate_keys",
        "ohlc_invariants",
        "negative_values",
        "zero_volume_runs",
        "split_sanity",
        "coverage_delisting_summary",
    }


def test_quality_cli_preserves_standing_report_on_scan_failure(tmp_path, monkeypatch):
    summary_path = tmp_path / "quality.json"
    summary_path.write_text('{"gate":{"passed":true}}\n')
    monkeypatch.setattr(
        cli_mod,
        "check_quality",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("broken scan")),
    )

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(tmp_path),
            "quality",
            "--summary-json",
            str(summary_path),
        ],
    )

    assert result.exit_code == 2
    assert "quality scan failed: broken scan" in result.output
    assert "Traceback" not in result.output
    assert summary_path.read_text() == '{"gate":{"passed":true}}\n'


def test_quality_cli_summary_write_failure_is_a_bounded_operational_error(
    tmp_path, monkeypatch
):
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()

    def fail_write(*args, **kwargs):
        raise PermissionError("read-only destination")

    monkeypatch.setattr(cli_mod, "_write_json_atomic", fail_write)
    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(tmp_path),
            "quality",
            "--summary-json",
            str(tmp_path / "quality.json"),
        ],
    )

    assert result.exit_code == 2
    assert "could not write quality summary" in result.output
    assert "Traceback" not in result.output


def test_quality_cli_names_unrun_checks_and_uses_library_zero_run_default(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()

    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(tmp_path),
            "quality",
            "--dataset",
            "eod",
            "--block-on",
            "ohlc_invariants",
        ],
    )
    help_result = CliRunner().invoke(main, ["quality", "--help"])

    assert result.exit_code == 1
    assert "Quality checks not run: ohlc_invariants" in result.output
    assert f"default: {DEFAULT_ZERO_VOLUME_RUN_LENGTH}" in help_result.output
