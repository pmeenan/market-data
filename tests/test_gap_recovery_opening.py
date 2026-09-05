"""Five-minute opening-window study: session-relative checkpoints and fidelity."""

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from test_gap_recovery import _EVENT, _PARAMS, _SESSIONS, _eod

from marketdata.config import Config
from marketdata.research import registered_event_studies, run_registered_event_study
from marketdata.store.bars import INTRADAY_SCHEMA, BarStore
from marketdata.store.meta import MetaStore
from marketdata.studies.compare import compare_gap_studies
from marketdata.studies.gap_recovery import STUDY_NAME as COARSE
from marketdata.studies.gap_recovery_opening import (
    CHECKPOINT_MINUTES,
    STUDY_NAME,
    checkpoint_label,
)

_OPEN_UTC_HOUR = 14  # 09:30 New York time in EST
_INSTRUMENTS = {
    "gap-id": ("GAPY", "Stock"),
    "spy-id": ("SPY", "ETF"),
    "thin-id": ("THIN", "Stock"),
}


def _price(ticker: str, day, end_minutes: int) -> float:
    if ticker == "SPY":
        return 501.0 if day == _EVENT else 500.0
    if day == _EVENT:
        return 95.0 + 0.01 * end_minutes
    return 100.0


def _ts(day, minutes_from_open: int) -> datetime:
    return datetime(day.year, day.month, day.day, _OPEN_UTC_HOUR, 30, tzinfo=UTC) + (
        timedelta(minutes=minutes_from_open)
    )


def _five_minute(ticker, *, volume=1_000, drop_after=None):
    rows = []
    for day in _SESSIONS:
        for start in range(0, 390, 5):
            if drop_after is not None and day == _EVENT and start + 5 > drop_after:
                continue
            close = _price(ticker, day, start + 5)
            rows.append(
                {
                    "ticker": ticker,
                    "ts": _ts(day, start),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": volume,
                }
            )
    return pl.DataFrame(rows).cast(INTRADAY_SCHEMA)


def _hourly(ticker, *, volume=1_000, override=None):
    rows = []
    for day in _SESSIONS:
        for hour in range(10, 16):
            end_minutes = (hour - 9) * 60 + 30
            close = _price(ticker, day, end_minutes)
            if override and (day, hour) in override:
                close = override[(day, hour)]
            rows.append(
                {
                    "ticker": ticker,
                    "ts": _ts(day, end_minutes - 60),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": volume,
                }
            )
    return pl.DataFrame(rows).cast(INTRADAY_SCHEMA)


def _fixture(tmp_path, *, hourly_override=None, drop_after=None):
    config = Config(tmp_path / "data", None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, (ticker, asset_type) in _INSTRUMENTS.items():
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(
                instrument_id,
                ticker,
                _SESSIONS[0],
                _SESSIONS[-1],
                exchange="NYSE",
                asset_type=asset_type,
            )
    closes = [98.9 if day == _EVENT else 100.0 for day in _SESSIONS]
    bars = BarStore(config.data_dir)
    bars.publish_eod(
        {
            "gap-id": _eod("GAPY", closes, opens={_EVENT: 95.0}),
            "thin-id": _eod("THIN", closes, opens={_EVENT: 95.0}),
            "spy-id": _eod("SPY", [500.0] * len(_SESSIONS), opens={_EVENT: 499.0}),
        }
    )
    bars.publish_intraday(
        {
            "gap-id": _hourly("GAPY", override=hourly_override),
            "thin-id": _hourly("THIN", volume=0),
            "spy-id": _hourly("SPY"),
        },
        freq="1hour",
    )
    bars.publish_intraday(
        {
            "gap-id": _five_minute("GAPY", drop_after=drop_after),
            "thin-id": _five_minute("THIN", volume=0),
            "spy-id": _five_minute("SPY"),
        },
        freq="5min",
    )
    return config


def _run(config, **overrides):
    published = run_registered_event_study(config, STUDY_NAME, {**_PARAMS, **overrides})
    return published, pl.read_parquet(published.observation_path)


def _metric_rows(config, run_id):
    with MetaStore(config.meta_path) as meta:
        return [dict(row) for row in meta.research_metrics(run_id)]


def test_labels_name_availability_time_and_bar():
    assert checkpoint_label(5) == "09:35_close_of_09:30_bar"
    assert checkpoint_label(90) == "11:00_close_of_10:55_bar"
    assert checkpoint_label(390) == "16:00_close_of_15:55_bar"
    assert CHECKPOINT_MINUTES == (5, 15, 30, 60, 90, 150, 210, 270, 330, 390)


def test_opening_window_returns_are_hand_calculated_and_consistent(tmp_path):
    config = _fixture(tmp_path)
    assert STUDY_NAME in registered_event_studies()

    published, observations = _run(config)

    assert observations["instrument_id"].unique().to_list() == ["gap-id"]
    assert observations["observation_label"].to_list() == [
        checkpoint_label(m) for m in CHECKPOINT_MINUTES
    ]
    first = observations.row(0, named=True)
    assert first["observation_label"] == "09:35_close_of_09:30_bar"
    assert first["checkpoint_available_ts"] == _ts(_EVENT, 5)
    assert first["checkpoint_price"] == pytest.approx(95.05)
    assert first["measured_return"] == pytest.approx(95.05 / 95.0 - 1.0)
    assert first["first_bar_return"] == pytest.approx(95.05 / 95.0 - 1.0)
    assert first["measured_return_from_first_bar"] == pytest.approx(0.0)
    assert first["max_favorable_excursion"] == pytest.approx(96.05 / 95.0 - 1.0)
    assert first["max_adverse_excursion"] == pytest.approx(94.05 / 95.0 - 1.0)
    assert first["benchmark_return"] == pytest.approx(501.0 / 499.0 - 1.0)
    assert first["hourly_close_matches_5min"] is None
    assert first["asset_type"] == "Stock"
    assert first["market_regime"] == "calm"
    assert first["market_trend"] == "up"
    assert first["five_min_density"] == pytest.approx(1.0)
    eleven = observations.filter(
        pl.col("observation_label") == "11:00_close_of_10:55_bar"
    ).row(0, named=True)
    assert eleven["checkpoint_price"] == pytest.approx(95.9)
    assert eleven["hourly_close_matches_5min"] is True
    assert eleven["measured_return_from_first_bar"] == pytest.approx(95.9 / 95.05 - 1)
    assert eleven["bars_through_checkpoint"] == 18
    assert eleven["zero_volume_bars_through_checkpoint"] == 0
    last = observations.row(-1, named=True)
    assert last["observation_label"] == "16:00_close_of_15:55_bar"
    assert last["checkpoint_price"] == pytest.approx(98.9)
    assert last["reached_target_at_checkpoint"] is True
    assert last["gap_recovered_fraction_raw_basis"] == pytest.approx(3.9 / 5.0)
    assert observations["outcome_status"].unique().to_list() == ["evaluable"]

    metrics = _metric_rows(config, published.run_id)
    names = {(m["metric_name"], m["dimensions_json"]) for m in metrics}
    assert ("events", '{"asset_type":"Stock","period":"validation"}') in names
    consistency = [
        m["value"]
        for m in metrics
        if m["metric_name"] == "hourly_close_matches_5min_rate"
    ]
    assert consistency and set(consistency) == {1.0}
    assert any(
        m["metric_name"] == "median_return"
        and '"market_regime":"calm"' in m["dimensions_json"]
        for m in metrics
    )
    audit = {
        m["metric_name"]: m["value"] for m in metrics if m["dimensions_json"] == "{}"
    }
    assert audit["event_audit.candidates"] == 2
    assert audit["event_audit.selected"] == 1


def test_hourly_disagreement_is_measured_not_hidden(tmp_path):
    config = _fixture(tmp_path, hourly_override={(_EVENT, 10): 95.0})

    published, observations = _run(config)

    eleven = observations.filter(
        pl.col("observation_label") == "11:00_close_of_10:55_bar"
    ).row(0, named=True)
    assert eleven["checkpoint_price"] == pytest.approx(95.9)
    assert eleven["hourly_close_matches_5min"] is False
    rates = {
        m["dimensions_json"]: m["value"]
        for m in _metric_rows(config, published.run_id)
        if m["metric_name"] == "hourly_close_matches_5min_rate"
    }
    assert (
        rates['{"checkpoint":"11:00_close_of_10:55_bar","period":"validation"}'] == 0.0
    )
    assert (
        rates['{"checkpoint":"12:00_close_of_11:55_bar","period":"validation"}'] == 1.0
    )


def test_absent_late_bars_are_missing_outcomes_and_density_screens_thin_names(
    tmp_path,
):
    config = _fixture(tmp_path, drop_after=300)

    published, observations = _run(config, min_5min_density=0.0, min_hourly_density=0.0)

    assert observations["instrument_id"].unique().sort().to_list() == [
        "gap-id",
        "thin-id",
    ]
    missing = observations.filter(pl.col("outcome_status") == "missing_outcome")
    assert missing.select("instrument_id", "observation_label").rows() == [
        ("gap-id", "15:00_close_of_14:55_bar"),
        ("gap-id", "16:00_close_of_15:55_bar"),
    ]
    thin = observations.filter(
        (pl.col("instrument_id") == "thin-id")
        & (pl.col("observation_label") == "16:00_close_of_15:55_bar")
    ).row(0, named=True)
    assert thin["five_min_density"] == pytest.approx(0.0)
    assert thin["zero_volume_bars_through_checkpoint"] == 78
    audit = {
        m["metric_name"]: m["value"]
        for m in _metric_rows(config, published.run_id)
        if m["dimensions_json"] == "{}"
    }
    assert audit["event_audit.missing_outcome"] == 1
    assert audit["event_audit.evaluable"] == 1

    # The default density screen removes THIN entirely.
    _, screened = _run(config)
    assert screened["instrument_id"].unique().to_list() == ["gap-id"]


def test_comparison_pairs_common_events_at_shared_availability_times(tmp_path):
    config = _fixture(tmp_path)
    coarse = run_registered_event_study(config, COARSE, _PARAMS)
    opening, _ = _run(config)

    comparison = compare_gap_studies(
        config, coarse_run_id=coarse.run_id, opening_run_id=opening.run_id
    )

    assert (comparison.coarse_events, comparison.opening_events) == (1, 1)
    assert comparison.common_events == 1
    agreement = comparison.checkpoint_agreement
    assert agreement.height == 6
    assert agreement["compared"].to_list() == [1] * 6
    assert agreement["exact_agreement_share"].to_list() == [1.0] * 6
    early = comparison.opening_window
    assert early["checkpoint"].to_list() == [
        "09:35_close_of_09:30_bar",
        "09:45_close_of_09:40_bar",
        "10:00_close_of_09:55_bar",
        "10:30_close_of_10:25_bar",
    ]
    assert early["median_return"][0] == pytest.approx(95.05 / 95.0 - 1.0)
    assert comparison.to_dict()["common_events"] == 1
    with pytest.raises(ValueError, match="not a succeeded"):
        compare_gap_studies(
            config, coarse_run_id=opening.run_id, opening_run_id=coarse.run_id
        )


def test_quality_memory_limit_is_validated_and_recorded(tmp_path):
    config = _fixture(tmp_path)
    with pytest.raises(ValueError, match="memory limit"):
        _run(config, quality_memory_limit="lots")

    published, _ = _run(config, quality_memory_limit="2GB")

    with MetaStore(config.meta_path) as meta:
        recorded = meta.research_parameters(published.run_id)
    assert recorded["_event_runner"]["quality_policy"]["memory_limit"] == "2GB"
