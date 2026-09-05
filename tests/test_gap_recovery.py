"""Coarse gap-recovery study: hand-calculated returns, causality, price bases."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from marketdata.calendar import session_schedule
from marketdata.config import Config
from marketdata.research import registered_event_studies, run_registered_event_study
from marketdata.store.bars import EOD_SCHEMA, INTRADAY_SCHEMA, BarStore
from marketdata.store.meta import MetaStore
from marketdata.studies.gap_recovery import STUDY_NAME, run_gap_recovery_study

_SESSIONS = session_schedule(date(2023, 12, 18), date(2024, 1, 12))[
    "session_date"
].to_list()
_EVENT = date(2024, 1, 8)
_EST_HOURS_UTC = {10: 15, 11: 16, 12: 17, 13: 18, 14: 19, 15: 20}
_PARAMS = {
    "start": "2023-12-18",
    "end": "2024-01-12",
    "gap_threshold": -0.03,
    "min_adv_dollars": 0.0,
    "lookback_sessions": 6,
    "min_hourly_density": 0.9,
    "benchmark_ticker": "SPY",
    "target_return": 0.01,
    "periods": {
        "development": ["2023-12-01", "2024-01-07"],
        "validation": ["2024-01-08", "2024-01-31"],
    },
    "metric_periods": ["development", "validation"],
}


def _eod(ticker, closes, *, opens=None, split_day=None, dividend_day=None):
    rows = len(_SESSIONS)
    opens = opens or dict()
    frame = {
        "ticker": [ticker] * rows,
        "date": _SESSIONS,
        "open": [],
        "high": [],
        "low": [],
        "close": [],
        "volume": [1_000_000] * rows,
        "adj_open": [],
        "adj_high": [],
        "adj_low": [],
        "adj_close": [],
        "adj_volume": [1_000_000] * rows,
        "div_cash": [0.5 if day == dividend_day else 0.0 for day in _SESSIONS],
        "split_factor": [2.0 if day == split_day else 1.0 for day in _SESSIONS],
    }
    for day, close in zip(_SESSIONS, closes, strict=True):
        open_ = opens.get(day, close)
        raw_scale = 0.5 if split_day is not None and day >= split_day else 1.0
        # Adjusted prices are continuous; raw prices halve from the split day.
        frame["open"].append(open_ * raw_scale)
        frame["high"].append(max(open_, close) * raw_scale * 1.01)
        frame["low"].append(min(open_, close) * raw_scale * 0.99)
        frame["close"].append(close * raw_scale)
        frame["adj_open"].append(open_)
        frame["adj_high"].append(max(open_, close) * 1.01)
        frame["adj_low"].append(min(open_, close) * 0.99)
        frame["adj_close"].append(close)
    return pl.DataFrame(frame).cast(EOD_SCHEMA)


def _hourly(ticker, closes_by_day, *, volume=1_000, bad_ohlc=()):
    rows = []
    for day, closes in closes_by_day.items():
        for hour, close in closes.items():
            # A vendor bar whose high sits below its close violates the OHLC
            # ordering invariant; the study must not use it as a checkpoint.
            high = close - 2.0 if (day, hour) in bad_ohlc else close + 1.0
            rows.append(
                {
                    "ticker": ticker,
                    "ts": datetime(
                        day.year, day.month, day.day, _EST_HOURS_UTC[hour], tzinfo=UTC
                    ),
                    "open": close - 0.5,
                    "high": high,
                    "low": close - 1.0,
                    "close": close,
                    "volume": volume,
                }
            )
    return pl.DataFrame(rows).cast(INTRADAY_SCHEMA)


def _flat_hourly(level):
    return {day: {hour: level for hour in _EST_HOURS_UTC} for day in _SESSIONS}


def _fixture(tmp_path, *, gap_hourly=None, dividend_day=None, split=False, bad_ohlc=()):
    config = Config(tmp_path / "data", None)
    config.ensure_dirs()
    instruments = {
        "gap-id": "GAPY",
        "spy-id": "SPY",
        "thin-id": "THIN",
        "split-id": "SPLT",
    }
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, ticker in instruments.items():
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(
                instrument_id, ticker, date(2023, 1, 1), date(2024, 12, 31)
            )
    flat = [100.0] * len(_SESSIONS)
    gap_closes = list(flat)
    gap_closes[_SESSIONS.index(_EVENT)] = 98.0
    split_closes = list(flat)
    eod = {
        # GAPY opens 5% below the prior close of 100 and closes at 98.
        "gap-id": _eod(
            "GAPY", gap_closes, opens={_EVENT: 95.0}, dividend_day=dividend_day
        ),
        "spy-id": _eod("SPY", [500.0] * len(_SESSIONS), opens={_EVENT: 499.0}),
        # THIN gaps identically but its IEX hourly bars carry zero volume.
        "thin-id": _eod("THIN", gap_closes, opens={_EVENT: 95.0}),
        # SPLT splits 2:1 on the event day: raw open 50 vs raw prior close 100,
        # but the adjusted gap is zero.
        "split-id": _eod("SPLT", split_closes, split_day=_EVENT if split else None),
    }
    bars = BarStore(config.data_dir)
    bars.publish_eod(eod)
    gap_hourly = gap_hourly or {
        **_flat_hourly(100.0),
        _EVENT: {10: 96.0, 11: 97.0, 12: 97.5, 13: 96.5, 14: 98.5, 15: 98.0},
    }
    hourly = {
        "gap-id": _hourly("GAPY", gap_hourly, bad_ohlc=bad_ohlc),
        "spy-id": _hourly(
            "SPY",
            {**_flat_hourly(500.0), _EVENT: {h: 501.0 for h in _EST_HOURS_UTC}},
        ),
        "thin-id": _hourly("THIN", gap_hourly, volume=0, bad_ohlc=bad_ohlc),
        "split-id": _hourly("SPLT", _flat_hourly(100.0)),
    }
    bars.publish_intraday(hourly, freq="1hour")
    return config


def _observations(config, published):
    return pl.read_parquet(published.observation_path)


def test_study_is_registered_and_hand_calculated_returns_match(tmp_path):
    config = _fixture(tmp_path)
    assert STUDY_NAME in registered_event_studies()

    published = run_registered_event_study(config, STUDY_NAME, _PARAMS)
    observations = _observations(config, published)

    assert observations["instrument_id"].unique().to_list() == ["gap-id"]
    assert observations["event_date"].unique().to_list() == [_EVENT]
    assert observations["period"].unique().to_list() == ["validation"]
    labels = observations["observation_label"].to_list()
    assert labels[0] == "11:00_close_of_10:00_bar"
    assert labels[-1] == "session_close"
    assert len(labels) == 7

    first = observations.row(0, named=True)
    assert first["gap_return"] == pytest.approx(95.0 / 100.0 - 1.0)
    assert first["open_raw"] == 95.0 and first["prior_close_raw"] == 100.0
    assert first["measured_return"] == pytest.approx(96.0 / 95.0 - 1.0)
    assert first["gap_recovered_fraction_raw_basis"] == pytest.approx(0.2)
    assert first["benchmark_return"] == pytest.approx(501.0 / 499.0 - 1.0)
    assert first["excess_return"] == pytest.approx(
        (96.0 / 95.0 - 1.0) - (501.0 / 499.0 - 1.0)
    )
    assert first["checkpoint_available_ts"] == datetime(2024, 1, 8, 16, tzinfo=UTC)
    assert first["max_favorable_excursion"] == pytest.approx(97.0 / 95.0 - 1.0)
    assert first["max_adverse_excursion"] == pytest.approx(95.0 / 95.0 - 1.0)
    assert first["reached_target_at_checkpoint"] is True
    assert first["hourly_density"] == pytest.approx(1.0)
    assert first["event_day_corporate_action"] is False
    assert "09:30-09:59" in first["opening_interval_note"]
    close = observations.row(-1, named=True)
    assert close["measured_return"] == pytest.approx(98.0 / 95.0 - 1.0)
    assert close["checkpoint_available_ts"] == datetime(2024, 1, 8, 21, tzinfo=UTC)
    assert observations["outcome_status"].unique().to_list() == ["evaluable"]

    with MetaStore(config.meta_path) as meta:
        rows = meta.research_metrics(published.run_id)
    by_name = {}
    for row in rows:
        by_name.setdefault(str(row["metric_name"]), []).append(row)
    audit = {str(r["metric_name"]): float(r["value"]) for r in rows}
    # GAPY and THIN are candidates; THIN fails the density screen; SPLT and
    # SPY are never candidates.
    assert audit["event_audit.candidates"] == 2
    assert audit["event_audit.eligible"] == 2
    assert audit["event_audit.selected"] == 1
    assert audit["event_audit.evaluable"] == 1
    assert len(by_name["events"]) == 1
    hit_rates = {str(r["value"]) for r in by_name["hit_rate_target"]}
    assert hit_rates == {"1.0"}
    assert len(by_name["mean_return"]) == 7


def test_future_rows_and_same_day_final_fields_cannot_change_selection(tmp_path):
    config = _fixture(tmp_path)
    before = _observations(
        config, run_registered_event_study(config, STUDY_NAME, _PARAMS)
    )

    bars = BarStore(config.data_dir)
    eod = bars.read_canonical_eod("gap-id")
    perturbed = eod.with_columns(
        pl.when(pl.col("date") >= _EVENT)
        .then(pl.col("close") * 3.0)
        .otherwise(pl.col("close"))
        .alias("close"),
        pl.when(pl.col("date") >= _EVENT)
        .then(pl.col("high") * 3.0)
        .otherwise(pl.col("high"))
        .alias("high"),
        pl.when(pl.col("date") >= _EVENT)
        .then(pl.col("adj_close") * 3.0)
        .otherwise(pl.col("adj_close"))
        .alias("adj_close"),
        pl.when(pl.col("date") >= _EVENT)
        .then(pl.col("adj_high") * 3.0)
        .otherwise(pl.col("adj_high"))
        .alias("adj_high"),
        pl.when(pl.col("date") >= _EVENT)
        .then(pl.lit(999_999_999, dtype=pl.Int64))
        .otherwise(pl.col("volume"))
        .alias("volume"),
    ).drop("instrument_id")
    bars.publish_eod({"gap-id": perturbed.with_columns(pl.lit("GAPY").alias("ticker"))})
    after = _observations(
        config, run_registered_event_study(config, STUDY_NAME, _PARAMS)
    )

    decision_columns = [
        "instrument_id",
        "event_date",
        "observation_label",
        "gap_return",
        "gap_vol_normalized",
        "adv_dollars",
        "realized_vol",
        "prior_5_return",
        "hourly_density",
        "period",
    ]
    # Tripling closes from the event day onward manufactures later gap events;
    # those are real future events. Selection and every decision feature of
    # the original event (and any earlier one) must be byte-identical.
    unchanged = pl.col("event_date") <= _EVENT
    assert (
        before.filter(unchanged)
        .select(decision_columns)
        .equals(after.filter(unchanged).select(decision_columns))
    )
    assert after.filter(pl.col("event_date") > _EVENT).height > 0
    # Only the session-close outcome moved with the perturbed close.
    assert after.filter(unchanged & (pl.col("observation_label") == "session_close"))[
        "measured_return"
    ][0] == pytest.approx(3 * 98.0 / 95.0 - 1.0)


def test_split_day_is_not_a_gap_and_dividend_day_is_flagged(tmp_path):
    config = _fixture(tmp_path, dividend_day=_EVENT, split=True)

    observations = _observations(
        config, run_registered_event_study(config, STUDY_NAME, _PARAMS)
    )

    assert observations["instrument_id"].unique().to_list() == ["gap-id"]
    assert observations["event_day_corporate_action"].unique().to_list() == [True]
    assert observations["gap_recovered_fraction_raw_basis"].null_count() == 7
    assert observations["measured_return"][0] == pytest.approx(96.0 / 95.0 - 1.0)


def test_density_screen_and_missing_checkpoints_are_explicit(tmp_path):
    config = _fixture(
        tmp_path,
        gap_hourly={
            **_flat_hourly(100.0),
            _EVENT: {10: 96.0, 11: 97.0, 12: 97.5, 13: 96.5, 15: 98.0},
        },
        # The 13:00 bar exists but violates OHLC ordering; the 14:00 bar is
        # absent. Both are explicit missing outcomes, not silent drops.
        bad_ohlc={(_EVENT, 13)},
    )
    parameters = {**_PARAMS, "min_hourly_density": 0.0}

    published = run_registered_event_study(config, STUDY_NAME, parameters)
    observations = _observations(config, published)

    assert observations["instrument_id"].unique().sort().to_list() == [
        "gap-id",
        "thin-id",
    ]
    thin = observations.filter(pl.col("instrument_id") == "thin-id")
    assert thin["hourly_density"][0] == pytest.approx(0.0)
    missing = observations.filter(pl.col("outcome_status") == "missing_outcome")
    assert missing.select("instrument_id", "observation_label").rows() == [
        ("gap-id", "14:00_close_of_13:00_bar"),
        ("gap-id", "15:00_close_of_14:00_bar"),
        ("thin-id", "14:00_close_of_13:00_bar"),
        ("thin-id", "15:00_close_of_14:00_bar"),
    ]
    # The invalid 13:00 bar also cannot feed the excursion through 13:00.
    twelve = observations.filter(
        (pl.col("instrument_id") == "gap-id")
        & (pl.col("observation_label") == "13:00_close_of_12:00_bar")
    )
    assert twelve["max_favorable_excursion"][0] == pytest.approx(98.5 / 95.0 - 1.0)
    with MetaStore(config.meta_path) as meta:
        audit = {
            str(r["metric_name"]): float(r["value"])
            for r in meta.research_metrics(published.run_id)
        }
    assert audit["event_audit.selected"] == 2
    assert audit["event_audit.missing_outcome"] == 2
    assert audit["event_audit.evaluable"] == 0


def test_parameters_are_validated_before_any_run(tmp_path):
    config = _fixture(tmp_path)
    with pytest.raises(ValueError, match="unknown gap_recovery parameters"):
        run_gap_recovery_study(config, {"bogus": 1})
    with pytest.raises(ValueError, match="chronological"):
        run_gap_recovery_study(
            config,
            {
                **_PARAMS,
                "periods": {
                    "a": ["2024-01-01", "2024-01-10"],
                    "b": ["2024-01-05", "2024-01-31"],
                },
            },
        )
    with pytest.raises(ValueError, match="benchmark"):
        run_gap_recovery_study(config, {**_PARAMS, "benchmark_ticker": "NOPE"})
