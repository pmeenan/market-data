"""Vectorized event-runner eligibility, gate, and outcome contracts."""

from datetime import UTC, date, datetime

import duckdb
import polars as pl
import pytest

from marketdata.config import Config
from marketdata.research import (
    EventLookback,
    EventQualityPolicy,
    EventStudyGateError,
    ResearchMetric,
    ResearchOutput,
    run_event_study,
    verify_research_input_fingerprint,
)
from marketdata.store.bars import EOD_SCHEMA, INTRADAY_SCHEMA, BarStore
from marketdata.store.meta import MetaStore

_JAN_3 = date(2024, 1, 3)
_JAN_4 = date(2024, 1, 4)
_JAN_5 = date(2024, 1, 5)
_JAN_6 = date(2024, 1, 6)


def _eod_frame(ticker: str, dates: list[date], *, bad_volume: bool = False):
    rows = len(dates)
    opens = [90.0 if day in {_JAN_4, _JAN_6} else 100.0 for day in dates]
    return pl.DataFrame(
        {
            "ticker": [ticker] * rows,
            "date": dates,
            "open": opens,
            "high": [102.0] * rows,
            "low": [89.0] * rows,
            "close": [100.0] * rows,
            "volume": [-1 if bad_volume else 1_000] * rows,
            "adj_open": opens,
            "adj_high": [102.0] * rows,
            "adj_low": [89.0] * rows,
            "adj_close": [100.0] * rows,
            "adj_volume": [1_000] * rows,
            "div_cash": [0.0] * rows,
            "split_factor": [1.0] * rows,
        }
    ).cast(EOD_SCHEMA)


def _hourly_frame(ticker: str, day: date, close: float = 95.0):
    return pl.DataFrame(
        {
            "ticker": [ticker],
            "ts": [datetime(day.year, day.month, day.day, 15, tzinfo=UTC)],
            "open": [94.0],
            "high": [96.0],
            "low": [93.0],
            "close": [close],
            "volume": [1_000],
        }
    ).cast(INTRADAY_SCHEMA)


def _five_minute_frame(ticker: str, *, omit_minute: int | None = None):
    timestamps = [
        datetime(2024, 1, 4, 14, minute, tzinfo=UTC)
        for minute in range(30, 60, 5)
        if minute != omit_minute
    ]
    rows = len(timestamps)
    return pl.DataFrame(
        {
            "ticker": [ticker] * rows,
            "ts": timestamps,
            "open": [100.0] * rows,
            "high": [101.0] * rows,
            "low": [99.0] * rows,
            "close": [100.5] * rows,
            "volume": [1_000] * rows,
        }
    ).cast(INTRADAY_SCHEMA)


def _fixture(tmp_path) -> Config:
    config = Config(tmp_path, None)
    config.ensure_dirs()
    histories = {
        "local-id": ("LOCAL", [date(2023, 12, 20), _JAN_3, _JAN_4]),
        "future-missing-id": ("FUTURE", [_JAN_3, _JAN_4]),
        "lookback-gap-id": ("GAP", [date(2023, 12, 20), _JAN_4]),
        "identity-gap-id": ("IDENT", [_JAN_3, _JAN_4]),
        "weekend-id": ("WEEKEND", [_JAN_5, _JAN_6]),
    }
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, (ticker, _dates) in histories.items():
            meta.upsert_instrument(instrument_id)
            alias_end = (
                _JAN_3 if instrument_id == "identity-gap-id" else date(2024, 1, 31)
            )
            meta.add_instrument_alias(
                instrument_id, ticker, date(2023, 1, 1), alias_end
            )
        # The candidate surface deliberately cannot see this table, and the
        # selected FUTURE event is not a member of this stored universe.
        meta.set_universe(2024, [{"ticker": "LOCAL", "rank": 1}])
    bars = BarStore(config.data_dir)
    bars.publish_eod(
        {
            instrument_id: _eod_frame(ticker, dates)
            for instrument_id, (ticker, dates) in histories.items()
        }
    )
    bars.publish_intraday({"local-id": _hourly_frame("LOCAL", _JAN_4)}, freq="1hour")
    return config


def _build_candidates(context):
    with pytest.raises(duckdb.CatalogException):
        context.connection.execute("SELECT * FROM meta.universe").fetchall()
    frame = context.connection.execute(
        """SELECT instrument_id, date AS event_date, open AS raw_open, adj_open,
                  lag(adj_close) OVER (
                      PARTITION BY instrument_id ORDER BY date
                  ) AS prior_close
             FROM eod
         QUALIFY date IN (DATE '2024-01-04', DATE '2024-01-06')"""
    ).pl()
    return frame.with_columns(
        pl.when(pl.col("event_date") == _JAN_6)
        .then(pl.lit(_JAN_5))
        .otherwise(pl.lit(_JAN_3))
        .alias("lookback_start"),
        pl.when(pl.col("event_date") == _JAN_6)
        .then(pl.lit(_JAN_5))
        .otherwise(pl.lit(_JAN_3))
        .alias("lookback_end"),
        (
            pl.col("event_date").cast(pl.Datetime("us"))
            + pl.duration(hours=14, minutes=30)
        )
        .dt.replace_time_zone("UTC")
        .alias("decision_ts"),
    )


def _select_gap_events(context, candidates):
    assert context.dataset_keys == ("eod",)
    return candidates.filter(
        (pl.col("adj_open") / pl.col("prior_close") - 1.0) <= -0.05
    )


def _observe_hourly(context, selected):
    assert context.dataset_keys == ("eod", "intraday_1hour")
    context.connection.register("selected_events", selected)
    observations = context.connection.execute(
        """SELECT selected.instrument_id, selected.event_date,
                  '11:00_close_of_10:00_bar' AS observation_label,
                  hourly.ts + INTERVAL '1 hour' AS checkpoint_available_ts,
                  CASE WHEN hourly.close IS NULL THEN 'missing_outcome'
                       ELSE 'evaluable' END AS outcome_status,
                  hourly.close AS checkpoint_price,
                  hourly.close / selected.raw_open - 1.0 AS measured_return
             FROM selected_events AS selected
             LEFT JOIN intraday_1hour AS hourly
               ON hourly.instrument_id = selected.instrument_id
              AND CAST(hourly.ts AS DATE) = selected.event_date
              AND date_part('hour', timezone('America/New_York', hourly.ts)) = 10
            ORDER BY selected.instrument_id"""
    ).pl()
    mean_return = observations["measured_return"].mean()
    return ResearchOutput(
        observations,
        metrics=(
            ResearchMetric(
                "mean_checkpoint_return",
                float(mean_return) if mean_return is not None else 0.0,
                dimensions={"checkpoint": "11:00_close_of_10:00_bar"},
                unit="return",
            ),
        ),
    )


def _run_fixture(
    config: Config,
    *,
    build_candidates=_build_candidates,
    select_events=_select_gap_events,
    observe_events=_observe_hourly,
):
    return run_event_study(
        config,
        study_name="runner-fixture",
        study_schema_version=1,
        parameters={"gap_threshold": -0.05},
        selection_dataset_keys=["eod"],
        outcome_dataset_keys=["intraday_1hour"],
        lookbacks=[EventLookback("eod", "lookback_start", "lookback_end")],
        quality_policy=EventQualityPolicy(
            dataset_keys=("eod", "intraday_1hour"),
            blocking_checks=(
                "duplicate_keys",
                "ohlc_invariants",
                "negative_values",
                "off_session_intraday",
            ),
            start=_JAN_3,
            end=_JAN_6,
        ),
        build_candidates=build_candidates,
        select_events=select_events,
        observe_events=observe_events,
    )


def _metric_values(config: Config, run_id: str) -> dict[str, float]:
    with MetaStore(config.meta_path) as meta:
        return {
            str(row["metric_name"]): float(row["value"])
            for row in meta.research_metrics(run_id)
            if str(row["metric_name"]).startswith("event_audit.")
            and str(row["metric_name"]) != "event_audit.quality_findings"
        }


def test_runner_freezes_locally_eligible_events_before_outcomes(tmp_path):
    config = _fixture(tmp_path)

    first = _run_fixture(config)

    observations = pl.read_parquet(first.observation_path).sort("instrument_id")
    assert observations.select("instrument_id", "outcome_status").rows() == [
        ("future-missing-id", "missing_outcome"),
        ("local-id", "evaluable"),
    ]
    metrics = _metric_values(config, first.run_id)
    assert metrics["event_audit.candidates"] == 5
    assert metrics["event_audit.eligible"] == 2
    assert metrics["event_audit.identity_excluded"] == 1
    assert metrics["event_audit.calendar_excluded"] == 1
    assert metrics["event_audit.lookback_incomplete"] == 1
    assert metrics["event_audit.selected"] == 2
    assert metrics["event_audit.evaluable"] == 1
    assert metrics["event_audit.missing_outcome"] == 1
    with MetaStore(config.meta_path) as meta:
        engine = meta.research_parameters(first.run_id)["_event_runner"]
    assert engine["semantics"] == "event_study_without_portfolio_or_order_simulation"
    assert engine["quality_policy"]["blocking_checks"] == [
        "duplicate_keys",
        "ohlc_invariants",
        "negative_values",
        "off_session_intraday",
    ]

    BarStore(config.data_dir).publish_intraday(
        {"future-missing-id": _hourly_frame("FUTURE", _JAN_4, close=96.0)},
        freq="1hour",
    )
    second = _run_fixture(config)
    later = pl.read_parquet(second.observation_path).sort("instrument_id")

    assert later["instrument_id"].to_list() == observations["instrument_id"].to_list()
    assert later["outcome_status"].to_list() == ["evaluable", "evaluable"]
    assert _metric_values(config, second.run_id)["event_audit.selected"] == 2


@pytest.mark.parametrize("adjustment", [0.5, 0.98])
def test_same_session_return_ignores_common_historical_adjustment(tmp_path, adjustment):
    """Split/dividend-like historical restatements cannot manufacture a rebound."""
    config = _fixture(tmp_path)
    frame = _eod_frame("LOCAL", [date(2023, 12, 20), _JAN_3, _JAN_4])
    frame = frame.with_columns(
        (pl.col(name) * adjustment).alias(name)
        for name in ("adj_open", "adj_high", "adj_low", "adj_close")
    )
    BarStore(config.data_dir).publish_eod({"local-id": frame})

    published = _run_fixture(config)
    observation = pl.read_parquet(published.observation_path).filter(
        pl.col("instrument_id") == "local-id"
    )
    assert observation["measured_return"].item() == pytest.approx(95.0 / 90.0 - 1.0)
    assert observation["checkpoint_available_ts"].item() == datetime(
        2024, 1, 4, 16, tzinfo=UTC
    )
    assert observation["observation_label"].item() == "11:00_close_of_10:00_bar"


def test_example_signal_ignores_future_selection_rows(tmp_path):
    """The example obeys causality even though the runner exposes full EOD views."""
    config = _fixture(tmp_path)
    before = pl.read_parquet(_run_fixture(config).observation_path)
    future = _eod_frame("LOCAL", [_JAN_5]).with_columns(
        pl.lit(1_000.0).alias("high"),
        pl.lit(900.0).alias("close"),
        pl.lit(1_000.0).alias("adj_high"),
        pl.lit(900.0).alias("adj_close"),
        pl.lit(999_999, dtype=pl.Int64).alias("volume"),
    )
    BarStore(config.data_dir).publish_eod({"local-id": future})
    after = pl.read_parquet(_run_fixture(config).observation_path)
    columns = ["instrument_id", "event_date", "outcome_status", "measured_return"]
    assert (
        before.select(columns)
        .sort("instrument_id")
        .equals(after.select(columns).sort("instrument_id"))
    )


def test_empty_candidate_scope_publishes_an_empty_success(tmp_path):
    config = _fixture(tmp_path)

    published = _run_fixture(
        config,
        build_candidates=lambda context: _build_candidates(context).head(0),
    )

    assert pl.read_parquet(published.observation_path).is_empty()
    metrics = _metric_values(config, published.run_id)
    assert metrics["event_audit.candidates"] == 0
    assert metrics["event_audit.selected"] == 0
    with MetaStore(config.meta_path) as meta:
        assert meta.research_run(published.run_id)["status"] == "succeeded"


def test_empty_outcome_dataset_scope_is_valid_for_selected_events(tmp_path):
    config = _fixture(tmp_path)

    published = _run_fixture(
        config,
        build_candidates=lambda context: _build_candidates(context).filter(
            pl.col("instrument_id") == "future-missing-id"
        ),
    )

    observations = pl.read_parquet(published.observation_path)
    assert observations.select("instrument_id", "outcome_status").rows() == [
        ("future-missing-id", "missing_outcome")
    ]
    with MetaStore(config.meta_path) as meta:
        assert meta.research_run(published.run_id)["status"] == "succeeded"


def test_selector_must_return_unchanged_audited_rows(tmp_path):
    config = _fixture(tmp_path)

    def mutate_prices(context, eligible):
        return eligible.with_columns((pl.col("adj_open") + 1.0).alias("adj_open"))

    with pytest.raises(ValueError, match="must not mutate"):
        _run_fixture(config, select_events=mutate_prices)


def test_event_outcome_metrics_are_mutually_exclusive(tmp_path):
    config = _fixture(tmp_path)

    def mixed_outcomes(context, selected):
        observations = selected.select(*("instrument_id", "event_date")).join(
            pl.DataFrame({"observation_label": ["first", "second"]}), how="cross"
        )
        return ResearchOutput(
            observations.with_columns(
                pl.when(
                    (pl.col("instrument_id") == "local-id")
                    & (pl.col("observation_label") == "second")
                )
                .then(pl.lit("missing_outcome"))
                .otherwise(pl.lit("evaluable"))
                .alias("outcome_status")
            )
        )

    published = _run_fixture(config, observe_events=mixed_outcomes)

    metrics = _metric_values(config, published.run_id)
    assert metrics["event_audit.selected"] == 2
    assert metrics["event_audit.evaluable"] == 1
    assert metrics["event_audit.missing_outcome"] == 1
    assert "event_audit.quality_excluded" not in metrics


def test_alias_metadata_changes_invalidate_the_input_fingerprint(tmp_path):
    config = _fixture(tmp_path)
    published = _run_fixture(config)
    assert verify_research_input_fingerprint(config, published.run_id).matches

    with MetaStore(config.meta_path) as meta:
        meta.add_instrument_alias("local-id", "LOCAL2", _JAN_4, date(2024, 1, 31))

    status = verify_research_input_fingerprint(config, published.run_id)
    assert not status.matches
    assert status.metadata_changed
    assert status.changed_files == ()


def test_declared_quality_failure_blocks_publication(tmp_path):
    config = _fixture(tmp_path)
    BarStore(config.data_dir).publish_eod(
        {"local-id": _eod_frame("LOCAL", [_JAN_4], bad_volume=True)}
    )

    with pytest.raises(EventStudyGateError, match="negative_values"):
        _run_fixture(config)

    with MetaStore(config.meta_path) as meta:
        run = meta.research_runs()[-1]
        assert run["status"] == "failed"
        assert "negative_values" in run["error_summary"]
        assert run["observation_path"] is None


def test_observer_cannot_drop_a_selected_event_with_no_outcome(tmp_path):
    config = _fixture(tmp_path)

    def drop_missing(context, selected):
        output = _observe_hourly(context, selected)
        return ResearchOutput(
            output.observations.filter(pl.col("outcome_status") == "evaluable")
        )

    with pytest.raises(ValueError, match="retain every selected event"):
        _run_fixture(config, observe_events=drop_missing)

    with MetaStore(config.meta_path) as meta:
        assert meta.research_runs()[-1]["status"] == "failed"


def test_eod_lookback_cannot_include_the_unfinished_event_day(tmp_path):
    config = _fixture(tmp_path)

    def noncausal(context):
        return _build_candidates(context).with_columns(
            pl.col("event_date").alias("lookback_end")
        )

    with pytest.raises(ValueError, match="invalid or noncausal"):
        _run_fixture(config, build_candidates=noncausal)


def test_candidate_instrument_ids_must_be_canonical(tmp_path):
    config = _fixture(tmp_path)

    def padded(context):
        return _build_candidates(context).with_columns(
            (pl.lit(" ") + pl.col("instrument_id")).alias("instrument_id")
        )

    with pytest.raises(ValueError, match="must not contain whitespace"):
        _run_fixture(config, build_candidates=padded)


def test_static_quality_policy_errors_do_not_create_catalog_rows(tmp_path):
    config = _fixture(tmp_path)

    with pytest.raises(ValueError, match="zero_volume_run_length"):
        run_event_study(
            config,
            study_name="invalid-quality-policy",
            study_schema_version=1,
            parameters={},
            selection_dataset_keys=["eod"],
            lookbacks=[EventLookback("eod", "lookback_start", "lookback_end")],
            quality_policy=EventQualityPolicy(
                dataset_keys=("eod",),
                blocking_checks=("duplicate_keys",),
                start=_JAN_3,
                end=_JAN_4,
                zero_volume_run_length=1,
            ),
            build_candidates=_build_candidates,
            select_events=_select_gap_events,
            observe_events=_observe_hourly,
        )

    with MetaStore(config.meta_path) as meta:
        assert meta.research_runs() == []


def test_zero_expected_intraday_labels_are_a_declaration_error(tmp_path):
    config = _fixture(tmp_path)

    def candidates(context):
        return pl.DataFrame(
            {
                "instrument_id": ["local-id"],
                "event_date": [_JAN_4],
                "decision_ts": [datetime(2024, 1, 4, 16, 0, tzinfo=UTC)],
                "window_start": [datetime(2024, 1, 4, 14, 30, tzinfo=UTC)],
                "window_end": [datetime(2024, 1, 4, 14, 55, tzinfo=UTC)],
            }
        )

    with pytest.raises(ValueError, match="contains no expected bar labels"):
        run_event_study(
            config,
            study_name="empty-hourly-window",
            study_schema_version=1,
            parameters={},
            selection_dataset_keys=["intraday_1hour"],
            lookbacks=[EventLookback("intraday_1hour", "window_start", "window_end")],
            quality_policy=EventQualityPolicy(
                dataset_keys=("intraday_1hour",),
                blocking_checks=("duplicate_keys",),
                start=_JAN_4,
                end=_JAN_4,
            ),
            build_candidates=candidates,
            select_events=lambda context, eligible: eligible,
            observe_events=lambda context, selected: ResearchOutput(
                pl.DataFrame(
                    schema={
                        "instrument_id": pl.Utf8,
                        "event_date": pl.Date,
                        "observation_label": pl.Utf8,
                        "outcome_status": pl.Utf8,
                    }
                )
            ),
        )


def test_intraday_lookback_requires_every_completed_frequency_label(tmp_path):
    config = Config(tmp_path, None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, ticker in (("complete-id", "FULL"), ("gap-id", "GAP")):
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(
                instrument_id, ticker, date(2024, 1, 1), date(2024, 1, 31)
            )
    BarStore(config.data_dir).publish_intraday(
        {
            "complete-id": _five_minute_frame("FULL"),
            "gap-id": _five_minute_frame("GAP", omit_minute=45),
        },
        freq="5min",
    )

    def candidates(context):
        frame = context.connection.execute(
            "SELECT DISTINCT instrument_id FROM intraday_5min ORDER BY instrument_id"
        ).pl()
        return frame.with_columns(
            pl.lit(_JAN_4).alias("event_date"),
            pl.lit(datetime(2024, 1, 4, 15, 0, tzinfo=UTC)).alias("decision_ts"),
            pl.lit(datetime(2024, 1, 4, 14, 30, tzinfo=UTC)).alias("window_start"),
            pl.lit(datetime(2024, 1, 4, 14, 55, tzinfo=UTC)).alias("window_end"),
        )

    def observations(context, selected):
        return ResearchOutput(
            selected.select("instrument_id", "event_date").with_columns(
                observation_label=pl.lit("decision"),
                outcome_status=pl.lit("evaluable"),
            )
        )

    published = run_event_study(
        config,
        study_name="intraday-audit-fixture",
        study_schema_version=1,
        parameters={},
        selection_dataset_keys=["intraday_5min"],
        lookbacks=[EventLookback("intraday_5min", "window_start", "window_end")],
        quality_policy=EventQualityPolicy(
            dataset_keys=("intraday_5min",),
            blocking_checks=(
                "duplicate_keys",
                "ohlc_invariants",
                "negative_values",
                "off_session_intraday",
            ),
            start=_JAN_4,
            end=_JAN_4,
        ),
        build_candidates=candidates,
        select_events=lambda context, eligible: eligible,
        observe_events=observations,
    )

    result = pl.read_parquet(published.observation_path)
    assert result["instrument_id"].to_list() == ["complete-id"]
    metrics = _metric_values(config, published.run_id)
    assert metrics["event_audit.eligible"] == 1
    assert metrics["event_audit.lookback_incomplete"] == 1


@pytest.mark.parametrize(
    "blocking_check",
    ["missing_expected_sessions", "coverage_delisting_summary"],
)
def test_full_history_state_cannot_be_a_local_event_gate(tmp_path, blocking_check):
    config = _fixture(tmp_path)

    with pytest.raises(ValueError, match="full-history coverage"):
        run_event_study(
            config,
            study_name="invalid-gate",
            study_schema_version=1,
            parameters={},
            selection_dataset_keys=["eod"],
            lookbacks=[EventLookback("eod", "lookback_start", "lookback_end")],
            quality_policy=EventQualityPolicy(
                dataset_keys=("eod",),
                blocking_checks=(blocking_check,),
                start=_JAN_3,
                end=_JAN_4,
            ),
            build_candidates=_build_candidates,
            select_events=_select_gap_events,
            observe_events=_observe_hourly,
        )

    with MetaStore(config.meta_path) as meta:
        assert meta.research_runs() == []
