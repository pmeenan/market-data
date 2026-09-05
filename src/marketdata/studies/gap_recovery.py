"""Coarse morning gap-down recovery study (M3).

Descriptive event study only: it measures conditional same-session returns
after a large opening gap down, using the shared as-of feature path in
:mod:`marketdata.features`. It does not simulate fills, capital, or orders
(D-015/D-036); those belong to the M5 simulator.

Price bases (research-protocol.md): the gap and every prior-window feature use
adjusted EOD prices; every same-session return divides a raw hourly close by
the raw EOD open. The two are never mixed in one ratio. The recovered-gap
fraction is computed on the raw basis alone (raw prior close, raw open, raw
checkpoint) and is therefore meaningless on a corporate-action day; those
events are flagged and excluded from fraction summaries but retained.

Timing: direct hourly bars are fixed clock-hour bins from 10:00 New York time
(D-012), so the earliest checkpoint is the 10:00 bar's close, available at
11:00. The 09:30-09:59 interval is deliberately absent from this coarse study
and is labelled as such; the five-minute extension is M4 work.

Quality gates: ``duplicate_keys`` and ``split_sanity`` block publication
because the study cannot compensate for them. The stored vendor data always
carries a small residue of synthesized off-session rows (RE-004), hourly
OHLC-ordering violations (about 0.03% of rows), and a handful of negative
EOD volumes; the runner still measures and persists those finding counts,
while the study excludes off-session and OHLC-invalid bars itself (a
checkpoint that lands on one is an explicit ``missing_outcome``) and treats
a negative volume as unknown in the liquidity feature.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

import polars as pl

from marketdata.config import Config
from marketdata.features import (
    DEFAULT_LOOKBACK_SESSIONS,
    register_eod_decision_features,
    register_hourly_density_features,
    register_session_opens,
)
from marketdata.research import (
    EventLookback,
    EventQualityPolicy,
    EventStudyContext,
    PublishedResearchRun,
    ResearchMetric,
    ResearchOutput,
    run_event_study,
)
from marketdata.store.meta import MetaStore

STUDY_NAME = "gap_recovery"
STUDY_SCHEMA_VERSION = 1
# Bar start hours (New York time) whose closes serve as checkpoints. The close
# of the H:00 bar is available at (H+1):00.
CHECKPOINT_HOURS: tuple[int, ...] = (10, 11, 12, 13, 14, 15)
SESSION_CLOSE_LABEL = "session_close"
OPENING_INTERVAL_NOTE = (
    "09:30-09:59 has no direct hourly bar (D-012); the first checkpoint is the "
    "10:00 bar close, available at 11:00"
)
DEFAULT_PARAMETERS: dict[str, Any] = {
    "start": "2017-01-03",
    "end": "2026-08-27",
    "gap_threshold": -0.03,
    "min_adv_dollars": 50_000_000.0,
    "lookback_sessions": DEFAULT_LOOKBACK_SESSIONS,
    "min_hourly_density": 0.9,
    "benchmark_ticker": "SPY",
    "target_return": 0.01,
    # Frozen chronological periods (research-protocol.md). Metrics are
    # published for development and validation only; test events are
    # observed but never summarized by this study.
    "periods": {
        "development": ["2017-01-01", "2022-12-31"],
        "validation": ["2023-01-01", "2024-12-31"],
        "test": ["2025-01-01", "2099-12-31"],
    },
    "metric_periods": ["development", "validation"],
}
_DECISION_FEATURE_COLUMNS = (
    "open_raw",
    "adj_open",
    "prior_close_raw",
    "prior_adj_close",
    "gap_return",
    "gap_vol_normalized",
    "adv_dollars",
    "realized_vol",
    "prior_5_return",
    "hourly_density",
    "event_day_corporate_action",
    "benchmark_gap_return",
    "benchmark_open_raw",
    "period",
)


def run_gap_recovery_study(
    config: Config, parameters: Mapping[str, Any]
) -> PublishedResearchRun:
    """Publish one coarse gap-recovery run through the shared event runner."""
    params = _normalize_parameters(parameters)
    start = date.fromisoformat(params["start"])
    end = date.fromisoformat(params["end"])
    lookback = int(params["lookback_sessions"])
    with MetaStore(config.meta_path) as meta:
        benchmark_id = _resolve_benchmark(
            meta, str(params["benchmark_ticker"]), start, end
        )
    params["benchmark_instrument_id"] = benchmark_id
    periods = {
        name: (date.fromisoformat(bounds[0]), date.fromisoformat(bounds[1]))
        for name, bounds in params["periods"].items()
    }

    def build_candidates(context: EventStudyContext) -> pl.DataFrame:
        con = context.connection
        register_eod_decision_features(con, lookback_sessions=lookback)
        register_session_opens(con, start, end)
        register_hourly_density_features(con, start, end, lookback_sessions=lookback)
        frame = con.execute(
            """SELECT f.instrument_id, f.date AS event_date,
                      s.session_open AS decision_ts,
                      f.lookback_start_date AS lookback_start,
                      f.prior_date AS lookback_end,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 10 HOUR)
                          AT TIME ZONE 'America/New_York' AS hourly_lookback_start,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 10 HOUR)
                          AT TIME ZONE 'America/New_York' AS hourly_lookback_end,
                      f.open_raw, f.adj_open, f.prior_close_raw, f.prior_adj_close,
                      f.gap_return, f.gap_vol_normalized, f.adv_dollars,
                      f.realized_vol, f.prior_5_return,
                      f.event_day_corporate_action,
                      d.hourly_density,
                      b.gap_return AS benchmark_gap_return,
                      b.open_raw AS benchmark_open_raw
                 FROM eod_decision_features AS f
                 JOIN session_opens AS s ON s.session_date = f.date
                 LEFT JOIN hourly_density_features AS d
                   ON d.instrument_id = f.instrument_id AND d.date = f.date
                 LEFT JOIN eod_decision_features AS b
                   ON b.instrument_id = ? AND b.date = f.date
                WHERE f.date BETWEEN ? AND ?
                  AND f.instrument_id <> ?
                  AND f.prior_sessions_present = ?
                  AND f.gap_return <= ?
                  AND f.adv_dollars >= ?
                ORDER BY f.instrument_id, f.date""",
            [
                benchmark_id,
                start,
                end,
                benchmark_id,
                lookback,
                float(params["gap_threshold"]),
                float(params["min_adv_dollars"]),
            ],
        ).pl()
        return frame.with_columns(_period_expression(periods).alias("period"))

    def select_events(context: EventStudyContext, candidates: pl.DataFrame):
        return candidates.filter(
            pl.col("hourly_density").fill_null(0.0)
            >= float(params["min_hourly_density"])
        )

    def observe_events(context: EventStudyContext, selected: pl.DataFrame):
        return _observe(context, selected, params, benchmark_id)

    return run_event_study(
        config,
        study_name=STUDY_NAME,
        study_schema_version=STUDY_SCHEMA_VERSION,
        parameters={**params, "opening_interval_note": OPENING_INTERVAL_NOTE},
        selection_dataset_keys=["eod", "intraday_1hour"],
        outcome_dataset_keys=["intraday_1hour"],
        lookbacks=[
            EventLookback("eod", "lookback_start", "lookback_end"),
            EventLookback(
                "intraday_1hour", "hourly_lookback_start", "hourly_lookback_end"
            ),
        ],
        quality_policy=EventQualityPolicy(
            dataset_keys=("eod", "intraday_1hour"),
            blocking_checks=("duplicate_keys", "split_sanity"),
            start=start,
            end=end,
        ),
        build_candidates=build_candidates,
        select_events=select_events,
        observe_events=observe_events,
    )


def _observe(
    context: EventStudyContext,
    selected: pl.DataFrame,
    params: Mapping[str, Any],
    benchmark_id: str,
) -> ResearchOutput:
    con = context.connection
    con.register("selected_events", selected)
    checkpoints = pl.DataFrame(
        {
            "checkpoint_hour": list(CHECKPOINT_HOURS),
            "observation_label": [
                f"{hour + 1:02d}:00_close_of_{hour:02d}:00_bar"
                for hour in CHECKPOINT_HOURS
            ],
        },
        schema={"checkpoint_hour": pl.Int32, "observation_label": pl.Utf8},
    )
    con.register("checkpoints", checkpoints)
    start = selected["event_date"].min() if selected.height else None
    end = selected["event_date"].max() if selected.height else None
    if start is not None:
        register_session_opens(con, start, end, view_name="outcome_sessions")
    else:
        con.execute(
            "CREATE OR REPLACE VIEW outcome_sessions AS "
            "SELECT NULL::DATE AS session_date, NULL::TIMESTAMPTZ AS session_open, "
            "NULL::TIMESTAMPTZ AS session_close, NULL::BOOLEAN AS is_early_close "
            "WHERE false"
        )
    hourly = con.execute(
        """WITH bars AS (
               SELECT h.instrument_id, h.ts, h.open, h.high, h.low, h.close,
                      CAST(h.ts AT TIME ZONE 'America/New_York' AS DATE) AS session_date,
                      hour(h.ts AT TIME ZONE 'America/New_York') AS ny_hour,
                      minute(h.ts AT TIME ZONE 'America/New_York') AS ny_minute
                 FROM intraday_1hour AS h
                WHERE h.instrument_id IN (
                        SELECT instrument_id FROM selected_events UNION SELECT ?
                      )
                  AND CAST(h.ts AT TIME ZONE 'America/New_York' AS DATE) IN (
                        SELECT DISTINCT event_date FROM selected_events
                      )
           ),
           valid AS (
               SELECT bars.*
                 FROM bars
                 JOIN outcome_sessions AS s ON s.session_date = bars.session_date
                WHERE bars.ny_minute = 0
                  AND bars.ny_hour >= 10
                  AND bars.ts + INTERVAL 1 HOUR <= s.session_close
                  AND bars.high >= greatest(bars.open, bars.close)
                  AND bars.low <= least(bars.open, bars.close)
                  AND bars.close > 0
           ),
           grid AS (
               SELECT e.instrument_id, e.event_date, e.open_raw, e.prior_close_raw,
                      e.benchmark_open_raw, c.checkpoint_hour, c.observation_label
                 FROM selected_events AS e CROSS JOIN checkpoints AS c
           )
           SELECT g.instrument_id, g.event_date, g.observation_label,
                  g.checkpoint_hour,
                  own.ts + INTERVAL 1 HOUR AS checkpoint_available_ts,
                  own.close AS checkpoint_price,
                  (SELECT max(high) FROM valid AS v
                    WHERE v.instrument_id = g.instrument_id
                      AND v.session_date = g.event_date
                      AND v.ny_hour <= g.checkpoint_hour) AS max_high_through_checkpoint,
                  (SELECT min(low) FROM valid AS v
                    WHERE v.instrument_id = g.instrument_id
                      AND v.session_date = g.event_date
                      AND v.ny_hour <= g.checkpoint_hour) AS min_low_through_checkpoint,
                  bench.close AS benchmark_checkpoint_price
             FROM grid AS g
             LEFT JOIN valid AS own
               ON own.instrument_id = g.instrument_id
              AND own.session_date = g.event_date
              AND own.ny_hour = g.checkpoint_hour
             LEFT JOIN valid AS bench
               ON bench.instrument_id = ?
              AND bench.session_date = g.event_date
              AND bench.ny_hour = g.checkpoint_hour
            ORDER BY g.instrument_id, g.event_date, g.checkpoint_hour""",
        [benchmark_id, benchmark_id],
    ).pl()
    closes = con.execute(
        """SELECT e.instrument_id, e.event_date,
                  ? AS observation_label,
                  CAST(NULL AS INTEGER) AS checkpoint_hour,
                  s.session_close AS checkpoint_available_ts,
                  own.close AS checkpoint_price,
                  own.high AS max_high_through_checkpoint,
                  own.low AS min_low_through_checkpoint,
                  bench.close AS benchmark_checkpoint_price
             FROM selected_events AS e
             JOIN outcome_sessions AS s ON s.session_date = e.event_date
             LEFT JOIN eod AS own
               ON own.instrument_id = e.instrument_id AND own.date = e.event_date
             LEFT JOIN eod AS bench
               ON bench.instrument_id = ? AND bench.date = e.event_date
            ORDER BY e.instrument_id, e.event_date""",
        [SESSION_CLOSE_LABEL, benchmark_id],
    ).pl()
    outcomes = pl.concat(
        [hourly.select(closes.columns), closes], how="vertical_relaxed"
    )
    observations = (
        selected.select("instrument_id", "event_date", *_DECISION_FEATURE_COLUMNS)
        .join(outcomes, on=["instrument_id", "event_date"], how="inner")
        .with_columns(
            pl.when(pl.col("checkpoint_price").is_null())
            .then(pl.lit("missing_outcome"))
            .otherwise(pl.lit("evaluable"))
            .alias("outcome_status"),
            (pl.col("checkpoint_price") / pl.col("open_raw") - 1.0).alias(
                "measured_return"
            ),
            (pl.col("max_high_through_checkpoint") / pl.col("open_raw") - 1.0).alias(
                "max_favorable_excursion"
            ),
            (pl.col("min_low_through_checkpoint") / pl.col("open_raw") - 1.0).alias(
                "max_adverse_excursion"
            ),
            (
                pl.col("benchmark_checkpoint_price") / pl.col("benchmark_open_raw")
                - 1.0
            ).alias("benchmark_return"),
        )
        .with_columns(
            (pl.col("measured_return") - pl.col("benchmark_return")).alias(
                "excess_return"
            ),
            pl.when(
                ~pl.col("event_day_corporate_action")
                & (pl.col("prior_close_raw") != pl.col("open_raw"))
            )
            .then(
                (pl.col("checkpoint_price") - pl.col("open_raw"))
                / (pl.col("prior_close_raw") - pl.col("open_raw"))
            )
            .otherwise(None)
            .alias("gap_recovered_fraction_raw_basis"),
            (pl.col("measured_return") >= float(params["target_return"])).alias(
                "reached_target_at_checkpoint"
            ),
            pl.lit(OPENING_INTERVAL_NOTE).alias("opening_interval_note"),
        )
        .sort("instrument_id", "event_date", "checkpoint_hour", nulls_last=True)
        .drop("checkpoint_hour")
    )
    return ResearchOutput(observations, metrics=tuple(_metrics(observations, params)))


def _metrics(
    observations: pl.DataFrame, params: Mapping[str, Any]
) -> list[ResearchMetric]:
    metrics: list[ResearchMetric] = []
    if observations.is_empty():
        return metrics
    events = observations.select("instrument_id", "event_date", "period").unique()
    for period, count in events.group_by("period").len().sort("period").iter_rows():
        metrics.append(
            ResearchMetric(
                "events", int(count), dimensions={"period": str(period)}, unit="events"
            )
        )
    years = events.with_columns(pl.col("event_date").dt.year().alias("year"))
    for year, count in years.group_by("year").len().sort("year").iter_rows():
        metrics.append(
            ResearchMetric(
                "events_by_year",
                int(count),
                dimensions={"year": str(year)},
                unit="events",
            )
        )
    reportable = observations.filter(
        pl.col("period").is_in(list(params["metric_periods"]))
    )
    for (period, label), frame in reportable.group_by(
        "period", "observation_label", maintain_order=True
    ):
        dims = {"period": str(period), "checkpoint": str(label)}
        evaluable = frame.filter(pl.col("outcome_status") == "evaluable")
        selected_count = frame.height
        metrics.append(
            ResearchMetric(
                "evaluable", evaluable.height, dimensions=dims, unit="events"
            )
        )
        metrics.append(
            ResearchMetric(
                "missing_outcome",
                selected_count - evaluable.height,
                dimensions=dims,
                unit="events",
            )
        )
        if evaluable.is_empty():
            continue
        returns = evaluable["measured_return"]
        hits = int(evaluable["reached_target_at_checkpoint"].sum())
        stats = {
            "mean_return": returns.mean(),
            "median_return": returns.median(),
            "p10_return": returns.quantile(0.10, interpolation="linear"),
            "p90_return": returns.quantile(0.90, interpolation="linear"),
            "mean_excess_return": evaluable["excess_return"].mean(),
            "mean_favorable_excursion": evaluable["max_favorable_excursion"].mean(),
            "mean_adverse_excursion": evaluable["max_adverse_excursion"].mean(),
            "mean_gap_recovered_fraction": evaluable[
                "gap_recovered_fraction_raw_basis"
            ].mean(),
        }
        for name, value in stats.items():
            if value is not None:
                metrics.append(
                    ResearchMetric(name, float(value), dimensions=dims, unit="return")
                )
        metrics.append(
            ResearchMetric(
                "hit_rate_target",
                hits / evaluable.height,
                dimensions=dims,
                unit="fraction",
            )
        )
        # Missing-outcome sensitivity: count every unobservable event as a miss.
        metrics.append(
            ResearchMetric(
                "hit_rate_target_missing_as_miss",
                hits / selected_count,
                dimensions=dims,
                unit="fraction",
            )
        )
    return metrics


def _period_expression(periods: Mapping[str, tuple[date, date]]) -> pl.Expr:
    expression: pl.Expr | None = None
    for name, (start, end) in periods.items():
        condition = pl.col("event_date").is_between(start, end, closed="both")
        expression = (
            pl.when(condition).then(pl.lit(name))
            if expression is None
            else expression.when(condition).then(pl.lit(name))
        )
    if expression is None:
        return pl.lit("unassigned")
    return expression.otherwise(pl.lit("unassigned"))


def _normalize_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    unknown = sorted(set(parameters) - set(DEFAULT_PARAMETERS))
    if unknown:
        raise ValueError(f"unknown gap_recovery parameters: {unknown}")
    params: dict[str, Any] = {**DEFAULT_PARAMETERS, **dict(parameters)}
    start = date.fromisoformat(str(params["start"]))
    end = date.fromisoformat(str(params["end"]))
    if start > end:
        raise ValueError("gap_recovery start must not be after end")
    params["start"], params["end"] = start.isoformat(), end.isoformat()
    if float(params["gap_threshold"]) >= 0:
        raise ValueError("gap_threshold must be negative (a gap down)")
    if float(params["min_adv_dollars"]) < 0:
        raise ValueError("min_adv_dollars must not be negative")
    if int(params["lookback_sessions"]) < 6:
        raise ValueError("lookback_sessions must be at least 6 for the trend feature")
    if not 0.0 <= float(params["min_hourly_density"]) <= 1.0:
        raise ValueError("min_hourly_density must be between 0 and 1")
    if float(params["target_return"]) <= 0:
        raise ValueError("target_return must be positive")
    periods = params["periods"]
    if not isinstance(periods, Mapping) or not periods:
        raise ValueError("periods must be a non-empty mapping")
    normalized_periods: dict[str, list[str]] = {}
    previous_end: date | None = None
    for name, bounds in periods.items():
        if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            raise ValueError(f"period {name!r} must be a [start, end] pair")
        p_start, p_end = (date.fromisoformat(str(value)) for value in bounds)
        if p_start > p_end:
            raise ValueError(f"period {name!r} start is after its end")
        if previous_end is not None and p_start <= previous_end:
            raise ValueError("periods must be chronological and non-overlapping")
        previous_end = p_end
        normalized_periods[str(name)] = [p_start.isoformat(), p_end.isoformat()]
    params["periods"] = normalized_periods
    metric_periods = [str(name) for name in params["metric_periods"]]
    unknown_periods = sorted(set(metric_periods) - set(normalized_periods))
    if unknown_periods:
        raise ValueError(f"metric_periods are not declared periods: {unknown_periods}")
    params["metric_periods"] = metric_periods
    params["benchmark_ticker"] = str(params["benchmark_ticker"]).strip().upper()
    if not params["benchmark_ticker"]:
        raise ValueError("benchmark_ticker must not be blank")
    return params


def _resolve_benchmark(meta: MetaStore, ticker: str, start: date, end: date) -> str:
    report = meta.resolve_alias_range(ticker, start, end)
    instrument_ids = {
        segment.instrument_id
        for segment in report.segments
        if segment.instrument_id is not None
    }
    if not report.resolved or len(instrument_ids) != 1:
        raise ValueError(
            f"benchmark {ticker} must resolve to exactly one instrument over "
            f"{start}..{end}"
        )
    return instrument_ids.pop()
