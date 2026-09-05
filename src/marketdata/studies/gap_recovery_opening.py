"""Full opening-window gap-down recovery study over five-minute bars (M4).

This is the session-relative extension of :mod:`gap_recovery`. Selection is
identical (same as-of EOD features, same gap and liquidity screens, same
frozen periods) plus a five-minute IEX density screen, so the two studies can
be compared over their common events. Outcomes come from exchange-calendar
filtered five-minute bars from the 09:30 open: the first completed print is
the 09:30 bar's close, available at 09:35, which the direct hourly feed cannot
see (D-012). Direct hourly bars are read only to measure cross-frequency
consistency; they are never relabelled as session-aligned aggregates.

Two return bases are published per checkpoint: from the raw EOD opening print
(the decision-time observation, not an assumed fill) and from the 09:30 bar's
close (the first completed price a morning decision could have acted on).
Excursions, benchmark excess, zero-volume bar counts, and the hourly/5-minute
close agreement are recorded per observation so the data-fidelity questions in
research-protocol.md are answered on the actual cohort, not assumed.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

import polars as pl

from marketdata.config import Config
from marketdata.features import (
    register_eod_decision_features,
    register_intraday_density_features,
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
from marketdata.studies.gap_recovery import (
    DEFAULT_PARAMETERS as COARSE_DEFAULTS,
)
from marketdata.studies.gap_recovery import (
    OPENING_INTERVAL_NOTE,
    _normalize_parameters,
    _period_expression,
    _resolve_benchmark,
)

STUDY_NAME = "gap_recovery_opening"
STUDY_SCHEMA_VERSION = 1
# Minutes from the 09:30 open at which a completed five-minute bar's close is
# available. 5 = the 09:30 bar (available 09:35); 390 = the 15:55 bar
# (available at the 16:00 close).
CHECKPOINT_MINUTES: tuple[int, ...] = (5, 15, 30, 60, 90, 150, 210, 270, 330, 390)
FIRST_BAR_MINUTES = 5
DEFAULT_PARAMETERS: dict[str, Any] = {
    **COARSE_DEFAULTS,
    "min_5min_density": 0.8,
    # SPY prior-window realized daily volatility at or above this is 'stress'.
    "stress_vol_threshold": 0.015,
    # DuckDB cap for the quality scan; the five-minute archive exceeds the
    # 4GB CLI default even when scoped to the candidate cohort.
    "quality_memory_limit": "24GB",
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
    "prior_window_return",
    "hourly_density",
    "five_min_density",
    "event_day_corporate_action",
    "benchmark_gap_return",
    "benchmark_open_raw",
    "benchmark_realized_vol",
    "benchmark_prior_window_return",
    "market_regime",
    "market_trend",
    "period",
)


def checkpoint_label(minutes: int) -> str:
    """Name a checkpoint by its availability time and the bar it closes."""
    available_hour, available_minute = divmod(9 * 60 + 30 + minutes, 60)
    start_hour, start_minute = divmod(9 * 60 + 30 + minutes - 5, 60)
    return (
        f"{available_hour:02d}:{available_minute:02d}_close_of_"
        f"{start_hour:02d}:{start_minute:02d}_bar"
    )


def run_gap_recovery_opening_study(
    config: Config, parameters: Mapping[str, Any]
) -> PublishedResearchRun:
    """Publish one five-minute opening-window run through the shared runner."""
    params = _normalize_parameters(
        parameters, DEFAULT_PARAMETERS, study_name=STUDY_NAME
    )
    if not 0.0 <= float(params["min_5min_density"]) <= 1.0:
        raise ValueError("min_5min_density must be between 0 and 1")
    if float(params["stress_vol_threshold"]) <= 0:
        raise ValueError("stress_vol_threshold must be positive")
    start = date.fromisoformat(params["start"])
    end = date.fromisoformat(params["end"])
    lookback = int(params["lookback_sessions"])
    with MetaStore(config.meta_path) as meta:
        benchmark_id = _resolve_benchmark(
            meta, str(params["benchmark_ticker"]), start, end
        )
        asset_types = meta.instrument_asset_types()
    params["benchmark_instrument_id"] = benchmark_id
    periods = {
        name: (date.fromisoformat(bounds[0]), date.fromisoformat(bounds[1]))
        for name, bounds in params["periods"].items()
    }
    asset_frame = pl.DataFrame(
        {
            "instrument_id": list(asset_types),
            "asset_type": list(asset_types.values()),
        },
        schema={"instrument_id": pl.Utf8, "asset_type": pl.Utf8},
    )

    def build_candidates(context: EventStudyContext) -> pl.DataFrame:
        con = context.connection
        register_eod_decision_features(con, lookback_sessions=lookback)
        register_session_opens(con, start, end)
        register_intraday_density_features(
            con, start, end, freq="1hour", lookback_sessions=lookback
        )
        register_intraday_density_features(
            con, start, end, freq="5min", lookback_sessions=lookback
        )
        frame = con.execute(
            """SELECT f.instrument_id, f.date AS event_date,
                      s.session_open AS decision_ts,
                      f.lookback_start_date AS lookback_start,
                      f.prior_date AS lookback_end,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 10 HOUR)
                          AT TIME ZONE 'America/New_York' AS hourly_lookback_start,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 10 HOUR)
                          AT TIME ZONE 'America/New_York' AS hourly_lookback_end,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 570 MINUTE)
                          AT TIME ZONE 'America/New_York' AS five_min_lookback_start,
                      (CAST(f.prior_date AS TIMESTAMP) + INTERVAL 570 MINUTE)
                          AT TIME ZONE 'America/New_York' AS five_min_lookback_end,
                      f.open_raw, f.adj_open, f.prior_close_raw, f.prior_adj_close,
                      f.gap_return, f.gap_vol_normalized, f.adv_dollars,
                      f.realized_vol, f.prior_5_return, f.prior_window_return,
                      f.event_day_corporate_action,
                      h.hourly_density,
                      m.five_min_density,
                      b.gap_return AS benchmark_gap_return,
                      b.open_raw AS benchmark_open_raw,
                      b.realized_vol AS benchmark_realized_vol,
                      b.prior_window_return AS benchmark_prior_window_return
                 FROM eod_decision_features AS f
                 JOIN session_opens AS s ON s.session_date = f.date
                 LEFT JOIN density_features_1hour AS h
                   ON h.instrument_id = f.instrument_id AND h.date = f.date
                 LEFT JOIN density_features_5min AS m
                   ON m.instrument_id = f.instrument_id AND m.date = f.date
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
        stress = float(params["stress_vol_threshold"])
        return frame.with_columns(
            _period_expression(periods).alias("period"),
            pl.when(pl.col("benchmark_realized_vol").is_null())
            .then(pl.lit("unknown"))
            .when(pl.col("benchmark_realized_vol") >= stress)
            .then(pl.lit("stress"))
            .otherwise(pl.lit("calm"))
            .alias("market_regime"),
            pl.when(pl.col("benchmark_prior_window_return").is_null())
            .then(pl.lit("unknown"))
            .when(pl.col("benchmark_prior_window_return") < 0)
            .then(pl.lit("down"))
            .otherwise(pl.lit("up"))
            .alias("market_trend"),
        )

    def select_events(context: EventStudyContext, candidates: pl.DataFrame):
        return candidates.filter(
            (
                pl.col("hourly_density").fill_null(0.0)
                >= float(params["min_hourly_density"])
            )
            & (
                pl.col("five_min_density").fill_null(0.0)
                >= float(params["min_5min_density"])
            )
        )

    def observe_events(context: EventStudyContext, selected: pl.DataFrame):
        return _observe(context, selected, params, benchmark_id, asset_frame)

    return run_event_study(
        config,
        study_name=STUDY_NAME,
        study_schema_version=STUDY_SCHEMA_VERSION,
        parameters={**params, "opening_interval_note": OPENING_INTERVAL_NOTE},
        selection_dataset_keys=["eod", "intraday_1hour", "intraday_5min"],
        outcome_dataset_keys=["intraday_5min", "intraday_1hour"],
        lookbacks=[
            EventLookback("eod", "lookback_start", "lookback_end"),
            EventLookback(
                "intraday_1hour", "hourly_lookback_start", "hourly_lookback_end"
            ),
            EventLookback(
                "intraday_5min", "five_min_lookback_start", "five_min_lookback_end"
            ),
        ],
        quality_policy=EventQualityPolicy(
            dataset_keys=("eod", "intraday_1hour", "intraday_5min"),
            blocking_checks=("duplicate_keys", "split_sanity"),
            start=start,
            end=end,
            memory_limit=str(params["quality_memory_limit"]),
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
    asset_frame: pl.DataFrame,
) -> ResearchOutput:
    con = context.connection
    con.register("selected_events", selected)
    checkpoints = pl.DataFrame(
        {
            "checkpoint_minutes": list(CHECKPOINT_MINUTES),
            "observation_label": [checkpoint_label(m) for m in CHECKPOINT_MINUTES],
        },
        schema={"checkpoint_minutes": pl.Int32, "observation_label": pl.Utf8},
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
    outcomes = con.execute(
        """WITH ids AS (
               SELECT instrument_id FROM selected_events UNION SELECT ?
           ),
           days AS (SELECT DISTINCT event_date FROM selected_events),
           five AS (
               SELECT b.instrument_id, b.ts, b.open, b.high, b.low, b.close, b.volume,
                      s.session_date, s.session_close,
                      CAST(
                          date_diff('minute', s.session_open, b.ts) AS INTEGER
                      ) AS start_minutes
                 FROM intraday_5min AS b
                 JOIN outcome_sessions AS s
                   ON s.session_date = CAST(b.ts AT TIME ZONE 'America/New_York' AS DATE)
                WHERE b.instrument_id IN (SELECT instrument_id FROM ids)
                  AND CAST(b.ts AT TIME ZONE 'America/New_York' AS DATE)
                      IN (SELECT event_date FROM days)
           ),
           valid AS (
               SELECT *, start_minutes + 5 AS end_minutes
                 FROM five
                WHERE start_minutes >= 0
                  AND start_minutes % 5 = 0
                  AND ts + INTERVAL 5 MINUTE <= session_close
                  AND high >= greatest(open, close)
                  AND low <= least(open, close)
                  AND close > 0
           ),
           hourly AS (
               SELECT h.instrument_id, h.close,
                      CAST(h.ts AT TIME ZONE 'America/New_York' AS DATE) AS session_date,
                      CAST(
                          date_diff('minute', s.session_open, h.ts) AS INTEGER
                      ) + 60 AS end_minutes
                 FROM intraday_1hour AS h
                 JOIN outcome_sessions AS s
                   ON s.session_date = CAST(h.ts AT TIME ZONE 'America/New_York' AS DATE)
                WHERE h.instrument_id IN (SELECT instrument_id FROM ids)
                  AND CAST(h.ts AT TIME ZONE 'America/New_York' AS DATE)
                      IN (SELECT event_date FROM days)
                  AND minute(h.ts AT TIME ZONE 'America/New_York') = 0
                  AND hour(h.ts AT TIME ZONE 'America/New_York') >= 10
                  AND h.ts + INTERVAL 1 HOUR <= s.session_close
           ),
           grid AS (
               SELECT e.instrument_id, e.event_date, e.open_raw, e.prior_close_raw,
                      e.benchmark_open_raw, c.checkpoint_minutes, c.observation_label
                 FROM selected_events AS e CROSS JOIN checkpoints AS c
           )
           SELECT g.instrument_id, g.event_date, g.observation_label,
                  g.checkpoint_minutes,
                  own.ts + INTERVAL 5 MINUTE AS checkpoint_available_ts,
                  own.close AS checkpoint_price,
                  own.volume AS checkpoint_bar_volume,
                  first.close AS first_bar_close,
                  first.volume AS first_bar_volume,
                  stats.max_high AS max_high_through_checkpoint,
                  stats.min_low AS min_low_through_checkpoint,
                  stats.bar_count AS bars_through_checkpoint,
                  stats.zero_volume_bars AS zero_volume_bars_through_checkpoint,
                  bench.close AS benchmark_checkpoint_price,
                  hourly.close AS hourly_close_at_checkpoint
             FROM grid AS g
             LEFT JOIN valid AS own
               ON own.instrument_id = g.instrument_id
              AND own.session_date = g.event_date
              AND own.end_minutes = g.checkpoint_minutes
             LEFT JOIN valid AS first
               ON first.instrument_id = g.instrument_id
              AND first.session_date = g.event_date
              AND first.end_minutes = ?
             LEFT JOIN (
                  SELECT v.instrument_id, v.session_date, c.checkpoint_minutes,
                         max(v.high) AS max_high, min(v.low) AS min_low,
                         count(*) AS bar_count,
                         count(*) FILTER (WHERE v.volume = 0) AS zero_volume_bars
                    FROM valid AS v JOIN checkpoints AS c
                      ON v.end_minutes <= c.checkpoint_minutes
                   GROUP BY v.instrument_id, v.session_date, c.checkpoint_minutes
             ) AS stats
               ON stats.instrument_id = g.instrument_id
              AND stats.session_date = g.event_date
              AND stats.checkpoint_minutes = g.checkpoint_minutes
             LEFT JOIN valid AS bench
               ON bench.instrument_id = ?
              AND bench.session_date = g.event_date
              AND bench.end_minutes = g.checkpoint_minutes
             LEFT JOIN hourly
               ON hourly.instrument_id = g.instrument_id
              AND hourly.session_date = g.event_date
              AND hourly.end_minutes = g.checkpoint_minutes
            ORDER BY g.instrument_id, g.event_date, g.checkpoint_minutes""",
        [benchmark_id, FIRST_BAR_MINUTES, benchmark_id],
    ).pl()
    target = float(params["target_return"])
    observations = (
        selected.select("instrument_id", "event_date", *_DECISION_FEATURE_COLUMNS)
        .join(outcomes, on=["instrument_id", "event_date"], how="inner")
        .join(asset_frame, on="instrument_id", how="left")
        .with_columns(
            pl.col("asset_type").fill_null("unknown"),
            pl.when(pl.col("checkpoint_price").is_null())
            .then(pl.lit("missing_outcome"))
            .otherwise(pl.lit("evaluable"))
            .alias("outcome_status"),
            (pl.col("checkpoint_price") / pl.col("open_raw") - 1.0).alias(
                "measured_return"
            ),
            (pl.col("checkpoint_price") / pl.col("first_bar_close") - 1.0).alias(
                "measured_return_from_first_bar"
            ),
            (pl.col("first_bar_close") / pl.col("open_raw") - 1.0).alias(
                "first_bar_return"
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
            pl.when(pl.col("hourly_close_at_checkpoint").is_null())
            .then(None)
            .otherwise(
                pl.col("hourly_close_at_checkpoint") == pl.col("checkpoint_price")
            )
            .alias("hourly_close_matches_5min"),
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
            (pl.col("measured_return") >= target).alias("reached_target_at_checkpoint"),
            (pl.col("measured_return_from_first_bar") >= target).alias(
                "reached_target_from_first_bar"
            ),
            pl.lit("session_5min_start; checkpoint = bar close at bar end").alias(
                "bar_label_semantics"
            ),
        )
        .sort("instrument_id", "event_date", "checkpoint_minutes")
        .drop("checkpoint_minutes")
    )
    return ResearchOutput(observations, metrics=tuple(_metrics(observations, params)))


_BASE_STATS = (
    ("mean_return", "measured_return", "mean"),
    ("median_return", "measured_return", "median"),
    ("p10_return", "measured_return", "p10"),
    ("p90_return", "measured_return", "p90"),
    ("mean_return_from_first_bar", "measured_return_from_first_bar", "mean"),
    ("median_return_from_first_bar", "measured_return_from_first_bar", "median"),
    ("mean_first_bar_return", "first_bar_return", "mean"),
    ("mean_excess_return", "excess_return", "mean"),
    ("mean_favorable_excursion", "max_favorable_excursion", "mean"),
    ("mean_adverse_excursion", "max_adverse_excursion", "mean"),
    ("mean_gap_recovered_fraction", "gap_recovered_fraction_raw_basis", "mean"),
)
_SLICE_STATS = (
    ("median_return", "measured_return", "median"),
    ("mean_excess_return", "excess_return", "mean"),
)


def _stat(series: pl.Series, kind: str) -> float | None:
    if series.null_count() == series.len():
        return None
    if kind == "mean":
        value = series.mean()
    elif kind == "median":
        value = series.median()
    elif kind == "p10":
        value = series.quantile(0.10, interpolation="linear")
    else:
        value = series.quantile(0.90, interpolation="linear")
    return None if value is None else float(value)


def _metrics(
    observations: pl.DataFrame, params: Mapping[str, Any]
) -> list[ResearchMetric]:
    metrics: list[ResearchMetric] = []
    if observations.is_empty():
        return metrics
    events = observations.select(
        "instrument_id", "event_date", "period", "asset_type"
    ).unique()
    for period, count in events.group_by("period").len().sort("period").iter_rows():
        metrics.append(
            ResearchMetric(
                "events", int(count), dimensions={"period": str(period)}, unit="events"
            )
        )
    for period, asset_type, count in (
        events.group_by("period", "asset_type").len().sort("period", "asset_type")
    ).iter_rows():
        metrics.append(
            ResearchMetric(
                "events",
                int(count),
                dimensions={"period": str(period), "asset_type": str(asset_type)},
                unit="events",
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
    groupings = (
        (("period", "observation_label"), _BASE_STATS, True),
        (("period", "observation_label", "asset_type"), _SLICE_STATS, False),
        (("period", "observation_label", "market_regime"), _SLICE_STATS, False),
        (("period", "observation_label", "market_trend"), _SLICE_STATS, False),
    )
    for columns, stats, full in groupings:
        dimension_names = [
            "checkpoint" if column == "observation_label" else column
            for column in columns
        ]
        for keys, frame in reportable.group_by(*columns, maintain_order=True):
            dims = {
                name: str(value)
                for name, value in zip(dimension_names, keys, strict=True)
            }
            evaluable = frame.filter(pl.col("outcome_status") == "evaluable")
            metrics.append(
                ResearchMetric(
                    "evaluable", evaluable.height, dimensions=dims, unit="events"
                )
            )
            if full:
                metrics.append(
                    ResearchMetric(
                        "missing_outcome",
                        frame.height - evaluable.height,
                        dimensions=dims,
                        unit="events",
                    )
                )
            if evaluable.is_empty():
                continue
            for name, column, kind in stats:
                value = _stat(evaluable[column], kind)
                if value is not None:
                    metrics.append(
                        ResearchMetric(name, value, dimensions=dims, unit="return")
                    )
            hits = int(evaluable["reached_target_at_checkpoint"].sum())
            metrics.append(
                ResearchMetric(
                    "hit_rate_target",
                    hits / evaluable.height,
                    dimensions=dims,
                    unit="fraction",
                )
            )
            if full:
                metrics.append(
                    ResearchMetric(
                        "hit_rate_target_missing_as_miss",
                        hits / frame.height,
                        dimensions=dims,
                        unit="fraction",
                    )
                )
                first_bar = evaluable.filter(
                    pl.col("reached_target_from_first_bar").is_not_null()
                )
                if first_bar.height:
                    metrics.append(
                        ResearchMetric(
                            "hit_rate_target_from_first_bar",
                            int(first_bar["reached_target_from_first_bar"].sum())
                            / first_bar.height,
                            dimensions=dims,
                            unit="fraction",
                        )
                    )
                bars = int(evaluable["bars_through_checkpoint"].fill_null(0).sum())
                zero = int(
                    evaluable["zero_volume_bars_through_checkpoint"].fill_null(0).sum()
                )
                if bars:
                    metrics.append(
                        ResearchMetric(
                            "zero_volume_bar_share",
                            zero / bars,
                            dimensions=dims,
                            unit="fraction",
                        )
                    )
                compared = evaluable.filter(
                    pl.col("hourly_close_matches_5min").is_not_null()
                )
                if compared.height:
                    metrics.append(
                        ResearchMetric(
                            "hourly_close_matches_5min_rate",
                            int(compared["hourly_close_matches_5min"].sum())
                            / compared.height,
                            dimensions=dims,
                            unit="fraction",
                        )
                    )
    return metrics
