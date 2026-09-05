"""Multi-session gap-down recovery study over EOD bars (M4 second study).

The owner's target is recovery over hours *or days*, and the question is
which instruments (or groups) overreact and recover more reliably than the
pooled baseline. This study answers the "days" half on the widest sample the
warehouse has: EOD bars for every stored instrument back to 2006, no intraday
requirement. Selection reuses the shared as-of features; a
``min_abs_gap_vol_normalized`` parameter isolates news-sized gaps (many
standard deviations of prior daily volatility) as a proxy while no
point-in-time earnings source exists (D-036: unknown stays unknown).

Price basis: every outcome is adjusted close on the k-th XNYS session after
the event (k = 0 is the event session) over the adjusted open of the event
session, so cross-session returns stay on one continuous basis. Missing
sessions (delisting, halt, vendor gap) are explicit ``missing_outcome`` rows,
never silently liquidated at the last stored close.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any

import polars as pl

from marketdata.calendar import session_schedule
from marketdata.config import Config
from marketdata.features import (
    register_eod_decision_features,
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
    _normalize_parameters,
    _period_expression,
    _resolve_benchmark,
)

STUDY_NAME = "gap_recovery_multiday"
STUDY_SCHEMA_VERSION = 1
HORIZON_SESSIONS: tuple[int, ...] = (0, 1, 2, 3, 5, 10)
DEFAULT_PARAMETERS: dict[str, Any] = {
    key: value
    for key, value in COARSE_DEFAULTS.items()
    if key not in {"min_hourly_density"}
}
DEFAULT_PARAMETERS.update(
    {
        "start": "2007-01-03",
        # A zero threshold keeps every gap; 2.0 keeps gaps at least twice the
        # prior 20-session daily volatility (a news-sized move proxy).
        "min_abs_gap_vol_normalized": 0.0,
        "stress_vol_threshold": 0.015,
    }
)
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
    "event_day_corporate_action",
    "benchmark_gap_return",
    "benchmark_adj_open",
    "benchmark_realized_vol",
    "benchmark_prior_window_return",
    "market_regime",
    "market_trend",
    "period",
)


def horizon_label(sessions: int) -> str:
    return f"close_plus_{sessions}_sessions"


def run_gap_recovery_multiday_study(
    config: Config, parameters: Mapping[str, Any]
) -> PublishedResearchRun:
    """Publish one multi-session EOD gap-recovery run through the shared runner."""
    params = _normalize_parameters(
        parameters, DEFAULT_PARAMETERS, study_name=STUDY_NAME
    )
    if float(params["min_abs_gap_vol_normalized"]) < 0:
        raise ValueError("min_abs_gap_vol_normalized must not be negative")
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
        {"instrument_id": list(asset_types), "asset_type": list(asset_types.values())},
        schema={"instrument_id": pl.Utf8, "asset_type": pl.Utf8},
    )

    def build_candidates(context: EventStudyContext) -> pl.DataFrame:
        con = context.connection
        register_eod_decision_features(con, lookback_sessions=lookback)
        register_session_opens(con, start, end)
        frame = con.execute(
            """SELECT f.instrument_id, f.date AS event_date,
                      s.session_open AS decision_ts,
                      f.lookback_start_date AS lookback_start,
                      f.prior_date AS lookback_end,
                      f.open_raw, f.adj_open, f.prior_close_raw, f.prior_adj_close,
                      f.gap_return, f.gap_vol_normalized, f.adv_dollars,
                      f.realized_vol, f.prior_5_return, f.prior_window_return,
                      f.event_day_corporate_action,
                      b.gap_return AS benchmark_gap_return,
                      b.adj_open AS benchmark_adj_open,
                      b.realized_vol AS benchmark_realized_vol,
                      b.prior_window_return AS benchmark_prior_window_return
                 FROM eod_decision_features AS f
                 JOIN session_opens AS s ON s.session_date = f.date
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
        threshold = float(params["min_abs_gap_vol_normalized"])
        if threshold <= 0:
            return candidates
        return candidates.filter(pl.col("gap_vol_normalized") <= -threshold)

    def observe_events(context: EventStudyContext, selected: pl.DataFrame):
        return _observe(context, selected, params, benchmark_id, asset_frame)

    return run_event_study(
        config,
        study_name=STUDY_NAME,
        study_schema_version=STUDY_SCHEMA_VERSION,
        parameters=params,
        selection_dataset_keys=["eod"],
        outcome_dataset_keys=(),
        lookbacks=[EventLookback("eod", "lookback_start", "lookback_end")],
        quality_policy=EventQualityPolicy(
            dataset_keys=("eod",),
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
    asset_frame: pl.DataFrame,
) -> ResearchOutput:
    con = context.connection
    con.register("selected_events", selected)
    horizons = pl.DataFrame(
        {
            "horizon_sessions": list(HORIZON_SESSIONS),
            "observation_label": [horizon_label(k) for k in HORIZON_SESSIONS],
        },
        schema={"horizon_sessions": pl.Int32, "observation_label": pl.Utf8},
    )
    con.register("horizons", horizons)
    if selected.height:
        first = selected["event_date"].min()
        last = selected["event_date"].max()
        # Enough calendar to resolve the longest horizon after the last event.
        schedule = session_schedule(first, _plus_days(last, 40))
    else:
        schedule = session_schedule(date(2000, 1, 3), date(2000, 1, 3)).head(0)
    con.register(
        "sessions",
        schedule.with_row_index("session_index").select(
            "session_index", "session_date"
        ),
    )
    outcomes = con.execute(
        """WITH grid AS (
               SELECT e.instrument_id, e.event_date, e.adj_open, e.prior_adj_close,
                      e.benchmark_adj_open, h.horizon_sessions, h.observation_label,
                      CAST(ev.session_index AS INTEGER) + h.horizon_sessions
                          AS target_index
                 FROM selected_events AS e
                 JOIN sessions AS ev ON ev.session_date = e.event_date
                 CROSS JOIN horizons AS h
           ),
           targeted AS (
               SELECT g.*, t.session_date AS target_date
                 FROM grid AS g
                 LEFT JOIN sessions AS t
                   ON CAST(t.session_index AS INTEGER) = g.target_index
           ),
           path AS (
               SELECT g.instrument_id, g.event_date, g.horizon_sessions,
                      max(b.adj_high) AS max_adj_high, min(b.adj_low) AS min_adj_low,
                      count(*) AS sessions_observed
                 FROM targeted AS g
                 JOIN eod AS b
                   ON b.instrument_id = g.instrument_id
                  AND b.date BETWEEN g.event_date AND g.target_date
                GROUP BY g.instrument_id, g.event_date, g.horizon_sessions
           )
           SELECT g.instrument_id, g.event_date, g.observation_label,
                  g.horizon_sessions, g.target_date,
                  own.adj_close AS checkpoint_adj_close,
                  own.close AS checkpoint_raw_close,
                  path.max_adj_high, path.min_adj_low, path.sessions_observed,
                  bench.adj_close AS benchmark_adj_close
             FROM targeted AS g
             LEFT JOIN eod AS own
               ON own.instrument_id = g.instrument_id AND own.date = g.target_date
             LEFT JOIN path
               ON path.instrument_id = g.instrument_id
              AND path.event_date = g.event_date
              AND path.horizon_sessions = g.horizon_sessions
             LEFT JOIN eod AS bench
               ON bench.instrument_id = ? AND bench.date = g.target_date
            ORDER BY g.instrument_id, g.event_date, g.horizon_sessions""",
        [benchmark_id],
    ).pl()
    target = float(params["target_return"])
    observations = (
        selected.select("instrument_id", "event_date", *_DECISION_FEATURE_COLUMNS)
        .join(outcomes, on=["instrument_id", "event_date"], how="inner")
        .join(asset_frame, on="instrument_id", how="left")
        .with_columns(
            pl.col("asset_type").fill_null("unknown"),
            pl.when(pl.col("checkpoint_adj_close").is_null())
            .then(pl.lit("missing_outcome"))
            .otherwise(pl.lit("evaluable"))
            .alias("outcome_status"),
            pl.col("target_date").alias("checkpoint_session"),
            (pl.col("checkpoint_adj_close") / pl.col("adj_open") - 1.0).alias(
                "measured_return"
            ),
            (pl.col("max_adj_high") / pl.col("adj_open") - 1.0).alias(
                "max_favorable_excursion"
            ),
            (pl.col("min_adj_low") / pl.col("adj_open") - 1.0).alias(
                "max_adverse_excursion"
            ),
            (pl.col("benchmark_adj_close") / pl.col("benchmark_adj_open") - 1.0).alias(
                "benchmark_return"
            ),
        )
        .with_columns(
            (pl.col("measured_return") - pl.col("benchmark_return")).alias(
                "excess_return"
            ),
            pl.when(pl.col("prior_adj_close") != pl.col("adj_open"))
            .then(
                (pl.col("checkpoint_adj_close") - pl.col("adj_open"))
                / (pl.col("prior_adj_close") - pl.col("adj_open"))
            )
            .otherwise(None)
            .alias("gap_recovered_fraction_adjusted_basis"),
            (pl.col("measured_return") >= target).alias("reached_target_at_checkpoint"),
            (pl.col("max_favorable_excursion") >= target).alias(
                "touched_target_within_horizon"
            ),
            pl.lit(
                "adjusted close over adjusted event open; k=0 is the event session"
            ).alias("price_basis"),
        )
        .sort("instrument_id", "event_date", "horizon_sessions")
        .drop("horizon_sessions", "target_date")
    )
    return ResearchOutput(observations, metrics=tuple(_metrics(observations, params)))


def _plus_days(value: date, days: int) -> date:
    return value + timedelta(days=days)


_BASE_STATS = (
    ("mean_return", "measured_return", "mean"),
    ("median_return", "measured_return", "median"),
    ("p10_return", "measured_return", "p10"),
    ("p90_return", "measured_return", "p90"),
    ("mean_excess_return", "excess_return", "mean"),
    ("mean_favorable_excursion", "max_favorable_excursion", "mean"),
    ("mean_adverse_excursion", "max_adverse_excursion", "mean"),
    ("mean_gap_recovered_fraction", "gap_recovered_fraction_adjusted_basis", "mean"),
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
        names = ["checkpoint" if c == "observation_label" else c for c in columns]
        for keys, frame in reportable.group_by(*columns, maintain_order=True):
            dims = {name: str(value) for name, value in zip(names, keys, strict=True)}
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
                touched = int(evaluable["touched_target_within_horizon"].sum())
                metrics.append(
                    ResearchMetric(
                        "touch_rate_target_within_horizon",
                        touched / evaluable.height,
                        dimensions=dims,
                        unit="fraction",
                    )
                )
    return metrics
