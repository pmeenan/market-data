"""Two-pass overreaction-propensity backtest on a published run.

The owner's question is which tickers or industries overreact and recover
more reliably than the pooled baseline. The honest way to ask it is a
two-pass backtest:

1. **Selection pass.** On a selection window, rank instruments by their
   recovery hit rate at one checkpoint, shrunk toward the window's pooled
   baseline with a Beta-binomial prior of ``prior_strength`` pseudo-events so
   a handful of lucky events cannot reach the top. Freeze the instruments
   whose shrunk rate beats the baseline by at least ``min_lift`` with at
   least ``min_events`` events.
2. **Evaluation pass.** Apply that frozen list, unchanged, to a later
   evaluation window and report how the list's events did there against the
   pooled baseline of *every* instrument in that window.

A walk-forward mode repeats the two passes over rolling calendar years so
the selection rule is scored many times, not once. Owner-supplied group tags
(a private ``ticker,group`` CSV) aggregate both passes to groups, because the
warehouse has no vendor sector field. Nothing here publishes to the catalog
or claims executable performance.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from marketdata.config import Config
from marketdata.query import load_research_observations
from marketdata.store.meta import MetaStore

DEFAULT_PRIOR_STRENGTH = 20
DEFAULT_MIN_EVENTS = 10
DEFAULT_MIN_LIFT = 0.05
_TARGET = "reached_target_at_checkpoint"
_KEYS = ("instrument_id", "event_date")


@dataclass(frozen=True)
class Window:
    """Inclusive event-date bounds for one pass."""

    label: str
    start: date
    end: date


@dataclass(frozen=True)
class TwoPassResult:
    """One selection window scored on one evaluation window."""

    selection: Window
    evaluation: Window
    selection_baseline: float
    evaluation_baseline: float
    candidates: pl.DataFrame
    evaluated: pl.DataFrame
    groups: pl.DataFrame | None
    summary: Mapping[str, Any]


@dataclass(frozen=True)
class TwoPassReport:
    run_id: str
    checkpoint: str
    results: tuple[TwoPassResult, ...]
    pooled: Mapping[str, Any]
    tags: Mapping[str, str] | None = field(default=None)


def load_group_tags(path: str | Path) -> dict[str, str]:
    """Read a ``ticker,group`` CSV (header optional) into an uppercase map."""
    tags: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as handle:
        for row in csv.reader(handle):
            if len(row) < 2:
                continue
            ticker, group = row[0].strip().upper(), row[1].strip()
            if not ticker or not group or ticker == "TICKER":
                continue
            tags[ticker] = group
    if not tags:
        raise ValueError(f"no ticker,group rows found in {path}")
    return tags


def _scope(observations: pl.DataFrame, checkpoint: str) -> pl.DataFrame:
    required = {
        *_KEYS,
        "observation_label",
        "outcome_status",
        _TARGET,
        "measured_return",
    }
    missing = sorted(required - set(observations.columns))
    if missing:
        raise ValueError(f"observations lack columns: {missing}")
    scoped = observations.filter(
        (pl.col("observation_label") == checkpoint)
        & (pl.col("outcome_status") == "evaluable")
    )
    if scoped.is_empty():
        raise ValueError(f"no evaluable observations at checkpoint {checkpoint!r}")
    if "excess_return" not in scoped.columns:
        scoped = scoped.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("excess_return")
        )
    return scoped


def _in_window(frame: pl.DataFrame, window: Window) -> pl.DataFrame:
    return frame.filter(
        pl.col("event_date").is_between(window.start, window.end, closed="both")
    )


def _baseline(frame: pl.DataFrame) -> float:
    value = frame[_TARGET].mean()
    return float(value) if value is not None else float("nan")


def _per_instrument(
    frame: pl.DataFrame, baseline: float, prior_strength: int
) -> pl.DataFrame:
    stats = frame.group_by("instrument_id").agg(
        pl.len().alias("events"),
        pl.col(_TARGET).sum().alias("hits"),
        pl.col("measured_return").median().alias("median_return"),
        pl.col("excess_return").mean().alias("mean_excess_return"),
    )
    return stats.with_columns(
        (pl.col("hits") / pl.col("events")).alias("hit_rate"),
        (
            (pl.col("hits") + prior_strength * baseline)
            / (pl.col("events") + prior_strength)
        ).alias("shrunk_hit_rate"),
    ).with_columns((pl.col("shrunk_hit_rate") - baseline).alias("lift"))


def select_candidates(
    scoped: pl.DataFrame,
    window: Window,
    *,
    min_events: int = DEFAULT_MIN_EVENTS,
    min_lift: float = DEFAULT_MIN_LIFT,
    prior_strength: int = DEFAULT_PRIOR_STRENGTH,
) -> tuple[pl.DataFrame, float]:
    """Pass 1: rank every instrument in the window and freeze the candidates."""
    if min_events < 1 or prior_strength < 0:
        raise ValueError("min_events must be positive and prior_strength non-negative")
    frame = _in_window(scoped, window)
    baseline = _baseline(frame)
    ranked = _per_instrument(frame, baseline, prior_strength).with_columns(
        ((pl.col("events") >= min_events) & (pl.col("lift") >= min_lift)).alias(
            "selected"
        )
    )
    return (
        ranked.sort(
            ["selected", "shrunk_hit_rate", "events"], descending=[True, True, True]
        ),
        baseline,
    )


def evaluate_candidates(
    scoped: pl.DataFrame,
    window: Window,
    candidate_ids: Sequence[str],
    *,
    prior_strength: int = DEFAULT_PRIOR_STRENGTH,
) -> tuple[pl.DataFrame, float, dict[str, Any]]:
    """Pass 2: score the frozen list on the evaluation window against everyone."""
    frame = _in_window(scoped, window)
    baseline = _baseline(frame)
    ids = list(dict.fromkeys(candidate_ids))
    scored = _per_instrument(
        frame.filter(pl.col("instrument_id").is_in(ids)), baseline, prior_strength
    )
    seen = set(scored["instrument_id"].to_list())
    absent = pl.DataFrame(
        {"instrument_id": [i for i in ids if i not in seen]},
        schema={"instrument_id": pl.Utf8},
    )
    scored = pl.concat([scored, absent], how="diagonal_relaxed").with_columns(
        pl.col("events").fill_null(0), pl.col("hits").fill_null(0)
    )
    candidate_events = frame.filter(pl.col("instrument_id").is_in(ids))
    events = candidate_events.height
    hits = int(candidate_events[_TARGET].sum()) if events else 0
    held_up = int((scored["lift"].fill_null(-1.0) > 0).sum())
    summary = {
        "candidates": len(ids),
        "candidates_with_events": int((scored["events"] > 0).sum()),
        "candidates_above_baseline": held_up,
        "candidate_events": events,
        "candidate_hits": hits,
        "candidate_hit_rate": hits / events if events else None,
        "baseline_hit_rate": baseline,
        "lift": (hits / events - baseline) if events else None,
        "candidate_median_return": (
            float(candidate_events["measured_return"].median()) if events else None
        ),
        "baseline_median_return": (
            float(frame["measured_return"].median()) if frame.height else None
        ),
        "candidate_mean_excess": (
            float(candidate_events["excess_return"].mean())
            if events and candidate_events["excess_return"].null_count() < events
            else None
        ),
        "baseline_events": frame.height,
    }
    return (
        scored.sort(
            ["shrunk_hit_rate", "events"], descending=[True, True], nulls_last=True
        ),
        baseline,
        summary,
    )


def two_pass(
    scoped: pl.DataFrame,
    selection: Window,
    evaluation: Window,
    *,
    tickers: Mapping[str, str],
    min_events: int = DEFAULT_MIN_EVENTS,
    min_lift: float = DEFAULT_MIN_LIFT,
    prior_strength: int = DEFAULT_PRIOR_STRENGTH,
    group_tags: Mapping[str, str] | None = None,
) -> TwoPassResult:
    """Run both passes for one window pair."""
    if selection.end >= evaluation.start:
        raise ValueError(
            "the evaluation window must start after the selection window ends"
        )
    ranked, selection_baseline = select_candidates(
        scoped,
        selection,
        min_events=min_events,
        min_lift=min_lift,
        prior_strength=prior_strength,
    )
    candidate_ids = ranked.filter(pl.col("selected"))["instrument_id"].to_list()
    scored, evaluation_baseline, summary = evaluate_candidates(
        scoped, evaluation, candidate_ids, prior_strength=prior_strength
    )
    ticker_frame = pl.DataFrame(
        {"instrument_id": list(tickers), "ticker": list(tickers.values())},
        schema={"instrument_id": pl.Utf8, "ticker": pl.Utf8},
    )

    def _label(frame: pl.DataFrame) -> pl.DataFrame:
        frame = frame.join(ticker_frame, on="instrument_id", how="left").with_columns(
            pl.col("ticker").fill_null(pl.col("instrument_id"))
        )
        if group_tags:
            tag_frame = pl.DataFrame(
                {"ticker": list(group_tags), "group": list(group_tags.values())},
                schema={"ticker": pl.Utf8, "group": pl.Utf8},
            )
            frame = frame.join(tag_frame, on="ticker", how="left")
        else:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("group"))
        return frame.select("ticker", "group", pl.exclude("ticker", "group"))

    candidates = _label(ranked)
    evaluated = _label(scored)
    groups = None
    if group_tags:
        groups = _group_summary(
            scoped, selection, evaluation, tickers, group_tags, prior_strength
        )
    return TwoPassResult(
        selection=selection,
        evaluation=evaluation,
        selection_baseline=selection_baseline,
        evaluation_baseline=evaluation_baseline,
        candidates=candidates,
        evaluated=evaluated,
        groups=groups,
        summary=summary,
    )


def _group_summary(
    scoped: pl.DataFrame,
    selection: Window,
    evaluation: Window,
    tickers: Mapping[str, str],
    group_tags: Mapping[str, str],
    prior_strength: int,
) -> pl.DataFrame:
    """Score every tagged group on both windows (a group is its own list)."""
    id_to_group = {
        instrument_id: group_tags[ticker]
        for instrument_id, ticker in tickers.items()
        if ticker in group_tags
    }
    group_frame = pl.DataFrame(
        {"instrument_id": list(id_to_group), "group": list(id_to_group.values())},
        schema={"instrument_id": pl.Utf8, "group": pl.Utf8},
    )
    rows = []
    for label, window in (("select", selection), ("evaluate", evaluation)):
        frame = _in_window(scoped, window)
        baseline = _baseline(frame)
        tagged = frame.join(group_frame, on="instrument_id", how="inner")
        for group, part in tagged.group_by("group", maintain_order=True):
            events = part.height
            hits = int(part[_TARGET].sum())
            shrunk = (hits + prior_strength * baseline) / (events + prior_strength)
            rows.append(
                {
                    "group": str(group[0]),
                    "pass": label,
                    "tickers": part["instrument_id"].n_unique(),
                    "events": events,
                    "hits": hits,
                    "hit_rate": hits / events if events else None,
                    "shrunk_hit_rate": shrunk,
                    "baseline_hit_rate": baseline,
                    "lift": shrunk - baseline,
                    "median_return": float(part["measured_return"].median()),
                }
            )
    if not rows:
        return pl.DataFrame(schema={"group": pl.Utf8, "pass": pl.Utf8})
    return pl.DataFrame(rows).sort(["group", "pass"], descending=[False, True])


def walk_forward_windows(
    scoped: pl.DataFrame,
    *,
    select_years: int,
    exclude_periods: Sequence[str] = ("test",),
) -> list[tuple[Window, Window]]:
    """Yearly rolling pairs: select on the prior ``select_years``, evaluate on the next."""
    frame = scoped
    if "period" in frame.columns and exclude_periods:
        frame = frame.filter(~pl.col("period").is_in(list(exclude_periods)))
    if frame.is_empty():
        return []
    years = sorted(frame["event_date"].dt.year().unique().to_list())
    pairs: list[tuple[Window, Window]] = []
    for year in years:
        first = year - select_years
        if first < years[0]:
            continue
        pairs.append(
            (
                Window(
                    f"{first}-{year - 1}", date(first, 1, 1), date(year - 1, 12, 31)
                ),
                Window(str(year), date(year, 1, 1), date(year, 12, 31)),
            )
        )
    return pairs


def two_pass_run(
    config: Config,
    run_id: str,
    *,
    checkpoint: str,
    select_period: str = "development",
    evaluate_period: str = "validation",
    walk_forward_years: int | None = None,
    min_events: int = DEFAULT_MIN_EVENTS,
    min_lift: float = DEFAULT_MIN_LIFT,
    prior_strength: int = DEFAULT_PRIOR_STRENGTH,
    tags_path: str | Path | None = None,
) -> TwoPassReport:
    """Load a succeeded run and score the two-pass rule on named or rolling windows."""
    observations = load_research_observations(config, run_ids=[run_id])
    scoped = _scope(observations, checkpoint)
    tickers = _display_tickers(config, scoped["instrument_id"].unique().to_list())
    group_tags = load_group_tags(tags_path) if tags_path else None
    if walk_forward_years is not None:
        pairs = walk_forward_windows(scoped, select_years=walk_forward_years)
        if not pairs:
            raise ValueError(
                "not enough non-test history for the requested walk-forward"
            )
    else:
        if "period" not in scoped.columns:
            raise ValueError("this run has no period column; use --walk-forward-years")
        bounds = {}
        for name in (select_period, evaluate_period):
            part = scoped.filter(pl.col("period") == name)
            if part.is_empty():
                raise ValueError(
                    f"period {name!r} has no evaluable events at this checkpoint"
                )
            bounds[name] = Window(
                name, part["event_date"].min(), part["event_date"].max()
            )
        pairs = [(bounds[select_period], bounds[evaluate_period])]
    results = tuple(
        two_pass(
            scoped,
            selection,
            evaluation,
            tickers=tickers,
            min_events=min_events,
            min_lift=min_lift,
            prior_strength=prior_strength,
            group_tags=group_tags,
        )
        for selection, evaluation in pairs
    )
    events = sum(r.summary["candidate_events"] for r in results)
    hits = sum(r.summary["candidate_hits"] for r in results)
    baseline_events = sum(r.summary["baseline_events"] for r in results)
    baseline_hits = sum(
        round(r.summary["baseline_hit_rate"] * r.summary["baseline_events"])
        for r in results
    )
    pooled = {
        "windows": len(results),
        "windows_with_lift": sum(1 for r in results if (r.summary["lift"] or 0) > 0),
        "candidate_events": events,
        "candidate_hit_rate": hits / events if events else None,
        "baseline_hit_rate": baseline_hits / baseline_events
        if baseline_events
        else None,
    }
    if (
        pooled["candidate_hit_rate"] is not None
        and pooled["baseline_hit_rate"] is not None
    ):
        pooled["lift"] = pooled["candidate_hit_rate"] - pooled["baseline_hit_rate"]
    return TwoPassReport(
        run_id=run_id,
        checkpoint=checkpoint,
        results=results,
        pooled=pooled,
        tags=group_tags,
    )


def _display_tickers(config: Config, instrument_ids: Sequence[str]) -> dict[str, str]:
    with MetaStore(config.meta_path) as meta:
        rows = meta.instrument_aliases_for_instruments(instrument_ids)
    latest: dict[str, tuple[date, str]] = {}
    for row in rows:
        instrument_id = str(row["instrument_id"])
        end = date.fromisoformat(str(row["end_date"]))
        if instrument_id not in latest or end > latest[instrument_id][0]:
            latest[instrument_id] = (end, str(row["ticker"]))
    return {instrument_id: ticker for instrument_id, (_, ticker) in latest.items()}


def write_two_pass(report: TwoPassReport, path: str | Path) -> None:
    """Write candidates, evaluations, and groups for every window beside ``path``."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    parquet = target.suffix.lower() == ".parquet"
    frames: dict[str, list[pl.DataFrame]] = {
        "candidates": [],
        "evaluation": [],
        "groups": [],
    }
    for result in report.results:
        tag = pl.lit(f"{result.selection.label}->{result.evaluation.label}").alias(
            "window"
        )
        frames["candidates"].append(result.candidates.with_columns(tag))
        frames["evaluation"].append(result.evaluated.with_columns(tag))
        if result.groups is not None and result.groups.height:
            frames["groups"].append(result.groups.with_columns(tag))
    for name, parts in frames.items():
        if not parts:
            continue
        frame = pl.concat(parts, how="diagonal_relaxed")
        out = (
            target
            if name == "evaluation"
            else target.with_name(f"{target.stem}-{name}{target.suffix}")
        )
        if parquet:
            frame.write_parquet(out)
        else:
            frame.write_csv(out)


def format_two_pass(report: TwoPassReport, *, top: int = 25) -> list[str]:
    """Render a bounded terminal summary of every window pair."""
    lines = [
        f"Run {report.run_id}, checkpoint {report.checkpoint}: "
        f"{report.pooled['windows']} window pair(s), "
        f"{report.pooled['windows_with_lift']} with positive lift on evaluation.",
    ]
    if report.pooled.get("candidate_hit_rate") is not None:
        lines.append(
            f"Pooled evaluation: candidates {report.pooled['candidate_hit_rate']:.1%} "
            f"vs baseline {report.pooled['baseline_hit_rate']:.1%} "
            f"(lift {report.pooled['lift']:+.1%}) over "
            f"{report.pooled['candidate_events']:,} candidate events."
        )
    for result in report.results:
        s = result.summary
        lines.append("")
        lines.append(
            f"Select {result.selection.label} (baseline {result.selection_baseline:.1%}) -> "
            f"evaluate {result.evaluation.label} (baseline {result.evaluation_baseline:.1%}): "
            f"{s['candidates']} candidates, {s['candidates_above_baseline']} above baseline, "
            + (
                f"candidate hit rate {s['candidate_hit_rate']:.1%} (lift {s['lift']:+.1%}) "
                f"on {s['candidate_events']:,} events"
                if s["candidate_hit_rate"] is not None
                else "no candidate events"
            )
        )
        header = (
            f"  {'ticker':8} {'group':12} {'sel n':>6} {'sel%':>6} "
            f"{'eval n':>6} {'eval%':>6} {'shrunk':>7} {'lift':>6}"
        )
        lines.append(header)
        selected = result.candidates.filter(pl.col("selected")).select(
            "instrument_id",
            pl.col("events").alias("sel_events"),
            pl.col("hit_rate").alias("sel_rate"),
        )
        table = result.evaluated.join(selected, on="instrument_id", how="inner").sort(
            ["shrunk_hit_rate", "events"], descending=[True, True], nulls_last=True
        )
        for row in table.head(top).iter_rows(named=True):
            eval_rate = (
                f"{row['hit_rate']:6.1%}" if row["hit_rate"] is not None else "   n/a"
            )
            shrunk = (
                f"{row['shrunk_hit_rate']:7.1%}"
                if row["shrunk_hit_rate"] is not None
                else "    n/a"
            )
            lift = f"{row['lift']:+6.1%}" if row["lift"] is not None else "   n/a"
            lines.append(
                f"  {row['ticker']:8} {(row['group'] or '-')[:12]:12} "
                f"{row['sel_events']:6d} {row['sel_rate']:6.1%} "
                f"{row['events']:6d} {eval_rate} {shrunk} {lift}"
            )
        if result.groups is not None and result.groups.height:
            lines.append(
                "  Groups (each group scored as its own list on both windows):"
            )
            for row in result.groups.iter_rows(named=True):
                rate = (
                    f"{row['hit_rate']:6.1%}"
                    if row["hit_rate"] is not None
                    else "   n/a"
                )
                lines.append(
                    f"  {row['group'][:20]:20} {row['pass']:8} tickers={row['tickers']:3d} "
                    f"n={row['events']:5d} {rate} baseline={row['baseline_hit_rate']:6.1%} "
                    f"lift={row['lift']:+6.1%}"
                )
    return lines
