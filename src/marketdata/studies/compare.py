"""Compare the coarse hourly and full five-minute gap studies on common events."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from marketdata.config import Config
from marketdata.query import load_research_observations
from marketdata.store.meta import MetaStore
from marketdata.studies.gap_recovery import STUDY_NAME as COARSE_STUDY
from marketdata.studies.gap_recovery_opening import STUDY_NAME as OPENING_STUDY

# Availability times shared by the two studies: the coarse study's H:00 bar
# closes (available at H+1:00) and the opening study's 5-minute bar ending at
# the same minute.
_COMMON_LABELS = {
    "11:00_close_of_10:00_bar": "11:00_close_of_10:55_bar",
    "12:00_close_of_11:00_bar": "12:00_close_of_11:55_bar",
    "13:00_close_of_12:00_bar": "13:00_close_of_12:55_bar",
    "14:00_close_of_13:00_bar": "14:00_close_of_13:55_bar",
    "15:00_close_of_14:00_bar": "15:00_close_of_14:55_bar",
    "16:00_close_of_15:00_bar": "16:00_close_of_15:55_bar",
}


@dataclass(frozen=True)
class GapStudyComparison:
    """Common-event comparison between a coarse and an opening-window run."""

    coarse_run_id: str
    opening_run_id: str
    coarse_events: int
    opening_events: int
    common_events: int
    checkpoint_agreement: pl.DataFrame
    opening_window: pl.DataFrame

    def to_dict(self) -> dict[str, object]:
        return {
            "coarse_run_id": self.coarse_run_id,
            "opening_run_id": self.opening_run_id,
            "coarse_events": self.coarse_events,
            "opening_events": self.opening_events,
            "common_events": self.common_events,
            "checkpoint_agreement": self.checkpoint_agreement.to_dicts(),
            "opening_window": self.opening_window.to_dicts(),
        }


def compare_gap_studies(
    config: Config, *, coarse_run_id: str, opening_run_id: str
) -> GapStudyComparison:
    """Measure what five-minute coverage adds over the coarse study.

    ``checkpoint_agreement`` compares, per shared availability time and period,
    the coarse hourly-close return with the five-minute-close return for the
    same event: the count compared, the share of exact agreement, and the mean
    absolute difference. ``opening_window`` summarizes the checkpoints only the
    five-minute study can see (09:35 through 10:30) on the common events.
    """
    with MetaStore(config.meta_path) as meta:
        for run_id, expected in (
            (coarse_run_id, COARSE_STUDY),
            (opening_run_id, OPENING_STUDY),
        ):
            row = meta.research_run(run_id)
            if row is None or str(row["study_name"]) != expected:
                raise ValueError(f"run {run_id!r} is not a succeeded {expected} run")
    coarse = load_research_observations(config, run_ids=[coarse_run_id])
    opening = load_research_observations(config, run_ids=[opening_run_id])
    keys = ["instrument_id", "event_date"]
    coarse_events = coarse.select(keys).unique()
    opening_events = opening.select(keys).unique()
    common = coarse_events.join(opening_events, on=keys, how="inner")

    label_map = pl.DataFrame(
        {
            "coarse_label": list(_COMMON_LABELS),
            "opening_label": list(_COMMON_LABELS.values()),
        }
    )
    paired = (
        coarse.join(common, on=keys, how="semi")
        .select(
            *keys,
            "period",
            pl.col("observation_label").alias("coarse_label"),
            pl.col("measured_return").alias("coarse_return"),
            pl.col("outcome_status").alias("coarse_status"),
        )
        .join(label_map, on="coarse_label", how="inner")
        .join(
            opening.select(
                *keys,
                pl.col("observation_label").alias("opening_label"),
                pl.col("measured_return").alias("opening_return"),
                pl.col("outcome_status").alias("opening_status"),
            ),
            on=[*keys, "opening_label"],
            how="left",
        )
    )
    both = paired.filter(
        (pl.col("coarse_status") == "evaluable")
        & (pl.col("opening_status") == "evaluable")
    )
    agreement = (
        both.with_columns(
            (pl.col("coarse_return") - pl.col("opening_return")).abs().alias("abs_diff")
        )
        .group_by("period", "coarse_label")
        .agg(
            pl.len().alias("compared"),
            (pl.col("abs_diff") <= 1e-12).mean().alias("exact_agreement_share"),
            pl.col("abs_diff").mean().alias("mean_abs_return_diff"),
            pl.col("abs_diff").max().alias("max_abs_return_diff"),
        )
        .rename({"coarse_label": "checkpoint"})
        .sort("period", "checkpoint")
    )
    early = (
        opening.join(common, on=keys, how="semi")
        .filter(
            pl.col("observation_label").is_in(
                [
                    "09:35_close_of_09:30_bar",
                    "09:45_close_of_09:40_bar",
                    "10:00_close_of_09:55_bar",
                    "10:30_close_of_10:25_bar",
                ]
            )
            & (pl.col("outcome_status") == "evaluable")
        )
        .group_by("period", "observation_label")
        .agg(
            pl.len().alias("evaluable"),
            pl.col("measured_return").median().alias("median_return"),
            pl.col("measured_return_from_first_bar")
            .median()
            .alias("median_return_from_first_bar"),
            pl.col("reached_target_at_checkpoint").mean().alias("hit_rate_target"),
            pl.col("excess_return").mean().alias("mean_excess_return"),
            pl.col("zero_volume_bars_through_checkpoint")
            .sum()
            .alias("zero_volume_bars"),
            pl.col("bars_through_checkpoint").sum().alias("bars"),
        )
        .rename({"observation_label": "checkpoint"})
        .sort("period", "checkpoint")
    )
    return GapStudyComparison(
        coarse_run_id=coarse_run_id,
        opening_run_id=opening_run_id,
        coarse_events=coarse_events.height,
        opening_events=opening_events.height,
        common_events=common.height,
        checkpoint_agreement=agreement,
        opening_window=early,
    )
