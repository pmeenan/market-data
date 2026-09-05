"""Multi-session EOD gap-recovery study and the propensity ranking."""

from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner
from test_gap_recovery import _EVENT, _PARAMS, _SESSIONS, _eod

from marketdata.cli import main
from marketdata.config import Config
from marketdata.research import registered_event_studies, run_registered_event_study
from marketdata.store.bars import BarStore
from marketdata.store.meta import MetaStore
from marketdata.studies.gap_recovery_multiday import STUDY_NAME, horizon_label
from marketdata.studies.propensity import (
    Window,
    _scope,
    load_group_tags,
    two_pass,
    walk_forward_windows,
)

_MULTIDAY_PARAMS = {
    key: value for key, value in _PARAMS.items() if key != "min_hourly_density"
}


def _fixture(tmp_path) -> Config:
    config = Config(tmp_path / "data", None)
    config.ensure_dirs()
    with MetaStore(config.meta_path) as meta:
        meta.activate_canonical_generation()
        for instrument_id, ticker, asset_type in (
            ("gap-id", "GAPY", "Stock"),
            ("spy-id", "SPY", "ETF"),
        ):
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(
                instrument_id,
                ticker,
                _SESSIONS[0],
                _SESSIONS[-1],
                asset_type=asset_type,
            )
    index = _SESSIONS.index(_EVENT)
    closes = [100.0] * len(_SESSIONS)
    closes[index] = 96.0  # event session close
    closes[index + 1] = 97.0  # +1
    closes[index + 2] = 99.0  # +2
    closes[index + 3] = 101.0  # +3
    spy = [500.0] * len(_SESSIONS)
    spy[index] = 502.0
    BarStore(config.data_dir).publish_eod(
        {
            "gap-id": _eod("GAPY", closes, opens={_EVENT: 95.0}),
            "spy-id": _eod("SPY", spy, opens={_EVENT: 499.0}),
        }
    )
    return config


def test_multiday_outcomes_are_adjusted_basis_and_missing_beyond_history(tmp_path):
    config = _fixture(tmp_path)
    assert STUDY_NAME in registered_event_studies()

    published = run_registered_event_study(config, STUDY_NAME, _MULTIDAY_PARAMS)
    observations = pl.read_parquet(published.observation_path)

    assert observations["instrument_id"].unique().to_list() == ["gap-id"]
    assert observations["observation_label"].to_list() == [
        horizon_label(k) for k in (0, 1, 2, 3, 5, 10)
    ]
    by_label = {
        row["observation_label"]: row for row in observations.iter_rows(named=True)
    }
    zero = by_label["close_plus_0_sessions"]
    assert zero["checkpoint_session"] == _EVENT
    assert zero["measured_return"] == pytest.approx(96.0 / 95.0 - 1.0)
    assert zero["benchmark_return"] == pytest.approx(502.0 / 499.0 - 1.0)
    assert zero["gap_recovered_fraction_adjusted_basis"] == pytest.approx(1.0 / 5.0)
    assert zero["reached_target_at_checkpoint"] is True
    one = by_label["close_plus_1_sessions"]
    assert one["checkpoint_session"] == date(2024, 1, 9)
    assert one["measured_return"] == pytest.approx(97.0 / 95.0 - 1.0)
    # The +3 close is 101; its adjusted high (101 * 1.01) bounds the excursion.
    three = by_label["close_plus_3_sessions"]
    assert three["max_favorable_excursion"] == pytest.approx(101.0 * 1.01 / 95.0 - 1.0)
    assert three["max_adverse_excursion"] == pytest.approx(95.0 * 0.99 / 95.0 - 1.0)
    assert three["touched_target_within_horizon"] is True
    # +5 and +10 sessions fall after the stored history: explicit misses.
    assert by_label["close_plus_5_sessions"]["outcome_status"] == "missing_outcome"
    assert by_label["close_plus_10_sessions"]["outcome_status"] == "missing_outcome"
    assert zero["asset_type"] == "Stock"
    assert zero["market_regime"] == "calm"

    with MetaStore(config.meta_path) as meta:
        audit = {
            str(r["metric_name"]): float(r["value"])
            for r in meta.research_metrics(published.run_id)
            if str(r["dimensions_json"]) == "{}"
        }
    assert audit["event_audit.selected"] == 1
    assert audit["event_audit.missing_outcome"] == 1


def test_news_sized_gap_screen_uses_prior_volatility(tmp_path):
    config = _fixture(tmp_path)
    # Prior sessions are flat, so realized volatility is zero and the
    # normalized gap is null: a positive threshold excludes the event.
    published = run_registered_event_study(
        config, STUDY_NAME, {**_MULTIDAY_PARAMS, "min_abs_gap_vol_normalized": 2.0}
    )
    assert pl.read_parquet(published.observation_path).is_empty()


def _synthetic_observations() -> pl.DataFrame:
    rows = []

    def add(instrument, year, hits, misses):
        for index, outcome in enumerate([True] * hits + [False] * misses):
            rows.append(
                {
                    "instrument_id": instrument,
                    "event_date": date(year, 1 + index % 12, 1 + index // 12),
                    "period": "development" if year < 2023 else "validation",
                    "observation_label": "close_plus_1_sessions",
                    "outcome_status": "evaluable",
                    "reached_target_at_checkpoint": outcome,
                    "measured_return": 0.02 if outcome else -0.01,
                    "excess_return": 0.01 if outcome else -0.01,
                }
            )

    for year in (2020, 2021, 2022):
        add("strong", year, 10, 3)  # reliably above baseline
        add("lucky", year, 2, 2)  # too few events to select
        add("base", year, 12, 18)  # defines the 40% baseline
        add("fade", year, 10, 3)  # strong in selection, collapses later
    add("strong", 2023, 15, 5)
    add("lucky", 2023, 3, 0)
    add("base", 2023, 20, 30)
    add("fade", 2023, 2, 18)
    return pl.DataFrame(rows)


def test_two_pass_selects_on_one_window_and_scores_the_frozen_list_on_the_next():
    observations = _synthetic_observations()
    tickers = {"strong": "STRG", "lucky": "LCKY", "base": "BASE", "fade": "FADE"}
    scoped = _scope(observations, "close_plus_1_sessions")
    selection = Window("development", date(2020, 1, 1), date(2022, 12, 31))
    evaluation = Window("validation", date(2023, 1, 1), date(2023, 12, 31))

    result = two_pass(
        scoped,
        selection,
        evaluation,
        tickers=tickers,
        min_events=10,
        min_lift=0.05,
        prior_strength=20,
        group_tags={"STRG": "tech", "FADE": "tech", "BASE": "other"},
    )

    selected = result.candidates.filter(pl.col("selected"))["ticker"].to_list()
    assert selected == ["STRG", "FADE"] or selected == ["FADE", "STRG"]
    lucky = result.candidates.filter(pl.col("ticker") == "LCKY").row(0, named=True)
    assert lucky["events"] == 12 and lucky["selected"] is False
    evaluated = {row["ticker"]: row for row in result.evaluated.iter_rows(named=True)}
    assert evaluated["STRG"]["hit_rate"] == pytest.approx(0.75)
    assert evaluated["STRG"]["lift"] > 0
    assert evaluated["FADE"]["hit_rate"] == pytest.approx(0.10)
    assert evaluated["FADE"]["lift"] < 0
    summary = result.summary
    assert summary["candidates"] == 2
    assert summary["candidates_above_baseline"] == 1
    assert summary["candidate_events"] == 40 and summary["candidate_hits"] == 17
    assert summary["baseline_events"] == 93
    tech = result.groups.filter(
        (pl.col("group") == "tech") & (pl.col("pass") == "evaluate")
    ).row(0, named=True)
    assert tech["tickers"] == 2 and tech["events"] == 40
    with pytest.raises(ValueError, match="must start after"):
        two_pass(scoped, evaluation, selection, tickers=tickers)
    with pytest.raises(ValueError, match="no evaluable observations"):
        _scope(observations, "nope")


def test_walk_forward_windows_roll_by_year_and_skip_test_period():
    observations = _synthetic_observations().with_columns(
        pl.when(pl.col("event_date").dt.year() == 2023)
        .then(pl.lit("test"))
        .otherwise(pl.col("period"))
        .alias("period")
    )
    scoped = _scope(observations, "close_plus_1_sessions")

    pairs = walk_forward_windows(scoped, select_years=1)

    assert [(s.label, e.label) for s, e in pairs] == [
        ("2020-2020", "2021"),
        ("2021-2021", "2022"),
    ]
    assert walk_forward_windows(scoped, select_years=5) == []


def test_group_tags_file_and_rank_cli(tmp_path):
    tags = tmp_path / "tags.csv"
    tags.write_text("ticker,group\nGAPY,tech\n\nbad-row\n")
    assert load_group_tags(tags) == {"GAPY": "tech"}

    config = _fixture(tmp_path)
    published = run_registered_event_study(config, STUDY_NAME, _MULTIDAY_PARAMS)
    out = tmp_path / "private" / "ranking.csv"
    result = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(config.data_dir),
            "research-rank",
            published.run_id,
            "--checkpoint",
            "close_plus_1_sessions",
            "--min-events",
            "1",
            "--tags",
            str(tags),
            "--out",
            str(out),
        ],
    )

    # The fixture has one validation event and no development events, so the
    # named-period form fails closed rather than inventing an empty pass.
    assert result.exit_code != 0
    assert "no evaluable events" in result.output

    rolling = CliRunner().invoke(
        main,
        [
            "--data-dir",
            str(config.data_dir),
            "research-rank",
            published.run_id,
            "--checkpoint",
            "close_plus_1_sessions",
            "--walk-forward-years",
            "1",
        ],
    )
    assert rolling.exit_code != 0
    assert "not enough non-test history" in rolling.output
