"""Listing-episode detection and live EOD repair tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

import marketdata.identity_episodes as episode_repair
from marketdata.calendar import session_schedule
from marketdata.identity_episodes import (
    recover_interrupted_eod_episode_repairs,
    repair_eod_episodes,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import CANONICAL_EOD_SCHEMA, eod_frame


def _rows(dates: list[date], *, volume: int = 100) -> pl.DataFrame:
    return eod_frame(
        "TEST",
        [
            {
                "date": f"{day}T00:00:00.000Z",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": volume,
                "adjOpen": 10.0,
                "adjHigh": 11.0,
                "adjLow": 9.0,
                "adjClose": 10.5,
                "adjVolume": volume,
                "divCash": 0.0,
                "splitFactor": 1.0,
            }
            for day in dates
        ],
    )


def _identity(
    meta: MetaStore,
    instrument_id: str,
    ticker: str,
    start: date,
    end: date,
) -> None:
    meta.upsert_instrument(instrument_id, description="source")
    meta.add_instrument_alias(instrument_id, ticker, start, end)
    meta.add_vendor_identifier(
        instrument_id,
        "eod",
        "ticker",
        ticker,
        start,
        end,
        validation_state="validated",
    )
    meta.set_coverage(instrument_id, "eod", start, end)


def test_gap_repair_is_dry_run_then_recoverable_and_idempotent(tmp_path):
    bars = BarStore(tmp_path)
    alias_start, alias_end = date(2022, 1, 3), date(2024, 12, 31)
    first_dates = session_schedule(date(2022, 1, 3), date(2022, 2, 28))[
        "session_date"
    ].to_list()[:20]
    second_dates = session_schedule(date(2024, 6, 3), date(2024, 7, 31))[
        "session_date"
    ].to_list()[:20]
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()
        _identity(meta, "source-id", "REUSE", alias_start, alias_end)
        bars.publish_eod({"source-id": _rows(first_dates + second_dates)})

        dry_run = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=False)
        assert not dry_run.applied
        assert (dry_run.split_sources, dry_run.created_episodes) == (1, 2)
        boundary = dry_run.repairs[0]["boundaries"][0]
        gap_sessions = session_schedule(first_dates[-1], second_dates[0])[
            "session_date"
        ].to_list()[1:-1]
        assert boundary["first_missing_or_zero"] == str(gap_sessions[0])
        assert boundary["last_missing_or_zero"] == str(gap_sessions[-1])
        assert bars.read_canonical_eod("source-id").height == 40

        applied = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert applied.applied
        assert Path(applied.backup_path).joinpath("meta.db").is_file()
        assert bars.read_canonical_eod("source-id") is None
        episodes = [row for row in meta.identity_episodes() if row["ticker"] == "REUSE"]
        assert len(episodes) == 2
        assert [row["display_label"] for row in episodes] == [
            "REUSE@20220103",
            "REUSE@20240603",
        ]
        assert all(
            bars.read_canonical_eod(row["instrument_id"]).height == 20
            for row in episodes
        )
        gap = meta.resolve_alias_range("REUSE", date(2023, 1, 1), date(2023, 1, 2))
        assert not gap.resolved
        assert gap.segments[0].status == "zero_matches"

        repeated = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert not repeated.applied
        assert repeated.split_sources == 0


def test_zero_volume_bridge_and_invalid_ohlc_are_quarantined(tmp_path):
    bars = BarStore(tmp_path)
    sessions = session_schedule(date(2023, 1, 3), date(2023, 7, 31))[
        "session_date"
    ].to_list()
    episode_dates = sessions[:20] + sessions[20:83] + sessions[83:103]
    frame = pl.concat(
        [
            _rows(episode_dates[:20], volume=100),
            _rows(episode_dates[20:83], volume=0),
            _rows(episode_dates[83:], volume=100),
        ]
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()
        _identity(meta, "bridge-id", "BRIDGE", sessions[0], sessions[-1])
        bars.publish_eod({"bridge-id": frame})

        _identity(meta, "bad-id", "BAD", sessions[0], sessions[-1])
        bad = bars.canonicalize_eod("bad-id", _rows(sessions[:2]))
        bad = bad.with_columns(
            pl.when(pl.col("date") == sessions[0])
            .then(pl.lit(12.0))
            .otherwise(pl.col("low"))
            .alias("low")
        ).cast(CANONICAL_EOD_SCHEMA)
        bad_path = bars.canonical_eod_path("bad-id")
        bad_path.parent.mkdir(parents=True, exist_ok=True)
        bad.write_parquet(bad_path)

        meta.upsert_instrument("out-of-scope-id")
        outside = bad.with_columns(instrument_id=pl.lit("out-of-scope-id"))
        bars.publish_eod({"out-of-scope-id": outside})

        result = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)

        assert result.split_sources == 1
        assert result.quarantined_rows == 64
        quarantined = pl.read_parquet(result.quarantine_path)
        assert set(quarantined["quarantine_reason"]) == {
            "ohlc_invariants",
            "zero_volume_episode_bridge",
        }
        repaired_bad = bars.read_canonical_eod("bad-id")
        assert repaired_bad["date"].to_list() == [sessions[1]]
        assert bars.read_canonical_eod("out-of-scope-id").height == 2


def test_uncovered_gap_does_not_define_an_episode_boundary(tmp_path):
    bars = BarStore(tmp_path)
    first_dates = session_schedule(date(2022, 1, 3), date(2022, 2, 28))[
        "session_date"
    ].to_list()[:20]
    second_dates = session_schedule(date(2024, 6, 3), date(2024, 7, 31))[
        "session_date"
    ].to_list()[:20]
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()
        _identity(meta, "source-id", "REUSE", date(2022, 1, 3), date(2024, 12, 31))
        bars.publish_eod({"source-id": _rows(first_dates + second_dates)})
        meta.set_coverage("source-id", "eod", second_dates[0], date(2024, 12, 31))

        result = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=False)

        assert result.split_sources == 0


def test_retry_after_identity_registration_rebuilds_staging(tmp_path, monkeypatch):
    bars = BarStore(tmp_path)
    first_dates = session_schedule(date(2022, 1, 3), date(2022, 2, 28))[
        "session_date"
    ].to_list()[:20]
    second_dates = session_schedule(date(2024, 6, 3), date(2024, 7, 31))[
        "session_date"
    ].to_list()[:20]
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()
        _identity(meta, "source-id", "REUSE", date(2022, 1, 3), date(2024, 12, 31))
        bars.publish_eod({"source-id": _rows(first_dates + second_dates)})

        def fail_before_swap(*_args):
            raise RuntimeError("simulated pre-swap crash")

        monkeypatch.setattr(episode_repair, "_swap_eod_root", fail_before_swap)
        with pytest.raises(RuntimeError, match="pre-swap crash"):
            repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert bars.read_canonical_eod("source-id").height == 40

        monkeypatch.undo()
        assert recover_interrupted_eod_episode_repairs(bars, meta) == 1
        assert meta.identity_episodes() == []
        retried = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert retried.applied
        assert retried.split_sources == 1
        assert bars.read_canonical_eod("source-id") is None


def test_retry_finishes_metadata_after_swapped_root(tmp_path, monkeypatch):
    bars = BarStore(tmp_path)
    first_dates = session_schedule(date(2022, 1, 3), date(2022, 2, 28))[
        "session_date"
    ].to_list()[:20]
    second_dates = session_schedule(date(2024, 6, 3), date(2024, 7, 31))[
        "session_date"
    ].to_list()[:20]
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.activate_canonical_generation()
        _identity(meta, "source-id", "REUSE", date(2022, 1, 3), date(2024, 12, 31))
        bars.publish_eod({"source-id": _rows(first_dates + second_dates)})
        original_swap = episode_repair._swap_eod_root

        def swap_then_fail(*args):
            original_swap(*args)
            raise RuntimeError("simulated post-swap crash")

        monkeypatch.setattr(episode_repair, "_swap_eod_root", swap_then_fail)
        with pytest.raises(RuntimeError, match="post-swap crash"):
            repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert bars.read_canonical_eod("source-id") is None

        monkeypatch.undo()
        recovered = repair_eod_episodes(bars, meta, min_gap_sessions=63, apply=True)
        assert recovered.applied
        assert recovered.recovered_sources == 1
        assert recovered.split_sources == 0
        assert meta.instrument_alias_records("source-id") == []
