"""Identity-safe v1 quarantine and canonical migration fixtures."""

import json
from datetime import date

import polars as pl

from marketdata.migration import migrate_v1_bars
from marketdata.reconcile import reconcile_canonical
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import eod_frame, instrument_bucket, intraday_frame


def _eod_row(day: date, close: float = 100.0) -> dict:
    return {
        "date": f"{day.isoformat()}T00:00:00.000Z",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "adjOpen": close,
        "adjHigh": close,
        "adjLow": close,
        "adjClose": close,
        "adjVolume": 1000,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def _intraday_row(day: date) -> dict:
    return {
        "date": f"{day.isoformat()}T15:00:00.000Z",
        "open": 10.0,
        "high": 10.5,
        "low": 9.5,
        "close": 10.2,
        "volume": 100,
    }


def test_migration_quarantines_generation_and_reports_unsafe_sources(
    tmp_path, monkeypatch
):
    bars = BarStore(tmp_path)
    meta = MetaStore(tmp_path / "meta.db")
    company = meta.upsert_instrument("stable-company")
    old_listing = meta.upsert_instrument("old-reused-listing")
    new_listing = meta.upsert_instrument("new-reused-listing")
    gap_instrument = meta.upsert_instrument("gap-instrument")
    overlap_peer = meta.upsert_instrument("overlap-peer")

    meta.add_instrument_alias(company, "OLD", date(2020, 1, 1), date(2020, 12, 31))
    meta.add_instrument_alias(company, "NEW", date(2021, 1, 1), date(2021, 12, 31))
    meta.add_instrument_alias(
        old_listing, "REUSE", date(2019, 1, 1), date(2020, 12, 31)
    )
    meta.add_instrument_alias(
        new_listing, "REUSE", date(2022, 1, 1), date(2023, 12, 31)
    )
    meta.add_instrument_alias(
        gap_instrument, "GAP", date(2020, 1, 1), date(2020, 12, 31)
    )
    meta.add_instrument_alias(
        gap_instrument, "GAP", date(2022, 1, 1), date(2022, 12, 31)
    )
    meta.add_instrument_alias(company, "OVERLAP", date(2020, 1, 1), date(2020, 12, 31))
    meta.add_instrument_alias(
        overlap_peer, "OVERLAP", date(2020, 6, 1), date(2020, 12, 31)
    )
    meta.add_instrument_alias(company, "INTRA", date(2023, 1, 1), date(2025, 12, 31))

    bars.write_eod("OLD", eod_frame("OLD", [_eod_row(date(2020, 6, 1))]))
    bars.write_eod("NEW", eod_frame("NEW", [_eod_row(date(2021, 6, 1))]))
    bars.write_eod(
        "REUSE",
        eod_frame(
            "REUSE",
            [_eod_row(date(2020, 6, 1)), _eod_row(date(2022, 6, 1))],
        ),
    )
    bars.write_eod(
        "GAP",
        eod_frame("GAP", [_eod_row(date(2020, 6, 1)), _eod_row(date(2022, 6, 1))]),
    )
    bars.write_eod(
        "OVERLAP",
        eod_frame(
            "OVERLAP",
            [_eod_row(date(2020, 5, 1)), _eod_row(date(2020, 7, 1))],
        ),
    )
    bars.write_intraday(
        "INTRA", intraday_frame("INTRA", [_intraday_row(date(2023, 6, 1))])
    )
    meta.set_ticker_coverage_v1("OLD", "eod", date(2000, 1, 1), date(2025, 1, 1))
    bars.write_intraday(
        "INTRA", intraday_frame("INTRA", [_intraday_row(date(2025, 6, 1))])
    )

    original_publish = bars.publish_eod

    def assert_boundary(frames, **kwargs):
        assert not (tmp_path / "eod").exists()
        assert not (tmp_path / "intraday").exists()
        return original_publish(frames, **kwargs)

    monkeypatch.setattr(bars, "publish_eod", assert_boundary)
    report = migrate_v1_bars(bars, meta)

    assert meta.storage_generation() == "v2"
    assert meta.ticker_coverage_v1("eod") == {}
    assert report.counts() == {"conflict": 2, "migrated": 4, "unresolved": 1}
    by_ticker = {}
    for item in report.items:
        by_ticker.setdefault(item.ticker, set()).add(item.status)
    assert by_ticker == {
        "GAP": {"unresolved"},
        "NEW": {"migrated"},
        "OLD": {"migrated"},
        "OVERLAP": {"conflict"},
        "REUSE": {"conflict"},
        "INTRA": {"migrated"},
    }
    assert (tmp_path / "quarantine/v1-ticker-bars/eod/OLD.parquet").exists()
    assert (
        tmp_path / "quarantine/v1-ticker-bars/intraday/1hour/INTRA/2023.parquet"
    ).exists()
    assert not (tmp_path / "eod").exists()
    assert not (tmp_path / "intraday").exists()

    canonical = bars.read_canonical_eod(company)
    assert canonical["date"].to_list() == [date(2020, 6, 1), date(2021, 6, 1)]
    assert canonical["instrument_id"].unique().to_list() == [company]
    assert "ticker" not in canonical.columns
    assert bars.read_canonical_eod(old_listing) is None
    assert bars.read_canonical_eod(new_listing) is None

    assert meta.get_coverage(company, "eod") == (
        date(2020, 6, 1),
        date(2021, 6, 1),
    )
    assert meta.get_coverage(company, "intraday_1hour") is None
    assert [
        (issue.issue, issue.instrument_id) for issue in report.reconciliation_issues
    ] == [("disconnected_years", company)]
    assert (tmp_path / "quarantine/v1-ticker-bars/migration-report.json").exists()

    # Sources stay quarantined, so rerunning is a deterministic merge-upsert.
    (tmp_path / "eod").mkdir()  # legacy init may recreate an empty root
    rerun = migrate_v1_bars(bars, meta)
    assert rerun.to_dict() == report.to_dict()
    assert bars.read_canonical_eod(company).height == 2
    meta.close()


def test_reconcile_omits_wrong_bucket_rows_and_atomically_drops_stale_coverage(
    tmp_path,
):
    bars = BarStore(tmp_path)
    with MetaStore(tmp_path / "meta.db") as meta:
        first = meta.upsert_instrument("first")
        wrong = meta.upsert_instrument("wrong")
        meta.set_coverage(wrong, "eod", date(2000, 1, 1), date(2000, 1, 2))
        bars.publish_eod({first: eod_frame("FIRST", [_eod_row(date(2024, 1, 2))])})
        correct_path = bars.canonical_eod_path(first)
        frame = pl.read_parquet(correct_path).with_columns(instrument_id=pl.lit(wrong))
        frame.write_parquet(correct_path)

        report = reconcile_canonical(bars, meta)

        assert report.coverage == {}
        assert meta.coverage("eod") == {}
        assert len(report.issues) == 1
        assert report.issues[0].issue == "wrong_bucket"


def test_invalid_null_envelope_is_reported_without_aborting_migration(tmp_path):
    bars = BarStore(tmp_path)
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("valid-instrument")
        meta.add_instrument_alias(
            instrument_id, "VALID", date(2024, 1, 1), date(2024, 12, 31)
        )
        bars.write_eod("VALID", eod_frame("VALID", [_eod_row(date(2024, 1, 2))]))
        invalid = eod_frame("NULLDATE", [_eod_row(date(2024, 1, 2))]).with_columns(
            pl.lit(None, dtype=pl.Date).alias("date")
        )
        invalid.write_parquet(bars.eod_path("NULLDATE"))

        report = migrate_v1_bars(bars, meta)

        assert report.counts() == {"invalid_source": 1, "migrated": 1}
        invalid_item = next(item for item in report.items if item.ticker is None)
        assert "null or invalid" in invalid_item.detail
        assert bars.read_canonical_eod(instrument_id).height == 1


def test_bad_frame_does_not_block_valid_bucket_peer(tmp_path):
    bars = BarStore(tmp_path)
    with MetaStore(tmp_path / "meta.db") as meta:
        good_id = meta.upsert_instrument("instrument-1")
        bad_id = next(
            f"bad-{number}"
            for number in range(1000)
            if instrument_bucket(f"bad-{number}") == instrument_bucket(good_id)
        )
        meta.upsert_instrument(bad_id)
        meta.add_instrument_alias(good_id, "GOOD", date(2024, 1, 1), date(2024, 12, 31))
        meta.add_instrument_alias(bad_id, "BAD", date(2024, 1, 1), date(2024, 12, 31))
        bars.write_eod("GOOD", eod_frame("GOOD", [_eod_row(date(2024, 1, 2))]))
        malformed = eod_frame("BAD", [_eod_row(date(2024, 1, 2))]).drop("open")
        malformed.write_parquet(bars.eod_path("BAD"))

        report = migrate_v1_bars(bars, meta)

        assert report.counts() == {"migrated": 1, "publish_failed": 1}
        assert bars.read_canonical_eod(good_id).height == 1
        assert bars.read_canonical_eod(bad_id) is None


def test_reconcile_reports_unknown_instrument_and_current_day_only(tmp_path):
    bars = BarStore(tmp_path)
    with MetaStore(tmp_path / "meta.db") as meta:
        known = meta.upsert_instrument("known")
        bars.publish_eod(
            {"unknown": eod_frame("UNKNOWN", [_eod_row(date(2024, 1, 2))])}
        )
        bars.publish_intraday(
            {known: intraday_frame("KNOWN", [_intraday_row(date.today())])}
        )

        report = reconcile_canonical(bars, meta)

        assert report.coverage == {}
        assert {(issue.instrument_id, issue.issue) for issue in report.issues} == {
            ("unknown", "unknown_instrument"),
            (known, "current_day_only"),
        }


def test_generation_boundary_move_failure_is_reported_and_publishes_nothing(
    tmp_path, monkeypatch
):
    import pytest

    bars = BarStore(tmp_path)
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("known")
        meta.add_instrument_alias(
            instrument_id, "KNOWN", date(2024, 1, 1), date(2024, 12, 31)
        )
        bars.write_eod("KNOWN", eod_frame("KNOWN", [_eod_row(date(2024, 1, 2))]))
        bars.write_intraday(
            "KNOWN", intraday_frame("KNOWN", [_intraday_row(date(2024, 1, 2))])
        )
        original_replace = type(tmp_path).replace

        def fail_intraday_move(self, target):
            if self == tmp_path / "intraday":
                raise OSError("simulated intraday move failure")
            return original_replace(self, target)

        monkeypatch.setattr(type(tmp_path), "replace", fail_intraday_move)
        with pytest.raises(RuntimeError, match="generation boundary"):
            migrate_v1_bars(bars, meta)

        assert meta.storage_generation() == "v1"
        assert not bars.has_canonical_bars()
        assert (tmp_path / "quarantine/v1-ticker-bars/eod/KNOWN.parquet").exists()
        assert (tmp_path / "intraday/1hour/KNOWN/2024.parquet").exists()
        payload = json.loads(
            (tmp_path / "quarantine/v1-ticker-bars/migration-report.json").read_text()
        )
        assert payload["items"][0]["status"] == "publish_failed"
        assert "simulated intraday move failure" in payload["items"][0]["detail"]
