import sqlite3
from datetime import date

from marketdata.identity import ACTIVE_ALIAS_END
from marketdata.store.meta import MetaStore


def test_schema_v7_migration_is_durable_and_reopenable(tmp_path):
    path = tmp_path / "meta.db"
    with MetaStore(path):
        pass
    with sqlite3.connect(path) as con:
        assert con.execute("PRAGMA user_version").fetchone()[0] == 7
    with MetaStore(path) as meta:
        assert meta.research_runs() == []


def test_research_parameter_names_are_unique_after_trimming(tmp_path):
    import pytest

    with MetaStore(tmp_path / "meta.db") as meta:
        with pytest.raises(ValueError, match="unique after trimming"):
            meta.create_research_run(
                run_id="duplicate-parameters",
                study_name="fixture-study",
                study_schema_version=1,
                parameters={"threshold": 1, " threshold": 2},
            )
        assert meta.research_runs() == []


def test_research_run_ids_are_normalized_across_lifecycle_calls(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.create_research_run(
            run_id=" padded-run ",
            study_name="fixture-study",
            study_schema_version=1,
            parameters={},
        )
        meta.succeed_research_run(
            run_id=" padded-run ",
            input_fingerprint="a" * 64,
            observation_path="results/fixture-study/padded-run/observations.parquet",
            manifest_path="results/fixture-study/padded-run/input_files.parquet",
            observation_count=0,
            metrics=[],
        )
        assert meta.research_run(" padded-run ")["status"] == "succeeded"


def test_research_run_selection_checks_sqlite_variable_limit(tmp_path):
    import pytest

    with MetaStore(tmp_path / "meta.db") as meta:
        meta._con.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 3)
        with pytest.raises(ValueError, match="at most 3"):
            meta.select_research_artifacts(["one", "two", "three", "four"])


def test_inflight_schema_v4_gets_additive_scheduler_columns(tmp_path):
    path = tmp_path / "meta.db"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE api_request_attempts (
            attempt_id INTEGER PRIMARY KEY,
            occurred_at TEXT NOT NULL,
            work_kind TEXT NOT NULL,
            operation TEXT NOT NULL,
            reserved_bytes INTEGER NOT NULL,
            observed_bytes INTEGER NOT NULL DEFAULT 0,
            settled INTEGER NOT NULL DEFAULT 0,
            complete INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE history_jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
        CREATE TABLE history_ranges (
            job_id TEXT NOT NULL,
            target_ordinal INTEGER NOT NULL,
            range_ordinal INTEGER NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO api_request_attempts
            (occurred_at, work_kind, operation, reserved_bytes, observed_bytes,
             settled, complete)
        VALUES
            ('2026-08-28T00:00:00+00:00', 'current', 'done', 100, 10, 1, 1),
            ('2026-08-28T00:00:01+00:00', 'current', 'partial', 100, 7, 1, 0);
        PRAGMA user_version = 4;
        """
    )
    con.close()

    with MetaStore(path) as meta:
        assert [row["bytes_known"] for row in meta.request_attempts()] == [1, 0]
        assert "cancelled" in {
            row["name"]
            for row in meta._con.execute("PRAGMA table_info('history_jobs')")
        }
        assert "terminal_blocked" in {
            row["name"]
            for row in meta._con.execute("PRAGMA table_info('history_ranges')")
        }


def test_universe_roundtrip(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.set_universe(
            2024,
            [
                {"ticker": "aapl", "rank": 1, "avg_dollar_volume": 1e10},
                {"ticker": "MSFT", "rank": 2, "avg_dollar_volume": 9e9},
            ],
        )
        meta.set_universe(2025, [{"ticker": "NVDA", "rank": 1}])

        rows = meta.universe(2024)
        assert [r["ticker"] for r in rows] == ["AAPL", "MSFT"]
        assert meta.universe_years() == [2024, 2025]
        assert meta.all_universe_tickers() == ["AAPL", "MSFT", "NVDA"]
        assert meta.latest_universe_tickers() == ["NVDA"]

        # replace semantics
        meta.set_universe(2024, [{"ticker": "TSLA", "rank": 1}])
        assert [r["ticker"] for r in meta.universe(2024)] == ["TSLA"]


def test_coverage(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        assert meta.get_ticker_coverage_v1("AAPL", "eod") is None
        meta.set_ticker_coverage_v1("aapl", "eod", date(2020, 1, 2), date(2024, 6, 28))
        assert meta.get_ticker_coverage_v1("AAPL", "eod") == (
            date(2020, 1, 2),
            date(2024, 6, 28),
        )

        # extend widens in both directions and never shrinks
        meta.extend_ticker_coverage_v1(
            "AAPL", "eod", date(1995, 1, 3), date(1999, 12, 31)
        )
        assert meta.get_ticker_coverage_v1("AAPL", "eod") == (
            date(1995, 1, 3),
            date(2024, 6, 28),
        )
        meta.extend_ticker_coverage_v1(
            "AAPL", "eod", date(2024, 6, 1), date(2024, 7, 1)
        )
        assert meta.get_ticker_coverage_v1("AAPL", "eod") == (
            date(1995, 1, 3),
            date(2024, 7, 1),
        )

        assert meta.ticker_coverage_v1("eod") == {
            "AAPL": (date(1995, 1, 3), date(2024, 7, 1))
        }
        meta.clear_ticker_coverage_v1("eod")
        assert meta.ticker_coverage_v1("eod") == {}


def test_instrument_coverage_is_exact_keyed_and_replaced_atomically(tmp_path):
    import pytest

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.upsert_instrument("opaque-lowercase-id")
        meta.set_coverage(
            "opaque-lowercase-id", "eod", date(2020, 1, 2), date(2024, 6, 28)
        )
        assert meta.get_coverage("opaque-lowercase-id", "eod") == (
            date(2020, 1, 2),
            date(2024, 6, 28),
        )
        assert meta.get_coverage("OPAQUE-LOWERCASE-ID", "eod") is None

        meta.replace_coverage(
            {
                ("opaque-lowercase-id", "intraday_1hour"): (
                    date(2024, 1, 2),
                    date(2024, 12, 30),
                )
            }
        )
        assert meta.coverage("eod") == {}
        assert meta.coverage("intraday_1hour") == {
            "opaque-lowercase-id": (date(2024, 1, 2), date(2024, 12, 30))
        }
        with pytest.raises(ValueError, match="dataset_key"):
            meta.coverage("iex")


def test_migration_from_v0(tmp_path):
    import sqlite3

    path = tmp_path / "meta.db"
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE watermarks (ticker TEXT, dataset TEXT, last_date TEXT, updated_at TEXT)"
    )
    con.commit()
    con.close()

    with MetaStore(path) as meta:
        # watermarks dropped, coverage available, version stamped
        assert meta.get_ticker_coverage_v1("AAPL", "eod") is None
        tables = {
            r["name"]
            for r in meta._con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "watermarks" not in tables
        assert "coverage" in tables
        assert meta._con.execute("PRAGMA user_version").fetchone()[0] == 7
        assert "ticker_coverage_v1" in tables


def test_migration_from_v1_preserves_existing_universe(tmp_path):
    import sqlite3

    path = tmp_path / "meta.db"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE universe (
            year INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            rank INTEGER,
            avg_dollar_volume REAL,
            PRIMARY KEY (year, ticker)
        );
        INSERT INTO universe VALUES (2025, 'AAPL', 1, 1000000.0);
        PRAGMA user_version = 1;
        """
    )
    con.close()

    with MetaStore(path) as meta:
        assert [row["ticker"] for row in meta.universe(2025)] == ["AAPL"]
        assert meta._con.execute("PRAGMA user_version").fetchone()[0] == 7
        assert (
            meta._con.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'instruments'"
            ).fetchone()[0]
            == 1
        )
        index_columns = [
            row["name"]
            for row in meta._con.execute(
                "PRAGMA index_info('vendor_identifiers_identity_lookup')"
            )
        ]
        assert index_columns[:3] == [
            "dataset_key",
            "identifier_type",
            "identifier_value",
        ]
        plan = meta._con.execute(
            """EXPLAIN QUERY PLAN
               SELECT DISTINCT instrument_id FROM vendor_identifiers
               WHERE dataset_key = ? AND identifier_type = ?
                 AND identifier_value = ? AND validation_state = 'validated'
                 AND valid_from <= ? AND valid_to >= ?""",
            ("eod", "permaTicker", "US0000001", "2025-01-01", "2020-01-01"),
        ).fetchall()
        assert "vendor_identifiers_identity_lookup" in " ".join(
            row["detail"] for row in plan
        )


def test_instrument_upsert_preserves_omitted_attributes_and_rejects_empty_ids(tmp_path):
    import pytest

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.upsert_instrument(
            "delisted", lifecycle_status="inactive", description="Old listing"
        )
        meta.upsert_instrument("delisted")
        row = meta._con.execute(
            """SELECT lifecycle_status, description FROM instruments
               WHERE instrument_id = 'delisted'"""
        ).fetchone()
        assert tuple(row) == ("inactive", "Old listing")

        with pytest.raises(ValueError, match="empty or whitespace"):
            meta.upsert_instrument("")
        with pytest.raises(ValueError, match="empty or whitespace"):
            meta.upsert_instrument("   ")


def test_identity_alias_resolution_reports_rename_reuse_gap_and_overlap(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        old = meta.upsert_instrument("old-company", description="Old listing")
        renamed = meta.upsert_instrument("renamed-company")
        reused = meta.upsert_instrument("new-fund")
        conflict = meta.upsert_instrument("conflicting-record")

        meta.add_instrument_alias(
            old,
            "OLD",
            date(2000, 1, 1),
            date(2009, 12, 31),
            exchange="NYSE",
            asset_type="Stock",
            evidence={"source": "fixture"},
        )
        meta.add_instrument_alias(
            old,
            "NEW",
            date(2010, 1, 1),
            date(2020, 12, 31),
            exchange="NYSE",
            asset_type="Stock",
        )
        meta.add_instrument_alias(
            renamed,
            "REUSE",
            date(2000, 1, 1),
            date(2010, 12, 31),
        )
        meta.add_instrument_alias(
            reused,
            "REUSE",
            date(2012, 1, 1),
            date(2025, 12, 31),
        )
        meta.add_instrument_alias(
            conflict,
            "REUSE",
            date(2018, 1, 1),
            date(2019, 12, 31),
        )

        rename = meta.resolve_alias_range("new", date(2015, 1, 1), date(2015, 1, 2))
        assert rename.resolved
        assert rename.segments[0].instrument_id == old

        report = meta.resolve_alias_range("reuse", date(2010, 1, 1), date(2020, 12, 31))
        assert [
            (segment.start, segment.end, segment.status) for segment in report.segments
        ] == [
            (date(2010, 1, 1), date(2010, 12, 31), "resolved"),
            (date(2011, 1, 1), date(2011, 12, 31), "zero_matches"),
            (date(2012, 1, 1), date(2017, 12, 31), "resolved"),
            (date(2018, 1, 1), date(2019, 12, 31), "multiple_matches"),
            (date(2020, 1, 1), date(2020, 12, 31), "resolved"),
        ]
        assert report.segments[0].instrument_id == renamed
        assert report.segments[-1].instrument_id == reused
        assert report.segments[3].instrument_ids == (conflict, reused)

        # None and the named closed sentinel are the same active-alias convention.
        alias_id = meta.add_instrument_alias(reused, "CURRENT", date(2025, 1, 1))
        assert (
            meta.add_instrument_alias(
                reused, "CURRENT", date(2025, 1, 1), ACTIVE_ALIAS_END
            )
            == alias_id
        )
        open_ended = meta.resolve_alias_range("CURRENT", date(2026, 1, 1), date.max)
        assert open_ended.resolved
        assert open_ended.segments[0].end == date.max


def test_vendor_identifier_resolution_is_dataset_specific(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("instrument-1")
        own_identifier_id = meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "US0000001",
            date(2000, 1, 1),
            date(2025, 12, 31),
            validation_state="validated",
            evidence={"probe": "eod metadata"},
        )
        eod = meta.resolve_vendor_identifier(
            instrument_id, "eod", date(2020, 1, 1), date(2020, 12, 31)
        )
        hourly = meta.resolve_vendor_identifier(
            instrument_id,
            "intraday_1hour",
            date(2020, 1, 1),
            date(2020, 12, 31),
        )
        five_minute = meta.resolve_vendor_identifier(
            instrument_id,
            "intraday_5min",
            date(2020, 1, 1),
            date(2020, 12, 31),
        )

        assert eod.status == "resolved"
        assert (eod.identifier_type, eod.identifier_value) == (
            "permaTicker",
            "US0000001",
        )
        assert hourly.status == "zero_matches"
        assert five_minute.status == "zero_matches"

        conflicting_instrument = meta.upsert_instrument("instrument-2")
        conflicting_identifier_id = meta.add_vendor_identifier(
            conflicting_instrument,
            "eod",
            "permaTicker",
            "US0000001",
            date(2019, 1, 1),
            date(2021, 12, 31),
            validation_state="validated",
        )
        conflict = meta.resolve_vendor_identifier(
            instrument_id, "eod", date(2020, 1, 1), date(2020, 12, 31)
        )
        assert conflict.status == "multiple_matches"
        assert conflict.conflicting_instrument_ids == (conflicting_instrument,)
        assert conflicting_identifier_id not in conflict.vendor_identifier_ids
        assert conflict.vendor_identifier_ids == (own_identifier_id,)

        meta.add_vendor_identifier(
            instrument_id,
            "intraday_1hour",
            "ticker",
            "SAFE",
            date(2020, 1, 1),
            date(2020, 12, 31),
            validation_state="rejected",
        )
        assert (
            meta.resolve_vendor_identifier(
                instrument_id,
                "intraday_1hour",
                date(2020, 1, 1),
                date(2020, 12, 31),
            ).status
            == "zero_matches"
        )


def test_vendor_identifier_resolution_uses_covering_evidence_rows(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("instrument-1")
        partial_id = meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "US0000001",
            date(2008, 1, 1),
            date(2012, 12, 31),
            validation_state="validated",
        )
        full_id = meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "US0000001",
            date(2000, 1, 1),
            date(2025, 12, 31),
            validation_state="validated",
        )

        resolved = meta.resolve_vendor_identifier(
            instrument_id, "eod", date(2010, 1, 1), date(2015, 12, 31)
        )
        assert resolved.status == "resolved"
        assert resolved.vendor_identifier_ids == (partial_id, full_id)
        assert resolved.vendor_identifier_id == full_id


def test_vendor_identifier_resolution_accepts_abutting_evidence(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("instrument-1")
        first_id = meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "US0000001",
            date(2000, 1, 1),
            date(2009, 12, 31),
            validation_state="validated",
        )
        second_id = meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "US0000001",
            date(2010, 1, 1),
            date(2025, 12, 31),
            validation_state="validated",
        )

        resolved = meta.resolve_vendor_identifier(
            instrument_id, "eod", date(2005, 1, 1), date(2015, 12, 31)
        )
        assert resolved.status == "resolved"
        assert resolved.vendor_identifier_ids == (first_id, second_id)
        assert resolved.vendor_identifier_id is None
        assert resolved.identifier_value == "US0000001"


def test_ticker_identifier_writes_are_case_normalized(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        instrument_id = meta.upsert_instrument("instrument-1")
        first_id = meta.add_vendor_identifier(
            instrument_id,
            "intraday_1hour",
            "Ticker",
            "aapl",
            date(2024, 1, 2),
            date(2024, 1, 31),
            validation_state="rejected",
        )
        second_id = meta.add_vendor_identifier(
            instrument_id,
            "intraday_1hour",
            "ticker",
            "AAPL",
            date(2024, 1, 2),
            date(2024, 1, 31),
            validation_state="validated",
        )

        assert second_id == first_id
        row = meta._con.execute(
            """SELECT identifier_type, identifier_value, validation_state
               FROM vendor_identifiers"""
        ).fetchone()
        assert tuple(row) == ("ticker", "AAPL", "validated")


def test_universe_resolution_is_recorded_without_guessing(tmp_path):
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.set_universe(
            2020,
            [
                {"ticker": "ONLY", "rank": 1},
                {"ticker": "MISSING", "rank": 2},
                {"ticker": "DUP", "rank": 3},
            ],
        )
        only = meta.upsert_instrument("only")
        duplicate_a = meta.upsert_instrument("duplicate-a")
        duplicate_b = meta.upsert_instrument("duplicate-b")
        meta.add_instrument_alias(only, "ONLY", date(2010, 1, 1), date(2021, 1, 1))
        meta.add_instrument_alias(
            duplicate_a, "DUP", date(2010, 1, 1), date(2020, 6, 30)
        )
        meta.add_instrument_alias(
            duplicate_b, "DUP", date(2020, 6, 1), date(2022, 12, 31)
        )

        reports = meta.resolve_universe(2020)
        assert [(report.ticker, report.status) for report in reports] == [
            ("ONLY", "resolved"),
            ("MISSING", "zero_matches"),
            ("DUP", "multiple_matches"),
        ]
        rows = meta._con.execute(
            """SELECT ticker, status, instrument_id
               FROM universe_resolutions ORDER BY rowid"""
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            ("ONLY", "resolved", only),
            ("MISSING", "zero_matches", None),
            ("DUP", "multiple_matches", None),
        ]

        resolved_at = meta._con.execute(
            """SELECT resolved_at FROM universe_resolutions
               WHERE year = 2020 AND ticker = 'ONLY'"""
        ).fetchone()[0]
        meta.set_universe(
            2020,
            [
                {"ticker": "ONLY", "rank": 1},
                {"ticker": "MISSING", "rank": 2},
                {"ticker": "DUP", "rank": 3},
            ],
        )
        assert (
            meta._con.execute(
                """SELECT resolved_at FROM universe_resolutions
                   WHERE year = 2020 AND ticker = 'ONLY'"""
            ).fetchone()[0]
            == resolved_at
        )


def test_universe_resolution_keeps_clean_midyear_reuse_fail_closed(tmp_path):
    """D-014 resolves yearly source rows only when exactly one instrument overlaps."""
    with MetaStore(tmp_path / "meta.db") as meta:
        meta.set_universe(2020, [{"ticker": "HANDOFF", "rank": 1}])
        old = meta.upsert_instrument("old")
        new = meta.upsert_instrument("new")
        meta.add_instrument_alias(old, "HANDOFF", date(2010, 1, 1), date(2020, 6, 30))
        meta.add_instrument_alias(new, "HANDOFF", date(2020, 7, 1), None)

        alias_report = meta.resolve_alias_range(
            "HANDOFF", date(2020, 1, 1), date(2020, 12, 31)
        )
        assert alias_report.resolved
        assert [segment.instrument_id for segment in alias_report.segments] == [
            old,
            new,
        ]
        [universe_report] = meta.resolve_universe(2020)
        assert universe_report.status == "multiple_matches"
        assert universe_report.instrument_id is None
