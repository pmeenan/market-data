from datetime import date

from marketdata.store.meta import MetaStore


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
        assert meta.get_coverage("AAPL", "eod") is None
        meta.set_coverage("aapl", "eod", date(2020, 1, 2), date(2024, 6, 28))
        assert meta.get_coverage("AAPL", "eod") == (date(2020, 1, 2), date(2024, 6, 28))

        # extend widens in both directions and never shrinks
        meta.extend_coverage("AAPL", "eod", date(1995, 1, 3), date(1999, 12, 31))
        assert meta.get_coverage("AAPL", "eod") == (date(1995, 1, 3), date(2024, 6, 28))
        meta.extend_coverage("AAPL", "eod", date(2024, 6, 1), date(2024, 7, 1))
        assert meta.get_coverage("AAPL", "eod") == (date(1995, 1, 3), date(2024, 7, 1))

        assert meta.coverage("eod") == {"AAPL": (date(1995, 1, 3), date(2024, 7, 1))}
        meta.clear_coverage("eod")
        assert meta.coverage("eod") == {}


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
        assert meta.get_coverage("AAPL", "eod") is None
        tables = {
            r["name"]
            for r in meta._con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "watermarks" not in tables
        assert "coverage" in tables
        assert meta._con.execute("PRAGMA user_version").fetchone()[0] == 1
