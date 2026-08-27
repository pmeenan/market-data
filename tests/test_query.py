from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from marketdata.config import Config
from marketdata.query import (
    connect,
    load_eod,
    load_eod_by_ticker,
    load_intraday,
    load_intraday_by_ticker,
    load_intraday_sessions,
)
from marketdata.store import MetaStore
from marketdata.store.bars import EOD_SCHEMA, INTRADAY_SCHEMA, BarStore


def _config(tmp_path) -> Config:
    return Config(data_dir=tmp_path, tiingo_token=None)


def _canonical(tmp_path, aliases: list[tuple[str, str, date, date]]) -> None:
    with MetaStore(tmp_path / "meta.db") as meta:
        for instrument_id, ticker, start, end in aliases:
            meta.upsert_instrument(instrument_id)
            meta.add_instrument_alias(instrument_id, ticker, start, end)
        meta.activate_canonical_generation()


def _eod(ticker: str, days: int = 5) -> pl.DataFrame:
    start = date(2024, 1, 2)
    dates = [start + timedelta(days=i) for i in range(days)]
    n = len(dates)
    return pl.DataFrame(
        {
            "ticker": [ticker] * n,
            "date": dates,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": [10] * n,
            "adj_open": [1.0] * n,
            "adj_high": [1.0] * n,
            "adj_low": [1.0] * n,
            "adj_close": [1.0] * n,
            "adj_volume": [10] * n,
            "div_cash": [0.0] * n,
            "split_factor": [1.0] * n,
        }
    ).cast(EOD_SCHEMA)


def _hourly(ticker: str) -> pl.DataFrame:
    ts = [
        datetime(2024, 6, 3, 14, 30, tzinfo=UTC) + timedelta(hours=i) for i in range(6)
    ]
    n = len(ts)
    return pl.DataFrame(
        {
            "ticker": [ticker] * n,
            "ts": ts,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": [10] * n,
        }
    ).cast(INTRADAY_SCHEMA)


def test_hourly_view_and_loader(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    _canonical(
        tmp_path,
        [("apple-id", "AAPL", date(1980, 1, 1), date(9999, 12, 31))],
    )
    bars.publish_eod({"apple-id": _eod("AAPL")})
    bars.publish_intraday({"apple-id": _hourly("AAPL")}, freq="1hour")

    con = connect(config)
    views = {
        r[0]
        for r in con.execute(
            "SELECT view_name FROM duckdb_views() WHERE NOT internal"
        ).fetchall()
    }
    assert {"eod", "eod_with_alias", "intraday_1hour"} <= views
    assert con.execute("SELECT count(*) FROM intraday_1hour").fetchone()[0] == 6
    assert (
        con.execute("SELECT DISTINCT ticker FROM intraday_1hour_with_alias").fetchone()[
            0
        ]
        == "AAPL"
    )

    df = load_intraday(
        config,
        instrument_ids=["apple-id"],
        start="2024-06-03",
        end="2024-06-03",
        freq="1hour",
    )
    assert df.height == 6
    assert df["instrument_id"].unique().to_list() == ["apple-id"]
    assert "ticker" not in df.columns


def test_session_loader_filters_and_labels_without_changing_raw_view(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    _canonical(
        tmp_path,
        [("apple-id", "AAPL", date(1980, 1, 1), date(9999, 12, 31))],
    )
    frame = (
        _hourly("AAPL")
        .head(3)
        .with_columns(
            ts=pl.Series(
                "ts",
                [
                    datetime(2024, 6, 3, 14, 0, tzinfo=UTC),
                    datetime(2024, 6, 3, 19, 0, tzinfo=UTC),
                    datetime(2024, 7, 4, 14, 0, tzinfo=UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            )
        )
    )
    bars.publish_intraday({"apple-id": frame}, freq="1hour")

    raw = load_intraday(config, instrument_ids=["apple-id"], freq="1hour")
    labelled = load_intraday_sessions(config, instrument_ids=["apple-id"], freq="1hour")

    assert raw.height == 3
    assert labelled["ts"].to_list() == [
        datetime(2024, 6, 3, 14, 0, tzinfo=UTC),
        datetime(2024, 6, 3, 19, 0, tzinfo=UTC),
    ]
    assert labelled["minutes_from_open"].to_list() == [30, 330]


def test_load_eod_filters(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    _canonical(
        tmp_path,
        [
            ("apple-id", "AAPL", date(1980, 1, 1), date(9999, 12, 31)),
            ("microsoft-id", "MSFT", date(1986, 1, 1), date(9999, 12, 31)),
        ],
    )
    bars.publish_eod({"apple-id": _eod("AAPL"), "microsoft-id": _eod("MSFT")})
    df = load_eod(config, instrument_ids=["apple-id"], start="2024-01-03")
    assert df["instrument_id"].unique().to_list() == ["apple-id"]
    assert df["date"].min() == date(2024, 1, 3)


def test_ticker_loader_requires_resolvable_explicit_range(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    _canonical(
        tmp_path,
        [
            ("old-id", "REUSE", date(2000, 1, 1), date(2010, 12, 31)),
            ("new-id", "REUSE", date(2012, 1, 1), date(2030, 12, 31)),
        ],
    )
    bars.publish_eod({"old-id": _eod("REUSE"), "new-id": _eod("REUSE")})

    loaded = load_eod_by_ticker(
        config, ["reuse"], start=date(2024, 1, 2), end=date(2024, 1, 6)
    )
    assert loaded["instrument_id"].unique().to_list() == ["new-id"]
    assert loaded["ticker"].unique().to_list() == ["REUSE"]

    with pytest.raises(ValueError, match="2011-01-01.*zero_matches"):
        load_eod_by_ticker(
            config, ["REUSE"], start=date(2011, 1, 1), end=date(2012, 1, 2)
        )

    duplicate = load_eod_by_ticker(
        config,
        ["REUSE", "reuse"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert duplicate.height == loaded.height


def test_instrument_selectors_are_fail_closed(tmp_path):
    config = _config(tmp_path)
    _canonical(
        tmp_path,
        [("apple-id", "AAPL", date(1980, 1, 1), date(9999, 12, 31))],
    )
    BarStore(tmp_path).publish_eod({"apple-id": _eod("AAPL")})

    assert load_eod(config, instrument_ids=[]).is_empty()
    with pytest.raises(ValueError, match="unknown instrument_ids"):
        load_eod(config, instrument_ids=["AAPL"])
    with pytest.raises(TypeError):
        load_eod(config, ["apple-id"])  # type: ignore[misc]


def test_missing_dataset_returns_typed_empty_frame(tmp_path):
    config = _config(tmp_path)
    _canonical(
        tmp_path,
        [("apple-id", "AAPL", date(1980, 1, 1), date(9999, 12, 31))],
    )
    BarStore(tmp_path).publish_intraday({"apple-id": _hourly("AAPL")}, freq="1hour")

    by_id = load_intraday(config, instrument_ids=["apple-id"], freq="5min")
    by_ticker = load_intraday_by_ticker(
        config,
        ["AAPL"],
        start=date(2024, 6, 3),
        end=date(2024, 6, 3),
        freq="5min",
    )
    assert by_id.is_empty() and "instrument_id" in by_id.columns
    assert by_ticker.is_empty() and "ticker" in by_ticker.columns


def test_intraday_dates_and_aliases_are_utc_in_non_utc_session(tmp_path):
    config = _config(tmp_path)
    _canonical(
        tmp_path,
        [
            ("apple-id", "OLD", date(2024, 6, 3), date(2024, 6, 3)),
            ("apple-id", "NEW", date(2024, 6, 4), date(2024, 6, 4)),
        ],
    )
    frame = (
        _hourly("AAPL")
        .head(2)
        .with_columns(
            ts=pl.Series(
                "ts",
                [
                    datetime(2024, 6, 4, 1, 0, tzinfo=UTC),
                    datetime(2024, 6, 4, 5, 0, tzinfo=UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            )
        )
    )
    BarStore(tmp_path).publish_intraday({"apple-id": frame}, freq="1hour")

    con = connect(config)
    con.execute("SET TimeZone='America/New_York'")
    assert con.execute(
        "SELECT array_agg(ticker ORDER BY ts) FROM intraday_1hour_with_alias"
    ).fetchone()[0] == ["NEW", "NEW"]
    assert (
        con.execute(
            "SELECT count(*) FROM intraday_1hour "
            "WHERE CAST(ts AT TIME ZONE 'UTC' AS DATE) = DATE '2024-06-04'"
        ).fetchone()[0]
        == 2
    )


def test_query_rejects_v1_and_never_scans_legacy_files(tmp_path):
    config = _config(tmp_path)
    bars = BarStore(tmp_path)
    MetaStore(tmp_path / "meta.db").close()
    bars.write_eod("AAPL", _eod("AAPL"))
    with pytest.raises(RuntimeError, match="require.*v2"):
        connect(config)
