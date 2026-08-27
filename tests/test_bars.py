from datetime import date

import polars as pl

from marketdata.store.bars import BarStore, eod_frame, intraday_frame

SAMPLE_EOD = [
    {
        "date": "2024-01-02T00:00:00.000Z",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 1000,
        "adjOpen": 100.0,
        "adjHigh": 102.0,
        "adjLow": 99.0,
        "adjClose": 101.0,
        "adjVolume": 1000,
        "divCash": 0.0,
        "splitFactor": 1.0,
    },
    {
        "date": "2024-01-03T00:00:00.000Z",
        "open": 101.0,
        "high": 103.0,
        "low": 100.0,
        "close": 102.5,
        "volume": 1500,
        "adjOpen": 101.0,
        "adjHigh": 103.0,
        "adjLow": 100.0,
        "adjClose": 102.5,
        "adjVolume": 1500,
        "divCash": 0.0,
        "splitFactor": 1.0,
    },
]

SAMPLE_INTRADAY = [
    {
        "date": "2024-01-02T14:30:00.000Z",
        "open": 100.0,
        "high": 100.5,
        "low": 99.9,
        "close": 100.2,
        "volume": 500,
    },
    {
        "date": "2025-01-02T14:30:00.000Z",
        "open": 110.0,
        "high": 110.5,
        "low": 109.9,
        "close": 110.2,
        "volume": 600,
    },
]


def test_eod_frame_shape():
    df = eod_frame("aapl", SAMPLE_EOD)
    assert df.height == 2
    assert df["ticker"].unique().to_list() == ["AAPL"]
    assert df["date"].to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
    assert df["adj_close"][1] == 102.5


def test_eod_write_and_merge(tmp_path):
    store = BarStore(tmp_path)
    df = eod_frame("AAPL", SAMPLE_EOD)
    assert store.write_eod("AAPL", df) == 2
    assert store.eod_last_date("AAPL") == date(2024, 1, 3)

    # Overlapping refetch with a restated close replaces, not duplicates
    restated = [
        dict(SAMPLE_EOD[1], close=999.0),
        {
            **SAMPLE_EOD[1],
            "date": "2024-01-04T00:00:00.000Z",
        },
    ]
    assert store.write_eod("AAPL", eod_frame("AAPL", restated)) == 3
    out = store.read_eod("AAPL")
    assert out.filter(pl.col("date") == date(2024, 1, 3))["close"][0] == 999.0
    assert store.eod_tickers() == ["AAPL"]


def test_intraday_split_by_year(tmp_path):
    store = BarStore(tmp_path)
    df = intraday_frame("msft", SAMPLE_INTRADAY)
    assert store.write_intraday("MSFT", df) == 2
    assert store.intraday_path("MSFT", 2024).exists()
    assert store.intraday_path("MSFT", 2025).exists()
    out = store.read_intraday("MSFT")
    assert out.height == 2
    assert out["ticker"].unique().to_list() == ["MSFT"]
