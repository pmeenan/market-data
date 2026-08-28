from datetime import date

import polars as pl

from marketdata.store.bars import (
    BarStore,
    eod_frame,
    instrument_bucket,
    intraday_frame,
)

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


def test_canonical_bucket_is_stable_and_eod_publish_batches_instruments(tmp_path):
    store = BarStore(tmp_path)
    assert instrument_bucket("instrument-1") == "c4"

    first = eod_frame("OLD", SAMPLE_EOD[:1])
    second = eod_frame("OTHER", SAMPLE_EOD[1:])
    # Find two opaque ids in one bucket so one physical file owns both slices.
    instrument_id = "instrument-1"
    peer_id = next(
        f"peer-{number}"
        for number in range(1000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(instrument_id)
    )
    store.publish_eod({instrument_id: first, peer_id: second})

    path = store.canonical_eod_path(instrument_id)
    assert path == store.canonical_eod_path(peer_id)
    stored = pl.read_parquet(path)
    assert set(stored["instrument_id"]) == {instrument_id, peer_id}
    assert "ticker" not in stored.columns

    restated = eod_frame("OLD", [dict(SAMPLE_EOD[0], close=777.0, high=800.0)])
    store.publish_eod({instrument_id: restated})
    assert store.read_canonical_eod(instrument_id)["close"][0] == 777.0
    assert store.read_canonical_eod(peer_id).height == 1

    replacement = eod_frame("NEW", SAMPLE_EOD[1:])
    store.publish_eod(
        {instrument_id: replacement},
        replace_instruments=frozenset({instrument_id}),
    )
    assert store.read_canonical_eod(instrument_id)["date"].to_list() == [
        date(2024, 1, 3)
    ]
    assert store.read_canonical_eod(peer_id).height == 1


def test_canonical_intraday_publish_splits_year_and_upserts(tmp_path):
    store = BarStore(tmp_path)
    frame = intraday_frame("MSFT", SAMPLE_INTRADAY)
    store.publish_intraday({"stable-id": frame}, freq="1hour")

    assert store.canonical_intraday_path("stable-id", 2024).exists()
    assert store.canonical_intraday_path("stable-id", 2025).exists()
    out = store.read_canonical_intraday("stable-id")
    assert out.height == 2
    assert out["instrument_id"].unique().to_list() == ["stable-id"]
    assert "ticker" not in out.columns


def test_failed_atomic_publish_preserves_existing_bucket(tmp_path, monkeypatch):
    store = BarStore(tmp_path)
    original = eod_frame("OLD", SAMPLE_EOD[:1])
    store.publish_eod({"stable-id": original})
    path = store.canonical_eod_path("stable-id")
    bytes_before = path.read_bytes()

    original_replace = type(path).replace

    def fail_temporary_replace(self, target):
        if self.name.endswith(".tmp"):
            raise OSError("simulated rename failure")
        return original_replace(self, target)

    monkeypatch.setattr(type(path), "replace", fail_temporary_replace)
    import pytest

    with pytest.raises(OSError, match="simulated rename failure"):
        store.publish_eod(
            {
                "stable-id": eod_frame(
                    "OLD", [dict(SAMPLE_EOD[0], close=999.0, high=1000.0)]
                )
            }
        )
    assert path.read_bytes() == bytes_before


def test_snapshot_replacement_requires_nonempty_frame_for_instrument(tmp_path):
    import pytest

    store = BarStore(tmp_path)
    instrument_id = "instrument-1"
    peer_id = next(
        f"peer-{number}"
        for number in range(1000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(instrument_id)
    )
    store.publish_eod(
        {
            instrument_id: eod_frame("ONE", SAMPLE_EOD[:1]),
            peer_id: eod_frame("TWO", SAMPLE_EOD[1:]),
        }
    )
    bytes_before = store.canonical_eod_path(instrument_id).read_bytes()

    with pytest.raises(ValueError, match="require frames"):
        store.publish_eod(
            {peer_id: eod_frame("TWO", SAMPLE_EOD[1:])},
            replace_instruments=frozenset({instrument_id}),
        )
    with pytest.raises(ValueError, match="must not be empty"):
        store.publish_eod(
            {instrument_id: eod_frame("ONE", [])},
            replace_instruments=frozenset({instrument_id}),
        )
    assert store.canonical_eod_path(instrument_id).read_bytes() == bytes_before


def test_canonical_eod_missing_instrument_returns_none_with_bucket_peer(tmp_path):
    store = BarStore(tmp_path)
    stored_id = "instrument-1"
    missing_id = next(
        f"missing-{number}"
        for number in range(1000)
        if instrument_bucket(f"missing-{number}") == instrument_bucket(stored_id)
    )
    store.publish_eod({stored_id: eod_frame("ONE", SAMPLE_EOD[:1])})
    assert store.read_canonical_eod(missing_id) is None
