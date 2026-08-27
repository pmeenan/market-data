"""Ingestion semantics tests against a fake Tiingo client (fully offline)."""

from datetime import date, timedelta

import responses

from marketdata.ingest import (
    REFRESH_WINDOW_DAYS,
    IngestTarget,
    backfill_eod_validated,
    backfill_intraday_validated,
    plan_validated_segments,
    reconcile,
    update_eod_validated,
)
from marketdata.ingest import (
    backfill_eod as _backfill_eod,
)
from marketdata.ingest import (
    backfill_intraday as _backfill_intraday,
)
from marketdata.ingest import (
    update_eod as _update_eod,
)
from marketdata.store import BarStore, MetaStore
from marketdata.store.bars import eod_frame, instrument_bucket
from marketdata.tiingo import BASE_URL, TiingoClient, TiingoError


def eod_row(
    d: date, close: float = 100.0, div: float = 0.0, split: float = 1.0
) -> dict:
    return {
        "date": f"{d.isoformat()}T00:00:00.000Z",
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
        "divCash": div,
        "splitFactor": split,
    }


def weekdays(start: date, end: date) -> list[date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


class FakeTiingo:
    """Serves a fixed per-ticker EOD history; records every request."""

    def __init__(self, history: dict[str, list[dict]], fail: set[str] = frozenset()):
        self.history = {t.upper(): rows for t, rows in history.items()}
        self.fail = {t.upper() for t in fail}
        self.eod_calls: list[tuple[str, date, date]] = []
        self.intraday_calls: list[tuple[str, date, date, str]] = []

    def eod(self, ticker, start=None, end=None):
        ticker = ticker.upper()
        if ticker in self.fail:
            raise TiingoError("simulated failure")
        start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
        self.eod_calls.append((ticker, start, end))
        return [
            r
            for r in self.history.get(ticker, [])
            if start <= date.fromisoformat(r["date"][:10]) <= end
        ]

    def intraday(self, ticker, start, end=None, freq="1hour"):
        start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
        self.intraday_calls.append((ticker.upper(), start, end, freq))
        rows = []
        for d in weekdays(start, end):
            if d.year >= 2024:  # simulate bounded IEX history
                rows.append(
                    {
                        "date": f"{d.isoformat()}T15:00:00.000Z",
                        "open": 10.0,
                        "high": 10.5,
                        "low": 9.5,
                        "close": 10.2,
                        "volume": 100,
                    }
                )
        return rows


def stores(tmp_path):
    bars, meta = BarStore(tmp_path), MetaStore(tmp_path / "meta.db")
    meta.activate_canonical_generation()
    return bars, meta


def _targets(meta, tickers):
    for ticker in tickers:
        meta.upsert_instrument(ticker)
    return [IngestTarget(ticker, ticker) for ticker in tickers]


def backfill_eod(client, bars, meta, tickers, *args, **kwargs):
    return _backfill_eod(client, bars, meta, _targets(meta, tickers), *args, **kwargs)


def update_eod(client, bars, meta, tickers, *args, **kwargs):
    return _update_eod(client, bars, meta, _targets(meta, tickers), *args, **kwargs)


def backfill_intraday(client, bars, meta, tickers, *args, **kwargs):
    return _backfill_intraday(
        client, bars, meta, _targets(meta, tickers), *args, **kwargs
    )


def test_ingestion_owns_bars_and_coverage_by_instrument_id(tmp_path):
    bars, meta = stores(tmp_path)
    meta.upsert_instrument("stable-id")
    client = FakeTiingo(
        {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 1, 12))]}
    )

    result = _backfill_eod(
        client,
        bars,
        meta,
        [IngestTarget("stable-id", "AAPL")],
        date(2024, 1, 1),
        date(2024, 1, 12),
    )

    assert result.fetched == ["stable-id"]
    assert meta.get_coverage("stable-id", "eod") == (
        date(2024, 1, 1),
        date(2024, 1, 12),
    )
    stored = bars.read_canonical_eod("stable-id")
    assert stored["instrument_id"].unique().to_list() == ["stable-id"]
    assert "ticker" not in stored.columns


@responses.activate
def test_csv_client_contract_reaches_eod_and_intraday_ingestion(tmp_path):
    bars, meta = stores(tmp_path)
    for instrument_id in ("eod-id", "intraday-id"):
        meta.upsert_instrument(instrument_id)
    responses.add(
        responses.GET,
        f"{BASE_URL}/tiingo/daily/aapl/prices",
        body=(
            "date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,"
            "adjVolume,divCash,splitFactor\n"
            "2024-01-02T00:00:00.000Z,101,102,99,100,123456,101,102,99,100,"
            "123456,,1\n"
        ),
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BASE_URL}/iex/aapl/prices",
        body=(
            "date,open,high,low,close,volume\n"
            "2024-01-02T10:00:00-05:00,100,101,99.5,,1234\n"
        ),
        status=200,
    )
    client = TiingoClient("test-token", min_request_interval=0.0)
    day = date(2024, 1, 2)

    eod_result = _backfill_eod(
        client, bars, meta, [IngestTarget("eod-id", "AAPL")], day, day
    )
    intraday_result = _backfill_intraday(
        client,
        bars,
        meta,
        [IngestTarget("intraday-id", "AAPL")],
        day,
        day,
    )

    assert eod_result.ok and intraday_result.ok
    assert bars.read_canonical_eod("eod-id")["div_cash"].to_list() == [None]
    assert bars.read_canonical_intraday("intraday-id")["close"].to_list() == [None]


def _identity_target(
    meta,
    instrument_id,
    ticker,
    dataset_key,
    start,
    end,
    *,
    identifier=None,
):
    meta.upsert_instrument(instrument_id)
    meta.add_instrument_alias(instrument_id, ticker, start, end)
    meta.add_vendor_identifier(
        instrument_id,
        dataset_key,
        "ticker",
        identifier or ticker,
        start,
        end,
        validation_state="validated",
    )


def test_validated_ingestion_allows_safe_peer_and_reports_blocked_segment(tmp_path):
    bars, meta = stores(tmp_path)
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, "good-id", "GOOD", "eod", start, end)
    meta.upsert_instrument("blocked-id")
    meta.add_instrument_alias("blocked-id", "BLOCKED", start, end)
    client = FakeTiingo({"GOOD": [eod_row(date(2024, 1, 2))]})

    result = backfill_eod_validated(client, bars, meta, ["GOOD", "BLOCKED"], start, end)

    assert result.fetched == ["good-id"]
    assert len(result.blocked) == 1
    assert "no unique validated identifier" in next(iter(result.blocked.values()))
    assert client.eod_calls == [("GOOD", start, end)]
    assert bars.read_canonical_eod("good-id") is not None
    assert bars.read_canonical_eod("blocked-id") is None


def test_validated_ingestion_preserves_bucket_batching(tmp_path, monkeypatch):
    bars, meta = stores(tmp_path)
    first = "stable-id"
    peer = next(
        f"peer-{number}"
        for number in range(10_000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(first)
    )
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, first, "ONE", "eod", start, end)
    _identity_target(meta, peer, "TWO", "eod", start, end)
    client = FakeTiingo(
        {"ONE": [eod_row(date(2024, 1, 2))], "TWO": [eod_row(date(2024, 1, 2))]}
    )
    calls: list[set[str]] = []
    publish = bars.publish_eod

    def recording_publish(frames, **kwargs):
        calls.append(set(frames))
        return publish(frames, **kwargs)

    monkeypatch.setattr(bars, "publish_eod", recording_publish)

    result = backfill_eod_validated(client, bars, meta, ["ONE", "TWO"], start, end)

    assert result.ok
    assert calls == [{first, peer}]


def test_validated_ingestion_batches_heterogeneous_ranges(tmp_path, monkeypatch):
    bars, meta = stores(tmp_path)
    first = "stable-id"
    peer = next(
        f"peer-{number}"
        for number in range(10_000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(first)
    )
    today = date.today()
    alias_start = date(2024, 1, 1)
    _identity_target(meta, first, "ONE", "eod", alias_start, date.max)
    _identity_target(meta, peer, "TWO", "eod", alias_start, date.max)
    client = FakeTiingo(
        {
            "ONE": [eod_row(today - timedelta(days=2))],
            "TWO": [eod_row(today - timedelta(days=2))],
        }
    )
    bars.publish_eod(
        {
            first: bars.canonicalize_eod(
                first, eod_frame("ONE", [eod_row(alias_start)])
            ),
            peer: bars.canonicalize_eod(peer, eod_frame("TWO", [eod_row(alias_start)])),
        }
    )
    meta.set_coverage(first, "eod", alias_start, today - timedelta(days=10))
    meta.set_coverage(peer, "eod", alias_start, today - timedelta(days=5))
    calls: list[set[str]] = []
    publish = bars.publish_eod

    def recording_publish(frames, **kwargs):
        calls.append(set(frames))
        return publish(frames, **kwargs)

    monkeypatch.setattr(bars, "publish_eod", recording_publish)

    result = update_eod_validated(client, bars, meta, ["ONE", "TWO"])

    assert result.ok
    assert calls == [{first, peer}]


def test_response_outside_validated_segment_is_rejected_before_write(tmp_path):
    class OutOfEnvelopeTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            super().eod(ticker, start, end)
            return [eod_row(date(2023, 12, 29))]

    bars, meta = stores(tmp_path)
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, "stable-id", "SAFE", "eod", start, end)

    result = backfill_eod_validated(
        OutOfEnvelopeTiingo({"SAFE": []}), bars, meta, ["SAFE"], start, end
    )

    assert len(result.failed) == 1
    assert "falls outside request" in next(iter(result.failed.values()))
    assert bars.read_canonical_eod("stable-id") is None
    assert meta.get_coverage("stable-id", "eod") is None


def test_response_identity_metadata_conflict_rejects_entire_response(tmp_path):
    class ConflictingMetadataTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            return [{**row, "ticker": "OTHER"} for row in rows]

    bars, meta = stores(tmp_path)
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, "stable-id", "SAFE", "eod", start, end)
    client = ConflictingMetadataTiingo(
        {"SAFE": [eod_row(date(2024, 1, 2)), eod_row(date(2024, 1, 3))]}
    )

    result = backfill_eod_validated(client, bars, meta, ["SAFE"], start, end)

    assert len(result.failed) == 1
    assert "conflicts with validated identity" in next(iter(result.failed.values()))
    assert bars.read_canonical_eod("stable-id") is None
    assert meta.get_coverage("stable-id", "eod") is None


def test_intraday_identity_evidence_is_exact_frequency(tmp_path):
    bars, meta = stores(tmp_path)
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, "stable-id", "SAFE", "intraday_1hour", start, end)
    client = FakeTiingo({})

    hourly = plan_validated_segments(meta, ["SAFE"], "intraday_1hour", start, end)
    five_minute = backfill_intraday_validated(
        client, bars, meta, ["SAFE"], start, end, freq="5min"
    )

    assert [segment.status for segment in hourly] == ["ready"]
    assert len(five_minute.blocked) == 1
    assert client.intraday_calls == []
    assert meta.get_coverage("stable-id", "intraday_5min") is None


def test_each_intraday_frequency_ingests_with_its_own_evidence(tmp_path):
    for freq in ("1hour", "5min"):
        root = tmp_path / freq
        bars, meta = stores(root)
        start, end = date(2024, 1, 1), date(2024, 1, 5)
        _identity_target(meta, "stable-id", "SAFE", f"intraday_{freq}", start, end)
        client = FakeTiingo({})

        result = backfill_intraday_validated(
            client, bars, meta, ["SAFE"], start, end, freq=freq
        )

        assert result.fetched == ["stable-id"]
        assert client.intraday_calls == [("SAFE", start, end, freq)]
        assert meta.get_coverage("stable-id", f"intraday_{freq}") == (start, end)
        assert bars.read_canonical_intraday("stable-id", freq=freq) is not None


def test_identity_gap_never_becomes_bridged_coverage(tmp_path):
    bars, meta = stores(tmp_path)
    instrument_id = meta.upsert_instrument("stable-id")
    meta.add_instrument_alias(
        instrument_id, "SAFE", date(2024, 1, 1), date(2024, 3, 31)
    )
    for start, end in (
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 3, 1), date(2024, 3, 31)),
    ):
        meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "ticker",
            "SAFE",
            start,
            end,
            validation_state="validated",
        )
    client = FakeTiingo(
        {
            "SAFE": [
                eod_row(date(2024, 1, 2)),
                eod_row(date(2024, 3, 1)),
            ]
        }
    )

    result = backfill_eod_validated(
        client,
        bars,
        meta,
        ["SAFE"],
        date(2024, 1, 1),
        date(2024, 3, 31),
    )

    assert meta.get_coverage("stable-id", "eod") == (
        date(2024, 3, 1),
        date(2024, 3, 31),
    )
    assert client.eod_calls == [("SAFE", date(2024, 3, 1), date(2024, 3, 31))]
    assert len(result.blocked) == 2
    assert any("not adjacent" in reason for reason in result.blocked.values())


def test_weekend_only_evidence_gap_does_not_block_coverage(tmp_path):
    bars, meta = stores(tmp_path)
    instrument_id = meta.upsert_instrument("stable-id")
    meta.add_instrument_alias(instrument_id, "SAFE", date(2024, 3, 1), date(2024, 3, 4))
    for start, end in (
        (date(2024, 3, 1), date(2024, 3, 1)),  # Friday
        (date(2024, 3, 4), date(2024, 3, 4)),  # Monday
    ):
        meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "ticker",
            "SAFE",
            start,
            end,
            validation_state="validated",
        )
    client = FakeTiingo(
        {"SAFE": [eod_row(date(2024, 3, 1)), eod_row(date(2024, 3, 4))]}
    )

    result = backfill_eod_validated(
        client, bars, meta, ["SAFE"], date(2024, 3, 1), date(2024, 3, 4)
    )

    assert result.ok
    assert result.fetched == [instrument_id]
    assert result.skipped == []
    assert meta.get_coverage(instrument_id, "eod") == (
        date(2024, 3, 1),
        date(2024, 3, 4),
    )


def test_newest_segment_failure_does_not_anchor_older_coverage(tmp_path):
    class FailFirstTiingo(FakeTiingo):
        def __init__(self, history):
            super().__init__(history)
            self.calls = 0

        def eod(self, ticker, start=None, end=None):
            self.calls += 1
            if self.calls == 1:
                raise TiingoError("transient newest-segment failure")
            return super().eod(ticker, start, end)

    bars, meta = stores(tmp_path)
    instrument_id = meta.upsert_instrument("stable-id")
    meta.add_instrument_alias(
        instrument_id, "SAFE", date(2024, 1, 1), date(2024, 3, 31)
    )
    for start, end in (
        (date(2024, 1, 1), date(2024, 1, 31)),
        (date(2024, 3, 1), date(2024, 3, 31)),
    ):
        meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "ticker",
            "SAFE",
            start,
            end,
            validation_state="validated",
        )
    client = FailFirstTiingo(
        {"SAFE": [eod_row(date(2024, 1, 2)), eod_row(date(2024, 3, 1))]}
    )

    result = backfill_eod_validated(
        client,
        bars,
        meta,
        ["SAFE"],
        date(2024, 1, 1),
        date(2024, 3, 31),
    )

    assert client.calls == 1
    assert meta.get_coverage(instrument_id, "eod") is None
    assert len(result.failed) == 2
    assert any("not attempted" in detail for detail in result.failed.values())


def test_permanent_identifier_can_authorize_rename_spanning_full_refresh(tmp_path):
    bars, meta = stores(tmp_path)
    instrument_id = meta.upsert_instrument("stable-id")
    meta.add_instrument_alias(instrument_id, "OLD", date(2024, 1, 1), date(2024, 6, 28))
    meta.add_instrument_alias(
        instrument_id, "NEW", date(2024, 7, 1), date(2024, 12, 31)
    )
    for valid_from, valid_to in (
        (date(2024, 1, 1), date(2024, 6, 28)),
        (date(2024, 7, 1), date(2024, 12, 31)),
    ):
        meta.add_vendor_identifier(
            instrument_id,
            "eod",
            "permaTicker",
            "PERMA",
            valid_from,
            valid_to,
            validation_state="validated",
        )
    history = {
        "PERMA": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 8, 30))]
    }
    client = FakeTiingo(history)
    _backfill_eod(
        client,
        bars,
        meta,
        [IngestTarget(instrument_id, "PERMA")],
        date(2024, 1, 1),
        date(2024, 8, 30),
    )
    history["PERMA"].extend(
        [
            eod_row(date(2024, 9, 3), div=0.25),
            eod_row(date(2024, 10, 1)),
        ]
    )

    result = backfill_eod_validated(
        client,
        bars,
        meta,
        ["NEW"],
        date(2024, 7, 1),
        date(2024, 10, 1),
    )

    assert result.refreshed == [instrument_id]
    assert client.eod_calls[-1] == (
        "PERMA",
        date(2024, 1, 1),
        date(2024, 10, 1),
    )
    assert bars.read_canonical_eod(instrument_id)["date"].min() == date(2024, 1, 1)


def test_bare_ticker_can_authorize_single_alias_full_refresh(tmp_path):
    bars, meta = stores(tmp_path)
    start, old_end, new_end = (
        date(2024, 1, 1),
        date(2024, 6, 28),
        date(2024, 7, 5),
    )
    _identity_target(meta, "stable-id", "SAFE", "eod", start, new_end)
    history = {"SAFE": [eod_row(d) for d in weekdays(start, old_end)]}
    client = FakeTiingo(history)
    backfill_eod_validated(client, bars, meta, ["SAFE"], start, old_end)
    history["SAFE"].append(eod_row(date(2024, 7, 1), div=0.25))

    result = backfill_eod_validated(client, bars, meta, ["SAFE"], start, new_end)

    assert result.refreshed == ["stable-id"]
    assert result.failed == {}
    assert client.eod_calls[-1] == ("SAFE", start, new_end)


def test_same_value_cross_type_identifiers_do_not_abort_batch(tmp_path):
    bars, meta = stores(tmp_path)
    start, end = date(2024, 1, 1), date(2024, 1, 5)
    _identity_target(meta, "ticker-id", "US123", "eod", start, end)
    meta.upsert_instrument("perma-id")
    meta.add_instrument_alias("perma-id", "OTHER", start, end)
    meta.add_vendor_identifier(
        "perma-id",
        "eod",
        "permaTicker",
        "US123",
        start,
        end,
        validation_state="validated",
    )
    client = FakeTiingo({"US123": [eod_row(date(2024, 1, 2))]})

    result = backfill_eod_validated(client, bars, meta, ["US123", "OTHER"], start, end)

    assert result.ok
    assert sorted(result.fetched) == ["perma-id", "ticker-id"]
    assert len(client.eod_calls) == 2


def test_force_backfill_does_not_shrink_existing_coverage(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo(
        {"SAFE": [eod_row(date(2024, 1, 2)), eod_row(date(2024, 2, 2))]}
    )
    backfill_eod(client, bars, meta, ["SAFE"], date(2024, 1, 1), date(2024, 2, 29))

    backfill_eod(
        client,
        bars,
        meta,
        ["SAFE"],
        date(2024, 2, 1),
        date(2024, 2, 29),
        force=True,
    )

    assert meta.get_coverage("SAFE", "eod") == (
        date(2024, 1, 1),
        date(2024, 2, 29),
    )


def test_validated_update_clamps_to_current_alias_and_refetches_overlap(tmp_path):
    bars, meta = stores(tmp_path)
    today = date.today()
    alias_start = date(2024, 1, 1)
    instrument_id = meta.upsert_instrument("stable-id")
    meta.add_instrument_alias(instrument_id, "SAFE", alias_start)
    meta.add_vendor_identifier(
        instrument_id,
        "eod",
        "ticker",
        "SAFE",
        alias_start,
        date.max,
        validation_state="validated",
    )
    last = today - timedelta(days=2)
    client = FakeTiingo(
        {
            "SAFE": [
                eod_row(alias_start),
                eod_row(today - timedelta(days=10)),
                eod_row(last),
            ]
        }
    )

    first = update_eod_validated(client, bars, meta, ["SAFE"])
    second = update_eod_validated(client, bars, meta, ["SAFE"])

    assert first.fetched == [instrument_id]
    assert second.fetched == [instrument_id]
    assert client.eod_calls[0] == ("SAFE", alias_start, today)
    assert client.eod_calls[1] == (
        "SAFE",
        last - timedelta(days=REFRESH_WINDOW_DAYS),
        today,
    )
    assert meta.get_coverage(instrument_id, "eod") == (alias_start, last)


def test_validated_update_treats_delisted_ticker_as_inactive(tmp_path):
    bars, meta = stores(tmp_path)
    instrument_id = meta.upsert_instrument("delisted-id")
    meta.add_instrument_alias(
        instrument_id, "GONE", date(2010, 1, 1), date(2020, 12, 31)
    )
    meta.add_vendor_identifier(
        instrument_id,
        "eod",
        "ticker",
        "GONE",
        date(2010, 1, 1),
        date(2020, 12, 31),
        validation_state="validated",
    )
    client = FakeTiingo({})

    result = update_eod_validated(client, bars, meta, ["GONE"])

    assert result.ok
    assert result.skipped == [instrument_id]
    assert result.blocked == {}
    assert client.eod_calls == []
    assert result.segments[0]["detail"] == "ticker has no alias active today"


def test_instrument_ingestion_rejects_v1_generation(tmp_path):
    import pytest

    bars = BarStore(tmp_path)
    meta = MetaStore(tmp_path / "meta.db")
    meta.upsert_instrument("stable-id")

    with pytest.raises(RuntimeError, match="require.*v2"):
        _backfill_eod(
            FakeTiingo({}),
            bars,
            meta,
            [IngestTarget("stable-id", "AAPL")],
            date(2024, 1, 1),
            date(2024, 1, 2),
        )


def test_invalid_canonical_frame_fails_one_target_and_continues(tmp_path):
    bars, meta = stores(tmp_path)
    meta.upsert_instrument("bad-id")
    meta.upsert_instrument("good-id")
    day = date(2024, 1, 2)
    client = FakeTiingo(
        {
            "BAD": [eod_row(day, close=10.0), eod_row(day, close=11.0)],
            "GOOD": [eod_row(day, close=20.0)],
        }
    )

    result = _backfill_eod(
        client,
        bars,
        meta,
        [IngestTarget("bad-id", "BAD"), IngestTarget("good-id", "GOOD")],
        day,
        day,
    )

    assert "duplicate canonical key" in result.failed["bad-id"]
    assert result.fetched == ["good-id"]
    assert bars.read_canonical_eod("bad-id") is None
    assert bars.read_canonical_eod("good-id") is not None


def test_publication_batches_instruments_that_share_a_bucket(tmp_path, monkeypatch):
    bars, meta = stores(tmp_path)
    first = "stable-id"
    peer = next(
        f"peer-{number}"
        for number in range(10_000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(first)
    )
    meta.upsert_instrument(first)
    meta.upsert_instrument(peer)
    day = date(2024, 1, 2)
    client = FakeTiingo({"ONE": [eod_row(day, 10.0)], "TWO": [eod_row(day, 20.0)]})
    calls: list[set[str]] = []
    publish = bars.publish_eod

    def recording_publish(frames, **kwargs):
        calls.append(set(frames))
        return publish(frames, **kwargs)

    monkeypatch.setattr(bars, "publish_eod", recording_publish)
    result = _backfill_eod(
        client,
        bars,
        meta,
        [IngestTarget(first, "ONE"), IngestTarget(peer, "TWO")],
        day,
        day,
    )

    assert result.ok
    assert calls == [{first, peer}]


def test_intraday_publication_batches_each_chunk_round(tmp_path, monkeypatch):
    bars, meta = stores(tmp_path)
    first = "stable-id"
    peer = next(
        f"peer-{number}"
        for number in range(10_000)
        if instrument_bucket(f"peer-{number}") == instrument_bucket(first)
    )
    meta.upsert_instrument(first)
    meta.upsert_instrument(peer)
    client = FakeTiingo({})
    calls: list[set[str]] = []
    publish = bars.publish_intraday

    def recording_publish(frames, **kwargs):
        calls.append(set(frames))
        return publish(frames, **kwargs)

    monkeypatch.setattr(bars, "publish_intraday", recording_publish)
    result = _backfill_intraday(
        client,
        bars,
        meta,
        [IngestTarget(first, "ONE"), IngestTarget(peer, "TWO")],
        date(2024, 1, 1),
        date(2024, 2, 29),
    )

    assert result.ok
    assert calls == [{first, peer}, {first, peer}]


def test_backfill_fetches_missing_leading_history(tmp_path):
    """The reviewer's blocker: rank-year fetch first, then full history."""
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(1995, 1, 2), date(2025, 12, 31))]
    }
    client = FakeTiingo(history)

    # Step 1: fetch the ranking year only
    backfill_eod(client, bars, meta, ["AAPL"], date(2025, 1, 1), date(2025, 12, 31))
    assert meta.get_coverage("AAPL", "eod") == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )

    # Step 2: full history from 1995 must fetch the LEADING gap, not start in 2026
    backfill_eod(client, bars, meta, ["AAPL"], date(1995, 1, 1), date(2025, 12, 31))
    assert (("AAPL", date(1995, 1, 1), date(2024, 12, 31))) in client.eod_calls
    assert meta.get_coverage("AAPL", "eod") == (
        date(1995, 1, 1),
        date(2025, 12, 31),
    )
    df = bars.read_canonical_eod("AAPL")
    assert df["date"].min() == date(1995, 1, 2)

    # Fully covered now: a rerun makes no requests
    n = len(client.eod_calls)
    result = backfill_eod(
        client, bars, meta, ["AAPL"], date(1995, 1, 1), date(2025, 12, 31)
    )
    assert len(client.eod_calls) == n and result.skipped == ["AAPL"]


def test_empty_recent_response_not_marked_covered(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({"NEWCO": []})  # nothing published yet
    today = date.today()

    backfill_eod(client, bars, meta, ["NEWCO"], today - timedelta(days=2), today)
    # publication lag: the range ends now, so it must NOT be marked covered
    assert meta.get_coverage("NEWCO", "eod") is None

    # a rerun tries again rather than skipping
    backfill_eod(client, bars, meta, ["NEWCO"], today - timedelta(days=2), today)
    assert len(client.eod_calls) == 2


def test_empty_historical_response_is_covered(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({"GONE": []})  # e.g. delisted before the range
    backfill_eod(client, bars, meta, ["GONE"], date(2010, 1, 1), date(2010, 12, 31))
    assert meta.get_coverage("GONE", "eod") == (
        date(2010, 1, 1),
        date(2010, 12, 31),
    )
    n = len(client.eod_calls)
    backfill_eod(client, bars, meta, ["GONE"], date(2010, 1, 1), date(2010, 12, 31))
    assert len(client.eod_calls) == n  # no refetch


def test_update_refetches_rolling_overlap(tmp_path):
    bars, meta = stores(tmp_path)
    today = date.today()
    last = today - timedelta(days=2)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    cov_last = meta.get_coverage("AAPL", "eod")[1]

    # correction lands inside the refresh window
    corrected = cov_last - timedelta(days=3)
    for r in client.history["AAPL"]:
        if r["date"][:10] == corrected.isoformat():
            r["close"] = 555.0
    update_eod(client, bars, meta, ["AAPL"])

    _, req_start, _ = client.eod_calls[-1]
    assert req_start == cov_last - timedelta(days=REFRESH_WINDOW_DAYS)
    df = bars.read_canonical_eod("AAPL")
    import polars as pl

    if corrected.weekday() < 5:
        assert df.filter(pl.col("date") == corrected)["close"][0] == 555.0
    assert meta.get_coverage("AAPL", "eod")[0] == date(2024, 1, 1)


def test_new_dividend_triggers_full_refresh(tmp_path):
    bars, meta = stores(tmp_path)
    today = date.today()
    last = today - timedelta(days=10)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)

    # a dividend appears after the old coverage edge
    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    result = update_eod(client, bars, meta, ["AAPL"])

    assert result.refreshed == ["AAPL"]
    # the refresh refetched from the start of coverage, not just the window
    assert client.eod_calls[-1][1] == date(2024, 1, 1)


def test_prefix_truncated_full_refresh_keeps_history(tmp_path):
    """A 'full snapshot' containing only the newest rows must not replace
    the file — that would silently erase stored history."""

    class PrefixTruncatingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.truncate:
                # simulate a vendor response with only the newest row
                return rows[-1:]
            return rows

    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = PrefixTruncatingTiingo(history)
    client.truncate = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    rows_before = bars.read_canonical_eod("AAPL").height

    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.truncate = True
    result = update_eod(client, bars, meta, ["AAPL"])

    # the truncated snapshot passes the max-date check (it contains the
    # dividend row) but must still be rejected
    assert result.refreshed == [] and "AAPL" in result.failed
    assert bars.read_canonical_eod("AAPL").height == rows_before  # history intact
    assert meta.get_coverage("AAPL", "eod")[1] == last


def _dividend_refresh_setup(tmp_path, client_cls):
    """Backfill history, then append a dividend that will trigger a full
    refresh on the next update. Returns (bars, meta, client, last)."""
    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = client_cls(history)
    client.sabotage = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)
    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.sabotage = True
    return bars, meta, client, last


def test_full_refresh_must_contain_trigger_dates(tmp_path):
    """A snapshot with all old dates but missing the dividend row that
    triggered the refresh must fail, not report success."""

    class OmittingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [r for r in rows if r["divCash"] == 0.0]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, OmittingTiingo)
    result = update_eod(client, bars, meta, ["AAPL"])
    assert result.refreshed == [] and "AAPL" in result.failed
    assert meta.get_coverage("AAPL", "eod")[1] == last  # retried next run


def test_full_refresh_must_agree_on_corp_action_values(tmp_path):
    """A snapshot containing the trigger date but with the dividend zeroed
    out must fail."""

    class ZeroingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [
                    {**r, "divCash": 0.0} if r["divCash"] != 0.0 else r for r in rows
                ]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, ZeroingTiingo)
    result = update_eod(client, bars, meta, ["AAPL"])
    assert result.refreshed == [] and "AAPL" in result.failed
    assert meta.get_coverage("AAPL", "eod")[1] == last


def test_backfill_failed_full_refresh_keeps_file_untouched(tmp_path):
    """Unlike update, backfill fetches missing segments. Those frames must
    remain staged if the subsequent full-refresh validation fails."""

    class OmittingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            if self.sabotage and str(start) == "2024-01-01":
                return [r for r in rows if r["divCash"] == 0.0]
            return rows

    bars, meta, client, last = _dividend_refresh_setup(tmp_path, OmittingTiingo)
    before = bars.read_canonical_eod("AAPL")

    result = backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date.today())

    assert result.refreshed == [] and "AAPL" in result.failed
    assert bars.read_canonical_eod("AAPL").equals(before)
    assert meta.get_coverage("AAPL", "eod")[1] == last


def test_backfill_reports_failures(tmp_path):
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history, fail={"BADCO"})
    result = backfill_eod(
        client, bars, meta, ["AAPL", "BADCO"], date(2024, 1, 1), date(2024, 6, 28)
    )
    assert result.fetched == ["AAPL"]
    assert "BADCO" in result.failed and not result.ok


def test_intraday_leading_backfill_and_freq_validation(tmp_path):
    bars, meta = stores(tmp_path)
    client = FakeTiingo({})
    import pytest

    for unsupported in ("1min", "2min", "15min", "30min"):
        with pytest.raises(ValueError):
            backfill_intraday(
                client, bars, meta, ["AAPL"], date(2024, 1, 1), freq=unsupported
            )

    backfill_intraday(
        client, bars, meta, ["AAPL"], date(2024, 6, 1), date(2024, 7, 31), freq="1hour"
    )
    assert meta.get_coverage("AAPL", "intraday_1hour") == (
        date(2024, 6, 1),
        date(2024, 7, 31),
    )

    # extending the range backwards fetches the leading gap
    backfill_intraday(
        client, bars, meta, ["AAPL"], date(2024, 4, 1), date(2024, 7, 31), freq="1hour"
    )
    assert meta.get_coverage("AAPL", "intraday_1hour") == (
        date(2024, 4, 1),
        date(2024, 7, 31),
    )
    assert all(c[3] == "1hour" for c in client.intraday_calls)
    df = bars.read_canonical_intraday("AAPL", freq="1hour")
    assert df["ts"].dt.date().min() == date(2024, 4, 1)  # a Monday: bars exist


def test_reconcile_rebuilds_coverage_from_parquet(tmp_path):
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date(2024, 6, 28))
    meta.replace_coverage({})
    assert meta.get_coverage("AAPL", "eod") is None

    counts = reconcile(bars, meta)
    assert counts["eod"] == 1
    first, last = meta.get_coverage("AAPL", "eod")
    assert first == date(2024, 1, 1) and last == date(2024, 6, 28)


def test_reconcile_removes_stale_coverage(tmp_path):
    """Coverage for a ticker with no Parquet file must not survive reconcile
    — a ghost entry would make later backfills skip real fetches."""
    bars, meta = stores(tmp_path)
    history = {
        "AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), date(2024, 6, 28))]
    }
    client = FakeTiingo(history)
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), date(2024, 6, 28))
    meta.upsert_instrument("GHOST")
    meta.set_coverage("GHOST", "eod", date(2020, 1, 1), date(2024, 12, 31))

    reconcile(bars, meta)
    assert meta.get_coverage("GHOST", "eod") is None
    assert meta.get_coverage("AAPL", "eod") is not None

    # and a backfill for GHOST now actually fetches
    backfill_eod(client, bars, meta, ["GHOST"], date(2024, 1, 1), date(2024, 6, 28))
    assert ("GHOST", date(2024, 1, 1), date(2024, 6, 28)) in client.eod_calls


def test_intraday_today_stays_refreshable(tmp_path):
    """A partial current day is written but not marked covered, so it is
    refetched on the next run."""
    bars, meta = stores(tmp_path)
    client = FakeTiingo({})
    today = date.today()
    start = today - timedelta(days=3)

    backfill_intraday(client, bars, meta, ["AAPL"], start, today, freq="1hour")
    cov = meta.get_coverage("AAPL", "intraday_1hour")
    if cov is not None:
        assert cov[1] <= today - timedelta(days=1)

    n = len(client.intraday_calls)
    backfill_intraday(client, bars, meta, ["AAPL"], start, today, freq="1hour")
    assert len(client.intraday_calls) > n  # today still gets refetched


def test_reconcile_caps_intraday_coverage_at_yesterday(tmp_path):
    """Rebuilding coverage from Parquet must not re-mark today's partial
    session as complete — and a ticker with only today's bars gets no
    coverage entry at all."""
    from marketdata.store.bars import intraday_frame

    bars, meta = stores(tmp_path)
    today = date.today()
    past = today - timedelta(days=5)

    def bar(d: date) -> dict:
        return {
            "date": f"{d.isoformat()}T15:00:00.000Z",
            "open": 10.0,
            "high": 10.5,
            "low": 9.5,
            "close": 10.2,
            "volume": 100,
        }

    meta.upsert_instrument("AAPL")
    meta.upsert_instrument("ONLYTODAY")
    bars.publish_intraday(
        {"AAPL": intraday_frame("AAPL", [bar(past), bar(today)])}, freq="1hour"
    )
    bars.publish_intraday(
        {"ONLYTODAY": intraday_frame("ONLYTODAY", [bar(today)])}, freq="1hour"
    )

    reconcile(bars, meta)
    first, last = meta.get_coverage("AAPL", "intraday_1hour")
    assert first == past and last <= today - timedelta(days=1)
    assert meta.get_coverage("ONLYTODAY", "intraday_1hour") is None


def test_empty_full_refresh_is_a_failure(tmp_path):
    """A corporate action whose full-history refetch comes back empty must
    not be reported as refreshed with the dividend silently missing."""

    class TruncatingTiingo(FakeTiingo):
        def eod(self, ticker, start=None, end=None):
            rows = super().eod(ticker, start, end)
            # full-history requests (from the coverage start) come back empty
            if str(start) == "2024-01-01" and self.history_truncated:
                return []
            return rows

    today = date.today()
    last = today - timedelta(days=10)
    bars, meta = stores(tmp_path)
    history = {"AAPL": [eod_row(d) for d in weekdays(date(2024, 1, 1), last)]}
    client = TruncatingTiingo(history)
    client.history_truncated = False
    backfill_eod(client, bars, meta, ["AAPL"], date(2024, 1, 1), last)

    ex_date = next(d for d in weekdays(last + timedelta(days=1), today))
    client.history["AAPL"].append(eod_row(ex_date, close=99.0, div=0.25))
    client.history_truncated = True
    result = update_eod(client, bars, meta, ["AAPL"])

    assert result.refreshed == []
    assert "AAPL" in result.failed and not result.ok
    # coverage untouched: the refresh will be retried next run
    assert meta.get_coverage("AAPL", "eod")[1] == last
