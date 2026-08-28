"""Safe Tiingo identity-bootstrap tests (offline)."""

from __future__ import annotations

from datetime import UTC, date, datetime

from marketdata.identity_bootstrap import _instrument_id, bootstrap_eod_identities
from marketdata.store import MetaStore
from marketdata.tiingo import TiingoError


def _archive(
    ticker: str,
    *,
    exchange: str = "NASDAQ",
    start: str = "2020-01-02",
    end: str = "2026-08-27",
) -> dict[str, str]:
    return {
        "ticker": ticker,
        "exchange": exchange,
        "assetType": "Stock",
        "priceCurrency": "USD",
        "startDate": start,
        "endDate": end,
    }


class FakeIdentityClient:
    def __init__(self, archive, metadata):
        self.archive = archive
        self.metadata = metadata
        self.metadata_calls: list[str] = []

    def supported_tickers(self, tickers=None):
        if tickers is None:
            return self.archive
        return [row for row in self.archive if row["ticker"] in tickers]

    def ticker_metadata(self, ticker):
        self.metadata_calls.append(ticker)
        return self.metadata[ticker]


class FailingMetadataClient(FakeIdentityClient):
    def __init__(self, archive, exc):
        super().__init__(archive, {})
        self.exc = exc

    def ticker_metadata(self, ticker):
        self.metadata_calls.append(ticker)
        raise self.exc


def _metadata(row, **overrides):
    result = {
        "ticker": row["ticker"],
        "exchangeCode": row["exchange"],
        "startDate": row["startDate"],
        "endDate": row["endDate"],
        "name": f"{row['ticker']} Incorporated",
        "description": "fixture",
    }
    result.update(overrides)
    return result


def test_bootstrap_validates_metadata_and_archive_bounded_reused_episodes(tmp_path):
    safe = _archive("SAFE")
    bad = _archive("BAD")
    reused_old = _archive("REUSE", start="2010-01-01", end="2015-01-01")
    reused_new = _archive("REUSE", start="2020-01-01")
    overlap_old = _archive("OVER", start="2010-01-01", end="2020-01-01")
    overlap_new = _archive("OVER", start="2015-01-01")
    client = FakeIdentityClient(
        [safe, bad, reused_old, reused_new, overlap_old, overlap_new],
        {
            "SAFE": _metadata(safe),
            "BAD": _metadata(bad, exchangeCode="NYSE"),
        },
    )

    def now():
        return datetime(2026, 8, 28, tzinfo=UTC)

    with MetaStore(tmp_path / "meta.db") as meta:
        result = bootstrap_eod_identities(
            client,
            meta,
            ["SAFE", "BAD", "REUSE", "OVER", "MISSING"],
            clock=now,
        )

        assert result.validated == ["OVER", "REUSE", "SAFE"]
        assert result.registered_episodes == 5
        assert result.overlaps == {"OVER": ["2015-01-01..2020-01-01"]}
        assert result.blocked == {
            "MISSING": "no in-scope Tiingo supported-tickers record",
        }
        assert "exchangeCode" in result.failed["BAD"]
        assert client.metadata_calls == ["BAD", "SAFE"]
        assert meta.request_usage(now=now(), rolling_days=32)["requests"] == 2

        report = meta.resolve_alias_range(
            "SAFE",
            datetime.fromisoformat(safe["startDate"]).date(),
            datetime.fromisoformat(safe["endDate"]).date(),
        )
        assert report.resolved
        instrument_id = report.segments[0].instrument_id
        assert instrument_id is not None
        identifier = meta.resolve_vendor_identifier(
            instrument_id,
            "eod",
            datetime.fromisoformat(safe["startDate"]).date(),
            datetime.fromisoformat(safe["endDate"]).date(),
        )
        assert (identifier.status, identifier.identifier_value) == (
            "resolved",
            "SAFE",
        )

        resumed = bootstrap_eod_identities(client, meta, ["SAFE"], clock=now)
        assert resumed.skipped == ["SAFE"]
        assert client.metadata_calls == ["BAD", "SAFE"]
        assert len(meta.instrument_ids()) == 5
        reuse = meta.resolve_alias_range(
            "REUSE",
            datetime.fromisoformat(reused_old["startDate"]).date(),
            datetime.fromisoformat(reused_new["endDate"]).date(),
        )
        assert [segment.status for segment in reuse.segments] == [
            "resolved",
            "zero_matches",
            "resolved",
        ]
        assert len(meta.identity_episodes()) == 5


def test_bootstrap_collapses_stale_snapshots_and_upgrades_singleton(tmp_path):
    stale = _archive("SNAP", end="2024-01-02")
    current = _archive("SNAP", end="2026-08-27")
    client = FakeIdentityClient([current, stale], {"SNAP": _metadata(current)})
    instrument_id = _instrument_id(current)

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.upsert_instrument(instrument_id)
        for record in (stale, current):
            start = date.fromisoformat(record["startDate"])
            end = date.fromisoformat(record["endDate"])
            meta.add_instrument_alias(instrument_id, "SNAP", start, end)
            meta.add_vendor_identifier(
                instrument_id,
                "eod",
                "ticker",
                "SNAP",
                start,
                end,
                validation_state="validated",
            )
        meta.record_identity_episode(
            instrument_id,
            source_instrument_id=None,
            dataset_key="eod",
            ticker="SNAP",
            display_label="SNAP@20200102",
            episode_ordinal=2,
            basis="archive_record",
            confidence="archive_bound",
            observed_first=None,
            observed_last=None,
        )

        result = bootstrap_eod_identities(client, meta, ["SNAP"])

        assert result.validated == ["SNAP"]
        assert result.registered_episodes == 1
        assert client.metadata_calls == ["SNAP"]
        aliases = meta.instrument_alias_records(instrument_id)
        assert [(row["start_date"], row["end_date"]) for row in aliases] == [
            ("2020-01-02", "2026-08-27")
        ]
        episode = meta.identity_episodes()[0]
        assert episode["confidence"] == "metadata_validated"
        assert episode["episode_ordinal"] == 1


def test_bootstrap_records_per_ticker_vendor_errors_as_partial(tmp_path):
    archive = _archive("SAFE")
    cases = ("Not found", "transport failed")

    for index, message in enumerate(cases):
        with MetaStore(tmp_path / str(index) / "meta.db") as meta:
            result = bootstrap_eod_identities(
                FailingMetadataClient([archive], TiingoError(message)),
                meta,
                ["SAFE"],
            )

        assert result.failed == {"SAFE": message}
        assert result.partial is True
        assert result.operational_failure is False


def test_bootstrap_skips_universe_resolution_when_identity_is_unchanged(
    tmp_path, monkeypatch
):
    archive = _archive("SAFE")
    client = FakeIdentityClient([archive], {"SAFE": _metadata(archive)})

    with MetaStore(tmp_path / "meta.db") as meta:
        meta.set_universe(2026, [{"ticker": "SAFE", "rank": 1}])
        first = bootstrap_eod_identities(client, meta, ["SAFE"])
        assert first.validated == ["SAFE"]

        resolutions = []
        original = MetaStore.resolve_universe

        def track_resolution(store, year):
            resolutions.append(year)
            return original(store, year)

        monkeypatch.setattr(MetaStore, "resolve_universe", track_resolution)
        second = bootstrap_eod_identities(client, meta, ["SAFE"])

    assert second.skipped == ["SAFE"]
    assert resolutions == []
