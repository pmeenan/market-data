"""Safe Tiingo identity-bootstrap tests (offline)."""

from __future__ import annotations

from datetime import UTC, datetime

from marketdata.identity_bootstrap import bootstrap_eod_identities
from marketdata.store import MetaStore


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

    def supported_tickers(self):
        return self.archive

    def ticker_metadata(self, ticker):
        self.metadata_calls.append(ticker)
        return self.metadata[ticker]


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


def test_bootstrap_validates_only_unique_matching_eod_records_and_resumes(tmp_path):
    safe = _archive("SAFE")
    bad = _archive("BAD")
    reused_old = _archive("REUSE", start="2010-01-01", end="2015-01-01")
    reused_new = _archive("REUSE", start="2020-01-01")
    client = FakeIdentityClient(
        [safe, bad, reused_old, reused_new],
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
            ["SAFE", "BAD", "REUSE", "MISSING"],
            clock=now,
        )

        assert result.validated == ["SAFE"]
        assert result.blocked == {
            "MISSING": "no in-scope Tiingo supported-tickers record",
            "REUSE": "2 in-scope Tiingo records reuse this ticker",
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
        assert meta.instrument_ids() == {instrument_id}
