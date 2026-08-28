import gzip
import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import responses

from marketdata.store.bars import eod_frame, intraday_frame
from marketdata.tiingo import (
    BASE_URL,
    SUPPORTED_TICKERS_URL,
    ResponseReservationExceeded,
    TiingoClient,
    TiingoError,
)

FIXTURES = Path(__file__).parent / "fixtures" / "tiingo"


@responses.activate
def test_supported_tickers_filters_while_reading_archive():
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr(
            "supported.csv",
            "ticker,exchange,assetType,priceCurrency,startDate,endDate\n"
            "AAPL,NASDAQ,Stock,USD,1980-12-12,2026-08-28\n"
            "MSFT,NASDAQ,Stock,USD,1986-03-13,2026-08-28\n",
        )
    responses.add(responses.GET, SUPPORTED_TICKERS_URL, body=archive.getvalue())

    rows = TiingoClient("test-token").supported_tickers({"msft"})

    assert [row["ticker"] for row in rows] == ["MSFT"]


@responses.activate
def test_eod_request_and_retry():
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    retry_body = b"temporarily unavailable"
    csv_body = (FIXTURES / "eod.csv").read_bytes()
    responses.add(responses.GET, url, body=retry_body, status=503)
    responses.add(responses.GET, url, body=csv_body, status=200)

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.eod("AAPL", "2024-01-01", "2024-01-05")
    assert rows[0]["close"] == "101.0"
    # two calls: the 503 then the successful retry
    assert len(responses.calls) == 2
    assert responses.calls[0].request.headers["Authorization"] == "Token test-token"
    assert responses.calls[0].request.headers["Accept-Encoding"] != "identity"
    assert "startDate=2024-01-01" in responses.calls[1].request.url
    assert "endDate=2024-01-05" in responses.calls[1].request.url
    assert "format=csv" in responses.calls[1].request.url
    assert client.request_count == 2
    assert client.response_bytes == len(retry_body) + len(csv_body)

    frame = eod_frame("AAPL", rows)
    assert frame.row(0, named=True)["close"] == 101.0
    assert frame.row(0, named=True)["volume"] == 123456


@responses.activate
def test_attempt_observer_accounts_for_each_retry_body(monkeypatch):
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    retry_body = b"retry later"
    csv_body = (FIXTURES / "eod.csv").read_bytes()
    responses.add(responses.GET, url, body=retry_body, status=503)
    responses.add(responses.GET, url, body=csv_body, status=200)
    monkeypatch.setattr("marketdata.tiingo.time.sleep", lambda _: None)

    class Observer:
        def __init__(self):
            self.next_id = 0
            self.settled = []

        def before_attempt(self, path="", params=None):
            self.next_id += 1
            return self.next_id

        def after_attempt(self, reservation, observed_bytes, *, complete, bytes_known):
            self.settled.append((reservation, observed_bytes, complete, bytes_known))

    observer = Observer()
    client = TiingoClient("test-token", min_request_interval=0.0)
    client.set_attempt_observer(observer)

    client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert observer.settled == [
        (1, len(retry_body), True, True),
        (2, len(csv_body), True, True),
    ]


@responses.activate
def test_intraday_request_parses_csv_and_normalizes():
    url = f"{BASE_URL}/iex/aapl/prices"
    responses.add(
        responses.GET,
        url,
        body=(FIXTURES / "intraday.csv").read_bytes(),
        status=200,
    )

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.intraday("AAPL", "2024-01-02", "2024-01-03", freq="1hour")

    assert len(rows) == 2
    assert rows[0]["volume"] == "1234"
    request_url = responses.calls[0].request.url
    assert "format=csv" in request_url
    assert "resampleFreq=1hour" in request_url
    assert "columns=open%2Chigh%2Clow%2Cclose%2Cvolume" in request_url

    frame = intraday_frame("AAPL", rows)
    assert frame["ts"].dt.hour().to_list() == [15, 16]
    assert frame["volume"].to_list() == [1234, 2345]


@responses.activate
def test_metadata_remains_json_and_is_metered():
    url = f"{BASE_URL}/tiingo/daily/aapl"
    body = b'{"ticker": "AAPL", "permaTicker": "US0000000001"}'
    responses.add(responses.GET, url, body=body, status=200)

    client = TiingoClient("test-token", min_request_interval=0.0)
    metadata = client.ticker_metadata("AAPL")

    assert metadata["ticker"] == "AAPL"
    assert client.request_count == 1
    assert client.response_bytes == len(body)


@responses.activate
def test_invalid_csv_is_rejected_after_response_is_metered(tmp_path):
    url = f"{BASE_URL}/iex/aapl/prices"
    body = b"date,open,high,low,close\n2024-01-02,1,2,0.5,1.5\n"
    responses.add(responses.GET, url, body=body, status=200)

    from marketdata.scheduler import PersistentAttemptObserver
    from marketdata.store import MetaStore

    client = TiingoClient("test-token", min_request_interval=0.0)
    with MetaStore(tmp_path / "meta.db") as meta:
        client.set_attempt_observer(
            PersistentAttemptObserver(
                meta, work_kind="historical", operation="rejected-csv"
            )
        )
        with pytest.raises(TiingoError, match="missing columns.*volume"):
            client.intraday("AAPL", "2024-01-02", "2024-01-03")

        usage = meta.request_usage(now=datetime.now(UTC), rolling_days=32)
        assert usage["requests"] == 1
        assert usage["observed_bytes"] == len(body)
        assert usage["charged_bytes"] == len(body)
        assert meta.request_attempts()[0]["operation"].endswith("/iex/aapl/prices")

    assert client.request_count == 1
    assert client.response_bytes == len(body)


@responses.activate
def test_attempt_observer_rejects_declared_body_larger_than_reservation():
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    responses.add(
        responses.GET,
        url,
        body=b"too large",
        headers={"Content-Length": "1000"},
        status=200,
    )

    class Observer:
        settled = []

        def before_attempt(self, path="", params=None):
            return 1

        def response_byte_limit(self, reservation):
            return 999

        def after_attempt(self, reservation, observed_bytes, *, complete, bytes_known):
            self.settled.append((reservation, observed_bytes, complete, bytes_known))

    observer = Observer()
    client = TiingoClient("test-token", min_request_interval=0.0)
    client.set_attempt_observer(observer)

    with pytest.raises(TiingoError, match="exceeds the reserved"):
        client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert observer.settled == [(1, 0, False, False)]


@pytest.mark.parametrize(
    "body",
    [
        b"",
        b"\xef\xbb\xbf",
        b"[]",
        (
            b"date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,"
            b"adjVolume,divCash,splitFactor\n"
        ),
    ],
)
@responses.activate
def test_empty_csv_variants_return_no_rows(body):
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    responses.add(responses.GET, url, body=body, status=200)

    client = TiingoClient("test-token", min_request_interval=0.0)

    assert client.eod("AAPL", "1900-01-01", "1900-01-02") == []
    assert client.response_bytes == len(body)


@responses.activate
def test_csv_empty_fields_normalize_to_null():
    eod_url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    eod_body = (
        b"date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,"
        b"adjVolume,divCash,splitFactor\n"
        b"2024-01-02T00:00:00.000Z,101,102,99,100,123456,101,102,99,100,"
        b"123456,,1\n"
    )
    responses.add(responses.GET, eod_url, body=eod_body, status=200)
    intraday_url = f"{BASE_URL}/iex/aapl/prices"
    intraday_body = (
        b"date,open,high,low,close,volume\n"
        b"2024-01-02T10:00:00-05:00,100,101,99.5,,1234\n"
    )
    responses.add(responses.GET, intraday_url, body=intraday_body, status=200)

    client = TiingoClient("test-token", min_request_interval=0.0)
    eod_rows = client.eod("AAPL", "2024-01-02", "2024-01-02")
    intraday_rows = client.intraday("AAPL", "2024-01-02", "2024-01-02")

    assert eod_rows[0]["divCash"] is None
    assert intraday_rows[0]["close"] is None
    assert eod_frame("AAPL", eod_rows)["div_cash"].to_list() == [None]
    assert intraday_frame("AAPL", intraday_rows)["close"].to_list() == [None]


@responses.activate
def test_gzip_response_meter_uses_encoded_bytes():
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    decoded = (FIXTURES / "eod.csv").read_bytes()
    encoded = gzip.compress(decoded)
    responses.add(
        responses.GET,
        url,
        body=encoded,
        headers={"Content-Encoding": "gzip"},
        status=200,
    )

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert rows[0]["close"] == "101.0"
    assert client.response_bytes == len(encoded)


@pytest.mark.parametrize("content_encoding", ["br", "zstd"])
def test_encoded_response_meter_does_not_use_decoded_length(
    content_encoding, monkeypatch
):
    wire_length = 37

    class EncodedResponse:
        status_code = 200
        headers = {"Content-Encoding": content_encoding}
        raw = SimpleNamespace(tell=lambda: wire_length)
        _content = b"decoded payload is a different size"

        @property
        def content(self):
            return self._content

        def close(self):
            pass

    client = TiingoClient("test-token", min_request_interval=0.0)
    monkeypatch.setattr(
        client._session, "get", lambda *args, **kwargs: EncodedResponse()
    )

    client._request("/encoded")

    assert client.response_bytes == wire_length


@responses.activate
def test_network_failure_is_retried_and_wrapped(monkeypatch):
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("reset"))
    responses.add(
        responses.GET,
        url,
        body=(FIXTURES / "eod.csv").read_bytes(),
        status=200,
    )
    monkeypatch.setattr("marketdata.tiingo.time.sleep", lambda _: None)

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert rows[0]["close"] == "101.0"
    assert client.request_count == 2


def test_partial_network_response_is_metered_and_wrapped(monkeypatch):
    class PartialResponse:
        raw = SimpleNamespace(tell=lambda: 17)
        headers = {}

        @property
        def content(self):
            raise requests.exceptions.ChunkedEncodingError("connection reset mid-body")

        def close(self):
            pass

    client = TiingoClient("test-token", min_request_interval=0.0, max_retries=0)
    monkeypatch.setattr(
        client._session, "get", lambda *args, **kwargs: PartialResponse()
    )

    with pytest.raises(TiingoError, match="transport failed after 1 attempts"):
        client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert client.request_count == 1
    assert client.response_bytes == 17


def test_zero_byte_connection_retries_release_byte_reservations(tmp_path, monkeypatch):
    from marketdata.scheduler import PersistentAttemptObserver
    from marketdata.store import MetaStore

    client = TiingoClient("test-token", min_request_interval=0.0, max_retries=2)
    monkeypatch.setattr("marketdata.tiingo.time.sleep", lambda _: None)
    monkeypatch.setattr(
        client._session,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            requests.ConnectionError("offline")
        ),
    )
    with MetaStore(tmp_path / "meta.db") as meta:
        client.set_attempt_observer(
            PersistentAttemptObserver(
                meta, work_kind="historical", operation="connection-outage"
            )
        )

        with pytest.raises(TiingoError, match="failed after 3 attempts"):
            client.eod("AAPL", "2024-01-01", "2024-01-05")

        usage = meta.request_usage(now=datetime.now(UTC), rolling_days=32)
        assert usage["requests"] == 3
        assert usage["charged_bytes"] == 0
        assert usage["incomplete_attempts"] == 3
        assert [row["bytes_known"] for row in meta.request_attempts()] == [1, 1, 1]


def test_chunked_response_stops_when_stream_crosses_reservation():
    class RawCounter:
        transferred = 0

        def tell(self):
            return self.transferred

    class ChunkedResponse:
        status_code = 200
        headers = {}

        def __init__(self):
            self.raw = RawCounter()
            self.chunks_read = 0

        def iter_content(self, chunk_size):
            for chunk in (b"a" * 6, b"b" * 6, b"c" * 6):
                self.chunks_read += 1
                self.raw.transferred += len(chunk)
                yield chunk

        def close(self):
            pass

    class Observer:
        settled = []

        def before_attempt(self, path="", params=None):
            return 1

        def response_byte_limit(self, reservation):
            return 10

        def after_attempt(self, reservation, observed_bytes, *, complete, bytes_known):
            self.settled.append((reservation, observed_bytes, complete, bytes_known))

    response = ChunkedResponse()
    client = TiingoClient("test-token", min_request_interval=0.0)
    client._session.get = lambda *args, **kwargs: response
    observer = Observer()
    client.set_attempt_observer(observer)

    with pytest.raises(ResponseReservationExceeded):
        client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert response.chunks_read == 2
    assert client.response_bytes == 12
    assert observer.settled == [(1, 12, False, True)]


@responses.activate
def test_retry_after_http_date_is_accepted(monkeypatch):
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    responses.add(
        responses.GET,
        url,
        body=b"retry later",
        headers={"Retry-After": "Thu, 01 Jan 1970 00:00:00 GMT"},
        status=503,
    )
    responses.add(
        responses.GET,
        url,
        body=(FIXTURES / "eod.csv").read_bytes(),
        status=200,
    )
    delays = []
    monkeypatch.setattr("marketdata.tiingo.time.sleep", delays.append)

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.eod("AAPL", "2024-01-01", "2024-01-05")

    assert rows[0]["close"] == "101.0"
    assert delays == [0.0]
