import responses

from marketdata.tiingo import BASE_URL, TiingoClient


@responses.activate
def test_eod_request_and_retry():
    url = f"{BASE_URL}/tiingo/daily/aapl/prices"
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, json=[{"date": "2024-01-02", "close": 101.0}])

    client = TiingoClient("test-token", min_request_interval=0.0)
    rows = client.eod("AAPL", "2024-01-01", "2024-01-05")
    assert rows[0]["close"] == 101.0
    # two calls: the 503 then the successful retry
    assert len(responses.calls) == 2
    assert responses.calls[0].request.headers["Authorization"] == "Token test-token"
    assert "startDate=2024-01-01" in responses.calls[1].request.url
