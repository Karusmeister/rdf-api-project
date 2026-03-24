"""Tests for app.krs_client — resilient async HTTP client for KRS Open API."""

import asyncio

import httpx
import pytest
import pytest_asyncio
import respx

from app import krs_client


@pytest_asyncio.fixture(autouse=True)
async def _setup_client(monkeypatch):
    """Start/stop the KRS client around each test with fast settings."""
    monkeypatch.setattr(krs_client, "_DELAY_S", 0.0)
    monkeypatch.setattr(krs_client, "_MAX_RETRIES", 3)
    monkeypatch.setattr(krs_client, "_BASE_URL", "https://api-krs.ms.gov.pl/api/krs")
    monkeypatch.setattr(krs_client, "_last_request_time", 0.0)

    krs_client._client = httpx.AsyncClient(
        base_url="https://api-krs.ms.gov.pl/api/krs",
        headers=krs_client._HEADERS,
        timeout=5,
    )
    yield
    if krs_client._client is not None:
        await krs_client._client.aclose()
        krs_client._client = None


SAMPLE_RESPONSE = {
    "odpis": {
        "rodzaj": "Aktualny",
        "naglowekA": {"numerKRS": "0000694720"},
    }
}


@respx.mock
@pytest.mark.asyncio
async def test_get_success():
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(200, json=SAMPLE_RESPONSE)
    )

    resp = await krs_client.get(
        "/OdpisAktualny/0000694720",
        params={"rejestr": "P", "format": "json"},
    )
    assert resp.status_code == 200
    assert resp.json()["odpis"]["naglowekA"]["numerKRS"] == "0000694720"


@respx.mock
@pytest.mark.asyncio
async def test_404_not_retried():
    route = respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/9999999999").mock(
        return_value=httpx.Response(
            404,
            json={"type": "not_found", "title": "Not Found", "status": 404},
        )
    )

    resp = await krs_client.get(
        "/OdpisAktualny/9999999999",
        params={"rejestr": "P", "format": "json"},
    )
    assert resp.status_code == 404
    assert route.call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_retry_on_503_then_success(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    route = respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720")
    route.side_effect = [
        httpx.Response(503, json={"error": "unavailable"}),
        httpx.Response(503, json={"error": "unavailable"}),
        httpx.Response(200, json=SAMPLE_RESPONSE),
    ]

    resp = await krs_client.get(
        "/OdpisAktualny/0000694720",
        params={"rejestr": "P", "format": "json"},
    )
    assert resp.status_code == 200
    assert route.call_count == 3


@respx.mock
@pytest.mark.asyncio
async def test_retry_on_429(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    route = respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720")
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(200, json=SAMPLE_RESPONSE),
    ]

    resp = await krs_client.get(
        "/OdpisAktualny/0000694720",
        params={"rejestr": "P", "format": "json"},
    )
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_exhausted_retries_raises(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(503, json={"error": "unavailable"})
    )

    with pytest.raises(httpx.HTTPStatusError) as exc_info:
        await krs_client.get(
            "/OdpisAktualny/0000694720",
            params={"rejestr": "P", "format": "json"},
        )
    assert exc_info.value.response.status_code == 503


@respx.mock
@pytest.mark.asyncio
async def test_connection_error_retries(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    route = respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720")
    route.side_effect = [
        httpx.ConnectError("connection refused"),
        httpx.Response(200, json=SAMPLE_RESPONSE),
    ]

    resp = await krs_client.get(
        "/OdpisAktualny/0000694720",
        params={"rejestr": "P", "format": "json"},
    )
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_connection_error_exhausted_raises(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    with pytest.raises(httpx.ConnectError):
        await krs_client.get(
            "/OdpisAktualny/0000694720",
            params={"rejestr": "P", "format": "json"},
        )


@respx.mock
@pytest.mark.asyncio
async def test_health_check_ok():
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000001").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    result = await krs_client.health_check()
    assert result["ok"] is True
    assert result["source"] == "krs_open_api"
    assert "latency_ms" in result


@respx.mock
@pytest.mark.asyncio
async def test_health_check_down(monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000001").mock(
        side_effect=httpx.ConnectError("down")
    )

    result = await krs_client.health_check()
    assert result["ok"] is False
    assert result["source"] == "krs_open_api"


def test_backoff_delay_increases():
    d1 = krs_client._backoff_delay(1)
    d2 = krs_client._backoff_delay(2)
    d3 = krs_client._backoff_delay(3)
    # Each base doubles: 1, 2, 4 — jitter adds up to 50% more
    assert 1.0 <= d1 <= 1.5
    assert 2.0 <= d2 <= 3.0
    assert 4.0 <= d3 <= 6.0


def test_client_not_initialised():
    original = krs_client._client
    krs_client._client = None
    with pytest.raises(RuntimeError, match="not initialised"):
        krs_client._get_client()
    krs_client._client = original
