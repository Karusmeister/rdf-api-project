"""Tests for app.monitoring.metrics — ring buffer and stats computation."""

import httpx
import pytest
import pytest_asyncio
import respx

from app import krs_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.monitoring.metrics import clear, get_stats, record_api_call


@pytest.fixture(autouse=True)
def _clear_buffer():
    """Clear the metrics buffer before each test."""
    clear()
    yield
    clear()


# ---------------------------------------------------------------------------
# record_api_call + get_stats
# ---------------------------------------------------------------------------


def test_record_and_stats():
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=100)
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=200)
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=300)

    stats = get_stats()
    assert stats["total_calls"] == 3
    assert stats["error_rate"] == 0.0
    assert stats["calls_per_source"] == {"ms_gov": 3}
    assert stats["p50_latency_ms"] == 200
    assert stats["p95_latency_ms"] == 300


def test_error_rate():
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=50)
    record_api_call(
        source="ms_gov", operation="get_entity", status_code=500,
        latency_ms=100, error="Internal Server Error",
    )

    stats = get_stats()
    assert stats["error_rate"] == 0.5
    assert stats["total_calls"] == 2


def test_cache_hit_rate():
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=5, cached=True)
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=100)

    stats = get_stats()
    assert stats["cache_hit_rate"] == 0.5


def test_stats_empty():
    stats = get_stats()
    assert stats["total_calls"] == 0
    assert stats["error_rate"] == 0.0
    assert stats["p50_latency_ms"] == 0


def test_stats_filter_by_source():
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=100)
    record_api_call(source="other", operation="search", status_code=200, latency_ms=200)

    ms_stats = get_stats(source="ms_gov")
    assert ms_stats["total_calls"] == 1
    assert ms_stats["calls_per_source"] == {"ms_gov": 1}

    other_stats = get_stats(source="other")
    assert other_stats["total_calls"] == 1


def test_clear():
    record_api_call(source="ms_gov", operation="get_entity", status_code=200, latency_ms=100)
    clear()
    assert get_stats()["total_calls"] == 0


# ---------------------------------------------------------------------------
# Adapter integration — metrics are recorded on calls
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def _setup_krs_client(monkeypatch):
    monkeypatch.setattr(krs_client, "_DELAY_S", 0.0)
    monkeypatch.setattr(krs_client, "_MAX_RETRIES", 1)
    monkeypatch.setattr(krs_client, "_last_request_time", 0.0)
    monkeypatch.setattr(krs_client, "_rate_limit_lock", None)
    monkeypatch.setattr(krs_client, "_rate_limit_lock_loop", None)

    krs_client._client = httpx.AsyncClient(
        base_url="https://api-krs.ms.gov.pl/api/krs",
        headers=krs_client._HEADERS,
        timeout=5,
    )
    yield
    if krs_client._client is not None:
        await krs_client._client.aclose()
        krs_client._client = None


@respx.mock
@pytest.mark.asyncio
async def test_adapter_records_metric_on_success(_setup_krs_client):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(
            200,
            json={
                "odpis": {
                    "rodzaj": "Aktualny",
                    "naglowekA": {"numerKRS": "0000694720"},
                    "dane": {"dzial1": {"danePodmiotu": {"nazwa": "Test"}}},
                }
            },
        )
    )

    adapter = MsGovKrsAdapter()
    await adapter.get_entity("0000694720")

    stats = get_stats()
    assert stats["total_calls"] == 1
    assert stats["error_rate"] == 0.0


@respx.mock
@pytest.mark.asyncio
async def test_adapter_records_metric_on_error(_setup_krs_client):
    from app.adapters.exceptions import UpstreamUnavailableError

    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        side_effect=httpx.ConnectError("down")
    )

    adapter = MsGovKrsAdapter()
    with pytest.raises(UpstreamUnavailableError):
        await adapter.get_entity("0000694720")

    stats = get_stats()
    assert stats["total_calls"] == 1
    assert stats["error_rate"] == 1.0


@respx.mock
@pytest.mark.asyncio
async def test_adapter_records_metric_on_404(_setup_krs_client):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/9999999999").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    adapter = MsGovKrsAdapter()
    result = await adapter.get_entity("9999999999")

    assert result is None
    stats = get_stats()
    assert stats["total_calls"] == 1
    assert stats["error_rate"] == 0.0  # 404 is not an error, just "not found"
