"""Tests for batch/worker.py — worker loop, stats, and backoff."""

import asyncio
import logging
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from app.config import settings
from batch.connections import Connection
from batch.progress import ProgressStore
from batch.worker import (
    WorkerStats,
    _make_client,
    _process_krs_with_backoff,
)


# ---------------------------------------------------------------------------
# WorkerStats
# ---------------------------------------------------------------------------

def test_worker_stats_defaults():
    s = WorkerStats()
    assert s.processed == 0
    assert s.found == 0
    assert s.not_found == 0
    assert s.errors == 0


def test_worker_stats_log_does_not_raise(caplog):
    with caplog.at_level(logging.INFO, logger="batch.worker"):
        s = WorkerStats()
        s.processed = 5
        s.found = 3
        s.log(worker_id=0)
    assert "worker=0" in caplog.text


# ---------------------------------------------------------------------------
# _make_client
# ---------------------------------------------------------------------------

def test_make_client_direct():
    conn = Connection(name="direct")
    client = _make_client(conn)
    assert isinstance(client, httpx.AsyncClient)


def test_make_client_with_socks5_proxy():
    """SOCKS5 proxy client can be constructed (socksio is installed)."""
    conn = Connection(name="pl192", proxy_url="socks5://u:p@pl192.nordvpn.com:1080")
    client = _make_client(conn)
    assert isinstance(client, httpx.AsyncClient)


# ---------------------------------------------------------------------------
# _process_krs_with_backoff — respx mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def rdf_base():
    return settings.rdf_base_url


@pytest.fixture
def _no_sleep(monkeypatch):
    """Patch asyncio.sleep to be a no-op for fast tests."""
    async def _fake_sleep(_):
        pass
    monkeypatch.setattr("batch.worker.asyncio.sleep", _fake_sleep)


@pytest.mark.asyncio
async def test_process_krs_found(rdf_base):
    """KRS entity exists — returns 'found'."""
    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json={"numerKRS": "0000000001", "nazwa": "Test"})
        )
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json={"listaElementow": []})
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "found"


@pytest.mark.asyncio
async def test_process_krs_found_but_doc_lookup_fails(rdf_base):
    """Entity exists but document search returns 500 → 'error', not 'found'."""
    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json={"numerKRS": "0000000001", "nazwa": "Test"})
        )
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(500)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "error"


@pytest.mark.asyncio
async def test_process_krs_not_found_empty_dict(rdf_base):
    """Empty response (no numerKRS) → 'not_found'."""
    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json={})
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "not_found"


@pytest.mark.asyncio
async def test_process_krs_not_found_empty_body(rdf_base):
    """Empty list response → 'not_found'."""
    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json=[])
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "not_found"


@pytest.mark.asyncio
async def test_process_krs_http_500_no_retry(rdf_base):
    """500 error → immediate 'error', no retry."""
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(500)

    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "error"
    assert call_count == 1  # no retry on 500


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_process_krs_429_retries_then_succeeds(rdf_base):
    """429 triggers backoff retry; succeeds on 3rd attempt."""
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(429)
        return httpx.Response(200, json={"numerKRS": "0000000001", "nazwa": "Test"})

    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=side_effect
        )
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json={"listaElementow": []})
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "found"
    assert call_count == 3


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_process_krs_429_max_retries_exceeded(rdf_base):
    """429 on every attempt → 'error' after max retries."""
    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(429)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "error"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_process_krs_503_retries(rdf_base):
    """503 also triggers backoff retry."""
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"numerKRS": "0000000001", "nazwa": "Ok"})

    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=side_effect
        )
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json={})
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "found"
    assert call_count == 2


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_process_krs_timeout_retries(rdf_base):
    """Timeout → retries up to MAX_RETRIES_NETWORK times, then 'error'."""
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        raise httpx.ConnectError("connection refused")

    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)
    assert result == "error"
    assert call_count == 3  # initial + 2 retries


# ---------------------------------------------------------------------------
# Progress store integration with worker
# ---------------------------------------------------------------------------

def test_progress_store_skips_done(tmp_path):
    """Worker should skip KRS numbers already marked in progress store."""
    store = ProgressStore(str(tmp_path / "test.duckdb"))
    store.mark(1, "found", worker_id=0)
    assert store.is_done(1) is True
    assert store.is_done(2) is False
