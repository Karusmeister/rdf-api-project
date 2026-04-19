"""Tests for app.jobs.krs_scanner — resumable sequential KRS scanner."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
import pytest
import pytest_asyncio
import respx

from app import krs_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import register as register_adapter
from app.db import connection as db_conn
from app.jobs import krs_scanner
from app.monitoring import metrics
from app.repositories import krs_repo
from app.scraper import db as scraper_db

# ---------------------------------------------------------------------------
# Sample KRS API responses
# ---------------------------------------------------------------------------

VALID_ENTITY_RESPONSE = {
    "odpis": {
        "rodzaj": "Aktualny",
        "naglowekA": {
            "rejestr": "RejP",
            "numerKRS": "0000000001",
            "dataRejestracjiWKRS": "01.01.2000",
            "dataOstatniegoWpisu": "10.01.2025",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "formaPrawna": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
                    "nazwa": "SCANNER TEST SP. Z O.O.",
                    "identyfikatory": {"regon": "11111111100000", "nip": "1111111111"},
                },
                "siedzibaIAdres": {
                    "siedziba": {"kraj": "POLSKA", "miejscowosc": "WARSZAWA"},
                    "adres": {"ulica": "UL. TESTOWA", "kodPocztowy": "00-001"},
                },
            }
        },
    }
}


def _krs_url(krs_int: int) -> str:
    return f"https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{str(krs_int).zfill(10)}"


# ---------------------------------------------------------------------------
# Shared test setup
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def _setup(monkeypatch, pg_dsn, clean_pg):
    """Wire up an isolated PostgreSQL DB, fast KRS client, and adapter."""
    monkeypatch.setattr("app.config.settings.database_url", pg_dsn)
    db_conn.reset()
    db_conn.connect()

    monkeypatch.setattr(scraper_db, "_schema_initialized", False)
    scraper_db.connect()
    monkeypatch.setattr(krs_repo, "_schema_initialized", False)
    krs_repo.connect()

    # Fast KRS client (no delays, no retries)
    monkeypatch.setattr(krs_client, "_DELAY_S", 0.0)
    monkeypatch.setattr(krs_client, "_MAX_RETRIES", 1)
    monkeypatch.setattr(krs_client, "_last_request_time", 0.0)
    monkeypatch.setattr(krs_client, "_rate_limit_lock", None)

    krs_client._client = httpx.AsyncClient(
        base_url="https://api-krs.ms.gov.pl/api/krs",
        headers=krs_client._HEADERS,
        timeout=5,
    )

    register_adapter("ms_gov", MsGovKrsAdapter())
    metrics.clear()

    # Reset scanner state
    krs_scanner._scan_lock = asyncio.Lock()
    krs_scanner._stop_event = asyncio.Event()
    krs_scanner._active_task = None

    yield

    if krs_client._client is not None:
        await krs_client._client.aclose()
        krs_client._client = None
    db_conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_scan_discovers_valid_entity():
    """Mock get_entity returns a KrsEntity for int 1, rest 404 → valid_count=1."""
    # KRS 1 returns valid entity
    respx.get(_krs_url(1)).mock(
        return_value=httpx.Response(200, json=VALID_ENTITY_RESPONSE)
    )
    # All others return 404
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    summary = await krs_scanner.run_scan(batch_size=5)

    assert summary["status"] == "completed"
    assert summary["valid_count"] == 1
    assert summary["probed_count"] == 5
    assert summary["error_count"] == 0

    # Entity was written to krs_companies (single unified table post-dedupe).
    entity = krs_repo.get_entity("0000000001")
    assert entity is not None
    assert entity["name"] == "SCANNER TEST SP. Z O.O."
    assert entity["source"] == "ms_gov_scan"

    conn = scraper_db.get_conn()
    reg = conn.execute(
        "SELECT name FROM krs_companies WHERE krs = '0000000001'"
    ).fetchone()
    assert reg is not None
    assert reg[0] == "SCANNER TEST SP. Z O.O."


@respx.mock
@pytest.mark.asyncio
async def test_scan_advances_cursor_after_each_probe():
    """Verify cursor is at N+1 after probing N."""
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    await krs_scanner.run_scan(batch_size=10)

    # Cursor should now be at 11 (started at 1, probed 1-10)
    assert krs_repo.get_cursor() == 11


@respx.mock
@pytest.mark.asyncio
async def test_scan_resumes_from_cursor():
    """A second scan should pick up where the first left off."""
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    first = await krs_scanner.run_scan(batch_size=5)
    assert first["krs_from"] == 1
    assert first["krs_to"] == 5

    second = await krs_scanner.run_scan(batch_size=5)
    assert second["krs_from"] == 6
    assert second["krs_to"] == 10


@respx.mock
@pytest.mark.asyncio
async def test_scan_stop_event_exits_gracefully():
    """Set _stop_event mid-run, assert stopped_reason='signal'."""
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            krs_scanner._stop_event.set()
        return httpx.Response(404, json={"status": 404})

    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(side_effect=side_effect)

    summary = await krs_scanner.run_scan(batch_size=100)

    assert summary["stopped_reason"] == "signal"
    assert summary["probed_count"] < 100
    assert summary["probed_count"] >= 3


@respx.mock
@pytest.mark.asyncio
async def test_scan_skipped_when_already_running():
    """Call run_scan while lock is held, assert status='skipped'."""
    await krs_scanner._scan_lock.acquire()
    try:
        summary = await krs_scanner.run_scan(batch_size=5)
        assert summary["status"] == "skipped"
        assert summary["reason"] == "already_running"
    finally:
        krs_scanner._scan_lock.release()


@respx.mock
@pytest.mark.asyncio
async def test_scan_errors_counted_but_not_fatal():
    """Mock raises AdapterError, assert error_count increments and run continues."""
    # KRS 1 returns 500 (will trigger AdapterError)
    respx.get(_krs_url(1)).mock(
        return_value=httpx.Response(500, json={"error": "internal"})
    )
    # KRS 2 returns valid entity
    response_2 = dict(VALID_ENTITY_RESPONSE)
    response_2 = {
        "odpis": {
            **VALID_ENTITY_RESPONSE["odpis"],
            "naglowekA": {
                **VALID_ENTITY_RESPONSE["odpis"]["naglowekA"],
                "numerKRS": "0000000002",
            },
        }
    }
    respx.get(_krs_url(2)).mock(
        return_value=httpx.Response(200, json=response_2)
    )
    # Rest 404
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    summary = await krs_scanner.run_scan(batch_size=5)

    assert summary["status"] == "completed"
    assert summary["error_count"] == 1
    assert summary["valid_count"] == 1
    assert summary["probed_count"] == 5


@respx.mock
@pytest.mark.asyncio
async def test_scan_run_recorded_in_db():
    """krs_scan_runs gets a complete row after a scan."""
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    await krs_scanner.run_scan(batch_size=10)

    last = krs_repo.get_last_scan_run()
    assert last is not None
    assert last["status"] == "completed"
    assert last["krs_from"] == 1
    assert last["krs_to"] == 10
    assert last["probed_count"] == 10
    assert last["stopped_reason"] == "batch_limit"
    assert last["finished_at"] is not None


@respx.mock
@pytest.mark.asyncio
async def test_start_scan_task_runs_in_background(monkeypatch):
    """Manual trigger scheduling reserves the slot and runs to completion."""
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )
    monkeypatch.setattr("app.config.settings.krs_scan_batch_size", 5)

    scheduled = await krs_scanner.start_scan_task()
    assert scheduled is True
    assert krs_scanner.is_scan_running() is True

    # Second call rejected
    second = await krs_scanner.start_scan_task()
    assert second is False

    # Wait for completion
    task = krs_scanner._active_task
    assert task is not None
    summary = await task

    assert summary["status"] == "completed"
    assert summary["probed_count"] == 5
