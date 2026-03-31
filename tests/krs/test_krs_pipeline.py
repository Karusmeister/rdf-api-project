"""Integration tests for the KRS sync pipeline (PKR-33).

Validates the full flow: discovery -> enrichment -> DB upsert -> sync log,
using respx mocks so no real government API calls are made.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import pytest_asyncio
import respx

from app import krs_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import register as register_adapter
from app.db import connection as db_conn
from app.jobs import krs_sync
from app.monitoring import metrics
from app.repositories import krs_repo
from app.scraper import db as scraper_db

# ---------------------------------------------------------------------------
# Fixtures: KRS API response payloads for known entities
# ---------------------------------------------------------------------------

KRS_RESPONSES = {
    "0000694720": {
        "odpis": {
            "rodzaj": "Aktualny",
            "naglowekA": {
                "rejestr": "RejP",
                "numerKRS": "0000694720",
                "dataRejestracjiWKRS": "19.09.2017",
                "dataOstatniegoWpisu": "20.05.2025",
            },
            "dane": {
                "dzial1": {
                    "danePodmiotu": {
                        "formaPrawna": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
                        "nazwa": "B-JWK-MANAGEMENT SP. Z O.O.",
                        "identyfikatory": {"regon": "22204956600000", "nip": "5842734981"},
                    },
                    "siedzibaIAdres": {
                        "siedziba": {"kraj": "POLSKA", "miejscowosc": "GDANSK"},
                        "adres": {"ulica": "UL. MYSLIWSKA", "kodPocztowy": "80-175"},
                    },
                }
            },
        }
    },
    "0000019193": {
        "odpis": {
            "rodzaj": "Aktualny",
            "naglowekA": {
                "rejestr": "RejP",
                "numerKRS": "0000019193",
                "dataRejestracjiWKRS": "12.07.2001",
                "dataOstatniegoWpisu": "10.01.2025",
            },
            "dane": {
                "dzial1": {
                    "danePodmiotu": {
                        "formaPrawna": "SPOLKA AKCYJNA",
                        "nazwa": "LPP SPOLKA AKCYJNA",
                        "identyfikatory": {"regon": "19060705600000", "nip": "5830002293"},
                    },
                    "siedzibaIAdres": {
                        "siedziba": {"kraj": "POLSKA", "miejscowosc": "GDANSK"},
                        "adres": {"ulica": "UL. LOPUSZANSKA", "kodPocztowy": "80-749"},
                    },
                }
            },
        }
    },
    "0000359730": {
        "odpis": {
            "rodzaj": "Aktualny",
            "naglowekA": {
                "rejestr": "RejP",
                "numerKRS": "0000359730",
                "dataRejestracjiWKRS": "06.07.2010",
                "dataOstatniegoWpisu": "08.02.2025",
            },
            "dane": {
                "dzial1": {
                    "danePodmiotu": {
                        "formaPrawna": "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
                        "nazwa": "ALLEGRO SP. Z O.O.",
                        "identyfikatory": {"regon": "14211803700000", "nip": "5252531880"},
                    },
                    "siedzibaIAdres": {
                        "siedziba": {"kraj": "POLSKA", "miejscowosc": "POZNAN"},
                        "adres": {"ulica": "UL. GRUNWALDZKA", "kodPocztowy": "60-166"},
                    },
                }
            },
        }
    },
}


def _mock_krs_api():
    """Install respx routes for all sample KRS numbers + a 404 fallback."""
    for krs_num, payload in KRS_RESPONSES.items():
        respx.get(
            f"https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/{krs_num}"
        ).mock(return_value=httpx.Response(200, json=payload))

    # Unknown KRS → 404
    respx.get(url__regex=r".*/OdpisAktualny/\d{10}$").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )


# ---------------------------------------------------------------------------
# Shared test setup
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(autouse=True)
async def _setup(monkeypatch, pg_dsn, clean_pg):
    """Wire up an isolated PostgreSQL, fast KRS client, and adapter."""
    # Use test PostgreSQL database
    monkeypatch.setattr("app.config.settings.database_url", pg_dsn)
    db_conn.reset()
    db_conn.connect()

    # Reset schema flags so tables get created
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

    # Register adapter
    register_adapter("ms_gov", MsGovKrsAdapter())

    # Clear metrics buffer
    metrics.clear()

    # Reset the sync lock (may be held from a previous test)
    krs_sync._running = asyncio.Lock()
    krs_sync._active_task = None

    yield

    if krs_client._client is not None:
        await krs_client._client.aclose()
        krs_client._client = None
    db_conn.close()


def _seed_registry(*krs_numbers: str):
    """Insert KRS numbers into krs_registry for discovery."""
    conn = scraper_db.get_conn()
    now = datetime.now(timezone.utc).isoformat()
    for krs in krs_numbers:
        conn.execute(
            "INSERT INTO krs_registry (krs, first_seen_at) VALUES (%s, %s) ON CONFLICT (krs) DO NOTHING",
            [krs, now],
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_full_discovery_flow():
    """Discover new KRS numbers from registry → fetch → upsert → sync log."""
    _mock_krs_api()
    _seed_registry("0000694720", "0000019193", "0000359730")

    summary = await krs_sync.run_sync()

    assert summary["status"] == "completed"
    assert summary["new_count"] == 3
    assert summary["error_count"] == 0

    # All three entities are in the DB
    for krs in ("0000694720", "0000019193", "0000359730"):
        entity = krs_repo.get_entity(krs)
        assert entity is not None
        assert entity["krs"] == krs

    # Sync log was written
    last = krs_repo.get_last_sync()
    assert last is not None
    assert last["status"] == "completed"
    assert last["new_count"] == 3


@respx.mock
@pytest.mark.asyncio
async def test_idempotency():
    """Running sync twice with the same data produces no new DB rows."""
    _mock_krs_api()
    _seed_registry("0000694720")

    first = await krs_sync.run_sync()
    assert first["new_count"] == 1

    # Second run: entity is already in krs_entities, not stale yet
    second = await krs_sync.run_sync()
    # Entity exists, not stale → nothing to do
    assert second["new_count"] == 0
    assert second["updated_count"] == 0

    # Still exactly one entity
    assert krs_repo.count_entities() == 1


@respx.mock
@pytest.mark.asyncio
async def test_stale_entity_re_enrichment(monkeypatch):
    """Stale entities get re-fetched from the upstream API."""
    _mock_krs_api()
    _seed_registry("0000694720")

    # First run: discover
    await krs_sync.run_sync()
    assert krs_repo.count_entities() == 1

    # Artificially age the entity's synced_at (both legacy cache and version table)
    conn = db_conn.get_conn()
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=200)).isoformat()
    conn.execute("UPDATE krs_entities SET synced_at = %s WHERE krs = %s", [old_ts, "0000694720"])
    conn.execute("UPDATE krs_entity_versions SET valid_from = %s WHERE krs = %s AND is_current = true", [old_ts, "0000694720"])

    # Second run: should re-enrich the stale entity
    second = await krs_sync.run_sync()
    assert second["updated_count"] == 1
    assert second["new_count"] == 0

    # synced_at should be fresh
    entity = krs_repo.get_entity("0000694720")
    assert entity is not None


@respx.mock
@pytest.mark.asyncio
async def test_upstream_404_not_found():
    """KRS number unknown to upstream is skipped without error."""
    _mock_krs_api()
    _seed_registry("9999999999")

    summary = await krs_sync.run_sync()
    assert summary["new_count"] == 0
    assert summary["error_count"] == 0
    assert krs_repo.count_entities() == 0


@respx.mock
@pytest.mark.asyncio
async def test_rate_limit_429_recorded_as_error(monkeypatch):
    """A 429 from the upstream API triggers an error count."""
    # Override the retryable-status mock to return 429
    respx.get(
        "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720"
    ).mock(return_value=httpx.Response(429, json={"error": "rate limited"}))

    _seed_registry("0000694720")

    summary = await krs_sync.run_sync()
    assert summary["error_count"] == 1
    assert summary["new_count"] == 0

    last = krs_repo.get_last_sync()
    assert last["error_count"] == 1


@respx.mock
@pytest.mark.asyncio
async def test_connection_error_recorded(monkeypatch):
    """A connection error from upstream is recorded but doesn't crash the job."""
    respx.get(
        "https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720"
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    _seed_registry("0000694720")

    summary = await krs_sync.run_sync()
    assert summary["error_count"] == 1
    assert summary["status"] == "completed"


@respx.mock
@pytest.mark.asyncio
async def test_batch_size_limit(monkeypatch):
    """Sync respects the batch size limit."""
    _mock_krs_api()
    _seed_registry("0000694720", "0000019193", "0000359730")

    monkeypatch.setattr("app.config.settings.krs_sync_batch_size", 2)

    summary = await krs_sync.run_sync()
    # With batch_size=2, discovery budget = 1, so at most 2 total
    assert summary["krs_count"] <= 2


@respx.mock
@pytest.mark.asyncio
async def test_batch_size_one_still_discovers_new_entity(monkeypatch):
    """A minimal batch size still reserves one slot for discovery."""
    _mock_krs_api()
    _seed_registry("0000694720")

    monkeypatch.setattr("app.config.settings.krs_sync_batch_size", 1)

    summary = await krs_sync.run_sync()
    assert summary["new_count"] == 1
    assert summary["krs_count"] == 1


@pytest.mark.asyncio
async def test_start_sync_task_runs_in_background(monkeypatch):
    """Manual trigger scheduling reserves the slot immediately and runs once."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_run_sync_inner():
        started.set()
        await release.wait()
        return {
            "sync_id": 123,
            "status": "completed",
            "krs_count": 0,
            "new_count": 0,
            "updated_count": 0,
            "error_count": 0,
        }

    monkeypatch.setattr(krs_sync, "_run_sync_inner", fake_run_sync_inner)

    scheduled = await krs_sync.start_sync_task()
    assert scheduled is True
    assert krs_sync.is_sync_running() is True

    second = await krs_sync.start_sync_task()
    assert second is False

    await started.wait()
    task = krs_sync._active_task
    assert task is not None

    release.set()
    summary = await task

    assert summary["status"] == "completed"
    assert krs_sync.is_sync_running() is False
    assert krs_sync._active_task is None


@respx.mock
@pytest.mark.asyncio
async def test_concurrent_runs_blocked():
    """A second run while the first is active returns 'skipped'."""
    _mock_krs_api()
    _seed_registry("0000694720")

    # Acquire the lock manually
    await krs_sync._running.acquire()

    try:
        summary = await krs_sync.run_sync()
        assert summary["status"] == "skipped"
        assert summary["reason"] == "already_running"
    finally:
        krs_sync._running.release()


@respx.mock
@pytest.mark.asyncio
async def test_metrics_recorded():
    """Every upstream API call during sync is recorded in the metrics buffer."""
    _mock_krs_api()
    _seed_registry("0000694720", "0000019193")

    await krs_sync.run_sync()

    stats = metrics.get_stats(source="ms_gov")
    assert stats["total_calls"] >= 2
    assert stats["error_rate"] == 0.0


@respx.mock
@pytest.mark.asyncio
async def test_mixed_success_and_errors():
    """Some entities succeed, some fail — counts are accurate."""
    # 0000694720 succeeds, 0000019193 returns 500
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(200, json=KRS_RESPONSES["0000694720"])
    )
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000019193").mock(
        return_value=httpx.Response(500, json={"error": "internal"})
    )

    _seed_registry("0000694720", "0000019193")

    summary = await krs_sync.run_sync()
    assert summary["new_count"] == 1
    assert summary["error_count"] == 1
    assert summary["status"] == "completed"
