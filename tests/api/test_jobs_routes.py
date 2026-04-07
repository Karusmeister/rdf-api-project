from __future__ import annotations

from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.adapters.registry import adapters
from app.db import connection as db_conn
from app.db import prediction_db
from app.main import app
from app.repositories import krs_repo
from app.scraper import db as scraper_db

_FAKE_ADMIN = {
    "id": "admin-1",
    "email": "admin@test.com",
    "name": "Admin",
    "auth_method": "local",
    "password_hash": None,
    "is_verified": True,
    "has_full_access": True,
    "is_active": True,
    "created_at": "2026-01-01",
    "last_login_at": None,
}


def _admin_headers():
    from app.auth import create_token
    token = create_token(_FAKE_ADMIN["id"], _FAKE_ADMIN["email"])
    return {"Authorization": f"Bearer {token}"}


def _reset_app_state() -> None:
    db_conn.close()
    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False
    krs_repo._schema_initialized = False
    adapters.clear()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client(monkeypatch):
    from app import rdf_client

    monkeypatch.setattr(rdf_client, "_client", object())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_trigger_returns_202_when_job_is_scheduled(mock_user, client, monkeypatch):
    from app.jobs import krs_sync

    async def fake_start_sync_task():
        return True

    monkeypatch.setattr(krs_sync, "start_sync_task", fake_start_sync_task)

    response = await client.post("/jobs/krs-sync/trigger", headers=_admin_headers())

    assert response.status_code == 202
    assert response.json() == {"status": "scheduled"}


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_trigger_returns_409_when_job_is_already_running(mock_user, client, monkeypatch):
    from app.jobs import krs_sync

    async def fake_start_sync_task():
        return False

    monkeypatch.setattr(krs_sync, "start_sync_task", fake_start_sync_task)

    response = await client.post("/jobs/krs-sync/trigger", headers=_admin_headers())

    assert response.status_code == 409
    assert response.json() == {"status": "skipped", "reason": "already_running"}


# ---------------------------------------------------------------------------
# KRS Scanner endpoints (PKR-42)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_scan_trigger_returns_202(mock_user, client, monkeypatch):
    from app.jobs import krs_scanner

    async def fake_start():
        return True

    monkeypatch.setattr(krs_scanner, "start_scan_task", fake_start)

    response = await client.post("/jobs/krs-scan/trigger", headers=_admin_headers())
    assert response.status_code == 202
    assert response.json() == {"status": "scheduled"}


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_scan_trigger_409_when_running(mock_user, client, monkeypatch):
    from app.jobs import krs_scanner

    async def fake_start():
        return False

    monkeypatch.setattr(krs_scanner, "start_scan_task", fake_start)

    response = await client.post("/jobs/krs-scan/trigger", headers=_admin_headers())
    assert response.status_code == 409
    assert response.json() == {"status": "skipped", "reason": "already_running"}


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_scan_stop_sets_event(mock_user, client, monkeypatch):
    from app.jobs import krs_scanner
    import asyncio

    krs_scanner._stop_event = asyncio.Event()

    response = await client.post("/jobs/krs-scan/stop", headers=_admin_headers())
    assert response.status_code == 200
    assert response.json() == {"status": "stop_requested"}
    assert krs_scanner._stop_event.is_set()


@pytest.mark.asyncio
async def test_scan_status_returns_cursor_and_last_run(client, monkeypatch):
    from app.jobs import krs_scanner

    monkeypatch.setattr(krs_scanner, "is_scan_running", lambda: False)
    monkeypatch.setattr(krs_repo, "get_cursor", lambda: 42)
    monkeypatch.setattr(krs_repo, "get_last_scan_run", lambda: {
        "id": 1, "status": "completed", "krs_from": 1, "krs_to": 41,
        "probed_count": 41, "valid_count": 20, "error_count": 0,
    })
    monkeypatch.setattr(krs_repo, "count_entities", lambda: 100)

    response = await client.get("/jobs/krs-scan/status")
    assert response.status_code == 200
    body = response.json()
    assert body["cursor"] == 42
    assert body["is_running"] is False
    assert body["last_run"]["krs_from"] == 1
    assert body["total_entities"] == 100


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_reset_cursor_rejected_when_running(mock_user, client, monkeypatch):
    from app.jobs import krs_scanner

    monkeypatch.setattr(krs_scanner, "is_scan_running", lambda: True)

    response = await client.post("/jobs/krs-scan/reset-cursor", json={"next_krs_int": 1}, headers=_admin_headers())
    assert response.status_code == 409
    assert response.json()["reason"] == "scan_running"


@pytest.mark.asyncio
@patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
async def test_reset_cursor_accepted(mock_user, client, monkeypatch):
    from app.jobs import krs_scanner

    monkeypatch.setattr(krs_scanner, "is_scan_running", lambda: False)

    advanced_to = None

    def fake_advance(val):
        nonlocal advanced_to
        advanced_to = val

    monkeypatch.setattr(krs_repo, "advance_cursor", fake_advance)

    response = await client.post("/jobs/krs-scan/reset-cursor", json={"next_krs_int": 500}, headers=_admin_headers())
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "cursor": 500}
    assert advanced_to == 500


@pytest.mark.asyncio
async def test_invalid_cron_does_not_break_lifespan_shutdown(pg_dsn, clean_pg, monkeypatch):
    from app import krs_client, rdf_client
    from app.config import settings

    _reset_app_state()
    monkeypatch.setattr(settings, "database_url", pg_dsn)
    monkeypatch.setattr(settings, "krs_sync_cron", "not a cron")

    async def noop():
        return None

    monkeypatch.setattr(rdf_client, "start", noop)
    monkeypatch.setattr(rdf_client, "stop", noop)
    monkeypatch.setattr(krs_client, "start", noop)
    monkeypatch.setattr(krs_client, "stop", noop)

    try:
        async with app.router.lifespan_context(app):
            pass
    finally:
        _reset_app_state()
