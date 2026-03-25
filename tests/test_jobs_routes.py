from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.adapters.registry import adapters
from app.db import connection as db_conn
from app.db import prediction_db
from app.main import app
from app.repositories import krs_repo
from app.scraper import db as scraper_db


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
async def test_trigger_returns_202_when_job_is_scheduled(client, monkeypatch):
    from app.jobs import krs_sync

    async def fake_start_sync_task():
        return True

    monkeypatch.setattr(krs_sync, "start_sync_task", fake_start_sync_task)

    response = await client.post("/jobs/krs-sync/trigger")

    assert response.status_code == 202
    assert response.json() == {"status": "scheduled"}


@pytest.mark.asyncio
async def test_trigger_returns_409_when_job_is_already_running(client, monkeypatch):
    from app.jobs import krs_sync

    async def fake_start_sync_task():
        return False

    monkeypatch.setattr(krs_sync, "start_sync_task", fake_start_sync_task)

    response = await client.post("/jobs/krs-sync/trigger")

    assert response.status_code == 409
    assert response.json() == {"status": "skipped", "reason": "already_running"}


@pytest.mark.asyncio
async def test_invalid_cron_does_not_break_lifespan_shutdown(tmp_path, monkeypatch):
    from app import krs_client, rdf_client
    from app.config import settings

    _reset_app_state()
    monkeypatch.setattr(settings, "scraper_db_path", str(tmp_path / "jobs_test.duckdb"))
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
