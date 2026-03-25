from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app import krs_client, rdf_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import adapters
from app.config import settings
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
def live_krs_number() -> str:
    return "0000694720"


@pytest.fixture
def missing_krs_number() -> str:
    return "9999999999"


@pytest_asyncio.fixture
async def live_krs_adapter():
    _reset_app_state()
    await krs_client.stop()
    await krs_client.start()
    try:
        yield MsGovKrsAdapter()
    finally:
        await krs_client.stop()
        _reset_app_state()


@pytest_asyncio.fixture
async def live_app_client(tmp_path):
    original_db_path = settings.scraper_db_path
    settings.scraper_db_path = str(tmp_path / "regression.duckdb")
    _reset_app_state()

    try:
        async with app.router.lifespan_context(app):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                yield client
    finally:
        settings.scraper_db_path = original_db_path
        await rdf_client.stop()
        await krs_client.stop()
        _reset_app_state()
