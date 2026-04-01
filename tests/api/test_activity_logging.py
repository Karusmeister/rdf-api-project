"""Tests that existing endpoints produce activity_log rows when called (PKR-73)."""

from unittest.mock import MagicMock, patch, call

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client(monkeypatch):
    from app import rdf_client
    monkeypatch.setattr(rdf_client, "_client", object())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest.fixture
def mock_activity_logger(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("app.routers.rdf.podmiot.activity_logger", mock)
    monkeypatch.setattr("app.routers.rdf.dokumenty.activity_logger", mock)
    monkeypatch.setattr("app.routers.analysis.routes.activity_logger", mock)
    monkeypatch.setattr("app.routers.predictions.routes.activity_logger", mock)
    return mock


# ---------------------------------------------------------------------------
# RDF endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lookup_logs_activity(client, monkeypatch, mock_activity_logger):
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {
            "podmiot": {"numerKRS": "0000694720", "nazwaPodmiotu": "TEST", "formaPrawna": "sp", "wykreslenie": ""},
            "czyPodmiotZnaleziony": True,
            "komunikatBledu": None,
        }

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)

    resp = await client.post("/api/podmiot/lookup", json={"krs": "694720"})
    assert resp.status_code == 200

    mock_activity_logger.log.assert_called_once()
    args = mock_activity_logger.log.call_args
    assert args[0][1] == "krs_lookup"
    assert args[0][2] == "694720"


@pytest.mark.asyncio
async def test_search_logs_activity(client, monkeypatch, mock_activity_logger):
    from app import rdf_client

    async def fake_wyszukiwanie(krs, page=0, page_size=10, sort_field=None, sort_dir="ASC"):
        return {
            "content": [],
            "metadaneWynikow": {"numerStrony": 0, "rozmiarStrony": 10, "liczbaStron": 0, "calkowitaLiczbaObiektow": 0},
        }

    monkeypatch.setattr(rdf_client, "wyszukiwanie", fake_wyszukiwanie)

    resp = await client.post("/api/dokumenty/search", json={"krs": "694720"})
    assert resp.status_code == 200

    mock_activity_logger.log.assert_called_once()
    args = mock_activity_logger.log.call_args
    assert args[0][1] == "document_search"


@pytest.mark.asyncio
async def test_metadata_logs_activity(client, monkeypatch, mock_activity_logger):
    from app import rdf_client

    async def fake_metadata(doc_id):
        return {"id": doc_id, "status": "ok"}

    monkeypatch.setattr(rdf_client, "metadata", fake_metadata)

    resp = await client.get("/api/dokumenty/metadata/test-doc-id")
    assert resp.status_code == 200

    mock_activity_logger.log.assert_called_once()
    assert mock_activity_logger.log.call_args[0][1] == "document_view"


@pytest.mark.asyncio
async def test_download_logs_activity(client, monkeypatch, mock_activity_logger):
    from app import rdf_client

    async def fake_download(doc_ids):
        return b"PK\x03\x04fake-zip"

    monkeypatch.setattr(rdf_client, "download", fake_download)

    resp = await client.post("/api/dokumenty/download", json={"document_ids": ["doc-1"]})
    assert resp.status_code == 200

    mock_activity_logger.log.assert_called_once()
    assert mock_activity_logger.log.call_args[0][1] == "document_download"


# ---------------------------------------------------------------------------
# Unauthenticated requests log user_id as None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unauthenticated_logs_null_user(client, monkeypatch, mock_activity_logger):
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {"podmiot": None, "czyPodmiotZnaleziony": False, "komunikatBledu": None}

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)

    resp = await client.post("/api/podmiot/lookup", json={"krs": "1"})
    assert resp.status_code == 200

    assert mock_activity_logger.log.call_args[0][0] is None  # user_id


# ---------------------------------------------------------------------------
# R3: wykreslenie schema — boolean, string, and absent values
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lookup_wykreslenie_boolean(client, monkeypatch, mock_activity_logger):
    """Upstream sometimes returns a boolean for wykreslenie — must not 500."""
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {
            "podmiot": {"numerKRS": "0000694720", "nazwaPodmiotu": "X", "formaPrawna": "sp", "wykreslenie": False},
            "czyPodmiotZnaleziony": True,
            "komunikatBledu": None,
        }

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)
    resp = await client.post("/api/podmiot/lookup", json={"krs": "694720"})
    assert resp.status_code == 200
    assert resp.json()["podmiot"]["wykreslenie"] is False


@pytest.mark.asyncio
async def test_lookup_wykreslenie_absent(client, monkeypatch, mock_activity_logger):
    """Upstream sometimes omits wykreslenie entirely — must not 500."""
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {
            "podmiot": {"numerKRS": "0000694720", "nazwaPodmiotu": "X", "formaPrawna": "sp"},
            "czyPodmiotZnaleziony": True,
            "komunikatBledu": None,
        }

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)
    resp = await client.post("/api/podmiot/lookup", json={"krs": "694720"})
    assert resp.status_code == 200
    assert resp.json()["podmiot"]["wykreslenie"] is None
