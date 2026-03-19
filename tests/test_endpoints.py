"""Integration tests for FastAPI endpoints - upstream calls are mocked with httpx."""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def client(monkeypatch):
    """AsyncClient pointing at the FastAPI app, with rdf_client methods patched."""
    from app import rdf_client

    # Prevent real lifespan from trying to open a connection
    monkeypatch.setattr(rdf_client, "_client", object())  # non-None sentinel

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_lookup_found(client, monkeypatch):
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {
            "podmiot": {
                "numerKRS": "0000694720",
                "nazwaPodmiotu": "TEST SP. Z O.O.",
                "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
                "wykreslenie": "",
            },
            "czyPodmiotZnaleziony": True,
            "komunikatBledu": None,
        }

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)

    resp = await client.post("/api/podmiot/lookup", json={"krs": "694720"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["czy_podmiot_znaleziony"] is True
    assert body["podmiot"]["numer_krs"] == "0000694720"
    assert body["podmiot"]["nazwa_podmiotu"] == "TEST SP. Z O.O."


@pytest.mark.asyncio
async def test_lookup_not_found(client, monkeypatch):
    from app import rdf_client

    async def fake_dane_podstawowe(krs):
        return {
            "podmiot": None,
            "czyPodmiotZnaleziony": False,
            "komunikatBledu": "Podmiot nie znaleziony",
        }

    monkeypatch.setattr(rdf_client, "dane_podstawowe", fake_dane_podstawowe)

    resp = await client.post("/api/podmiot/lookup", json={"krs": "0000000001"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["czy_podmiot_znaleziony"] is False
    assert body["podmiot"] is None


@pytest.mark.asyncio
async def test_search_documents(client, monkeypatch):
    from app import rdf_client

    async def fake_wyszukiwanie(krs, page, page_size, sort_field, sort_dir):
        return {
            "content": [
                {
                    "id": "I4YS9y2_bUJIUqJXUQBB1A==",
                    "rodzaj": "18",
                    "status": "NIEUSUNIETY",
                    "statusBezpieczenstwa": None,
                    "nazwa": None,
                    "okresSprawozdawczyPoczatek": "2024-01-01",
                    "okresSprawozdawczyKoniec": "2024-12-31",
                    "dataUsunieciaDokumentu": "",
                }
            ],
            "metadaneWynikow": {
                "numerStrony": 0,
                "rozmiarStrony": 10,
                "liczbaStron": 1,
                "calkowitaLiczbaObiektow": 1,
            },
        }

    monkeypatch.setattr(rdf_client, "wyszukiwanie", fake_wyszukiwanie)

    resp = await client.post("/api/dokumenty/search", json={"krs": "694720"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["content"]) == 1
    assert body["content"][0]["id"] == "I4YS9y2_bUJIUqJXUQBB1A=="
    assert body["metadane_wynikow"]["calkowita_liczba_obiektow"] == 1


@pytest.mark.asyncio
async def test_download_returns_zip(client, monkeypatch):
    from app import rdf_client

    fake_zip = b"PK\x03\x04fake zip content"

    async def fake_download(doc_ids):
        return fake_zip

    monkeypatch.setattr(rdf_client, "download", fake_download)

    resp = await client.post(
        "/api/dokumenty/download",
        json={"document_ids": ["I4YS9y2_bUJIUqJXUQBB1A=="]},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert resp.content == fake_zip
