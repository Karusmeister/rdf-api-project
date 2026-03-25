from __future__ import annotations

from urllib.parse import quote

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.e2e, pytest.mark.regression]


@pytest_asyncio.fixture
async def live_document(live_app_client, live_krs_number):
    response = await live_app_client.post(
        "/api/dokumenty/search",
        json={"krs": live_krs_number, "page": 0, "page_size": 10},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["content"]

    for document in payload["content"]:
        if document["status"] == "NIEUSUNIETY":
            return document

    pytest.fail("Live RDF search did not return a downloadable document")


@pytest.mark.asyncio
async def test_lookup_endpoint_returns_live_entity(live_app_client, live_krs_number):
    response = await live_app_client.post("/api/podmiot/lookup", json={"krs": "694720"})

    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["czy_podmiot_znaleziony"] is True
    assert payload["podmiot"]["numer_krs"] == live_krs_number
    assert payload["podmiot"]["nazwa_podmiotu"]
    assert payload["podmiot"]["forma_prawna"]


@pytest.mark.asyncio
async def test_document_types_endpoint_returns_live_categories(live_app_client):
    response = await live_app_client.post("/api/podmiot/document-types", json={"krs": "694720"})

    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload
    assert all(item["nazwa"] for item in payload)


@pytest.mark.asyncio
async def test_search_endpoint_returns_paginated_live_documents(live_app_client):
    response = await live_app_client.post(
        "/api/dokumenty/search",
        json={"krs": "694720", "page": 0, "page_size": 10, "sort_dir": "MALEJACO"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["metadane_wynikow"]["numer_strony"] == 0
    assert payload["metadane_wynikow"]["rozmiar_strony"] == 10
    assert payload["metadane_wynikow"]["calkowita_liczba_obiektow"] >= len(payload["content"]) >= 1
    assert any(item["status"] == "NIEUSUNIETY" for item in payload["content"])
    assert all(item["id"] for item in payload["content"])


@pytest.mark.asyncio
async def test_metadata_endpoint_round_trips_live_document(live_app_client, live_document):
    response = await live_app_client.get(
        f"/api/dokumenty/metadata/{quote(live_document['id'], safe='')}"
    )

    assert response.status_code == 200, response.text
    payload = response.json()

    assert "czyMSR" in payload
    assert payload["czyMSR"] in {True, False, None}
    assert "czyKorekta" in payload
    assert payload["czyKorekta"] in {True, False, None}
    assert payload["nazwaPliku"]


@pytest.mark.asyncio
async def test_download_endpoint_returns_live_zip(live_app_client, live_document):
    response = await live_app_client.post(
        "/api/dokumenty/download",
        json={"document_ids": [live_document["id"]]},
    )

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/zip")
    assert response.content.startswith(b"PK")
    assert len(response.content) > 0
