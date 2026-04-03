"""Integration tests for FastAPI endpoints - upstream calls are mocked with httpx."""

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.main import app

# ---------------------------------------------------------------------------
# Shared helpers for analysis tests
# ---------------------------------------------------------------------------

_SEARCH_PAGE = {
    "content": [
        {
            "id": "doc-2024",
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
        "rozmiarStrony": 100,
        "liczbaStron": 1,
        "calkowitaLiczbaObiektow": 1,
    },
}

_META = {
    "czyMSR": False,
    "czyKorekta": False,
    "dataDodania": "2025-03-01",
    "nazwaPliku": "sprawozdanie.zip",
}

_PARSED_STMT = {
    "company": {
        "name": "TEST SP. Z O.O.",
        "krs": "0000694720",
        "nip": "1234567890",
        "pkd": "62.01.Z",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "date_prepared": "2025-03-01",
        "schema_type": "JednostkaInna",
        "rzis_variant": "porownawczy",
        "cf_method": "posrednia",
    },
    "bilans": {"aktywa": None, "pasywa": None},
    "rzis": [],
    "cash_flow": [],
}


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


# ---------------------------------------------------------------------------
# Transport error handling (CR-004)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transport_error_returns_502(client, monkeypatch):
    """httpx.RequestError (timeout, DNS failure, etc.) must surface as 502, not 500."""
    from app import rdf_client

    async def raise_timeout(krs):
        req = httpx.Request("POST", "http://upstream/fake")
        raise httpx.ConnectTimeout("timed out", request=req)

    monkeypatch.setattr(rdf_client, "dane_podstawowe", raise_timeout)

    resp = await client.post("/api/podmiot/lookup", json={"krs": "694720"})
    assert resp.status_code == 502
    body = resp.json()
    assert body["detail"] == "Upstream connection error"
    assert body["error_type"] == "ConnectTimeout"


# ---------------------------------------------------------------------------
# Analysis endpoints (CR-006)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_available_periods(client, monkeypatch):
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    monkeypatch.setattr(rdf_client, "dane_podstawowe", lambda krs: _async(_LOOKUP_DATA))
    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(_SEARCH_PAGE))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))

    resp = await client.get("/api/analysis/available-periods/694720")
    assert resp.status_code == 200
    body = resp.json()
    assert body["krs"] == "0000694720"
    assert len(body["periods"]) == 1
    assert body["periods"][0]["period_end"] == "2024-12-31"


@pytest.mark.asyncio
async def test_available_periods_no_docs(client, monkeypatch):
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    empty_page = {**_SEARCH_PAGE, "content": [], "metadaneWynikow": {**_SEARCH_PAGE["metadaneWynikow"], "calkowitaLiczbaObiektow": 0}}
    monkeypatch.setattr(rdf_client, "dane_podstawowe", lambda krs: _async(_LOOKUP_DATA))
    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(empty_page))

    resp = await client.get("/api/analysis/available-periods/694720")
    assert resp.status_code == 200
    assert resp.json()["periods"] == []


@pytest.mark.asyncio
async def test_statement_endpoint(client, monkeypatch):
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(_SEARCH_PAGE))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))
    monkeypatch.setattr(rdf_client, "download", lambda doc_ids: _async(b"fake"))
    monkeypatch.setattr(xml_parser, "extract_xml_from_zip", lambda b: "<fake/>")
    monkeypatch.setattr(xml_parser, "parse_statement", lambda s: _PARSED_STMT)

    resp = await client.post("/api/analysis/statement", json={"krs": "694720"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["company"]["name"] == "TEST SP. Z O.O."


@pytest.mark.asyncio
async def test_statement_period_not_found(client, monkeypatch):
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(_SEARCH_PAGE))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))

    resp = await client.post(
        "/api/analysis/statement",
        json={"krs": "694720", "period_end": "2020-12-31"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_compare_endpoint(client, monkeypatch):
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    search_two_pages = {
        "content": [
            {**_SEARCH_PAGE["content"][0], "id": "doc-2024", "okresSprawozdawczyKoniec": "2024-12-31"},
            {**_SEARCH_PAGE["content"][0], "id": "doc-2023", "okresSprawozdawczyKoniec": "2023-12-31",
             "okresSprawozdawczyPoczatek": "2023-01-01"},
        ],
        "metadaneWynikow": {**_SEARCH_PAGE["metadaneWynikow"], "calkowitaLiczbaObiektow": 2},
    }
    stmt_2023 = {**_PARSED_STMT, "company": {**_PARSED_STMT["company"], "period_start": "2023-01-01", "period_end": "2023-12-31"}}

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(search_two_pages))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))
    monkeypatch.setattr(rdf_client, "download", lambda doc_ids: _async(b"fake"))
    monkeypatch.setattr(xml_parser, "extract_xml_from_zip", lambda b: "<fake/>")

    call_count = {"n": 0}
    def fake_parse(s):
        call_count["n"] += 1
        return _PARSED_STMT if call_count["n"] % 2 == 1 else stmt_2023
    monkeypatch.setattr(xml_parser, "parse_statement", fake_parse)

    resp = await client.post(
        "/api/analysis/compare",
        json={"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["current_period"]["end"] == "2024-12-31"
    assert body["previous_period"]["end"] == "2023-12-31"
    # Two-statement compare must never include kwota_b data
    assert body["bilans"] == {"aktywa": None, "pasywa": None}


@pytest.mark.asyncio
async def test_metadata_failure_not_cached(client, monkeypatch):
    """A transient metadata failure must not poison the cache with an empty list."""
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    call_count = {"n": 0}

    async def flaky_metadata(doc_id):
        call_count["n"] += 1
        if call_count["n"] == 1:
            req = httpx.Request("GET", "http://upstream/fake")
            raise httpx.ConnectTimeout("timeout", request=req)
        return _META

    monkeypatch.setattr(rdf_client, "dane_podstawowe", lambda krs: _async(_LOOKUP_DATA))
    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(_SEARCH_PAGE))
    monkeypatch.setattr(rdf_client, "metadata", flaky_metadata)

    # First call: metadata fails → result not cached
    resp1 = await client.get("/api/analysis/available-periods/694720")
    assert resp1.status_code == 200
    assert resp1.json()["periods"] == []  # doc was skipped due to error

    # Second call: metadata succeeds → full result returned (not the empty cached result)
    resp2 = await client.get("/api/analysis/available-periods/694720")
    assert resp2.status_code == 200
    assert len(resp2.json()["periods"]) == 1


# ---------------------------------------------------------------------------
# Cross-schema compare (FS-001 regression)
# ---------------------------------------------------------------------------

# Minimal parsed statements with actual bilans/rzis nodes so ratios compute.
def _make_parsed_stmt(schema_code, period_end, revenue_tag, revenue_val, net_tag, net_val):
    """Build a minimal parsed statement with one revenue and one net-profit RZiS node."""
    return {
        "company": {
            "name": "TEST SP. Z O.O.", "krs": "0000694720", "nip": "1234567890",
            "pkd": "62.01.Z", "period_start": f"{period_end[:4]}-01-01",
            "period_end": period_end, "date_prepared": "2025-03-01",
            "schema_type": "test", "schema_code": schema_code,
            "rzis_variant": None, "cf_method": None,
        },
        "bilans": {
            "aktywa": {"tag": "Aktywa", "label": "AKTYWA", "kwota_a": 1000.0, "kwota_b": 900.0, "kwota_b1": None, "depth": 0, "is_w_tym": False, "children": []},
            "pasywa": {"tag": "Pasywa", "label": "PASYWA", "kwota_a": 1000.0, "kwota_b": 900.0, "kwota_b1": None, "depth": 0, "is_w_tym": False, "children": [
                {"tag": "Pasywa_A", "label": "A", "kwota_a": 500.0, "kwota_b": 450.0, "kwota_b1": None, "depth": 1, "is_w_tym": False, "children": []},
                {"tag": "Pasywa_B", "label": "B", "kwota_a": 500.0, "kwota_b": 450.0, "kwota_b1": None, "depth": 1, "is_w_tym": False, "children": []},
            ]},
        },
        "rzis": [
            {"tag": f"RZiS.{revenue_tag}", "label": "Revenue", "kwota_a": revenue_val, "kwota_b": 0, "kwota_b1": None, "depth": 0, "is_w_tym": False, "children": []},
            {"tag": f"RZiS.{net_tag}", "label": "Net", "kwota_a": net_val, "kwota_b": 0, "kwota_b1": None, "depth": 0, "is_w_tym": False, "children": []},
        ],
        "cash_flow": [],
        "extras": {},
    }


@pytest.mark.asyncio
async def test_compare_cross_schema_yoy_deltas(client, monkeypatch):
    """revenue_change_pct / net_profit_change_pct must work across different schemas."""
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    # Current: SFJMIZ (revenue=A, net_profit=F)
    stmt_curr = _make_parsed_stmt("SFJMIZ", "2024-12-31", "A", 200.0, "F", 20.0)
    # Previous: SFJINZ (revenue=A, net_profit=L)
    stmt_prev = _make_parsed_stmt("SFJINZ", "2023-12-31", "A", 180.0, "L", 15.0)

    search_two = {
        "content": [
            {**_SEARCH_PAGE["content"][0], "id": "doc-2024", "okresSprawozdawczyKoniec": "2024-12-31"},
            {**_SEARCH_PAGE["content"][0], "id": "doc-2023", "okresSprawozdawczyKoniec": "2023-12-31",
             "okresSprawozdawczyPoczatek": "2023-01-01"},
        ],
        "metadaneWynikow": {**_SEARCH_PAGE["metadaneWynikow"], "calkowitaLiczbaObiektow": 2},
    }

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(search_two))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))
    monkeypatch.setattr(rdf_client, "download", lambda doc_ids: _async(b"fake"))
    monkeypatch.setattr(xml_parser, "extract_xml_from_zip", lambda b: "<fake/>")

    call_count = {"n": 0}
    def fake_parse(s):
        call_count["n"] += 1
        return stmt_curr if call_count["n"] % 2 == 1 else stmt_prev
    monkeypatch.setattr(xml_parser, "parse_statement", fake_parse)

    resp = await client.post(
        "/api/analysis/compare",
        json={"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"},
    )
    assert resp.status_code == 200
    body = resp.json()
    ratios = body["ratios"]

    # Revenue: both schemas use RZiS.A → (200-180)/180 * 100 = 11.11%
    assert ratios["revenue_change_pct"] == pytest.approx(11.11, abs=0.01)
    # Net profit: SFJMIZ uses RZiS.F (20), SFJINZ uses RZiS.L (15) → (20-15)/15 * 100 = 33.33%
    assert ratios["net_profit_change_pct"] == pytest.approx(33.33, abs=0.01)


@pytest.mark.asyncio
async def test_compare_sfzurt_net_profit_delta_is_null(client, monkeypatch):
    """SFZURT has no net_profit mapping → net_profit_change_pct must be null."""
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    # Current: SFZURT (revenue=I, no net_profit)
    stmt_curr = _make_parsed_stmt("SFZURT", "2024-12-31", "I", 5000.0, "XIV", 300.0)
    stmt_prev = _make_parsed_stmt("SFJINZ", "2023-12-31", "A", 4000.0, "L", 200.0)

    search_two = {
        "content": [
            {**_SEARCH_PAGE["content"][0], "id": "doc-2024", "okresSprawozdawczyKoniec": "2024-12-31"},
            {**_SEARCH_PAGE["content"][0], "id": "doc-2023", "okresSprawozdawczyKoniec": "2023-12-31",
             "okresSprawozdawczyPoczatek": "2023-01-01"},
        ],
        "metadaneWynikow": {**_SEARCH_PAGE["metadaneWynikow"], "calkowitaLiczbaObiektow": 2},
    }

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(search_two))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))
    monkeypatch.setattr(rdf_client, "download", lambda doc_ids: _async(b"fake"))
    monkeypatch.setattr(xml_parser, "extract_xml_from_zip", lambda b: "<fake/>")

    call_count = {"n": 0}
    def fake_parse(s):
        call_count["n"] += 1
        return stmt_curr if call_count["n"] % 2 == 1 else stmt_prev
    monkeypatch.setattr(xml_parser, "parse_statement", fake_parse)

    resp = await client.post(
        "/api/analysis/compare",
        json={"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"},
    )
    assert resp.status_code == 200
    ratios = resp.json()["ratios"]

    # SFZURT revenue at RZiS.I(5000), SFJINZ revenue at RZiS.A(4000) → delta = 25%
    assert ratios["revenue_change_pct"] == pytest.approx(25.0, abs=0.01)
    # SFZURT has no net_profit → null
    assert ratios["net_profit_change_pct"] is None


# ---------------------------------------------------------------------------
# Time-series with extras tags (FS-003 regression)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_time_series_extras_tags(client, monkeypatch):
    """Time-series must return non-null values for extras tags (EQ.*)."""
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    eq_node = {"tag": "EQ.A_I", "label": "EQ I", "kwota_a": 100.0, "kwota_b": 90.0, "kwota_b1": None, "depth": 0, "is_w_tym": False, "children": []}
    stmt = {
        "company": {
            "name": "TEST SP. Z O.O.", "krs": "0000694720", "nip": "1234567890",
            "pkd": "62.01.Z", "period_start": "2024-01-01", "period_end": "2024-12-31",
            "date_prepared": "2025-03-01", "schema_type": "JednostkaInna",
            "schema_code": "SFJINZ", "rzis_variant": "porownawczy", "cf_method": None,
        },
        "bilans": {"aktywa": None, "pasywa": None},
        "rzis": [],
        "cash_flow": [],
        "extras": {"equity_changes": [eq_node]},
    }

    monkeypatch.setattr(rdf_client, "wyszukiwanie", lambda *a, **kw: _async(_SEARCH_PAGE))
    monkeypatch.setattr(rdf_client, "metadata", lambda doc_id: _async(_META))
    monkeypatch.setattr(rdf_client, "download", lambda doc_ids: _async(b"fake"))
    monkeypatch.setattr(xml_parser, "extract_xml_from_zip", lambda b: "<fake/>")
    monkeypatch.setattr(xml_parser, "parse_statement", lambda s: stmt)

    resp = await client.post(
        "/api/analysis/time-series",
        json={"krs": "694720", "fields": ["EQ.A_I"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    series = body["series"]
    assert len(series) == 1
    assert series[0]["tag"] == "EQ.A_I"
    # Should have values from kwota_a (current) and kwota_b (extra period)
    assert any(v == 100.0 for v in series[0]["values"] if v is not None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LOOKUP_DATA = {
    "podmiot": {
        "numerKRS": "0000694720",
        "nazwaPodmiotu": "TEST SP. Z O.O.",
        "formaPrawna": "SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
        "wykreslenie": "",
    },
    "czyPodmiotZnaleziony": True,
    "komunikatBledu": None,
}


async def _async(value):
    return value
