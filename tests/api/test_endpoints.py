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
# jednostkainna-empty-statement-trees regression (backend_changes.json)
# ---------------------------------------------------------------------------
#
# Before this fix, /api/analysis/compare returned `rzis` and `cash_flow` as
# bare lists. The frontend typed them as `ComparisonNode | null` and had a
# null-guard on `cash_flow`, but the list was truthy and didn't match the
# contract, so empty statement sections rendered as a single-row stub table.
#
# New contract:
#   * rzis / cash_flow are either a single ComparisonNode (synthetic root
#     with the extracted top-level items as children) OR `None` when the
#     section is empty / all null.
# These tests cover both branches and guard against a silent regression to
# the bare-list shape.


@pytest.mark.asyncio
async def test_compare_empty_rzis_and_cash_flow_return_null(client, monkeypatch):
    """JednostkaInna filing with no RZiS or cash-flow data → both fields null.

    Mocks a parsed statement whose `rzis` and `cash_flow` lists are empty
    (the failure mode the bug report described). After the fix, the
    comparison response must expose them as JSON `null` so the frontend
    null-guard fires and renders the "unavailable" message instead of a
    stub table.
    """
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    empty_stmt = {
        "company": {
            "name": "JednostkaInna Test",
            "krs": "0000694720",
            "nip": "1234567890",
            "pkd": "6810Z",
            "period_start": "2024-01-01",
            "period_end": "2024-12-31",
            "date_prepared": "2025-03-01",
            "schema_type": "JednostkaInna",
            "schema_code": "SFJINZ",
            "rzis_variant": None,
            "cf_method": None,
        },
        "bilans": {"aktywa": None, "pasywa": None},
        "rzis": [],        # ← empty income statement
        "cash_flow": [],   # ← empty cash flow
        "extras": {},
    }
    prev_stmt = {
        **empty_stmt,
        "company": {**empty_stmt["company"], "period_end": "2023-12-31", "period_start": "2023-01-01"},
    }

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
        return empty_stmt if call_count["n"] % 2 == 1 else prev_stmt
    monkeypatch.setattr(xml_parser, "parse_statement", fake_parse)

    resp = await client.post(
        "/api/analysis/compare",
        json={"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"},
    )
    assert resp.status_code == 200
    body = resp.json()
    # Core fix: both fields must be JSON null, NOT a bare list, NOT a
    # stub root with empty children.
    assert body["rzis"] is None, (
        f"Empty rzis section must collapse to null, got {body['rzis']!r}"
    )
    assert body["cash_flow"] is None, (
        f"Empty cash_flow section must collapse to null, got {body['cash_flow']!r}"
    )


@pytest.mark.asyncio
async def test_compare_populated_rzis_and_cash_flow_return_single_tree(client, monkeypatch):
    """Populated statement → rzis/cash_flow are single ComparisonNode trees.

    Guards the positive path: when the underlying parsed statement actually
    has RZiS or cash-flow items, the endpoint must wrap them under a single
    synthetic root (not a bare list), with the extracted top-level items as
    children. This matches the frontend `ComparisonNode | null` contract.
    """
    from app import rdf_client
    from app.services import xml_parser

    xml_parser._cache.clear()

    rzis_node = {
        "tag": "RZiS.A", "label": "Przychody",
        "kwota_a": 100000.0, "kwota_b": 90000.0, "kwota_b1": None,
        "depth": 0, "is_w_tym": False, "children": [],
    }
    cf_node = {
        "tag": "CF.D", "label": "Przepływ netto",
        "kwota_a": 5000.0, "kwota_b": 4000.0, "kwota_b1": None,
        "depth": 0, "is_w_tym": False, "children": [],
    }
    curr_stmt = {
        "company": {
            "name": "Pop Test Co", "krs": "0000694720", "nip": "1234567890",
            "pkd": "62.01.Z", "period_start": "2024-01-01", "period_end": "2024-12-31",
            "date_prepared": "2025-03-01", "schema_type": "JednostkaInna",
            "schema_code": "SFJINZ", "rzis_variant": "porownawczy", "cf_method": "posrednia",
        },
        "bilans": {"aktywa": None, "pasywa": None},
        "rzis": [rzis_node],
        "cash_flow": [cf_node],
        "extras": {},
    }
    prev_stmt = {
        **curr_stmt,
        "company": {**curr_stmt["company"], "period_end": "2023-12-31", "period_start": "2023-01-01"},
        "rzis": [{**rzis_node, "kwota_a": 90000.0}],
        "cash_flow": [{**cf_node, "kwota_a": 4000.0}],
    }

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
        return curr_stmt if call_count["n"] % 2 == 1 else prev_stmt
    monkeypatch.setattr(xml_parser, "parse_statement", fake_parse)

    resp = await client.post(
        "/api/analysis/compare",
        json={"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"},
    )
    assert resp.status_code == 200
    body = resp.json()

    # --- rzis contract ---
    rzis = body["rzis"]
    assert isinstance(rzis, dict), (
        "rzis must be a single ComparisonNode, not a bare list (the frontend "
        "contract is ComparisonNode | null)"
    )
    assert rzis["tag"] == "RZiS"
    assert rzis["current"] is None
    assert rzis["previous"] is None
    assert isinstance(rzis["children"], list)
    assert len(rzis["children"]) == 1
    assert rzis["children"][0]["tag"] == "RZiS.A"
    assert rzis["children"][0]["current"] == 100000.0
    assert rzis["children"][0]["previous"] == 90000.0

    # --- cash_flow contract ---
    cf = body["cash_flow"]
    assert isinstance(cf, dict)
    assert cf["tag"] == "CF"
    assert cf["current"] is None
    assert len(cf["children"]) == 1
    assert cf["children"][0]["tag"] == "CF.D"
    assert cf["children"][0]["current"] == 5000.0


def test_build_comparison_empty_tree_helper_detects_null_descendants():
    """Unit coverage for `_is_empty_comparison_tree`.

    A synthesized root whose children all carry null `current` and
    `previous` values (e.g. because the source XML had no matching tags) is
    indistinguishable from a root with no children at all from the
    frontend's perspective. Both must be classified as empty so
    `build_comparison` collapses them to `None`.
    """
    from app.services.xml_parser import _is_empty_comparison_tree

    # Case 1: None itself.
    assert _is_empty_comparison_tree(None) is True

    # Case 2: root with no children and null values.
    assert _is_empty_comparison_tree({
        "tag": "RZiS", "label": "…",
        "current": None, "previous": None,
        "children": [],
    }) is True

    # Case 3: root with children, all descendants null.
    assert _is_empty_comparison_tree({
        "tag": "RZiS", "label": "…",
        "current": None, "previous": None,
        "children": [
            {"tag": "RZiS.A", "label": "…", "current": None, "previous": None, "children": []},
            {"tag": "RZiS.B", "label": "…", "current": None, "previous": None, "children": [
                {"tag": "RZiS.B1", "label": "…", "current": None, "previous": None, "children": []},
            ]},
        ],
    }) is True

    # Case 4: ANY non-null descendant → NOT empty.
    assert _is_empty_comparison_tree({
        "tag": "RZiS", "label": "…",
        "current": None, "previous": None,
        "children": [
            {"tag": "RZiS.A", "label": "…", "current": 100.0, "previous": None, "children": []},
        ],
    }) is False

    # Case 5: root itself has a current value → NOT empty.
    assert _is_empty_comparison_tree({
        "tag": "RZiS", "label": "…",
        "current": 42.0, "previous": None,
        "children": [],
    }) is False


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
