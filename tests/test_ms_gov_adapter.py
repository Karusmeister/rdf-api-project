"""Tests for MsGovKrsAdapter — concrete adapter for the official KRS Open API."""

from datetime import date, datetime, timezone

import httpx
import pytest
import pytest_asyncio
import respx

from app import krs_client
from app.adapters.base import KrsSourceAdapter
from app.adapters.exceptions import RateLimitedError, UpstreamUnavailableError
from app.adapters.ms_gov import MsGovKrsAdapter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_KRS_RESPONSE = {
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
                    "nazwa": "B-JWK-MANAGEMENT SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
                    "identyfikatory": {
                        "regon": "22204956600000",
                        "nip": "5842734981",
                    },
                },
                "siedzibaIAdres": {
                    "siedziba": {
                        "kraj": "POLSKA",
                        "miejscowosc": "GDANSK",
                    },
                    "adres": {
                        "ulica": "UL. MYSLIWSKA",
                        "nrDomu": "116",
                        "kodPocztowy": "80-175",
                    },
                },
            }
        },
    }
}


@pytest_asyncio.fixture(autouse=True)
async def _setup_client(monkeypatch):
    """Start/stop the KRS client around each test with fast settings."""
    monkeypatch.setattr(krs_client, "_DELAY_S", 0.0)
    monkeypatch.setattr(krs_client, "_MAX_RETRIES", 1)
    monkeypatch.setattr(krs_client, "_last_request_time", 0.0)
    monkeypatch.setattr(krs_client, "_rate_limit_lock", None)
    monkeypatch.setattr(krs_client, "_rate_limit_lock_loop", None)

    krs_client._client = httpx.AsyncClient(
        base_url="https://api-krs.ms.gov.pl/api/krs",
        headers=krs_client._HEADERS,
        timeout=5,
    )
    yield
    if krs_client._client is not None:
        await krs_client._client.aclose()
        krs_client._client = None


@pytest.fixture
def adapter():
    return MsGovKrsAdapter()


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_satisfies_protocol():
    assert isinstance(MsGovKrsAdapter(), KrsSourceAdapter)


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_found(adapter):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(200, json=SAMPLE_KRS_RESPONSE)
    )

    entity = await adapter.get_entity("0000694720")
    assert entity is not None
    assert entity.krs == "0000694720"
    assert entity.name == "B-JWK-MANAGEMENT SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA"
    assert entity.legal_form == "SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA"
    assert entity.nip == "5842734981"
    assert entity.regon == "22204956600000"
    assert entity.registered_at == date(2017, 9, 19)
    assert entity.last_changed_at == date(2025, 5, 20)
    assert entity.address_city == "GDANSK"
    assert entity.address_street == "UL. MYSLIWSKA"
    assert entity.address_postal_code == "80-175"
    assert entity.raw["odpis"]["rodzaj"] == "Aktualny"


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_pads_krs(adapter):
    """KRS numbers shorter than 10 digits get zero-padded."""
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000042").mock(
        return_value=httpx.Response(
            200,
            json={
                "odpis": {
                    "rodzaj": "Aktualny",
                    "naglowekA": {"numerKRS": "0000000042"},
                    "dane": {
                        "dzial1": {
                            "danePodmiotu": {"nazwa": "Test Corp"},
                        }
                    },
                }
            },
        )
    )

    entity = await adapter.get_entity("42")
    assert entity is not None
    assert entity.krs == "0000000042"


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_not_found(adapter):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/9999999999").mock(
        return_value=httpx.Response(
            404,
            json={"type": "not_found", "title": "Not Found", "status": 404},
        )
    )

    entity = await adapter.get_entity("9999999999")
    assert entity is None


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_upstream_error(adapter):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        return_value=httpx.Response(500, json={"error": "internal"})
    )

    with pytest.raises(UpstreamUnavailableError, match="ms_gov"):
        await adapter.get_entity("0000694720")


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_connection_error(adapter):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000694720").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    with pytest.raises(UpstreamUnavailableError, match="ms_gov"):
        await adapter.get_entity("0000694720")


# ---------------------------------------------------------------------------
# search — unsupported
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_raises_not_implemented(adapter):
    with pytest.raises(NotImplementedError, match="search endpoint"):
        await adapter.search(name="test")


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_health_check_ok(adapter):
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000001").mock(
        return_value=httpx.Response(404, json={"status": 404})
    )

    health = await adapter.health_check()
    assert health.ok is True
    assert health.source == "ms_gov"
    assert health.latency_ms >= 0
    assert isinstance(health.checked_at, datetime)


@respx.mock
@pytest.mark.asyncio
async def test_health_check_down(adapter, monkeypatch):
    monkeypatch.setattr(krs_client, "_backoff_delay", lambda attempt: 0.0)

    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000001").mock(
        side_effect=httpx.ConnectError("down")
    )

    health = await adapter.health_check()
    assert health.ok is False
    assert health.source == "ms_gov"


# ---------------------------------------------------------------------------
# Entity extraction edge cases
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_get_entity_missing_optional_fields(adapter):
    """Entity with minimal data still parses correctly."""
    respx.get("https://api-krs.ms.gov.pl/api/krs/OdpisAktualny/0000000001").mock(
        return_value=httpx.Response(
            200,
            json={
                "odpis": {
                    "rodzaj": "Aktualny",
                    "naglowekA": {"numerKRS": "0000000001"},
                    "dane": {
                        "dzial1": {
                            "danePodmiotu": {"nazwa": "Minimal Corp"},
                        }
                    },
                }
            },
        )
    )

    entity = await adapter.get_entity("0000000001")
    assert entity is not None
    assert entity.krs == "0000000001"
    assert entity.name == "Minimal Corp"
    assert entity.nip is None
    assert entity.regon is None
    assert entity.registered_at is None
    assert entity.address_city is None
