"""Tests for the KRS adapter interface, models, and registry."""

from datetime import date, datetime
from typing import Optional

import pytest
from pydantic import ValidationError

from app.adapters.base import KrsSourceAdapter
from app.adapters.models import (
    AdapterHealth,
    KrsEntity,
    SearchResponse,
    SearchResult,
)
from app.adapters import registry


# ---------------------------------------------------------------------------
# FakeKrsAdapter — test double that satisfies the Protocol
# ---------------------------------------------------------------------------


class FakeKrsAdapter:
    """In-memory adapter for testing. Implements KrsSourceAdapter protocol."""

    def __init__(self, entities: Optional[dict[str, KrsEntity]] = None):
        self._entities = entities or {}

    async def get_entity(self, krs: str) -> Optional[KrsEntity]:
        return self._entities.get(krs)

    async def search(
        self,
        *,
        name: Optional[str] = None,
        nip: Optional[str] = None,
        regon: Optional[str] = None,
        page: int = 0,
        page_size: int = 20,
    ) -> SearchResponse:
        matches = []
        for entity in self._entities.values():
            if name and name.lower() in entity.name.lower():
                matches.append(entity)
            elif nip and entity.nip == nip:
                matches.append(entity)
            elif regon and entity.regon == regon:
                matches.append(entity)

        start = page * page_size
        page_items = matches[start : start + page_size]
        return SearchResponse(
            results=[
                SearchResult(
                    krs=e.krs,
                    name=e.name,
                    legal_form=e.legal_form,
                    registered_at=e.registered_at,
                )
                for e in page_items
            ],
            total_count=len(matches),
            page=page,
            page_size=page_size,
        )

    async def health_check(self) -> AdapterHealth:
        return AdapterHealth(
            source="fake",
            ok=True,
            latency_ms=1,
            checked_at=datetime.now(),
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_ENTITY = KrsEntity(
    krs="0000694720",
    name="B-JWK-MANAGEMENT SP. Z O.O.",
    legal_form="SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA",
    status="AKTYWNY",
    registered_at=date(2017, 9, 19),
    last_changed_at=date(2025, 5, 20),
    nip="5842734981",
    regon="22204956600000",
    address_city="GDANSK",
    raw={"odpis": {"rodzaj": "Aktualny"}},
)


@pytest.fixture
def fake_adapter():
    return FakeKrsAdapter(entities={"0000694720": SAMPLE_ENTITY})


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_fake_adapter_satisfies_protocol():
    adapter = FakeKrsAdapter()
    assert isinstance(adapter, KrsSourceAdapter)


# ---------------------------------------------------------------------------
# get_entity tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_entity_found(fake_adapter):
    entity = await fake_adapter.get_entity("0000694720")
    assert entity is not None
    assert entity.krs == "0000694720"
    assert entity.name == "B-JWK-MANAGEMENT SP. Z O.O."
    assert entity.nip == "5842734981"
    assert entity.regon == "22204956600000"
    assert entity.registered_at == date(2017, 9, 19)
    assert entity.raw["odpis"]["rodzaj"] == "Aktualny"


@pytest.mark.asyncio
async def test_get_entity_not_found(fake_adapter):
    entity = await fake_adapter.get_entity("9999999999")
    assert entity is None


# ---------------------------------------------------------------------------
# search tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_by_name(fake_adapter):
    result = await fake_adapter.search(name="JWK")
    assert result.total_count == 1
    assert result.results[0].krs == "0000694720"


@pytest.mark.asyncio
async def test_search_by_nip(fake_adapter):
    result = await fake_adapter.search(nip="5842734981")
    assert result.total_count == 1


@pytest.mark.asyncio
async def test_search_no_match(fake_adapter):
    result = await fake_adapter.search(name="NONEXISTENT")
    assert result.total_count == 0
    assert result.results == []


@pytest.mark.asyncio
async def test_search_pagination(fake_adapter):
    result = await fake_adapter.search(name="JWK", page=1, page_size=10)
    assert result.total_count == 1
    assert result.results == []  # page 1 is empty when only 1 result


# ---------------------------------------------------------------------------
# health_check tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_check(fake_adapter):
    health = await fake_adapter.health_check()
    assert health.ok is True
    assert health.source == "fake"
    assert health.latency_ms >= 0
    assert isinstance(health.checked_at, datetime)


# ---------------------------------------------------------------------------
# Model validation
# ---------------------------------------------------------------------------


def test_krs_entity_model():
    entity = KrsEntity(krs="1", name="Test Corp", raw={})
    assert entity.krs == "0000000001"
    assert entity.legal_form is None
    assert entity.raw == {}


def test_krs_entity_rejects_empty_krs():
    with pytest.raises(ValidationError):
        KrsEntity(krs="", name="Test", raw={})


@pytest.mark.parametrize("bad_krs", ["abc", "12-34", "12345678901"])
def test_krs_entity_rejects_non_numeric_or_overlong_krs(bad_krs):
    with pytest.raises(ValidationError):
        KrsEntity(krs=bad_krs, name="Test", raw={})


def test_search_result_normalizes_krs():
    result = SearchResult(krs="42", name="Test")
    assert result.krs == "0000000042"


@pytest.mark.parametrize("bad_krs", ["abc", "12345678901"])
def test_search_result_rejects_invalid_krs(bad_krs):
    with pytest.raises(ValidationError):
        SearchResult(krs=bad_krs, name="Test")


def test_adapter_health_model():
    health = AdapterHealth(
        source="test", ok=True, latency_ms=42, checked_at=datetime(2026, 3, 24)
    )
    assert health.source == "test"
    assert health.latency_ms == 42


def test_search_response_model():
    resp = SearchResponse(
        results=[SearchResult(krs="0000000001", name="Test")],
        total_count=1,
        page=0,
        page_size=20,
    )
    assert len(resp.results) == 1


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


def test_register_and_get():
    adapter = FakeKrsAdapter()
    registry.register("test_source", adapter)
    assert registry.get("test_source") is adapter
    # cleanup
    del registry.adapters["test_source"]


def test_get_missing_raises():
    with pytest.raises(KeyError):
        registry.get("nonexistent_source")
