from __future__ import annotations

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.regression]


@pytest.mark.asyncio
async def test_get_entity_returns_live_krs_payload(live_krs_adapter, live_krs_number):
    entity = await live_krs_adapter.get_entity(live_krs_number)

    assert entity is not None
    assert entity.krs == live_krs_number
    assert entity.name
    assert entity.legal_form
    assert entity.raw["odpis"]["naglowekA"]["numerKRS"] == live_krs_number
    assert entity.raw["odpis"]["dane"]["dzial1"]["danePodmiotu"]["nazwa"] == entity.name


@pytest.mark.asyncio
async def test_get_entity_normalizes_short_krs(live_krs_adapter, live_krs_number):
    entity = await live_krs_adapter.get_entity("694720")

    assert entity is not None
    assert entity.krs == live_krs_number


@pytest.mark.asyncio
async def test_get_entity_returns_none_for_missing_live_krs(live_krs_adapter, missing_krs_number):
    entity = await live_krs_adapter.get_entity(missing_krs_number)

    assert entity is None


@pytest.mark.asyncio
async def test_health_check_reports_live_ms_gov_status(live_krs_adapter):
    health = await live_krs_adapter.health_check()

    assert health.source == "ms_gov"
    assert health.ok is True
    assert health.latency_ms >= 0
