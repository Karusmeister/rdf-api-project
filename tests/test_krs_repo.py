"""Tests for app.repositories.krs_repo — DuckDB storage for KRS entities."""

from datetime import date, datetime, timezone

import pytest

from app.db import connection as db_conn
from app.repositories import krs_repo


@pytest.fixture(autouse=True)
def _in_memory_db(monkeypatch):
    """Use a fresh in-memory DuckDB for each test."""
    monkeypatch.setattr("app.config.settings.scraper_db_path", ":memory:")
    db_conn.reset()
    krs_repo._schema_initialized = False
    krs_repo.connect()
    yield
    db_conn.close()
    db_conn.reset()
    krs_repo._schema_initialized = False


# ---------------------------------------------------------------------------
# upsert_entity
# ---------------------------------------------------------------------------


def test_upsert_entity_insert():
    krs_repo.upsert_entity(
        krs="0000694720",
        name="Test Corp",
        legal_form="SP. Z O.O.",
        nip="5842734981",
        regon="22204956600000",
        registered_at=date(2017, 9, 19),
        raw={"foo": "bar"},
    )

    entity = krs_repo.get_entity("0000694720")
    assert entity is not None
    assert entity["krs"] == "0000694720"
    assert entity["name"] == "Test Corp"
    assert entity["legal_form"] == "SP. Z O.O."
    assert entity["nip"] == "5842734981"
    assert entity["source"] == "ms_gov"


def test_upsert_entity_is_idempotent():
    krs_repo.upsert_entity(krs="0000000001", name="First Name")
    krs_repo.upsert_entity(krs="0000000001", name="Updated Name")

    entity = krs_repo.get_entity("0000000001")
    assert entity["name"] == "Updated Name"
    assert krs_repo.count_entities() == 1


def test_upsert_from_krs_entity():
    from app.adapters.models import KrsEntity

    entity = KrsEntity(
        krs="0000694720",
        name="Test Corp",
        legal_form="SP. Z O.O.",
        nip="5842734981",
        raw={"odpis": {}},
    )
    krs_repo.upsert_from_krs_entity(entity)

    row = krs_repo.get_entity("0000694720")
    assert row is not None
    assert row["name"] == "Test Corp"
    assert row["nip"] == "5842734981"


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------


def test_get_entity_not_found():
    assert krs_repo.get_entity("9999999999") is None


# ---------------------------------------------------------------------------
# list_stale
# ---------------------------------------------------------------------------


def test_list_stale():
    krs_repo.upsert_entity(krs="0000000001", name="Old Corp")
    krs_repo.upsert_entity(krs="0000000002", name="New Corp")

    # Everything was just inserted, so nothing should be stale relative to far future
    far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    stale = krs_repo.list_stale(far_future)
    assert len(stale) == 2

    # Nothing should be stale relative to the past
    far_past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    stale = krs_repo.list_stale(far_past)
    assert len(stale) == 0


# ---------------------------------------------------------------------------
# count_entities
# ---------------------------------------------------------------------------


def test_count_entities_empty():
    assert krs_repo.count_entities() == 0


def test_count_entities():
    krs_repo.upsert_entity(krs="0000000001", name="A")
    krs_repo.upsert_entity(krs="0000000002", name="B")
    assert krs_repo.count_entities() == 2


# ---------------------------------------------------------------------------
# Sync log
# ---------------------------------------------------------------------------


def test_sync_log_lifecycle():
    sync_id = krs_repo.log_sync_start(source="ms_gov")
    assert isinstance(sync_id, int)

    krs_repo.log_sync_finish(
        sync_id,
        krs_count=100,
        new_count=80,
        updated_count=20,
        error_count=0,
        status="completed",
    )

    last = krs_repo.get_last_sync("ms_gov")
    assert last is not None
    assert last["krs_count"] == 100
    assert last["new_count"] == 80
    assert last["status"] == "completed"
    assert last["finished_at"] is not None


def test_get_last_sync_no_runs():
    assert krs_repo.get_last_sync("ms_gov") is None


def test_sync_log_error_status():
    sync_id = krs_repo.log_sync_start()
    krs_repo.log_sync_finish(sync_id, error_count=5, status="failed")

    last = krs_repo.get_last_sync()
    assert last["status"] == "failed"
    assert last["error_count"] == 5
