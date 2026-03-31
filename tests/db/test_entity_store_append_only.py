"""Tests for batch/entity_store.py — append-only entity versioning."""

import pytest

from app.db.connection import make_connection
from batch.entity_store import EntityStore


@pytest.fixture()
def store(pg_dsn, clean_pg):
    return EntityStore(pg_dsn)


@pytest.fixture()
def db(store):
    """Return a read-only helper to query the store's DB."""
    class _DB:
        def query(self, sql, params=None):
            conn = make_connection(store._dsn)
            try:
                return conn.execute(sql, params or []).fetchall()
            finally:
                conn.close()

        def versions(self, krs):
            rows = self.query(
                "SELECT * FROM krs_entity_versions WHERE krs = %s ORDER BY version_id",
                [krs],
            )
            conn = make_connection(store._dsn)
            cols = [d[0] for d in conn.execute("SELECT * FROM krs_entity_versions LIMIT 0").description]
            conn.close()
            return [dict(zip(cols, r)) for r in rows]

    return _DB()


def test_first_upsert_creates_version(store, db):
    store.upsert_entity("0000000001", "Test Corp", legal_form="SA")
    versions = db.versions("0000000001")
    assert len(versions) == 1
    assert versions[0]["is_current"] is True
    assert versions[0]["name"] == "Test Corp"


def test_identical_upsert_no_new_version(store, db):
    store.upsert_entity("0000000002", "Stable Corp", raw={"a": 1})
    store.upsert_entity("0000000002", "Stable Corp", raw={"a": 1})
    versions = db.versions("0000000002")
    assert len(versions) == 1


def test_different_data_creates_new_version(store, db):
    store.upsert_entity("0000000003", "Name A")
    store.upsert_entity("0000000003", "Name B")
    versions = db.versions("0000000003")
    assert len(versions) == 2
    current = [v for v in versions if v["is_current"]]
    assert len(current) == 1
    assert current[0]["name"] == "Name B"
    old = [v for v in versions if not v["is_current"]]
    assert old[0]["valid_to"] is not None


def test_krs_registry_still_populated(store, db):
    store.upsert_entity("0000000004", "Registry Corp", legal_form="SP. Z O.O.")
    rows = db.query("SELECT company_name FROM krs_registry WHERE krs = '0000000004'")
    assert len(rows) == 1
    assert rows[0][0] == "Registry Corp"


def test_legacy_cache_still_populated(store, db):
    store.upsert_entity("0000000005", "Cache Corp")
    rows = db.query("SELECT name FROM krs_entities WHERE krs = '0000000005'")
    assert len(rows) == 1
    assert rows[0][0] == "Cache Corp"


def test_multiple_changes_only_one_current(store, db):
    for i in range(4):
        store.upsert_entity("0000000006", f"Name {i}")
    versions = db.versions("0000000006")
    assert len(versions) == 4
    current = [v for v in versions if v["is_current"]]
    assert len(current) == 1
    assert current[0]["name"] == "Name 3"
