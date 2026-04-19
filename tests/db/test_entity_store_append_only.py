"""Tests for batch/entity_store.py.

Post SCHEMA_DEDUPE_PLAN #2 there is no append-only version history — a
single krs_companies row captures the latest state. The filename is kept
for git-history stability; the contents verify the collapsed contract.
"""

import pytest

from app.db.connection import make_connection
from batch.entity_store import EntityStore


@pytest.fixture()
def store(pg_dsn, clean_pg):
    # init_schema=False because the krs_companies table is created by the
    # dedupe/003 migration via the pg_schema_initialized session fixture.
    return EntityStore(pg_dsn, init_schema=False)


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

        def companies(self, krs):
            conn = make_connection(store._dsn)
            try:
                cur = conn.execute("SELECT * FROM krs_companies WHERE krs = %s", [krs])
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in rows]
            finally:
                conn.close()

    return _DB()


def test_first_upsert_creates_row(store, db):
    store.upsert_entity("0000000001", "Test Corp", legal_form="SA")
    rows = db.companies("0000000001")
    assert len(rows) == 1
    assert rows[0]["name"] == "Test Corp"
    assert rows[0]["legal_form"] == "SA"


def test_identical_upsert_stays_single_row(store, db):
    store.upsert_entity("0000000002", "Stable Corp", raw={"a": 1})
    store.upsert_entity("0000000002", "Stable Corp", raw={"a": 1})
    rows = db.companies("0000000002")
    assert len(rows) == 1


def test_changed_data_overwrites_in_place(store, db):
    store.upsert_entity("0000000003", "Name A")
    store.upsert_entity("0000000003", "Name B")
    rows = db.companies("0000000003")
    assert len(rows) == 1
    assert rows[0]["name"] == "Name B"


def test_source_is_rdf_batch(store, db):
    """EntityStore writes come from the batch scanner, source = 'rdf_batch'."""
    store.upsert_entity("0000000004", "Registry Corp", legal_form="SP. Z O.O.")
    rows = db.companies("0000000004")
    assert len(rows) == 1
    assert rows[0]["source"] == "rdf_batch"


def test_multiple_changes_final_value_wins(store, db):
    for i in range(4):
        store.upsert_entity("0000000006", f"Name {i}")
    rows = db.companies("0000000006")
    assert len(rows) == 1
    assert rows[0]["name"] == "Name 3"
