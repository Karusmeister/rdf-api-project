"""Tests for app.repositories.krs_repo — PostgreSQL storage for KRS entities."""

from datetime import date, datetime, timezone

import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.repositories import krs_repo


@pytest.fixture(autouse=True)
def _isolated_db(pg_dsn, clean_pg):
    """Use a clean PostgreSQL database for each test."""
    db_conn.reset()
    krs_repo._schema_initialized = False
    with patch.object(settings, "database_url", pg_dsn):
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
        address_city="GDANSK",
        address_street="UL. MYSLIWSKA",
        address_postal_code="80-175",
        registered_at=date(2017, 9, 19),
        raw={"foo": "bar"},
    )

    entity = krs_repo.get_entity("0000694720")
    assert entity is not None
    assert entity["krs"] == "0000694720"
    assert entity["name"] == "Test Corp"
    assert entity["legal_form"] == "SP. Z O.O."
    assert entity["nip"] == "5842734981"
    assert entity["address_city"] == "GDANSK"
    assert entity["address_street"] == "UL. MYSLIWSKA"
    assert entity["address_postal_code"] == "80-175"
    assert entity["raw"] == {"foo": "bar"}
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
        address_street="UL. MYSLIWSKA",
        address_postal_code="80-175",
        raw={"odpis": {}},
    )
    krs_repo.upsert_from_krs_entity(entity)

    row = krs_repo.get_entity("0000694720")
    assert row is not None
    assert row["name"] == "Test Corp"
    assert row["nip"] == "5842734981"
    assert row["address_street"] == "UL. MYSLIWSKA"
    assert row["address_postal_code"] == "80-175"
    assert row["raw"] == {"odpis": {}}


def test_upsert_entity_preserves_empty_raw_dict():
    krs_repo.upsert_entity(krs="0000000001", name="Test Corp", raw={})

    entity = krs_repo.get_entity("0000000001")
    assert entity is not None
    assert entity["raw"] == {}


# ---------------------------------------------------------------------------
# get_entity
# ---------------------------------------------------------------------------


def test_get_entity_not_found():
    assert krs_repo.get_entity("9999999999") is None


def test_connect_creates_entity_versions_table():
    """DB-003: Legacy krs_entities table removed. Verify version table exists."""
    conn = db_conn.get_conn()
    tables = {
        row[0] for row in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ).fetchall()
    }
    assert "krs_entity_versions" in tables


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


# ---------------------------------------------------------------------------
# Scan cursor (PKR-39)
# ---------------------------------------------------------------------------


def test_get_cursor_returns_1_on_fresh_db():
    assert krs_repo.get_cursor() == 1


def test_advance_cursor():
    krs_repo.advance_cursor(42)
    assert krs_repo.get_cursor() == 42


def test_advance_cursor_idempotent():
    krs_repo.advance_cursor(42)
    krs_repo.advance_cursor(42)
    assert krs_repo.get_cursor() == 42


# ---------------------------------------------------------------------------
# Scan runs (PKR-39)
# ---------------------------------------------------------------------------


def test_scan_run_lifecycle():
    run_id = krs_repo.open_scan_run(krs_from=1)
    assert isinstance(run_id, int)

    krs_repo.update_scan_run(run_id, probed_count=50, valid_count=30, error_count=2)

    krs_repo.close_scan_run(
        run_id,
        status="completed",
        krs_to=50,
        stopped_reason="batch_limit",
        probed_count=50,
        valid_count=30,
        error_count=2,
    )

    last = krs_repo.get_last_scan_run()
    assert last is not None
    assert last["id"] == run_id
    assert last["status"] == "completed"
    assert last["krs_from"] == 1
    assert last["krs_to"] == 50
    assert last["probed_count"] == 50
    assert last["valid_count"] == 30
    assert last["error_count"] == 2
    assert last["stopped_reason"] == "batch_limit"
    assert last["finished_at"] is not None


def test_get_last_scan_run_no_runs():
    assert krs_repo.get_last_scan_run() is None


def test_multiple_scan_runs_returns_latest():
    run1 = krs_repo.open_scan_run(krs_from=1)
    krs_repo.close_scan_run(run1, status="completed", krs_to=100, stopped_reason="batch_limit")

    run2 = krs_repo.open_scan_run(krs_from=101)
    krs_repo.close_scan_run(run2, status="completed", krs_to=200, stopped_reason="batch_limit")

    last = krs_repo.get_last_scan_run()
    assert last["id"] == run2
    assert last["krs_from"] == 101


# ---------------------------------------------------------------------------
# Append-only entity versioning (PR2)
# ---------------------------------------------------------------------------


def _version_rows(krs: str):
    conn = db_conn.get_conn()
    return conn.execute(
        "SELECT * FROM krs_entity_versions WHERE krs = %s ORDER BY version_id",
        [krs],
    ).fetchall()


def _version_dicts(krs: str) -> list[dict]:
    conn = db_conn.get_conn()
    rows = conn.execute(
        "SELECT * FROM krs_entity_versions WHERE krs = %s ORDER BY version_id",
        [krs],
    ).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM krs_entity_versions LIMIT 0").description]
    return [dict(zip(cols, r)) for r in rows]


def test_upsert_creates_version():
    """First upsert should create exactly one version row."""
    krs_repo.upsert_entity(krs="0000000010", name="Version Corp")
    versions = _version_dicts("0000000010")
    assert len(versions) == 1
    assert versions[0]["is_current"] is True
    assert versions[0]["valid_to"] is None


def test_different_snapshots_create_two_versions():
    """Two different snapshots should produce two version rows."""
    krs_repo.upsert_entity(krs="0000000011", name="Name A", nip="1111111111")
    krs_repo.upsert_entity(krs="0000000011", name="Name B", nip="2222222222")
    versions = _version_dicts("0000000011")
    assert len(versions) == 2
    # Only the latest is current
    current_versions = [v for v in versions if v["is_current"]]
    assert len(current_versions) == 1
    assert current_versions[0]["name"] == "Name B"
    # Old version is closed
    old = [v for v in versions if not v["is_current"]]
    assert len(old) == 1
    assert old[0]["valid_to"] is not None


def test_identical_snapshot_no_new_version():
    """Same snapshot twice should NOT create a second version."""
    krs_repo.upsert_entity(
        krs="0000000012", name="Stable Corp", legal_form="SA",
        raw={"key": "value"},
    )
    krs_repo.upsert_entity(
        krs="0000000012", name="Stable Corp", legal_form="SA",
        raw={"key": "value"},
    )
    versions = _version_dicts("0000000012")
    assert len(versions) == 1
    assert versions[0]["is_current"] is True


def test_only_one_current_per_krs_after_multiple_changes():
    """After several changes, exactly one version should be current."""
    for i in range(5):
        krs_repo.upsert_entity(krs="0000000013", name=f"Name {i}")
    versions = _version_dicts("0000000013")
    assert len(versions) == 5
    current = [v for v in versions if v["is_current"]]
    assert len(current) == 1
    assert current[0]["name"] == "Name 4"


def test_version_appears_in_current_view():
    """get_entity reads from krs_entities_current view, backed by versions."""
    krs_repo.upsert_entity(krs="0000000014", name="View Corp", nip="9999999999")
    entity = krs_repo.get_entity("0000000014")
    assert entity is not None
    assert entity["name"] == "View Corp"
    assert entity["nip"] == "9999999999"


def test_count_entities_uses_current_view():
    """count_entities should count distinct current entities, not version rows."""
    krs_repo.upsert_entity(krs="0000000015", name="A")
    krs_repo.upsert_entity(krs="0000000015", name="B")  # version 2, same krs
    krs_repo.upsert_entity(krs="0000000016", name="C")
    assert krs_repo.count_entities() == 2


def test_list_stale_uses_current_view():
    """list_stale should work against krs_entities_current."""
    krs_repo.upsert_entity(krs="0000000017", name="Stale Corp")
    far_future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    stale = krs_repo.list_stale(far_future)
    krs_list = [s["krs"] for s in stale]
    assert "0000000017" in krs_list


def test_startup_guardrail_fails_fast_when_legacy_without_versions():
    """connect() should fail fast on legacy rows without append-only backfill."""
    conn = db_conn.get_conn()
    # Insert legacy data directly, ensure version table is empty
    conn.execute("DELETE FROM krs_entity_versions")
    conn.execute("""
        INSERT INTO krs_entities (krs, name, source) VALUES ('0000099999', 'Guard Corp', 'ms_gov')
        ON CONFLICT (krs) DO NOTHING
    """)
    # Re-connect — guardrail must block startup with clear migration hint.
    krs_repo._schema_initialized = False
    with pytest.raises(RuntimeError, match="Cutover blocked: krs_entity_versions is empty"):
        krs_repo.connect()
