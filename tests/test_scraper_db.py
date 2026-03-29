"""Tests for app/scraper/db.py — use an in-memory DuckDB via settings override."""
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.scraper import db as scraper_db


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    """Override DB path to a temp file and reset the shared connection."""
    db_path = str(tmp_path / "test.duckdb")
    db_conn.reset()
    scraper_db._schema_initialized = False
    with patch.object(settings, "scraper_db_path", db_path):
        scraper_db.connect()
        yield
        db_conn.close()
    db_conn.reset()
    scraper_db._schema_initialized = False


def test_schema_creation():
    conn = scraper_db.get_conn()
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert "krs_registry" in tables
    assert "krs_documents" in tables
    assert "scraper_runs" in tables


def test_upsert_krs():
    # Insert
    scraper_db.upsert_krs("0000694720", "ACME SP. Z O.O.", "SP. Z O.O.", True)
    conn = scraper_db.get_conn()
    row = conn.execute("SELECT * FROM krs_registry WHERE krs = '0000694720'").fetchone()
    assert row is not None
    assert row[1] == "ACME SP. Z O.O."

    # Update same KRS - name changes, is_active goes false
    scraper_db.upsert_krs("0000694720", "NEW NAME", None, False)
    row = conn.execute("SELECT company_name, is_active FROM krs_registry WHERE krs = '0000694720'").fetchone()
    assert row[0] == "NEW NAME"
    assert row[1] is False

    # Upsert with None name keeps existing name
    scraper_db.upsert_krs("0000694720", None, None, True)
    row = conn.execute("SELECT company_name FROM krs_registry WHERE krs = '0000694720'").fetchone()
    assert row[0] == "NEW NAME"


def test_insert_and_get_documents():
    scraper_db.upsert_krs("0000694720", None, None, True)
    now = datetime.now(timezone.utc).isoformat()
    docs = [
        {"document_id": "docAAA", "krs": "0000694720", "rodzaj": "18",
         "status": "NIEUSUNIETY", "nazwa": None, "okres_start": "2024-01-01",
         "okres_end": "2024-12-31", "discovered_at": now},
        {"document_id": "docBBB", "krs": "0000694720", "rodzaj": "3",
         "status": "NIEUSUNIETY", "nazwa": "Test", "okres_start": None,
         "okres_end": None, "discovered_at": now},
    ]
    scraper_db.insert_documents(docs)

    ids = scraper_db.get_known_document_ids("0000694720")
    assert "docAAA" in ids
    assert "docBBB" in ids

    # Duplicate insert should be ignored (ON CONFLICT DO NOTHING)
    scraper_db.insert_documents(docs)
    ids2 = scraper_db.get_known_document_ids("0000694720")
    assert ids == ids2


def test_mark_downloaded():
    scraper_db.upsert_krs("0000000001", None, None, True)
    now = datetime.now(timezone.utc).isoformat()
    scraper_db.insert_documents([{
        "document_id": "docXYZ", "krs": "0000000001", "rodzaj": "18",
        "status": "NIEUSUNIETY", "nazwa": None, "okres_start": None,
        "okres_end": None, "discovered_at": now,
    }])

    scraper_db.mark_downloaded(
        document_id="docXYZ",
        storage_path="krs/0000000001/docXYZ",
        storage_backend="local",
        file_size=12345,
        zip_size=4567,
        file_count=1,
        file_types="xml",
    )

    conn = scraper_db.get_conn()
    row = conn.execute(
        "SELECT is_downloaded, storage_path, file_count, file_types FROM krs_documents WHERE document_id = 'docXYZ'"
    ).fetchone()
    assert row[0] is True
    assert row[1] == "krs/0000000001/docXYZ"
    assert row[2] == 1
    assert row[3] == "xml"


def test_ordering_strategies():
    now = datetime.now(timezone.utc)

    # Insert 5 KRS with varying priority and last_checked_at
    entries = [
        ("0000000010", 5, now - timedelta(days=10)),
        ("0000000020", 1, now - timedelta(days=1)),
        ("0000000030", 5, now - timedelta(days=5)),
        ("0000000040", 0, None),
        ("0000000050", 3, now - timedelta(days=3)),
    ]
    conn = scraper_db.get_conn()
    for krs, priority, last_checked in entries:
        conn.execute("""
            INSERT INTO krs_registry (krs, is_active, check_priority, last_checked_at, first_seen_at)
            VALUES (?, true, ?, ?, ?)
        """, [krs, priority, last_checked, now])

    # priority_then_oldest: highest priority first, then oldest last_checked (NULLs first)
    results = scraper_db.get_krs_to_check("priority_then_oldest", 10, 24)
    krs_order = [r["krs"] for r in results]
    # priority 5 entries (0000000010, 0000000030) come before priority 1 (0000000020)
    assert krs_order.index("0000000010") < krs_order.index("0000000020")
    assert krs_order.index("0000000030") < krs_order.index("0000000020")
    # Among priority=5 entries, oldest last_checked comes first (0000000010 checked 10 days ago)
    assert krs_order.index("0000000010") < krs_order.index("0000000030")

    # oldest_first: NULL first
    results = scraper_db.get_krs_to_check("oldest_first", 10, 24)
    assert results[0]["krs"] == "0000000040"

    # sequential: ascending KRS
    results = scraper_db.get_krs_to_check("sequential", 10, 24)
    krs_nums = [r["krs"] for r in results]
    assert krs_nums == sorted(krs_nums)

    # newest_first: by first_seen_at DESC - all same here so just check it runs
    results = scraper_db.get_krs_to_check("newest_first", 10, 24)
    assert len(results) == 5

    # random: just verify it runs and returns correct count
    results = scraper_db.get_krs_to_check("random", 10, 24)
    assert len(results) == 5


def test_error_backoff():
    conn = scraper_db.get_conn()
    now = datetime.now(timezone.utc)
    # KRS with high error count, checked very recently
    conn.execute("""
        INSERT INTO krs_registry (krs, is_active, check_error_count, last_checked_at, first_seen_at)
        VALUES ('0000099999', true, 5, ?, ?)
    """, [now, now])

    results = scraper_db.get_krs_to_check("sequential", 100, 24)
    krs_list = [r["krs"] for r in results]
    assert "0000099999" not in krs_list


def test_run_lifecycle():
    import json
    scraper_db.create_run("run-001", "full_scan", json.dumps({"mode": "full_scan"}))

    conn = scraper_db.get_conn()
    row = conn.execute("SELECT status FROM scraper_runs WHERE run_id = 'run-001'").fetchone()
    assert row[0] == "running"

    scraper_db.finish_run("run-001", "completed", {
        "krs_checked": 10, "krs_new_found": 2,
        "documents_discovered": 5, "documents_downloaded": 5,
        "documents_failed": 0, "bytes_downloaded": 99999,
    })

    last = scraper_db.get_last_run()
    assert last is not None
    assert last["run_id"] == "run-001"
    assert last["status"] == "completed"
    assert last["krs_checked"] == 10
    assert last["documents_downloaded"] == 5


def test_stats():
    scraper_db.upsert_krs("0000111111", "Co A", None, True)
    scraper_db.upsert_krs("0000222222", "Co B", None, True)

    # Simulate checked state for 0000111111
    conn = scraper_db.get_conn()
    conn.execute("""
        UPDATE krs_registry SET last_checked_at = NOW(), total_documents = 3, total_downloaded = 2
        WHERE krs = '0000111111'
    """)

    stats = scraper_db.get_stats()
    assert stats["total_krs"] >= 2
    assert stats["krs_checked"] >= 1
    assert stats["krs_unchecked"] >= 1
    assert stats["total_documents"] >= 3
    assert stats["total_downloaded"] >= 2


def test_startup_guardrail_fails_fast_when_legacy_without_versions():
    """connect() should fail fast on legacy docs without append-only backfill."""
    conn = scraper_db.get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("DELETE FROM krs_document_versions")
    conn.execute("""
        INSERT INTO krs_documents (
            document_id, krs, rodzaj, status, discovered_at
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (document_id) DO NOTHING
    """, ["legacy-doc-guard", "0000099998", "18", "NIEUSUNIETY", now])

    scraper_db._schema_initialized = False
    with pytest.raises(RuntimeError, match="Cutover blocked: krs_document_versions is empty"):
        scraper_db.connect()
