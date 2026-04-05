"""Tests for pipeline.queue — verifies cross-database queue semantics."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from app.db import pipeline_db
from app.db.connection import make_connection
from pipeline.queue import (
    enqueue_changed_since,
    enqueue_krs,
    get_queue_stats,
    claim_pending,
    mark_completed,
    mark_failed,
)


def test_enqueue_krs_is_idempotent(dual_db):
    conn = pipeline_db.get_conn()
    enqueue_krs(conn, "0000000001", reason="new_document", document_id="doc1")
    enqueue_krs(conn, "0000000001", reason="new_document", document_id="doc1")
    row = conn.execute(
        "SELECT count(*) FROM pipeline_queue WHERE krs = %s", ["0000000001"]
    ).fetchone()
    assert row[0] == 1


def test_pipeline_queue_only_on_pipeline_db(dual_db):
    """Scraper DB must NOT have a pipeline_queue table."""
    scraper = make_connection(dual_db["pg_dsn"])
    row = scraper.execute(
        """
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'pipeline_queue'
        """
    ).fetchone()
    assert row[0] == 0

    pipeline = pipeline_db.get_conn()
    row = pipeline.execute(
        """
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'pipeline_queue'
        """
    ).fetchone()
    assert row[0] == 1


def test_enqueue_changed_since_reads_scraper_writes_pipeline(dual_db):
    scraper = make_connection(dual_db["pg_dsn"])
    # Ensure scraper schema has the needed tables
    scraper.execute(
        """
        INSERT INTO krs_registry (krs) VALUES ('0000000001')
        ON CONFLICT DO NOTHING
        """
    )
    # krs_document_versions uses a sequence for version_id; let defaults apply
    scraper.execute(
        """
        INSERT INTO krs_document_versions
            (document_id, krs, version_no, is_current, created_at)
        VALUES ('doc-A', '0000000001', 1, true, now())
        """
    )

    pipeline = pipeline_db.get_conn()
    count = enqueue_changed_since(
        scraper, pipeline, since=datetime.now(timezone.utc) - timedelta(hours=1)
    )
    assert count >= 1

    row = pipeline.execute(
        "SELECT krs, document_id FROM pipeline_queue WHERE document_id = 'doc-A'"
    ).fetchone()
    assert row is not None
    assert row[0] == "0000000001"


def test_claim_pending_and_mark_completed(dual_db):
    conn = pipeline_db.get_conn()
    enqueue_krs(conn, "0000000002", reason="manual", document_id="doc-c1")
    enqueue_krs(conn, "0000000003", reason="manual", document_id="doc-c2")

    claimed = claim_pending(conn, run_id=99, limit=10)
    assert len(claimed) == 2

    mark_completed(conn, run_id=99)
    stats = get_queue_stats(conn)
    assert stats["pending"] == 0
    assert stats["completed"] == 2


def test_mark_failed(dual_db):
    conn = pipeline_db.get_conn()
    enqueue_krs(conn, "0000000004", reason="manual", document_id="doc-f1")
    claim_pending(conn, run_id=7, limit=10)
    mark_failed(conn, "0000000004", "doc-f1", run_id=7, error="boom")
    row = conn.execute(
        "SELECT status, error_message FROM pipeline_queue WHERE krs = '0000000004'"
    ).fetchone()
    assert row[0] == "failed"
    assert row[1] == "boom"
