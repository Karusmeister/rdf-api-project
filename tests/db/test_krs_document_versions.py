"""Tests for the split-document storage contract.

File keeps its historical name for git-history stability. The subject is
now the post-dedupe krs_documents + krs_document_downloads pair; the
append-only version history was removed in SCHEMA_DEDUPE_PLAN #1.
"""
from datetime import datetime, timezone

import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.db.connection import make_connection
from app.scraper import db as scraper_db
from app.repositories import krs_repo
from batch.rdf_document_store import RdfDocumentStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_db(pg_dsn, clean_pg):
    db_conn.reset()
    scraper_db._schema_initialized = False
    krs_repo._schema_initialized = False
    with patch.object(settings, "database_url", pg_dsn):
        db_conn.connect()
        scraper_db._ensure_schema()
        krs_repo._ensure_schema()
        yield pg_dsn
        db_conn.close()
    db_conn.reset()
    scraper_db._schema_initialized = False
    krs_repo._schema_initialized = False


def _view_row(document_id: str) -> dict | None:
    conn = db_conn.get_conn()
    cur = conn.execute(
        "SELECT * FROM krs_documents_current WHERE document_id = %s", [document_id]
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _document_count(document_id: str) -> int:
    conn = db_conn.get_conn()
    return conn.execute(
        "SELECT count(*) FROM krs_documents WHERE document_id = %s", [document_id]
    ).fetchone()[0]


# ---------------------------------------------------------------------------
# app/scraper/db.py — shared-connection path
# ---------------------------------------------------------------------------

class TestScraperDbDocuments:

    def test_insert_creates_document_and_downloads_row(self):
        scraper_db.insert_documents([{
            "document_id": "doc-001", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        row = _view_row("doc-001")
        assert row is not None
        assert row["rodzaj"] == 18
        assert row["is_deleted"] is False
        assert row["is_downloaded"] is False

    def test_metadata_update_fills_columns(self):
        scraper_db.insert_documents([{
            "document_id": "doc-002", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_metadata("doc-002", {
            "filename": "report.xml", "is_ifrs": False,
            "is_correction": False, "date_filed": "2025-06-01",
        })
        row = _view_row("doc-002")
        assert row["filename"] == "report.xml"
        assert row["is_ifrs"] is False
        assert str(row["date_filed"]) == "2025-06-01"
        assert row["metadata_fetched_at"] is not None

    def test_download_sets_is_downloaded_and_file_type(self):
        scraper_db.insert_documents([{
            "document_id": "doc-003", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.mark_downloaded(
            "doc-003", "/tmp/report", "local", 1024, 512, 1, "xml",
        )
        row = _view_row("doc-003")
        assert row["is_downloaded"] is True
        assert row["storage_path"] == "/tmp/report"
        assert row["file_type"] == "xml"

    def test_duplicate_insert_is_idempotent(self):
        doc = {
            "document_id": "doc-004", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }
        scraper_db.insert_documents([doc])
        scraper_db.insert_documents([doc])
        assert _document_count("doc-004") == 1

    def test_full_lifecycle_leaves_single_row(self):
        scraper_db.insert_documents([{
            "document_id": "doc-005", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_metadata("doc-005", {"filename": "a.xml"})
        scraper_db.mark_downloaded("doc-005", "/p", "local", 100, 50, 1, "xml")

        assert _document_count("doc-005") == 1
        row = _view_row("doc-005")
        assert row["is_downloaded"] is True
        assert row["filename"] == "a.xml"

    def test_update_error_is_recorded_on_downloads_row(self):
        scraper_db.insert_documents([{
            "document_id": "doc-006", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_error("doc-006", "timeout")
        row = _view_row("doc-006")
        assert row["download_error"] == "timeout"
        # Document row itself is unchanged — only the downloads row moved.
        assert _document_count("doc-006") == 1

    def test_read_from_view(self):
        scraper_db.insert_documents([{
            "document_id": "doc-007", "krs": "0000000001",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        ids = scraper_db.get_known_document_ids("0000000001")
        assert "doc-007" in ids

    def test_undownloaded_lifecycle(self):
        scraper_db.insert_documents([{
            "document_id": "doc-008", "krs": "0000000002",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        assert "doc-008" in scraper_db.get_undownloaded_documents("0000000002")

        scraper_db.mark_downloaded("doc-008", "/p", "local", 100, 50, 1, "xml")
        assert "doc-008" not in scraper_db.get_undownloaded_documents("0000000002")

    def test_mark_downloaded_clears_prior_error(self):
        scraper_db.insert_documents([{
            "document_id": "doc-009", "krs": "0000000003",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_error("doc-009", "timeout")
        assert _view_row("doc-009")["download_error"] == "timeout"

        scraper_db.mark_downloaded("doc-009", "/p", "local", 100, 50, 1, "xml")
        row = _view_row("doc-009")
        assert row["download_error"] is None
        assert row["is_downloaded"] is True


# ---------------------------------------------------------------------------
# batch/rdf_document_store.py — short-lived-connection path
# ---------------------------------------------------------------------------

class TestBatchDocumentStore:

    @pytest.fixture()
    def store(self, pg_dsn):
        return RdfDocumentStore(pg_dsn)

    def _view(self, store, document_id):
        conn = make_connection(store._dsn)
        try:
            cur = conn.execute(
                "SELECT * FROM krs_documents_current WHERE document_id = %s",
                [document_id],
            )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        finally:
            conn.close()

    def test_insert_creates_row(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-001", "rodzaj": "18", "status": "NIEUSUNIETY",
        }])
        row = self._view(store, "bdoc-001")
        assert row is not None
        assert row["rodzaj"] == 18
        assert row["is_downloaded"] is False

    def test_metadata_update(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-002", "rodzaj": "18", "status": "NIEUSUNIETY",
        }])
        store.update_metadata("bdoc-002", {
            "nazwaPliku": "report.xml", "czyMSR": False,
        })
        row = self._view(store, "bdoc-002")
        assert row["filename"] == "report.xml"
        assert row["is_ifrs"] is False

    def test_download_marks_downloaded(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-003", "rodzaj": "18", "status": "NIEUSUNIETY",
        }])
        store.mark_downloaded("bdoc-003", "/p", "local", 100, 50, 1, "xml")
        row = self._view(store, "bdoc-003")
        assert row["is_downloaded"] is True
        assert row["file_type"] == "xml"

    def test_duplicate_insert_stays_single_row(self, store):
        doc = {"id": "bdoc-004", "rodzaj": "18", "status": "NIEUSUNIETY"}
        store.insert_documents("0000000010", [doc])
        store.insert_documents("0000000010", [doc])
        assert self._view(store, "bdoc-004") is not None

    def test_update_error_on_downloads_row(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-005", "rodzaj": "18", "status": "NIEUSUNIETY",
        }])
        store.update_error("bdoc-005", "connection reset")
        row = self._view(store, "bdoc-005")
        assert row["download_error"] == "connection reset"

    def test_full_lifecycle_final_state(self, store):
        """discovery -> metadata -> download collapses to one row."""
        store.insert_documents("0000000010", [{
            "id": "bdoc-006", "rodzaj": "18", "status": "NIEUSUNIETY",
        }])
        store.update_metadata("bdoc-006", {"nazwaPliku": "r.xml"})
        store.mark_downloaded("bdoc-006", "/p", "local", 100, 50, 1, "xml")

        row = self._view(store, "bdoc-006")
        assert row["is_downloaded"] is True
        assert row["filename"] == "r.xml"
