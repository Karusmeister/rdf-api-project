"""Tests for PR3: append-only document versioning.

Covers both app/scraper/db.py (shared connection) and batch/rdf_document_store.py
(short-lived connections).
"""
from datetime import datetime, timezone

import duckdb
import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.scraper import db as scraper_db
from app.repositories import krs_repo
from batch.rdf_document_store import RdfDocumentStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    db_conn.reset()
    scraper_db._schema_initialized = False
    krs_repo._schema_initialized = False
    with patch.object(settings, "scraper_db_path", db_path):
        db_conn.connect()
        scraper_db._ensure_schema()
        krs_repo._ensure_schema()
        yield db_path
        db_conn.close()
    db_conn.reset()
    scraper_db._schema_initialized = False
    krs_repo._schema_initialized = False


def _doc_versions(document_id: str) -> list[dict]:
    conn = db_conn.get_conn()
    rows = conn.execute(
        "SELECT * FROM krs_document_versions WHERE document_id = ? ORDER BY version_no",
        [document_id],
    ).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM krs_document_versions LIMIT 0").description]
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# app/scraper/db.py — shared connection path
# ---------------------------------------------------------------------------

class TestScraperDbAppendOnly:

    def test_insert_creates_version_1(self):
        scraper_db.insert_documents([{
            "document_id": "doc-001", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        versions = _doc_versions("doc-001")
        assert len(versions) == 1
        assert versions[0]["version_no"] == 1
        assert versions[0]["is_current"] is True
        assert versions[0]["change_reason"] == "discovery"

    def test_metadata_update_creates_version_2(self):
        scraper_db.insert_documents([{
            "document_id": "doc-002", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_metadata("doc-002", {
            "filename": "report.xml", "is_ifrs": False,
            "is_correction": False, "date_filed": "2025-06-01",
        })
        versions = _doc_versions("doc-002")
        assert len(versions) == 2
        assert versions[1]["filename"] == "report.xml"
        assert versions[1]["change_reason"] == "metadata_update"

    def test_download_creates_version_3(self):
        scraper_db.insert_documents([{
            "document_id": "doc-003", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_metadata("doc-003", {
            "filename": "report.xml",
        })
        scraper_db.mark_downloaded(
            "doc-003", "/tmp/report", "local", 1024, 512, 1, "xml",
        )
        versions = _doc_versions("doc-003")
        assert len(versions) == 3
        assert versions[2]["is_downloaded"] is True
        assert versions[2]["change_reason"] == "downloaded"

    def test_identical_update_no_new_version(self):
        scraper_db.insert_documents([{
            "document_id": "doc-004", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        # Re-insert the same document — should be no-op
        scraper_db.insert_documents([{
            "document_id": "doc-004", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        versions = _doc_versions("doc-004")
        assert len(versions) == 1

    def test_only_one_current_per_document(self):
        scraper_db.insert_documents([{
            "document_id": "doc-005", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_metadata("doc-005", {"filename": "a.xml"})
        scraper_db.mark_downloaded("doc-005", "/p", "local", 100, 50, 1, "xml")

        versions = _doc_versions("doc-005")
        current = [v for v in versions if v["is_current"]]
        assert len(current) == 1

    def test_error_creates_version(self):
        scraper_db.insert_documents([{
            "document_id": "doc-006", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_error("doc-006", "timeout")
        versions = _doc_versions("doc-006")
        assert len(versions) == 2
        assert versions[1]["download_error"] == "timeout"

    def test_read_from_current_view(self):
        scraper_db.insert_documents([{
            "document_id": "doc-007", "krs": "0000000001",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        ids = scraper_db.get_known_document_ids("0000000001")
        assert "doc-007" in ids

    def test_undownloaded_from_current_view(self):
        scraper_db.insert_documents([{
            "document_id": "doc-008", "krs": "0000000002",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        undownloaded = scraper_db.get_undownloaded_documents("0000000002")
        assert "doc-008" in undownloaded

        scraper_db.mark_downloaded("doc-008", "/p", "local", 100, 50, 1, "xml")
        undownloaded = scraper_db.get_undownloaded_documents("0000000002")
        assert "doc-008" not in undownloaded

    def test_error_then_download_clears_error_in_current_view(self):
        """Fix 2 regression: mark_downloaded must clear download_error."""
        scraper_db.insert_documents([{
            "document_id": "doc-009", "krs": "0000000003",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime.now(timezone.utc),
        }])
        scraper_db.update_document_error("doc-009", "timeout")

        # Verify error is set
        conn = db_conn.get_conn()
        row = conn.execute(
            "SELECT download_error FROM krs_documents_current WHERE document_id = 'doc-009'"
        ).fetchone()
        assert row[0] == "timeout"

        # Download should clear the error
        scraper_db.mark_downloaded("doc-009", "/p", "local", 100, 50, 1, "xml")
        row = conn.execute(
            "SELECT download_error, is_downloaded FROM krs_documents_current WHERE document_id = 'doc-009'"
        ).fetchone()
        assert row[0] is None, f"download_error should be NULL, got {row[0]}"
        assert row[1] is True


# ---------------------------------------------------------------------------
# batch/rdf_document_store.py — short-lived connection path
# ---------------------------------------------------------------------------

class TestBatchDocumentStoreAppendOnly:

    @pytest.fixture()
    def store(self, tmp_path):
        """Separate DB file for batch store (own short-lived connections)."""
        return RdfDocumentStore(str(tmp_path / "batch.duckdb"))

    def _versions(self, store, document_id):
        conn = duckdb.connect(store._db_path)
        rows = conn.execute(
            "SELECT * FROM krs_document_versions WHERE document_id = ? ORDER BY version_no",
            [document_id],
        ).fetchall()
        cols = [d[0] for d in conn.execute("SELECT * FROM krs_document_versions LIMIT 0").description]
        conn.close()
        return [dict(zip(cols, r)) for r in rows]

    def test_insert_creates_version(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-001", "rodzaj": "sprawozdanie", "status": "aktywny",
        }])
        versions = self._versions(store, "bdoc-001")
        assert len(versions) == 1
        assert versions[0]["is_current"] is True

    def test_metadata_creates_version(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-002", "rodzaj": "sprawozdanie", "status": "aktywny",
        }])
        store.update_metadata("bdoc-002", {
            "nazwaPliku": "report.xml", "czyMSR": False,
        })
        versions = self._versions(store, "bdoc-002")
        assert len(versions) == 2

    def test_download_creates_version(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-003", "rodzaj": "sprawozdanie", "status": "aktywny",
        }])
        store.mark_downloaded("bdoc-003", "/p", "local", 100, 50, 1, "xml")
        versions = self._versions(store, "bdoc-003")
        assert len(versions) == 2
        assert versions[1]["is_downloaded"] is True

    def test_identical_insert_no_new_version(self, store):
        doc = {"id": "bdoc-004", "rodzaj": "sprawozdanie", "status": "aktywny"}
        store.insert_documents("0000000010", [doc])
        store.insert_documents("0000000010", [doc])
        versions = self._versions(store, "bdoc-004")
        assert len(versions) == 1

    def test_error_creates_version(self, store):
        store.insert_documents("0000000010", [{
            "id": "bdoc-005", "rodzaj": "sprawozdanie", "status": "aktywny",
        }])
        store.update_error("bdoc-005", "connection reset")
        versions = self._versions(store, "bdoc-005")
        assert len(versions) == 2
        assert versions[1]["download_error"] == "connection reset"

    def test_full_lifecycle_version_count(self, store):
        """discovery -> metadata -> download = 3 versions."""
        store.insert_documents("0000000010", [{
            "id": "bdoc-006", "rodzaj": "sprawozdanie", "status": "aktywny",
        }])
        store.update_metadata("bdoc-006", {"nazwaPliku": "r.xml"})
        store.mark_downloaded("bdoc-006", "/p", "local", 100, 50, 1, "xml")

        versions = self._versions(store, "bdoc-006")
        assert len(versions) == 3
        current = [v for v in versions if v["is_current"]]
        assert len(current) == 1
        assert current[0]["version_no"] == 3
