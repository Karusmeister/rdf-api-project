"""Tests for batch/metadata_backfill.py — metadata backfill worker."""

import io
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx

from app.config import settings
from app.db.connection import make_connection
from batch.connections import Connection
from batch.metadata_backfill import (
    BackfillStats,
    _backfill_loop,
    _get_needs_metadata_batch,
)
from batch.rdf_document_store import RdfDocumentStore


_SAMPLE_DOC = {
    "id": "meta_test_doc==",
    "rodzaj": "SF",
    "status": "AKTUALNY",
    "nazwa": "Sprawozdanie finansowe 2023",
    "okresSprawozdawczyPoczatek": "2023-01-01",
    "okresSprawozdawczyKoniec": "2023-12-31",
}


def _make_sample_doc(doc_id: str) -> dict:
    return {**_SAMPLE_DOC, "id": doc_id}


def _make_zip(filename: str = "test.xml", content: bytes = b"<xml/>") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


def _insert_and_download(store: RdfDocumentStore, krs: str, doc_id: str) -> None:
    """Helper: insert a doc and mark it downloaded (no metadata)."""
    store.insert_documents(krs, [_make_sample_doc(doc_id)])
    store.mark_downloaded(
        doc_id,
        storage_path=f"krs/{krs}/{doc_id[:10]}",
        storage_backend="local",
        file_size=100,
        zip_size=50,
        file_count=1,
        file_types="xml",
    )


# ---------------------------------------------------------------------------
# BackfillStats
# ---------------------------------------------------------------------------

def test_backfill_stats_defaults():
    s = BackfillStats()
    assert s.total == 0
    assert s.success == 0
    assert s.failed == 0
    assert s.skipped == 0


def test_backfill_stats_log_does_not_raise(caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="batch.metadata_backfill"):
        s = BackfillStats()
        s.total = 10
        s.success = 8
        s.failed = 2
        s.log(worker_id=0)
    assert "metadata_backfill=0" in caplog.text


# ---------------------------------------------------------------------------
# _get_needs_metadata_batch — keyset pagination
# ---------------------------------------------------------------------------

def test_batch_fetch_returns_limited_page_size(pg_dsn, clean_pg):
    """Batch fetch returns at most batch_size rows."""
    store = RdfDocumentStore(pg_dsn)
    # Insert 5 docs, all downloaded, no metadata
    for i in range(5):
        _insert_and_download(store, "0000000002", f"batch_doc_{i:02d}==")

    results = _get_needs_metadata_batch(
        pg_dsn, worker_id=0, total_workers=1, batch_size=3,
    )
    assert len(results) == 3


def test_batch_fetch_keyset_advances_without_duplicates(pg_dsn, clean_pg):
    """Keyset pagination returns disjoint pages covering all docs."""
    store = RdfDocumentStore(pg_dsn)
    for i in range(7):
        _insert_and_download(store, "0000000002", f"keyset_doc_{i:02d}==")

    all_doc_ids = []
    after_krs = None
    after_doc_id = None

    for _ in range(10):  # safety limit
        batch = _get_needs_metadata_batch(
            pg_dsn, worker_id=0, total_workers=1, batch_size=3,
            after_krs=after_krs, after_doc_id=after_doc_id,
        )
        if not batch:
            break
        for doc_id, krs in batch:
            all_doc_ids.append(doc_id)
        after_doc_id, after_krs = batch[-1]

    # All 7 docs seen, no duplicates
    assert len(all_doc_ids) == 7
    assert len(set(all_doc_ids)) == 7


def test_batch_fetch_empty_when_none_qualify(pg_dsn, clean_pg):
    """Returns empty list when no docs match criteria."""
    results = _get_needs_metadata_batch(
        pg_dsn, worker_id=0, total_workers=1, batch_size=100,
    )
    assert results == []


def test_get_needs_metadata_returns_matching_docs(pg_dsn, clean_pg):
    """Only downloaded docs without metadata_fetched_at are returned."""
    store = RdfDocumentStore(pg_dsn)
    _insert_and_download(store, "0000000002", "meta_test_doc==")

    results = _get_needs_metadata_batch(
        pg_dsn, worker_id=0, total_workers=1, batch_size=100,
    )
    doc_ids = [r[0] for r in results]
    assert "meta_test_doc==" in doc_ids


def test_get_needs_metadata_excludes_already_backfilled(pg_dsn, clean_pg):
    """Docs with metadata already fetched are excluded."""
    store = RdfDocumentStore(pg_dsn)
    _insert_and_download(store, "0000000002", "meta_test_doc==")
    store.update_metadata("meta_test_doc==", {
        "nazwaPliku": "test.xml",
        "czyMSR": False,
        "czyKorekta": False,
    })

    results = _get_needs_metadata_batch(
        pg_dsn, worker_id=0, total_workers=1, batch_size=100,
    )
    doc_ids = [r[0] for r in results]
    assert "meta_test_doc==" not in doc_ids


def test_get_needs_metadata_excludes_not_downloaded(pg_dsn, clean_pg):
    """Docs that haven't been downloaded yet are excluded."""
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000002", [_SAMPLE_DOC])

    results = _get_needs_metadata_batch(
        pg_dsn, worker_id=0, total_workers=1, batch_size=100,
    )
    doc_ids = [r[0] for r in results]
    assert "meta_test_doc==" not in doc_ids


# ---------------------------------------------------------------------------
# _backfill_loop — end-to-end with mocked HTTP
# ---------------------------------------------------------------------------

@pytest.fixture
def _no_sleep(monkeypatch):
    async def _fake_sleep(_):
        pass
    monkeypatch.setattr("batch.metadata_backfill.asyncio.sleep", _fake_sleep)
    monkeypatch.setattr("batch.rdf_worker.asyncio.sleep", _fake_sleep)


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_backfill_loop_updates_metadata(pg_dsn, clean_pg):
    """Backfill loop fetches metadata and updates the document store."""
    store = RdfDocumentStore(pg_dsn)
    _insert_and_download(store, "0000000002", "meta_test_doc==")

    rdf_base = settings.rdf_base_url
    meta_response = {
        "nazwaPliku": "backfilled.xml",
        "czyMSR": True,
        "czyKorekta": False,
        "dataDodania": "2024-01-01",
    }

    conn = Connection(name="direct")
    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/meta_test_doc%3D%3D").mock(
            return_value=httpx.Response(200, json=meta_response)
        )

        await _backfill_loop(
            worker_id=0,
            total_workers=1,
            connection=conn,
            concurrency=5,
            delay=0,
            dsn=pg_dsn,
            batch_size=100,
        )

    # Verify metadata was written
    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT filename, is_ifrs, metadata_fetched_at FROM krs_document_versions WHERE document_id = %s AND is_current = true",
        ["meta_test_doc=="],
    ).fetchone()
    wrapper.close()
    assert row[0] == "backfilled.xml"
    assert row[1] is True
    assert row[2] is not None


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_backfill_loop_handles_empty(pg_dsn, clean_pg):
    """Backfill loop exits cleanly when no docs need metadata."""
    conn = Connection(name="direct")
    await _backfill_loop(
        worker_id=0,
        total_workers=1,
        connection=conn,
        concurrency=5,
        delay=0,
        dsn=pg_dsn,
        batch_size=100,
    )
    # No exception = success


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_backfill_loop_processes_multiple_batches(pg_dsn, clean_pg):
    """Backfill loop pages through multiple batches with batch_size=2."""
    store = RdfDocumentStore(pg_dsn)
    doc_ids = [f"multi_batch_{i:02d}==" for i in range(5)]
    for doc_id in doc_ids:
        _insert_and_download(store, "0000000002", doc_id)

    rdf_base = settings.rdf_base_url
    meta_response = {
        "nazwaPliku": "file.xml",
        "czyMSR": False,
        "czyKorekta": False,
    }

    conn = Connection(name="direct")
    with respx.mock:
        # Mock metadata endpoint for all doc IDs
        for doc_id in doc_ids:
            import urllib.parse
            encoded = urllib.parse.quote(doc_id, safe="")
            respx.get(f"{rdf_base}/dokumenty/{encoded}").mock(
                return_value=httpx.Response(200, json=meta_response)
            )

        await _backfill_loop(
            worker_id=0,
            total_workers=1,
            connection=conn,
            concurrency=5,
            delay=0,
            dsn=pg_dsn,
            batch_size=2,  # forces 3 batches: 2+2+1
        )

    # All 5 docs should have metadata now
    wrapper = make_connection(pg_dsn)
    for doc_id in doc_ids:
        row = wrapper.execute(
            "SELECT metadata_fetched_at FROM krs_document_versions WHERE document_id = %s AND is_current = true",
            [doc_id],
        ).fetchone()
        assert row[0] is not None, f"Doc {doc_id} should have metadata"
    wrapper.close()
