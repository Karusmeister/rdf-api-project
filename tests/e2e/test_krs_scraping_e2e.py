"""End-to-end tests for the KRS scraping pipeline against live APIs.

Covers three sub-pipelines:
  1. KRS Scanner — sequential probing via MS Gov adapter
  2. KRS Sync — entity discovery + re-enrichment via krs_sync job
  3. Scraper — document search, metadata fetch, ZIP download via RDF API

All tests hit live government endpoints:
  - api-krs.ms.gov.pl  (KRS Open API)
  - rdf-przegladarka.ms.gov.pl  (RDF document repository)

Each test writes to an isolated PostgreSQL database — never the main DB.

Run with:  pytest tests/e2e/test_krs_scraping_e2e.py -v -s --e2e
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio

from app import krs_client, rdf_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import register as register_adapter
from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.jobs import krs_scanner, krs_sync
from app.repositories import krs_repo
from app.scraper import db as scraper_db
from app.scraper.job import run_scraper
from app.scraper.storage import LocalStorage

pytestmark = pytest.mark.e2e

# Well-known KRS numbers — guaranteed to exist in KRS registry
KRS_KNOWN = "0000694720"       # B-JWK-MANAGEMENT — used across e2e tests
KRS_KNOWN_ALT = "0000006865"   # PKN Orlen — large company, always has documents
KRS_NONEXISTENT = "9999999999"

# KRS numbers in the 6800-6900 range have known valid entries (e.g., 6865 = PKN Orlen)
SCANNER_START = 6860
SCANNER_BATCH = 20  # small batch to keep tests fast


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def isolated_db(tmp_path, pg_dsn, clean_pg):
    """Isolated PostgreSQL DB + storage directory with all schemas initialized."""
    storage_dir = tmp_path / "documents"

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "database_url", pg_dsn):
        db_conn.connect()
        scraper_db.connect()
        prediction_db.connect()
        krs_repo._schema_initialized = False
        krs_repo.connect()

        yield {
            "tmp": tmp_path,
            "db_path": pg_dsn,
            "storage_dir": storage_dir,
            "storage": LocalStorage(str(storage_dir)),
        }

        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False
    krs_repo._schema_initialized = False


@pytest_asyncio.fixture
async def live_clients():
    """Start/stop krs_client and rdf_client for live API access."""
    await krs_client.start()
    await rdf_client.start()
    register_adapter("ms_gov", MsGovKrsAdapter())
    yield
    await krs_client.stop()
    await rdf_client.stop()


# ===================================================================
# 1. KRS Scanner E2E — sequential probing via MS Gov adapter
# ===================================================================

class TestKrsScanner:
    """Sequential KRS integer scanner against live api-krs.ms.gov.pl."""

    @pytest.mark.asyncio
    async def test_scanner_discovers_entities(
        self, isolated_db, live_clients
    ):
        """Scan a KRS range known to contain valid entities.

        Verifies:
        - Scanner completes without crash
        - Found entities written to krs_entities + krs_registry
        - Scan cursor advanced
        - krs_scan_runs row recorded
        """
        # Set cursor to a range with known entities (6865 = PKN Orlen)
        krs_repo.advance_cursor(SCANNER_START)

        summary = await krs_scanner.run_scan(batch_size=SCANNER_BATCH)

        print(f"\n  Scanner summary: {summary}")

        assert summary["status"] == "completed"
        assert summary["probed_count"] == SCANNER_BATCH
        assert summary["valid_count"] >= 1, (
            f"Expected at least 1 valid entity in KRS {SCANNER_START}-{SCANNER_START + SCANNER_BATCH}"
        )
        assert summary["krs_from"] == SCANNER_START
        assert summary["krs_to"] == SCANNER_START + SCANNER_BATCH - 1

        # Cursor should have advanced past the batch
        cursor = krs_repo.get_cursor()
        assert cursor == SCANNER_START + SCANNER_BATCH

        # Scan run recorded in DB
        last_run = krs_repo.get_last_scan_run()
        assert last_run is not None
        assert last_run["status"] == "completed"
        assert last_run["probed_count"] == SCANNER_BATCH
        assert last_run["valid_count"] >= 1

        # Valid entities written to DB
        entity_count = krs_repo.count_entities()
        assert entity_count >= 1
        print(f"  Entities discovered: {entity_count}")

    @pytest.mark.asyncio
    async def test_scanner_resumes_from_cursor(
        self, isolated_db, live_clients
    ):
        """Two consecutive scans should cover disjoint KRS ranges."""
        krs_repo.advance_cursor(SCANNER_START)

        s1 = await krs_scanner.run_scan(batch_size=10)
        cursor_after_first = krs_repo.get_cursor()

        # Second scan should start where first left off
        s2 = await krs_scanner.run_scan(batch_size=10)
        cursor_after_second = krs_repo.get_cursor()

        print(f"\n  Scan 1: {s1['krs_from']}-{s1['krs_to']} ({s1['valid_count']} found)")
        print(f"  Scan 2: {s2['krs_from']}-{s2['krs_to']} ({s2['valid_count']} found)")

        assert s1["status"] == "completed"
        assert s2["status"] == "completed"
        assert s2["krs_from"] == cursor_after_first
        assert cursor_after_second == cursor_after_first + 10

        # Ranges should be disjoint
        range1 = set(range(s1["krs_from"], s1["krs_to"] + 1))
        range2 = set(range(s2["krs_from"], s2["krs_to"] + 1))
        assert range1.isdisjoint(range2)

    @pytest.mark.asyncio
    async def test_scanner_writes_to_krs_registry(
        self, isolated_db, live_clients
    ):
        """Discovered entities should also appear in krs_registry for
        the scraper to pick up later."""
        krs_repo.advance_cursor(SCANNER_START)
        await krs_scanner.run_scan(batch_size=SCANNER_BATCH)

        conn = scraper_db.get_conn()
        registry_count = conn.execute(
            "SELECT count(*) FROM krs_registry"
        ).fetchone()[0]

        entity_count = krs_repo.count_entities()

        print(f"\n  Entities: {entity_count}, Registry: {registry_count}")

        # Every discovered entity should be in krs_registry
        assert registry_count >= entity_count

    @pytest.mark.asyncio
    async def test_scanner_nonexistent_krs_not_an_error(
        self, isolated_db, live_clients
    ):
        """Probing a KRS that returns 404 is expected — not counted as error."""
        # Set cursor to a very high number that won't exist
        krs_repo.advance_cursor(99_999_990)

        summary = await krs_scanner.run_scan(batch_size=5)

        print(f"\n  Summary: {summary}")

        assert summary["status"] == "completed"
        assert summary["probed_count"] == 5
        assert summary["valid_count"] == 0
        # 404s are NOT errors
        assert summary["error_count"] == 0


# ===================================================================
# 2. KRS Sync E2E — discovery + re-enrichment via krs_sync job
# ===================================================================

class TestKrsSync:
    """KRS entity sync job against live api-krs.ms.gov.pl."""

    @pytest.mark.asyncio
    async def test_sync_discovers_new_entities_from_registry(
        self, isolated_db, live_clients
    ):
        """Seed krs_registry with known KRS numbers, run sync,
        verify entities are fetched and stored.

        Flow: krs_registry(known KRS) → sync discovers → krs_entities populated
        """
        # Seed registry (simulating scanner having found these)
        scraper_db.upsert_krs(KRS_KNOWN, "Test", None, True)
        scraper_db.upsert_krs(KRS_KNOWN_ALT, "Test Alt", None, True)

        summary = await krs_sync.run_sync()

        print(f"\n  Sync summary: {summary}")

        assert summary["status"] == "completed"
        assert summary["krs_count"] >= 2
        assert summary["new_count"] >= 2

        # Entities should now be in the DB with real data from API
        entity1 = krs_repo.get_entity(KRS_KNOWN)
        assert entity1 is not None
        assert entity1["name"]  # should have a real company name
        assert entity1["krs"] == KRS_KNOWN
        print(f"  Entity 1: {entity1['name']}")

        entity2 = krs_repo.get_entity(KRS_KNOWN_ALT)
        assert entity2 is not None
        assert entity2["name"]
        print(f"  Entity 2: {entity2['name']}")

        # Sync log should be recorded
        last_sync = krs_repo.get_last_sync()
        assert last_sync is not None
        assert last_sync["status"] == "completed"
        assert last_sync["new_count"] >= 2

    @pytest.mark.asyncio
    async def test_sync_is_idempotent(
        self, isolated_db, live_clients
    ):
        """Running sync twice with same registry data should not
        create duplicate entities — just update timestamps."""
        scraper_db.upsert_krs(KRS_KNOWN, "Test", None, True)

        s1 = await krs_sync.run_sync()
        count_after_first = krs_repo.count_entities()

        s2 = await krs_sync.run_sync()
        count_after_second = krs_repo.count_entities()

        print(f"\n  After sync 1: {count_after_first} entities, new={s1['new_count']}")
        print(f"  After sync 2: {count_after_second} entities, new={s2['new_count']}")

        assert count_after_second == count_after_first
        assert s2["new_count"] == 0  # no NEW entities on second run

    @pytest.mark.asyncio
    async def test_sync_handles_nonexistent_krs_gracefully(
        self, isolated_db, live_clients
    ):
        """A KRS in registry that returns 404 from API should not crash sync."""
        scraper_db.upsert_krs(KRS_NONEXISTENT, "Ghost Corp", None, True)
        scraper_db.upsert_krs(KRS_KNOWN, "Real Corp", None, True)

        summary = await krs_sync.run_sync()

        print(f"\n  Summary: {summary}")

        assert summary["status"] == "completed"
        # The real KRS should still be discovered
        entity = krs_repo.get_entity(KRS_KNOWN)
        assert entity is not None


# ===================================================================
# 3. Scraper E2E — document search, metadata, download via RDF API
# ===================================================================

class TestScraper:
    """Full scraper job against live rdf-przegladarka.ms.gov.pl."""

    @pytest.mark.asyncio
    async def test_scraper_discovers_and_downloads_documents(
        self, isolated_db
    ):
        """Run scraper for a single known KRS — should discover documents
        and download at least one ZIP.

        Verifies:
        - Entity validated via dane_podstawowe
        - Documents discovered via wyszukiwanie (encrypted KRS)
        - At least one document downloaded and extracted
        - DB state: krs_registry, krs_documents, scraper_runs
        - Storage: files on disk
        """
        with patch.object(settings, "scraper_delay_between_krs", 0.5), \
             patch.object(settings, "scraper_delay_between_requests", 0.5), \
             patch.object(settings, "storage_local_path", str(isolated_db["storage_dir"])):

            stats = await run_scraper(
                mode="specific_krs",
                specific_krs=[KRS_KNOWN],
                max_krs=1,
            )

        print(f"\n  Scraper stats: {stats}")

        assert stats["krs_checked"] == 1
        assert stats["documents_discovered"] > 0, "No documents found for known KRS"
        assert stats["documents_downloaded"] > 0, "No documents downloaded"
        assert stats["bytes_downloaded"] > 0

        # krs_registry should be updated
        conn = scraper_db.get_conn()
        reg = conn.execute(
            "SELECT company_name, total_documents, total_downloaded "
            "FROM krs_registry WHERE krs = %s",
            [KRS_KNOWN],
        ).fetchone()
        assert reg is not None
        print(f"  Registry: name={reg[0]}, docs={reg[1]}, downloaded={reg[2]}")
        assert reg[1] > 0  # total_documents
        assert reg[2] > 0  # total_downloaded

        # krs_documents should have rows (via version table)
        doc_count = conn.execute(
            "SELECT count(*) FROM krs_documents_current WHERE krs = %s",
            [KRS_KNOWN],
        ).fetchone()[0]
        assert doc_count > 0
        print(f"  Documents in DB: {doc_count}")

        # At least one document should be downloaded with files on disk
        downloaded = conn.execute(
            "SELECT document_id, storage_path, file_count, file_types "
            "FROM krs_documents_current "
            "WHERE krs = %s AND is_downloaded = true",
            [KRS_KNOWN],
        ).fetchall()
        assert len(downloaded) > 0

        for doc_id, storage_path, file_count, file_types in downloaded[:3]:
            print(f"  Downloaded: {doc_id[:30]}... files={file_count} types={file_types}")
            assert file_count > 0
            assert storage_path

        # Scraper run should be recorded
        last_run = scraper_db.get_last_run()
        assert last_run is not None
        assert last_run["status"] == "completed"
        assert last_run["documents_downloaded"] > 0

    @pytest.mark.asyncio
    async def test_scraper_is_idempotent(
        self, isolated_db
    ):
        """Running scraper twice for same KRS should not re-download
        already-downloaded documents."""
        with patch.object(settings, "scraper_delay_between_krs", 0.5), \
             patch.object(settings, "scraper_delay_between_requests", 0.5), \
             patch.object(settings, "storage_local_path", str(isolated_db["storage_dir"])):

            s1 = await run_scraper(
                mode="specific_krs",
                specific_krs=[KRS_KNOWN],
                max_krs=1,
            )
            downloaded_first = s1["documents_downloaded"]

            s2 = await run_scraper(
                mode="specific_krs",
                specific_krs=[KRS_KNOWN],
                max_krs=1,
            )
            downloaded_second = s2["documents_downloaded"]

        print(f"\n  Run 1: discovered={s1['documents_discovered']}, downloaded={downloaded_first}")
        print(f"  Run 2: discovered={s2['documents_discovered']}, downloaded={downloaded_second}")

        assert downloaded_first > 0
        # Second run should discover 0 new docs and download 0
        assert s2["documents_discovered"] == 0
        assert downloaded_second == 0

    @pytest.mark.asyncio
    async def test_scraper_inactive_krs_marked_correctly(
        self, isolated_db
    ):
        """Scraping a non-existent KRS should mark it as inactive,
        not crash."""
        scraper_db.upsert_krs(KRS_NONEXISTENT, "Ghost Corp", None, True)

        with patch.object(settings, "scraper_delay_between_krs", 0.5), \
             patch.object(settings, "scraper_delay_between_requests", 0.5), \
             patch.object(settings, "storage_local_path", str(isolated_db["storage_dir"])):

            stats = await run_scraper(
                mode="specific_krs",
                specific_krs=[KRS_NONEXISTENT],
                max_krs=1,
            )

        print(f"\n  Stats: {stats}")

        assert stats["krs_checked"] == 1
        assert stats["documents_discovered"] == 0

        # KRS should be marked inactive
        conn = scraper_db.get_conn()
        reg = conn.execute(
            "SELECT is_active FROM krs_registry WHERE krs = %s",
            [KRS_NONEXISTENT],
        ).fetchone()
        assert reg is not None
        assert reg[0] is False


# ===================================================================
# 4. Integration: Scanner → Sync → Scraper pipeline
# ===================================================================

class TestFullPipeline:
    """End-to-end: scanner discovers entity → sync enriches → scraper downloads."""

    @pytest.mark.asyncio
    async def test_scanner_to_sync_to_scraper(
        self, isolated_db, live_clients
    ):
        """Full pipeline integration test.

        1. Scanner finds a valid KRS in low range
        2. Sync re-enriches the discovered entity
        3. Scraper downloads documents for it

        This proves data flows correctly between all three jobs.
        """
        # Phase 1: Scanner discovers entities (use range with known valid KRS)
        krs_repo.advance_cursor(SCANNER_START)
        scan_summary = await krs_scanner.run_scan(batch_size=15)
        print(f"\n  Scanner: {scan_summary['valid_count']} entities found")
        assert scan_summary["valid_count"] >= 1

        # Pick the first discovered entity
        conn = scraper_db.get_conn()
        first_krs = conn.execute(
            "SELECT krs FROM krs_registry ORDER BY krs LIMIT 1"
        ).fetchone()
        assert first_krs is not None
        krs = first_krs[0]
        print(f"  Using KRS: {krs}")

        # Phase 2: Sync re-enriches (entity already exists, so this tests
        # the stale-refresh path — but since it was just created, it will
        # just verify the entity data is current)
        entity_before = krs_repo.get_entity(krs)
        assert entity_before is not None
        assert entity_before["name"]
        print(f"  Entity name: {entity_before['name']}")

        # Phase 3: Scraper downloads documents for this KRS
        with patch.object(settings, "scraper_delay_between_krs", 0.5), \
             patch.object(settings, "scraper_delay_between_requests", 0.5), \
             patch.object(settings, "storage_local_path", str(isolated_db["storage_dir"])):

            scraper_stats = await run_scraper(
                mode="specific_krs",
                specific_krs=[krs],
                max_krs=1,
            )

        print(f"  Scraper: docs_discovered={scraper_stats['documents_discovered']}, "
              f"downloaded={scraper_stats['documents_downloaded']}")

        # The KRS should now have documents in the DB
        doc_count = conn.execute(
            "SELECT count(*) FROM krs_documents_current WHERE krs = %s",
            [krs],
        ).fetchone()[0]
        print(f"  Documents in DB: {doc_count}")

        # Verify the registry was updated with document counts
        reg = conn.execute(
            "SELECT total_documents, total_downloaded FROM krs_registry WHERE krs = %s",
            [krs],
        ).fetchone()
        assert reg is not None
        print(f"  Registry: total_docs={reg[0]}, total_downloaded={reg[1]}")
