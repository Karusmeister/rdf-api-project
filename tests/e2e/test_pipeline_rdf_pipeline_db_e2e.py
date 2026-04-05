"""End-to-end test for the new rdf-pipeline path.

Downloads a real KRS financial statement via the live RDF API (same KRS as
``test_pipeline_e2e.py``), registers it in the scraper test database, then
runs ``pipeline.runner.run_pipeline()`` against the sibling ``rdf_test_pipeline``
database with BigQuery disabled. Verifies the full ETL → feature → scoring
path writes to the *pipeline* DB — not the legacy prediction_db.

Run with:   pytest tests/e2e/test_pipeline_rdf_pipeline_db_e2e.py --e2e -v -s
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app import rdf_client
from app.config import settings
from app.db import connection as db_conn
from app.db import pipeline_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage, make_doc_dir

pytestmark = pytest.mark.e2e

KRS = "0000694720"


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def _pipeline_dsn_from(pg_dsn: str) -> str:
    base, _ = pg_dsn.rsplit("/", 1)
    return f"{base}/rdf_test_pipeline"


@pytest.fixture(scope="module")
def dual_schemas(tmp_path_factory, pg_dsn, pg_schema_initialized):
    """Initialise both scraper and pipeline schemas for this e2e module."""
    tmp = tmp_path_factory.mktemp("pipeline_e2e")
    storage = LocalStorage(str(tmp / "documents"))

    pipeline_dsn = _pipeline_dsn_from(pg_dsn)

    # Create the sibling DB if missing.
    import psycopg2
    admin_base = pg_dsn.rsplit("/", 1)[0]
    try:
        admin = psycopg2.connect(f"{admin_base}/postgres")
    except Exception:
        admin = psycopg2.connect(pg_dsn)
    admin.autocommit = True
    cur = admin.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'rdf_test_pipeline'")
    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE rdf_test_pipeline")
    admin.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    pipeline_db.reset()

    with patch.object(settings, "database_url", pg_dsn), \
         patch.object(settings, "pipeline_database_url", pipeline_dsn):
        db_conn.connect()
        scraper_db._ensure_schema()
        pipeline_db.connect()

        # Truncate pipeline tables from previous runs
        pconn = pipeline_db.get_conn()
        for table in (
            "population_stats", "pipeline_queue", "pipeline_runs",
            "predictions", "prediction_runs", "model_registry",
            "computed_features", "feature_set_members", "feature_sets",
            "feature_definitions", "financial_line_items",
            "raw_financial_data", "financial_reports", "companies",
            "etl_attempts", "bankruptcy_events",
        ):
            try:
                pconn.execute(f"TRUNCATE TABLE {table} CASCADE")
            except Exception:
                pass

        yield {"storage": storage, "pg_dsn": pg_dsn, "pipeline_dsn": pipeline_dsn}

        db_conn.close()

    db_conn.reset()
    pipeline_db.close()
    pipeline_db.reset()
    scraper_db._schema_initialized = False


@pytest.fixture(scope="module")
def downloaded_doc(dual_schemas, event_loop):
    """Fetch + download a real financial statement and register it in the scraper DB."""
    storage = dual_schemas["storage"]

    async def _fetch():
        await rdf_client.start()
        try:
            search = await rdf_client.wyszukiwanie(KRS, page=0, page_size=50)
            stmts = [
                d for d in search["content"]
                if d["rodzaj"] == "18" and d["status"] == "NIEUSUNIETY"
            ]
            assert stmts, f"No statements for KRS {KRS}"
            stmts.sort(key=lambda d: d.get("okresSprawozdawczyKoniec", ""), reverse=True)

            chosen = None
            for s in stmts:
                meta = await rdf_client.metadata(s["id"])
                if not meta.get("czyMSR", False):
                    chosen = (s, meta)
                    break
            assert chosen is not None
            doc, meta = chosen

            zip_bytes = await rdf_client.download([doc["id"]])
            assert zip_bytes

            doc_dir = make_doc_dir(KRS, doc["id"])
            storage.save_extracted(doc_dir, zip_bytes, doc["id"])

            return {
                "document_id": doc["id"],
                "doc_dir": doc_dir,
                "period_end": doc.get("okresSprawozdawczyKoniec"),
                "period_start": doc.get("okresSprawozdawczyPoczatek"),
                "nazwa": meta.get("nazwaPliku"),
                "zip_size": len(zip_bytes),
            }
        finally:
            await rdf_client.stop()

    info = event_loop.run_until_complete(_fetch())

    # Register in the scraper test DB
    scraper_db.upsert_krs(KRS.zfill(10), None, None, True)
    scraper_db.insert_documents([{
        "document_id": info["document_id"],
        "krs": KRS.zfill(10),
        "rodzaj": "18",
        "status": "NIEUSUNIETY",
        "nazwa": info["nazwa"],
        "okres_start": info["period_start"],
        "okres_end": info["period_end"],
        "discovered_at": datetime.now(timezone.utc),
    }])
    scraper_db.mark_downloaded(
        document_id=info["document_id"],
        storage_path=info["doc_dir"],
        storage_backend="local",
        file_size=0,
        zip_size=info["zip_size"],
        file_count=1,
        file_types="xml",
    )
    return info


def _seed_pipeline_prereqs():
    from app.services.maczynska import COEFFICIENTS
    conn = pipeline_db.get_conn()
    # Minimal feature defs — acceptance parity; live XML may or may not
    # have matching tags, the pipeline handles missing values gracefully.
    for fid in ("x1_maczynska", "x2_maczynska", "x3_maczynska",
                "x4_maczynska", "x5_maczynska", "x6_maczynska"):
        conn.execute(
            """
            INSERT INTO feature_definitions
                (id, name, category, computation_logic, is_active)
            VALUES (%s, %s, 'maczynska', 'custom', TRUE)
            ON CONFLICT (id) DO NOTHING
            """,
            [fid, fid],
        )
    conn.execute(
        """
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, description,
             hyperparameters, is_active, is_baseline)
        VALUES ('maczynska_1994_v1', 'maczynska', 'discriminant', '1994_v1',
                'maczynska_6', 'e2e', %s, TRUE, TRUE)
        ON CONFLICT (name, version) DO NOTHING
        """,
        [json.dumps({"coefficients": COEFFICIENTS})],
    )


def test_pipeline_runner_ingests_real_rdf_document(downloaded_doc, dual_schemas):
    """Full real-data run of pipeline.runner.run_pipeline()."""
    _seed_pipeline_prereqs()

    from pipeline.queue import enqueue_krs
    from pipeline.runner import run_pipeline

    conn = pipeline_db.get_conn()
    enqueue_krs(
        conn, KRS.zfill(10),
        reason="e2e_manual",
        document_id=downloaded_doc["document_id"],
    )

    with patch.object(settings, "database_url", dual_schemas["pg_dsn"]), \
         patch.object(settings, "pipeline_database_url", dual_schemas["pipeline_dsn"]):
        metrics = run_pipeline(
            trigger="e2e",
            limit=5,
            skip_bq=True,
            engine="postgres",
            storage=dual_schemas["storage"],
        )

    assert metrics.status == "completed", f"errors: {metrics.errors}"
    assert metrics.etl_docs == 1
    assert metrics.etl_line_items > 0

    # Report persisted in pipeline DB
    row = conn.execute(
        """
        SELECT krs, ingestion_status, fiscal_year
        FROM financial_reports
        WHERE id = %s
        """,
        [downloaded_doc["document_id"]],
    ).fetchone()
    assert row is not None
    assert row[0] == KRS.zfill(10)
    assert row[1] == "completed"
    assert row[2] >= 2015  # any reasonable fiscal year

    # Line items present
    n = conn.execute(
        "SELECT count(*) FROM financial_line_items WHERE report_id = %s",
        [downloaded_doc["document_id"]],
    ).fetchone()[0]
    assert n > 0

    # Queue marked completed, metrics row saved
    q = conn.execute(
        "SELECT status FROM pipeline_queue WHERE document_id = %s",
        [downloaded_doc["document_id"]],
    ).fetchone()
    assert q[0] == "completed"

    run = conn.execute(
        "SELECT status, etl_docs_parsed FROM pipeline_runs WHERE run_id = %s",
        [metrics.run_id],
    ).fetchone()
    assert run[0] == "completed"
    assert run[1] == 1
