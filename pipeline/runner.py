"""Pipeline runner — orchestrates the full daily pipeline.

Steps:
    1. Detect changed KRS  (read scraper DB → write pipeline_queue)
    2. Claim pending queue items
    3. ETL: parse XML → write to pipeline DB (batch COPY)
    4. Features: compute in PostgreSQL
    5. Scoring: run all active models
    6. Sync to BigQuery  (skippable via --skip-bq)
    7. Population stats  (BigQuery → back to PG)
    8. Mark queue items completed
    9. Write pipeline_runs row with metrics
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.db.connection import ConnectionWrapper
from pipeline.db import DualConns, open_dual_connections
from pipeline.etl_batch import ingest_batch
from pipeline.feature_compute import compute_features_for_reports
from pipeline.metrics import PipelineMetrics, create_pipeline_run
from pipeline.queue import (
    claim_pending,
    enqueue_changed_since,
    mark_completed,
    mark_failed,
)
from pipeline.scoring import score_reports

logger = logging.getLogger(__name__)


def _ensure_schema(pipeline_conn: ConnectionWrapper) -> None:
    """Ensure pipeline schema exists on the supplied connection.

    In production the schema is created by `pipeline_db.connect()` at API
    startup; in a standalone Cloud Run Job we need to handle the case where
    this is the first time the DB is touched.
    """
    # Re-use the DDL from app.db.pipeline_db without switching its module-
    # level connection: we temporarily bind the provided conn.
    from app.db import pipeline_db as pdb
    if pdb._conn is None or pdb._conn.closed:
        pdb._conn = pipeline_conn
        pdb._schema_initialized = False
    pdb._ensure_schema()


def _discover_changes(
    conns: DualConns,
    since: Optional[datetime] = None,
) -> int:
    """Enqueue documents downloaded since `since` (default: last 48h)."""
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(hours=48)
    return enqueue_changed_since(conns.scraper, conns.pipeline, since)


def run_pipeline(
    trigger: str = "scheduled",
    limit: Optional[int] = None,
    skip_bq: bool = False,
    engine: str = "postgres",
    since: Optional[datetime] = None,
    storage=None,
) -> PipelineMetrics:
    """Execute the full pipeline once. Returns a populated PipelineMetrics."""
    t_start = time.monotonic()

    with open_dual_connections() as conns:
        _ensure_schema(conns.pipeline)

        run_id = create_pipeline_run(conns.pipeline, trigger)
        metrics = PipelineMetrics(run_id=run_id)

        try:
            # 1. Discover newly downloaded documents
            queued = _discover_changes(conns, since=since)
            metrics.krs_queued = queued

            # 2. Claim pending queue items
            claimed = claim_pending(conns.pipeline, run_id=run_id, limit=limit)

            if not claimed:
                logger.info("pipeline_no_work", extra={"event": "pipeline_no_work"})
                metrics.status = "completed"
                metrics.total_seconds = round(time.monotonic() - t_start, 3)
                metrics.save(conns.pipeline)
                return metrics

            items = [(c["krs"], c["document_id"]) for c in claimed if c["document_id"]]

            # 3. ETL
            t_etl = time.monotonic()
            try:
                etl_result = ingest_batch(items, conns.scraper, conns.pipeline, storage=storage)
                metrics.etl_docs = etl_result.docs_parsed
                metrics.etl_line_items = etl_result.line_items_written
                metrics.krs_processed = etl_result.docs_parsed
                metrics.krs_failed = etl_result.docs_failed
                for err in etl_result.errors:
                    metrics.errors.append(f"etl:{err.get('document_id')}={err.get('error')}")
            finally:
                metrics.etl_seconds = round(time.monotonic() - t_etl, 3)
            report_ids = list(etl_result.report_ids)

            # 4. Features
            t_feat = time.monotonic()
            try:
                feat_result = compute_features_for_reports(conns.pipeline, report_ids)
                metrics.features_computed = feat_result["computed"]
                metrics.features_failed = feat_result["failed"]
            finally:
                metrics.features_seconds = round(time.monotonic() - t_feat, 3)

            # 5. Scoring
            t_score = time.monotonic()
            try:
                score_result = score_reports(conns.pipeline, report_ids)
                metrics.predictions_written = score_result["predictions_written"]
            finally:
                metrics.predictions_seconds = round(time.monotonic() - t_score, 3)

            # 6. BigQuery sync
            if not skip_bq and engine == "bigquery":
                t_bq = time.monotonic()
                try:
                    from pipeline import bq_sync, bq_compute, bq_results_sync, bq_schema
                    from google.cloud import bigquery
                    from app.config import settings as _settings
                    client = bigquery.Client(project=_settings.gcp_project_id)
                    bq_schema.ensure_tables(client, _settings.bq_dataset)

                    sync_result = bq_sync.sync_run(conns.pipeline, run_id=run_id)
                    metrics.bq_sync_rows = (
                        sync_result.line_items_rows
                        + sync_result.features_rows
                        + sync_result.predictions_rows
                    )
                    metrics.bq_sync_seconds = round(time.monotonic() - t_bq, 3)

                    # 7. Population stats via BQ
                    t_stats = time.monotonic()
                    bq_compute.compute_population_stats(client, _settings.bq_dataset)
                    bq_results_sync.sync_population_stats_to_pg(
                        client, _settings.bq_dataset, conns.pipeline
                    )
                    metrics.stats_refreshed = True
                    metrics.stats_seconds = round(time.monotonic() - t_stats, 3)
                except Exception as e:
                    logger.error("bq_sync_error",
                                 extra={"event": "bq_sync_error", "error": str(e)},
                                 exc_info=True)
                    metrics.errors.append(f"bq:{e}")

            # 8. Mark queue items completed
            done = mark_completed(conns.pipeline, run_id)
            logger.info("pipeline_queue_marked_completed",
                        extra={"event": "pipeline_queue_marked_completed",
                               "count": done, "run_id": run_id})

            metrics.status = "completed" if not metrics.errors else "completed_with_errors"

        except Exception as e:
            logger.error("pipeline_fatal_error",
                         extra={"event": "pipeline_fatal_error", "error": str(e)},
                         exc_info=True)
            metrics.status = "failed"
            metrics.errors.append(f"fatal:{e}")
        finally:
            metrics.total_seconds = round(time.monotonic() - t_start, 3)
            try:
                metrics.save(conns.pipeline)
            except Exception:
                logger.error("pipeline_metrics_save_error", exc_info=True)

    return metrics
