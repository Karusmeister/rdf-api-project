"""Tests for pipeline.metrics — creating and saving a run row."""
from __future__ import annotations

from app.db import pipeline_db
from pipeline.metrics import PipelineMetrics, create_pipeline_run


def test_create_pipeline_run_and_save(dual_db):
    conn = pipeline_db.get_conn()
    run_id = create_pipeline_run(conn, trigger="test")
    assert run_id > 0

    metrics = PipelineMetrics(run_id=run_id)
    metrics.krs_queued = 5
    metrics.krs_processed = 4
    metrics.krs_failed = 1
    metrics.etl_docs = 4
    metrics.etl_line_items = 123
    metrics.features_computed = 30
    metrics.predictions_written = 4
    metrics.total_seconds = 12.5
    metrics.status = "completed"
    metrics.save(conn)

    row = conn.execute(
        """
        SELECT status, krs_queued, krs_processed, krs_failed,
               etl_docs_parsed, etl_line_items_written, features_computed,
               predictions_written, total_duration_seconds
        FROM pipeline_runs WHERE run_id = %s
        """,
        [run_id],
    ).fetchone()
    assert row[0] == "completed"
    assert row[1] == 5
    assert row[2] == 4
    assert row[3] == 1
    assert row[4] == 4
    assert row[5] == 123
    assert row[6] == 30
    assert row[7] == 4
    assert float(row[8]) == 12.5
