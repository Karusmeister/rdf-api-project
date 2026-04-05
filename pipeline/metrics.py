"""Pipeline run metrics accumulator."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from app.db.connection import ConnectionWrapper


def create_pipeline_run(conn: ConnectionWrapper, trigger: str) -> int:
    """Insert a new pipeline_runs row (status='running') and return its run_id."""
    row = conn.execute(
        """
        INSERT INTO pipeline_runs (trigger, status)
        VALUES (%s, 'running')
        RETURNING run_id
        """,
        [trigger],
    ).fetchone()
    return int(row[0])


@dataclass
class PipelineMetrics:
    run_id: int
    krs_queued: int = 0
    krs_processed: int = 0
    krs_failed: int = 0
    etl_docs: int = 0
    etl_line_items: int = 0
    etl_seconds: float = 0.0
    features_computed: int = 0
    features_failed: int = 0
    features_seconds: float = 0.0
    predictions_written: int = 0
    predictions_seconds: float = 0.0
    bq_sync_rows: int = 0
    bq_sync_seconds: float = 0.0
    stats_refreshed: bool = False
    stats_seconds: float = 0.0
    total_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)
    status: str = "running"

    def save(self, conn: ConnectionWrapper) -> None:
        """Write metrics to pipeline_runs (UPDATE by run_id)."""
        err_msg = "; ".join(self.errors)[:4000] if self.errors else None
        conn.execute(
            """
            UPDATE pipeline_runs
            SET finished_at = now(),
                status = %s,
                krs_queued = %s,
                krs_processed = %s,
                krs_failed = %s,
                etl_docs_parsed = %s,
                etl_line_items_written = %s,
                etl_duration_seconds = %s,
                features_computed = %s,
                features_failed = %s,
                features_duration_seconds = %s,
                predictions_written = %s,
                predictions_duration_seconds = %s,
                bq_sync_rows = %s,
                bq_sync_duration_seconds = %s,
                stats_refreshed = %s,
                stats_duration_seconds = %s,
                total_duration_seconds = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            [
                self.status,
                self.krs_queued, self.krs_processed, self.krs_failed,
                self.etl_docs, self.etl_line_items, self.etl_seconds,
                self.features_computed, self.features_failed, self.features_seconds,
                self.predictions_written, self.predictions_seconds,
                self.bq_sync_rows, self.bq_sync_seconds,
                self.stats_refreshed, self.stats_seconds,
                self.total_seconds,
                err_msg,
                self.run_id,
            ],
        )
