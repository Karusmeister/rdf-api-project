"""PostgreSQL → GCS/BigQuery sync.

Exports incremental batches from the pipeline DB to GCS parquet files, then
loads them into BigQuery tables in `rdf_analytics`.

Uses pyarrow + google-cloud-bigquery + google-cloud-storage. All heavy
imports are deferred into function bodies so importing this module has no
external dependencies — this keeps Phase 4 fully optional (--skip-bq).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from app.config import settings
from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    line_items_rows: int = 0
    features_rows: int = 0
    predictions_rows: int = 0
    companies_rows: int = 0
    gcs_uris: list[str] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.gcs_uris is None:
            self.gcs_uris = []


def _query_to_parquet_bytes(conn: ConnectionWrapper, sql: str, params: list) -> tuple[int, bytes]:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq
    import io

    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        return 0, b""
    data = {col: [r[i] for r in rows] for i, col in enumerate(cols)}
    table = pa.table(data)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return len(rows), buf.getvalue()


def _upload_to_gcs(bucket: str, path: str, blob: bytes) -> str:
    from google.cloud import storage
    client = storage.Client(project=settings.gcp_project_id)
    b = client.bucket(bucket)
    gcs_blob = b.blob(path)
    gcs_blob.upload_from_string(blob, content_type="application/octet-stream")
    return f"gs://{bucket}/{path}"


def _load_gcs_to_bq(
    client: Any,
    dataset: str,
    table: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_APPEND",
) -> int:
    from google.cloud import bigquery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )
    load_job = client.load_table_from_uri(
        gcs_uri, f"{client.project}.{dataset}.{table}", job_config=job_config
    )
    load_job.result()
    table_ref = client.get_table(f"{client.project}.{dataset}.{table}")
    return int(table_ref.num_rows or 0)


def sync_run(
    pipeline_conn: ConnectionWrapper,
    run_id: int,
    krs_list: Optional[list[str]] = None,
) -> SyncResult:
    """Export + load everything touched by a single pipeline run.

    If `krs_list` is provided, we restrict the export to those KRS numbers;
    otherwise we export every report that was written in this run (via
    prediction_run_id).
    """
    from google.cloud import bigquery

    result = SyncResult()
    bucket = settings.pipeline_gcs_bucket
    dataset = settings.bq_dataset
    bq_client = bigquery.Client(project=settings.gcp_project_id)

    krs_filter_sql = ""
    params: list = []
    if krs_list:
        krs_filter_sql = "AND fr.krs = ANY(%s)"
        params = [krs_list]

    # ---- line items ----
    li_sql = f"""
        SELECT fli.report_id, fr.krs, fr.fiscal_year, fli.section, fli.tag_path,
               fli.label_pl, fli.value_current, fli.value_previous,
               fli.schema_code, fli.extraction_version, now() AS exported_at
        FROM latest_financial_line_items fli
        JOIN financial_reports fr ON fr.id = fli.report_id
        WHERE TRUE {krs_filter_sql}
    """
    rows, blob = _query_to_parquet_bytes(pipeline_conn, li_sql, params)
    if rows:
        uri = _upload_to_gcs(bucket, f"line_items/run={run_id}/data.parquet", blob)
        _load_gcs_to_bq(bq_client, dataset, "financial_line_items", uri)
        result.line_items_rows = rows
        result.gcs_uris.append(uri)

    # ---- computed features ----
    cf_sql = f"""
        SELECT cf.report_id, cf.krs, cf.fiscal_year, cf.feature_definition_id,
               cf.value, cf.is_valid, cf.computation_version, cf.computed_at
        FROM latest_computed_features cf
        JOIN financial_reports fr ON fr.id = cf.report_id
        WHERE cf.is_valid = true {krs_filter_sql}
    """
    rows, blob = _query_to_parquet_bytes(pipeline_conn, cf_sql, params)
    if rows:
        uri = _upload_to_gcs(bucket, f"features/run={run_id}/data.parquet", blob)
        _load_gcs_to_bq(bq_client, dataset, "computed_features", uri)
        result.features_rows = rows
        result.gcs_uris.append(uri)

    # ---- predictions ----
    #
    # Parity contract with the pipeline Postgres DB: BQ mirrors every column
    # that feeds the API response — raw_score/risk_category/classification
    # (the numbers), feature_contributions + feature_snapshot (why the
    # score came out that way), and the JSON-serialised contributions carry
    # the `_warnings` reserved key so nothing is lost in the PG → BQ hop.
    # BigQuery PARQUET loader accepts JSON as STRING, so we cast both JSON
    # columns on the SELECT side.
    pr_sql = f"""
        SELECT p.id, p.prediction_run_id, p.krs, p.report_id, fr.fiscal_year,
               pr.model_id, p.raw_score, p.probability, p.classification,
               p.risk_category,
               p.feature_contributions::text AS feature_contributions,
               p.feature_snapshot::text AS feature_snapshot,
               p.created_at
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        JOIN financial_reports fr ON fr.id = p.report_id
        WHERE TRUE {krs_filter_sql}
    """
    rows, blob = _query_to_parquet_bytes(pipeline_conn, pr_sql, params)
    if rows:
        uri = _upload_to_gcs(bucket, f"predictions/run={run_id}/data.parquet", blob)
        _load_gcs_to_bq(bq_client, dataset, "predictions", uri)
        result.predictions_rows = rows
        result.gcs_uris.append(uri)

    # ---- companies (full refresh, small table) ----
    co_sql = """
        SELECT krs, nip, regon, pkd_code, incorporation_date, voivodeship
        FROM companies
    """
    rows, blob = _query_to_parquet_bytes(pipeline_conn, co_sql, [])
    if rows:
        uri = _upload_to_gcs(bucket, "companies/latest.parquet", blob)
        _load_gcs_to_bq(bq_client, dataset, "companies", uri,
                        write_disposition="WRITE_TRUNCATE")
        result.companies_rows = rows
        result.gcs_uris.append(uri)

    logger.info(
        "bq_sync_complete",
        extra={"event": "bq_sync_complete", "run_id": run_id,
               "line_items": result.line_items_rows,
               "features": result.features_rows,
               "predictions": result.predictions_rows,
               "companies": result.companies_rows},
    )
    return result
