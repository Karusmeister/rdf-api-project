"""Unit tests for pipeline.bq_sync with all google.cloud dependencies mocked.

These tests don't exercise real BigQuery — they verify the control flow:
 - rows are pulled from the pipeline DB
 - parquet is generated via pyarrow (if installed) or is mocked out
 - the GCS upload + BQ load functions are called with the right URIs and
   write dispositions
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from app.config import settings
from app.db import pipeline_db


def _seed_minimal_report(conn, report_id="rpt-bq", krs="0000010001"):
    conn.execute(
        """
        INSERT INTO companies (krs, nip, pkd_code)
        VALUES (%s, '1234567890', '62.01.Z')
        ON CONFLICT DO NOTHING
        """,
        [krs],
    )
    conn.execute(
        """
        INSERT INTO financial_reports
            (id, logical_key, report_version, krs, fiscal_year,
             period_start, period_end, ingestion_status)
        VALUES (%s, %s, 1, %s, 2023, '2023-01-01', '2023-12-31', 'completed')
        ON CONFLICT DO NOTHING
        """,
        [report_id, f"{krs}|KRS|annual|2023|2023-12-31", krs],
    )
    conn.execute(
        """
        INSERT INTO financial_line_items
            (report_id, section, tag_path, extraction_version,
             value_current, value_previous, currency, schema_code)
        VALUES (%s, 'Bilans', 'Aktywa', 1, 1000.0, 900.0, 'PLN', 'SFJINZ')
        ON CONFLICT DO NOTHING
        """,
        [report_id],
    )
    conn.execute(
        """
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, computation_version)
        VALUES (%s, 'roa', %s, 2023, 0.05, TRUE, 1)
        ON CONFLICT DO NOTHING
        """,
        [report_id, krs],
    )
    # Need a prediction_run + prediction for the predictions export
    conn.execute(
        """
        INSERT INTO prediction_runs (id, model_id, status)
        VALUES ('pr-bq', 'maczynska_1994_v1', 'completed')
        ON CONFLICT DO NOTHING
        """
    )
    conn.execute(
        """
        INSERT INTO predictions
            (id, prediction_run_id, krs, report_id, raw_score,
             classification, risk_category)
        VALUES ('p-bq', 'pr-bq', %s, %s, 2.5, 0, 'low')
        ON CONFLICT DO NOTHING
        """,
        [krs, report_id],
    )
    return report_id


@pytest.fixture
def fake_google_cloud(monkeypatch):
    """Inject fake google.cloud.bigquery + google.cloud.storage modules.

    The fakes capture all calls so tests can assert on them. We also fake
    pyarrow if it's unavailable in the sandbox.
    """
    calls = {
        "load_jobs": [],       # (dataset.table, uri, write_disposition)
        "gcs_uploads": [],     # (bucket, path, content_type)
    }

    # ---- google.cloud.bigquery ----
    bq = types.ModuleType("google.cloud.bigquery")

    class _SourceFormat:
        PARQUET = "PARQUET"

    class _LoadJobConfig:
        def __init__(self, **kw):
            self.source_format = kw.get("source_format")
            self.write_disposition = kw.get("write_disposition", "WRITE_APPEND")

    class _LoadJob:
        def result(self):
            return None

    class _TableRef:
        def __init__(self, num_rows):
            self.num_rows = num_rows

    class _Client:
        project = "test-project"

        def __init__(self, project=None):
            self.project = project or "test-project"

        def load_table_from_uri(self, uri, table_ref, job_config=None):
            calls["load_jobs"].append((table_ref, uri, job_config.write_disposition))
            return _LoadJob()

        def get_table(self, ref):
            return _TableRef(num_rows=42)

    class _SchemaField:
        def __init__(self, name, type, mode="NULLABLE", **_):
            self.name = name
            self.type = type
            self.mode = mode

    class _Table:
        def __init__(self, ref, schema=None):
            self.ref = ref
            self.table_id = ref.split(".")[-1]
            self.schema = schema
            self.range_partitioning = None
            self.clustering_fields = None

    class _RangePartitioning:
        def __init__(self, field=None, range_=None):
            self.field = field
            self.range_ = range_

    class _PartitionRange:
        def __init__(self, start=None, end=None, interval=None):
            self.start = start
            self.end = end
            self.interval = interval

    bq.Client = _Client
    bq.LoadJobConfig = _LoadJobConfig
    bq.SourceFormat = _SourceFormat
    bq.SchemaField = _SchemaField
    bq.Table = _Table
    bq.RangePartitioning = _RangePartitioning
    bq.PartitionRange = _PartitionRange

    # ---- google.cloud.storage ----
    storage = types.ModuleType("google.cloud.storage")

    class _Blob:
        def __init__(self, name, bucket):
            self.name = name
            self.bucket = bucket

        def upload_from_string(self, data, content_type=None):
            calls["gcs_uploads"].append((self.bucket, self.name, content_type))

    class _Bucket:
        def __init__(self, name):
            self.name = name

        def blob(self, path):
            return _Blob(path, self.name)

    class _StorageClient:
        def __init__(self, project=None):
            self.project = project

        def bucket(self, name):
            return _Bucket(name)

    storage.Client = _StorageClient

    gc = types.ModuleType("google.cloud")
    gc.bigquery = bq  # type: ignore[attr-defined]
    gc.storage = storage  # type: ignore[attr-defined]
    google = types.ModuleType("google")
    google.cloud = gc  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", gc)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bq)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage)

    return calls


def test_sync_run_uploads_and_loads_all_tables(dual_db, fake_google_cloud, monkeypatch):
    """Happy path: all four exports (line_items, features, predictions,
    companies) produce a GCS upload and a BQ load job."""
    pytest.importorskip("pyarrow", reason="bq_sync requires pyarrow for parquet")

    monkeypatch.setattr(settings, "gcp_project_id", "test-project")
    monkeypatch.setattr(settings, "bq_dataset", "rdf_analytics")
    monkeypatch.setattr(settings, "pipeline_gcs_bucket", "rdf-pipeline-staging")

    conn = pipeline_db.get_conn()
    _seed_minimal_report(conn)

    from pipeline import bq_sync

    result = bq_sync.sync_run(conn, run_id=777)

    # Each of the 4 exports had at least 1 row, so all 4 were loaded
    assert result.line_items_rows >= 1
    assert result.features_rows >= 1
    assert result.predictions_rows >= 1
    assert result.companies_rows >= 1

    uploads = fake_google_cloud["gcs_uploads"]
    upload_paths = [u[1] for u in uploads]
    assert any("line_items/run=777" in p for p in upload_paths)
    assert any("features/run=777" in p for p in upload_paths)
    assert any("predictions/run=777" in p for p in upload_paths)
    assert any("companies/latest.parquet" in p for p in upload_paths)

    loads = fake_google_cloud["load_jobs"]
    # companies must be WRITE_TRUNCATE; others WRITE_APPEND
    truncate_tables = [t for (t, _, disp) in loads if disp == "WRITE_TRUNCATE"]
    append_tables = [t for (t, _, disp) in loads if disp == "WRITE_APPEND"]
    assert any("companies" in t for t in truncate_tables)
    assert any("financial_line_items" in t for t in append_tables)
    assert any("computed_features" in t for t in append_tables)
    assert any("predictions" in t for t in append_tables)


def test_sync_run_empty_pipeline_db_uploads_nothing(dual_db, fake_google_cloud, monkeypatch):
    """With nothing in the pipeline DB, sync_run must not upload or load."""
    pytest.importorskip("pyarrow")

    monkeypatch.setattr(settings, "gcp_project_id", "test-project")
    monkeypatch.setattr(settings, "bq_dataset", "rdf_analytics")
    monkeypatch.setattr(settings, "pipeline_gcs_bucket", "rdf-pipeline-staging")

    from pipeline import bq_sync

    conn = pipeline_db.get_conn()
    result = bq_sync.sync_run(conn, run_id=1)

    assert result.line_items_rows == 0
    assert result.features_rows == 0
    assert result.predictions_rows == 0
    assert result.companies_rows == 0
    assert fake_google_cloud["gcs_uploads"] == []
    assert fake_google_cloud["load_jobs"] == []


def test_bq_schema_ensure_tables_creates_missing(fake_google_cloud):
    """bq_schema.ensure_tables must call create_table for each missing table
    and skip ones that already exist."""
    from pipeline import bq_schema

    existing = {"financial_line_items"}
    created: list[str] = []

    class _FakeClient:
        project = "test-project"

        def get_table(self, ref):
            name = ref.split(".")[-1]
            if name in existing:
                return object()
            raise Exception("NotFound")

        def create_table(self, table):
            created.append(table.table_id)
            return table

    bq_schema.ensure_tables(_FakeClient(), "rdf_analytics")

    # Every table in TABLES except the one in `existing` should be created
    expected = set(bq_schema.TABLES.keys()) - existing
    assert set(created) == expected
