"""Parity tests: rows produced by the pipeline in Postgres must survive the
BQ-sync export step with identical values and row counts.

We don't actually talk to BigQuery — we intercept `bq_sync._upload_to_gcs`
and `bq_sync._load_gcs_to_bq` so the function runs end-to-end but stops at
the parquet-generation step. What we assert is:

  * row counts in the `SyncResult` match row counts in the Postgres pipeline
    DB for each exported table
  * the parquet payload for predictions contains every column in the PG row
    (raw_score, risk_category, feature_contributions, feature_snapshot)
  * Maczynska and Poznanski predictions both round-trip through the export
"""
from __future__ import annotations

import io
import json

import pytest

from app.db import pipeline_db
from pipeline.catalog import ensure_builtin_catalog
from pipeline.scoring import score_reports


def _seed_report(conn, report_id, krs, features):
    conn.execute(
        """
        INSERT INTO companies (krs, nip, pkd_code)
        VALUES (%s, '1111111111', '62.01.Z')
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
             value_current, currency, schema_code)
        VALUES (%s, 'Bilans', 'Aktywa', 1, 1000.0, 'PLN', 'SFJINZ')
        ON CONFLICT DO NOTHING
        """,
        [report_id],
    )
    for fid, val in features.items():
        conn.execute(
            """
            INSERT INTO computed_features
                (report_id, feature_definition_id, krs, fiscal_year, value,
                 is_valid, computation_version)
            VALUES (%s, %s, %s, 2023, %s, TRUE, 1)
            ON CONFLICT DO NOTHING
            """,
            [report_id, fid, krs, val],
        )


def test_bq_sync_row_counts_match_pg(dual_db, monkeypatch):
    pytest.importorskip("pyarrow", reason="bq_sync requires pyarrow")

    conn = pipeline_db.get_conn()
    ensure_builtin_catalog(conn)

    # Seed two reports, one with Maczynska inputs, one with Poznanski inputs.
    maczynska_features = {
        "x1_maczynska": 0.5, "x2_maczynska": 2.0, "x3_maczynska": 0.1,
        "x4_maczynska": 0.3, "x5_maczynska": 1.5, "x6_maczynska": 0.8,
    }
    poznanski_features = {
        "x1_poznanski": 0.05, "x2_poznanski": 1.2,
        "x3_poznanski": 0.6, "x4_poznanski": 0.08,
    }
    _seed_report(conn, "rpt-parity-1", "0000011111", maczynska_features)
    _seed_report(conn, "rpt-parity-2", "0000022222", poznanski_features)
    score_reports(conn, ["rpt-parity-1", "rpt-parity-2"])

    # Expected counts from Postgres
    li_count = conn.execute("SELECT count(*) FROM latest_financial_line_items").fetchone()[0]
    cf_count = conn.execute(
        "SELECT count(*) FROM latest_computed_features WHERE is_valid = TRUE"
    ).fetchone()[0]
    pred_count = conn.execute("SELECT count(*) FROM predictions").fetchone()[0]
    co_count = conn.execute("SELECT count(*) FROM companies").fetchone()[0]

    assert pred_count >= 2  # at least one per model path
    assert cf_count >= 10   # 6 maczynska + 4 poznanski
    assert li_count >= 1
    assert co_count >= 1

    # Intercept GCS upload + BQ load so we can inspect the parquet bytes
    # without needing any google-cloud library.
    uploads: list[tuple[str, bytes]] = []

    def _fake_upload(bucket, path, blob):
        uploads.append((path, blob))
        return f"gs://{bucket}/{path}"

    loaded: list[tuple[str, str, str]] = []

    def _fake_load(client, dataset, table, gcs_uri, write_disposition="WRITE_APPEND"):
        loaded.append((table, gcs_uri, write_disposition))
        return 0

    from pipeline import bq_sync
    monkeypatch.setattr(bq_sync, "_upload_to_gcs", _fake_upload)
    monkeypatch.setattr(bq_sync, "_load_gcs_to_bq", _fake_load)

    # Fake the BQ client import so sync_run's top-level `from google.cloud
    # import bigquery` succeeds without the real dependency.
    import sys, types
    gc = types.ModuleType("google.cloud")
    bq_mod = types.ModuleType("google.cloud.bigquery")

    class _FakeClient:
        project = "test-project"

        def __init__(self, project=None):
            self.project = project or "test-project"

    bq_mod.Client = _FakeClient
    gc.bigquery = bq_mod  # type: ignore[attr-defined]
    google = types.ModuleType("google")
    google.cloud = gc  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.cloud", gc)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bq_mod)

    from app.config import settings
    monkeypatch.setattr(settings, "gcp_project_id", "test-project")
    monkeypatch.setattr(settings, "bq_dataset", "rdf_analytics")
    monkeypatch.setattr(settings, "pipeline_gcs_bucket", "rdf-pipeline-staging")

    result = bq_sync.sync_run(conn, run_id=321)

    # Row-count parity: every non-zero count on the PG side must round-trip.
    assert result.line_items_rows == li_count
    assert result.features_rows == cf_count
    assert result.predictions_rows == pred_count
    assert result.companies_rows == co_count

    # Parquet payload for predictions must include our new columns.
    import pyarrow.parquet as pq
    pred_upload = next((b for p, b in uploads if "predictions/" in p), None)
    assert pred_upload is not None
    table = pq.read_table(io.BytesIO(pred_upload))
    columns = set(table.schema.names)
    for col in (
        "id", "prediction_run_id", "krs", "report_id", "fiscal_year",
        "model_id", "raw_score", "risk_category",
        "feature_contributions", "feature_snapshot", "created_at",
    ):
        assert col in columns, f"predictions parquet missing column {col}"

    # Per-row parity check: pick the Maczynska row and assert the PG row and
    # the parquet row agree on raw_score + risk_category.
    df = table.to_pandas()
    df_maz = df[df["model_id"] == "maczynska_1994_v1"]
    assert not df_maz.empty
    row = conn.execute(
        """
        SELECT p.raw_score, p.risk_category
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE pr.model_id = 'maczynska_1994_v1'
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert float(df_maz.iloc[0]["raw_score"]) == pytest.approx(row[0], abs=1e-9)
    assert df_maz.iloc[0]["risk_category"] == row[1]

    # feature_contributions is exported as a JSON string — make sure it is
    # valid JSON and contains either `_intercept` (Poznanski) or the
    # model's raw contributions.
    contribs = json.loads(df_maz.iloc[0]["feature_contributions"])
    assert isinstance(contribs, dict)
    assert any(k.startswith("x") for k in contribs.keys())
