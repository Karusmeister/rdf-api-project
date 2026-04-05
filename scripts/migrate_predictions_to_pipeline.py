"""One-shot migration: copy existing prediction tables from rdf-postgres → rdf-pipeline.

Tables migrated (idempotent, uses ON CONFLICT DO NOTHING):
    - companies
    - financial_reports
    - raw_financial_data
    - financial_line_items
    - computed_features
    - model_registry
    - prediction_runs
    - predictions
    - bankruptcy_events

Auth tables (users, user_krs_access, etc.) stay on rdf-postgres.

Usage:
    python scripts/migrate_predictions_to_pipeline.py
    python scripts/migrate_predictions_to_pipeline.py --dry-run
    python scripts/migrate_predictions_to_pipeline.py --batch-size 5000
"""
from __future__ import annotations

import argparse
import logging
import sys

import psycopg2
from psycopg2.extras import execute_values

from app.config import settings

logger = logging.getLogger(__name__)

# (source_table, columns) — these are copied verbatim. Order matters for FKs
# (though we don't have FK constraints, logical parents come first).
MIGRATIONS: list[tuple[str, list[str]]] = [
    ("companies",
     ["krs", "nip", "regon", "pkd_code", "incorporation_date", "voivodeship", "updated_at"]),
    ("financial_reports",
     ["id", "logical_key", "report_version", "supersedes_report_id", "krs",
      "data_source_id", "report_type", "fiscal_year", "period_start", "period_end",
      "taxonomy_version", "source_document_id", "source_file_path", "schema_code",
      "ingestion_status", "ingestion_error", "created_at"]),
    ("raw_financial_data",
     ["report_id", "section", "extraction_version", "data_json",
      "taxonomy_version", "created_at"]),
    ("financial_line_items",
     ["report_id", "section", "tag_path", "extraction_version", "label_pl",
      "value_current", "value_previous", "currency", "schema_code"]),
    ("feature_definitions",
     ["id", "name", "description", "category", "formula_description",
      "formula_numerator", "formula_denominator", "required_tags",
      "computation_logic", "version", "is_active", "created_at"]),
    ("feature_sets",
     ["id", "name", "description", "is_active", "created_at"]),
    ("feature_set_members",
     ["feature_set_id", "feature_definition_id", "ordinal"]),
    ("computed_features",
     ["report_id", "feature_definition_id", "krs", "fiscal_year", "value",
      "is_valid", "error_message", "source_extraction_version",
      "computation_version", "computed_at"]),
    ("model_registry",
     ["id", "name", "model_type", "version", "feature_set_id", "description",
      "hyperparameters", "training_metrics", "training_date", "training_data_spec",
      "artifact_path", "is_active", "is_baseline", "created_at"]),
    ("prediction_runs",
     ["id", "model_id", "run_date", "parameters", "companies_scored", "status",
      "error_message", "duration_seconds", "created_at"]),
    ("predictions",
     ["id", "prediction_run_id", "krs", "report_id", "raw_score", "probability",
      "classification", "risk_category", "feature_contributions", "created_at"]),
    ("bankruptcy_events",
     ["id", "krs", "event_type", "event_date", "data_source_id", "court_case_ref",
      "announcement_id", "is_confirmed", "notes", "created_at"]),
]


def _copy_table(src_conn, dst_conn, table: str, columns: list[str],
                batch_size: int, dry_run: bool) -> int:
    cols_sql = ", ".join(columns)
    cur = src_conn.cursor()
    cur.execute(f"SELECT {cols_sql} FROM {table}")

    total = 0
    dst_cur = dst_conn.cursor()
    placeholders = ", ".join(columns)
    insert_sql = (
        f"INSERT INTO {table} ({cols_sql}) VALUES %s ON CONFLICT DO NOTHING"
    )

    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        if not dry_run:
            execute_values(dst_cur, insert_sql, rows)
        total += len(rows)
        print(f"  {table}: {total} rows")
    return total


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    src = psycopg2.connect(settings.database_url)
    src.autocommit = True
    dst = psycopg2.connect(settings.pipeline_database_url)
    dst.autocommit = True

    # Ensure pipeline schema exists
    from app.db import pipeline_db
    pipeline_db._conn = None  # type: ignore[attr-defined]
    pipeline_db.connect()

    totals: dict[str, int] = {}
    for table, columns in MIGRATIONS:
        print(f"Migrating {table}...")
        try:
            totals[table] = _copy_table(src, dst, table, columns,
                                         args.batch_size, args.dry_run)
        except Exception as e:
            print(f"  ERROR: {e}")
            totals[table] = -1

    print("\nMigration summary:")
    for t, n in totals.items():
        print(f"  {t:30s} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
