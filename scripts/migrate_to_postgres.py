"""Migrate data from DuckDB Parquet exports to PostgreSQL.

Usage:
    python scripts/export_duckdb.py data/scraper.duckdb   # step 1: export
    python scripts/migrate_to_postgres.py                  # step 2: load

Set DATABASE_URL to target the correct PostgreSQL instance.
Uses DuckDB to read Parquet → CSV pipe → psycopg2 COPY for fast bulk loading.
"""
import io
import os
import sys
from pathlib import Path

import duckdb
import psycopg2

EXPORT_DIR = Path("data/export")

# Tables in dependency order (parents first).
# Views (krs_documents_current, krs_entities_current, etc.) are excluded.
TABLE_ORDER = [
    # Batch progress (no dependencies)
    "batch_progress",
    "batch_rdf_progress",
    # Entity registry
    "krs_registry",
    "krs_entities",
    "krs_entity_versions",
    # Document registry
    "krs_documents",
    "krs_document_versions",
    "scraper_runs",
    # Scanner state
    "krs_scan_cursor",
    "krs_scan_runs",
    "krs_sync_log",
    # Prediction layer
    "companies",
    "financial_reports",
    "raw_financial_data",
    "financial_line_items",
    "etl_attempts",
    # Feature layer
    "feature_definitions",
    "feature_sets",
    "feature_set_members",
    "computed_features",
    # Model layer
    "model_registry",
    "prediction_runs",
    "predictions",
    "bankruptcy_events",
    "assessment_jobs",
]


def main():
    dsn = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/rdf")

    if not EXPORT_DIR.exists():
        print(f"Export directory {EXPORT_DIR} not found. Run scripts/export_duckdb.py first.")
        sys.exit(1)

    src = duckdb.connect()
    dst = psycopg2.connect(dsn)
    cur = dst.cursor()

    total_rows = 0

    for table in TABLE_ORDER:
        parquet_path = EXPORT_DIR / f"{table}.parquet"
        if not parquet_path.exists():
            print(f"  SKIP {table} (no parquet file)")
            continue

        count = src.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        if count == 0:
            print(f"  SKIP {table} (empty)")
            continue

        # Export to CSV in memory via DuckDB
        csv_path = f"/tmp/_migrate_{table}.csv"
        src.execute(f"COPY (SELECT * FROM '{parquet_path}') TO '{csv_path}' (FORMAT CSV, HEADER TRUE)")

        # Get column names from the CSV header
        with open(csv_path, "r") as f:
            header = f.readline().strip()
        col_names = header

        # Use COPY for fast bulk loading
        try:
            with open(csv_path, "r") as f:
                cur.copy_expert(
                    f"COPY {table} ({col_names}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
                    f,
                )
            dst.commit()
            total_rows += count
            print(f"  {table}: {count:,} rows loaded")
        except Exception as e:
            dst.rollback()
            print(f"  ERROR {table}: {e}")

        # Cleanup temp file
        try:
            os.unlink(csv_path)
        except OSError:
            pass

    # Reset sequences to max existing IDs
    _reset_sequences(cur, dst)

    src.close()
    dst.close()
    print(f"\nMigration complete: {total_rows:,} total rows loaded")


def _reset_sequences(cur, conn):
    """Reset PostgreSQL sequences to match the max IDs after bulk load."""
    seq_table_col = [
        ("seq_krs_entity_versions", "krs_entity_versions", "version_id"),
        ("seq_krs_document_versions", "krs_document_versions", "version_id"),
        ("seq_krs_sync_log", "krs_sync_log", "id"),
        ("seq_krs_scan_runs", "krs_scan_runs", "id"),
        ("seq_etl_attempts", "etl_attempts", "attempt_id"),
    ]
    for seq, table, col in seq_table_col:
        try:
            cur.execute(f"SELECT setval('{seq}', COALESCE((SELECT MAX({col}) FROM {table}), 0) + 1)")
            conn.commit()
            val = cur.fetchone()[0]
            print(f"  sequence {seq} -> {val}")
        except Exception as e:
            conn.rollback()
            print(f"  WARN sequence {seq}: {e}")


if __name__ == "__main__":
    main()
