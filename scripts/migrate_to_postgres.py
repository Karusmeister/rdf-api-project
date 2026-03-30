"""Migrate data from DuckDB Parquet exports to PostgreSQL.

Usage:
    python scripts/export_duckdb.py data/scraper.duckdb   # step 1: export
    python scripts/migrate_to_postgres.py                  # step 2: load

Set DATABASE_URL to target the correct PostgreSQL instance.
"""
import os
import sys
from pathlib import Path

import duckdb
import psycopg2

EXPORT_DIR = Path("data/export")

# Tables in dependency order (parents first)
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

    # Use DuckDB to read Parquet files (it's excellent at this)
    src = duckdb.connect()
    dst = psycopg2.connect(dsn)
    dst.autocommit = False
    cur = dst.cursor()

    total_rows = 0

    for table in TABLE_ORDER:
        parquet_path = EXPORT_DIR / f"{table}.parquet"
        if not parquet_path.exists():
            print(f"  SKIP {table} (no parquet file)")
            continue

        df = src.execute(f"SELECT * FROM '{parquet_path}'").fetchdf()
        if df.empty:
            print(f"  SKIP {table} (empty)")
            continue

        cols = list(df.columns)
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        rows_inserted = 0
        for _, row in df.iterrows():
            values = [None if str(v) == "NaT" or str(v) == "nan" else v for v in row.tolist()]
            try:
                cur.execute(insert_sql, values)
                rows_inserted += 1
            except Exception as e:
                dst.rollback()
                print(f"  ERROR {table} row: {e}")
                continue

        dst.commit()
        total_rows += rows_inserted
        print(f"  {table}: {rows_inserted:,} rows loaded")

    src.close()
    dst.close()
    print(f"\nMigration complete: {total_rows:,} total rows loaded")


if __name__ == "__main__":
    main()
