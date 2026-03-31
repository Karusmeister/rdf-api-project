"""Export all DuckDB tables to Parquet files for PostgreSQL migration."""
import sys
from pathlib import Path

import duckdb


def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/scraper.duckdb"
    out_dir = Path("data/export")
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_path, read_only=True)

    tables = [r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]

    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.execute(f"COPY {table} TO '{out_dir}/{table}.parquet' (FORMAT PARQUET)")
        print(f"  {table}: {count:,} rows -> {table}.parquet")

    conn.close()
    print(f"\nExported {len(tables)} tables to {out_dir}/")


if __name__ == "__main__":
    main()
