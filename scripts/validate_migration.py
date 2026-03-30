"""Validate migration: compare DuckDB vs PostgreSQL row counts."""
import os
import sys

import duckdb
import psycopg2


def main():
    duck_path = sys.argv[1] if len(sys.argv) > 1 else "data/scraper.duckdb"
    pg_dsn = os.environ.get("DATABASE_URL", "postgresql://localhost:5432/rdf")

    duck = duckdb.connect(duck_path, read_only=True)
    pg = psycopg2.connect(pg_dsn)
    pg_cur = pg.cursor()

    tables = [r[0] for r in duck.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()]

    all_ok = True
    print(f"\n  {'Table':40s} {'DuckDB':>10s}  {'PostgreSQL':>10s}  Status")
    print("  " + "-" * 75)

    for table in sorted(tables):
        duck_count = duck.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        try:
            pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
            pg_count = pg_cur.fetchone()[0]
        except Exception:
            pg.rollback()
            pg_count = -1

        status = "OK" if duck_count == pg_count else "MISMATCH"
        if status == "MISMATCH":
            all_ok = False
        print(f"  {table:40s} {duck_count:>10,}  {pg_count:>10,}  {status}")

    duck.close()
    pg.close()

    print(f"\n  {'ALL TABLES MATCH' if all_ok else 'MISMATCHES FOUND'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
