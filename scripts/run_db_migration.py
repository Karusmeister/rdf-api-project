"""Run migration scripts (.sql or .py) against the scraper DuckDB database.

Usage:
    python scripts/run_db_migration.py scripts/db_migrations/001_append_only_backfill.py
    python scripts/run_db_migration.py scripts/db_migrations/002_drop_multisource_tables.sql
    python scripts/run_db_migration.py <file> --db data/scraper.duckdb

By default uses SCRAPER_DB_PATH env var (or app.config.settings fallback).
"""

import importlib.util
import os
import sys
from pathlib import Path

import duckdb


def _default_db_path() -> str:
    """Resolve DB path from env var, falling back to app config default."""
    return os.environ.get("SCRAPER_DB_PATH", "data/scraper.duckdb")


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Bootstrap all required tables/views so migration can reference them."""
    from app.db import connection as shared_conn
    from app.repositories import krs_repo
    from app.scraper import db as scraper_db
    from app.db import prediction_db

    original = getattr(shared_conn, "_conn", None)
    shared_conn._conn = conn
    try:
        krs_repo._schema_initialized = False
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False
        krs_repo._init_schema()
        scraper_db._init_schema()
        prediction_db._init_schema()
    finally:
        shared_conn._conn = original
        krs_repo._schema_initialized = False
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False


def _parse_statements(sql: str) -> list[str]:
    """Strip comment-only lines and split into executable statements."""
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    clean_sql = "\n".join(lines)
    return [s.strip() for s in clean_sql.split(";") if s.strip()]


def run_migration(migration_path: str, db_path: str | None = None) -> None:
    """Run a migration file (.sql transactionally, or .py via run_backfill)."""
    if db_path is None:
        db_path = _default_db_path()

    path = Path(migration_path)

    if path.suffix == ".py":
        _run_python_migration(path, db_path)
    elif path.suffix == ".sql":
        _run_sql_migration(path, db_path)
    else:
        raise ValueError(f"Unsupported migration file type: {path.suffix}")


def _run_python_migration(path: Path, db_path: str) -> None:
    """Load a Python migration module and call its run_backfill(db_path)."""
    spec = importlib.util.spec_from_file_location("migration", path.resolve())
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if hasattr(mod, "run_backfill"):
        mod.run_backfill(db_path)
    else:
        raise AttributeError(f"Python migration {path} has no run_backfill() function")


def _run_sql_migration(path: Path, db_path: str) -> None:
    """Run a SQL migration file transactionally, bootstrapping schema first."""
    sql = path.read_text()
    statements = _parse_statements(sql)
    if not statements:
        print(f"No executable statements in {path}.")
        return

    conn = duckdb.connect(db_path)
    try:
        _init_schema(conn)
        conn.execute("BEGIN")
        try:
            for stmt in statements:
                conn.execute(stmt)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        print(f"Migration {path} applied successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_db_migration.py <file.sql|file.py> [--db <path>]")
        sys.exit(1)

    migration_file = sys.argv[1]
    db = None
    if "--db" in sys.argv:
        idx = sys.argv.index("--db")
        if idx + 1 < len(sys.argv):
            db = sys.argv[idx + 1]
        else:
            print("Error: --db requires a path argument")
            sys.exit(1)
    elif len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        db = sys.argv[2]  # positional fallback

    run_migration(migration_file, db)
