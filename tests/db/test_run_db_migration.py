"""Tests for scripts/run_db_migration.py — migration runner.

DuckDB-specific migration tests are skipped after the PostgreSQL migration.
Pure SQL parsing tests remain active as they have no DuckDB dependency.
"""
import pytest
from pathlib import Path
from unittest.mock import patch

from scripts.run_db_migration import _parse_statements


# ---------------------------------------------------------------------------
# Statement parsing (no DuckDB dependency)
# ---------------------------------------------------------------------------

def test_parse_strips_comments():
    sql = "-- comment\nSELECT 1;\n-- another\nSELECT 2;"
    stmts = _parse_statements(sql)
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_parse_empty_file():
    assert _parse_statements("-- only comments\n") == []


# ---------------------------------------------------------------------------
# Full migration (DuckDB-specific — skipped after PostgreSQL migration)
# ---------------------------------------------------------------------------

@pytest.fixture()
def fresh_db(tmp_path):
    return str(tmp_path / "mig_test.duckdb")


@pytest.mark.skip(reason="DuckDB migration tests - not applicable after PostgreSQL migration")
def test_python_migration_succeeds_on_fresh_db(fresh_db):
    """Python backfill runner should bootstrap schema and complete."""
    import duckdb
    from app.config import settings

    py_path = "scripts/db_migrations/001_append_only_backfill.py"
    if not Path(py_path).exists():
        pytest.skip("migration file not found")
    with patch.object(settings, "scraper_db_path", fresh_db):
        from scripts.run_db_migration import run_migration
        run_migration(py_path, fresh_db)
    conn = duckdb.connect(fresh_db, read_only=True)
    tables = {
        row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    conn.close()
    assert "krs_entity_versions" in tables
    assert "krs_document_versions" in tables
    assert "etl_attempts" in tables


@pytest.mark.skip(reason="DuckDB migration tests - not applicable after PostgreSQL migration")
def test_python_migration_is_idempotent(fresh_db):
    """Running the same Python migration twice should not raise."""
    from app.config import settings

    py_path = "scripts/db_migrations/001_append_only_backfill.py"
    if not Path(py_path).exists():
        pytest.skip("migration file not found")
    with patch.object(settings, "scraper_db_path", fresh_db):
        from scripts.run_db_migration import run_migration
        run_migration(py_path, fresh_db)
        run_migration(py_path, fresh_db)


@pytest.mark.skip(reason="DuckDB migration tests - not applicable after PostgreSQL migration")
def test_sql_migration_rolls_back_on_error(fresh_db, tmp_path):
    """If a SQL statement fails, the entire migration should be rolled back."""
    import duckdb
    from app.config import settings

    bad_sql = tmp_path / "bad.sql"
    bad_sql.write_text(
        "CREATE TABLE _mig_test (id INTEGER);\n"
        "INSERT INTO _mig_test VALUES (1);\n"
        "INSERT INTO nonexistent_table VALUES (99);\n"
    )
    with patch.object(settings, "scraper_db_path", fresh_db):
        from scripts.run_db_migration import run_migration
        with pytest.raises(duckdb.CatalogException):
            run_migration(str(bad_sql), fresh_db)

    conn = duckdb.connect(fresh_db, read_only=True)
    tables = {
        row[0] for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    # DuckDB DDL is not transactional — table may exist,
    # but the INSERT should have been rolled back.
    if "_mig_test" in tables:
        count = conn.execute("SELECT count(*) FROM _mig_test").fetchone()[0]
        assert count == 0, "INSERT should have been rolled back"
    conn.close()


@pytest.mark.skip(reason="DuckDB migration tests - not applicable after PostgreSQL migration")
def test_sql_drop_migration(fresh_db):
    """SQL drop migration should run on a bootstrapped DB."""
    from app.config import settings

    sql_path = "scripts/db_migrations/002_drop_multisource_tables.sql"
    if not Path(sql_path).exists():
        pytest.skip("migration file not found")
    with patch.object(settings, "scraper_db_path", fresh_db):
        from scripts.run_db_migration import run_migration
        run_migration(sql_path, fresh_db)
