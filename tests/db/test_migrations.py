"""Dedicated regression tests for the custom migration runner (CR3-MIG-001).

These tests exercise the runner directly against a disposable `tmp_path`
migrations tree instead of the real repo directory, so they can freely
introduce malformed filenames, version collisions, and drift states that
would be poisonous to run against the production migrations.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

from app.db import connection as db_conn
from app.db import migrations as db_migrations
from app.db.migrations import MigrationError


@pytest.fixture
def isolated_migration_env(pg_dsn, tmp_path, monkeypatch):
    """Provide a fresh PG connection and a disposable migrations root.

    The runner resolves `_MIGRATIONS_ROOT` at import time, so we
    monkeypatch it to point at `tmp_path` for the duration of the test.
    `schema_migrations` is isolated to a unique namespace per test so
    nothing leaks to other test_prediction_db cases sharing the session DB.
    """
    migrations_root = tmp_path / "migrations"
    migrations_root.mkdir()
    monkeypatch.setattr(db_migrations, "_MIGRATIONS_ROOT", migrations_root)

    # Use a direct connection — we need autocommit=True for the tracking
    # table CREATE and the tests themselves manage transactions via the
    # runner's explicit commit/rollback dance.
    raw = psycopg2.connect(pg_dsn)
    raw.autocommit = True
    conn = db_conn.ConnectionWrapper(raw)
    db_migrations._ensure_tracking_table(conn)

    # Reserve a namespace that the real fixtures never touch so applied
    # rows don't collide with the session-wide `prediction` migrations.
    namespace = f"cr3_test_{tmp_path.name}"
    (migrations_root / namespace).mkdir()

    yield conn, migrations_root, namespace

    # Clean up any tracking rows we inserted.
    conn.execute(
        "DELETE FROM schema_migrations WHERE version LIKE %s",
        [f"{namespace}/%"],
    )
    raw.close()


def _write_migration(
    ns_dir: Path,
    filename: str,
    body: str = "SELECT 1",
) -> Path:
    """Helper: write `body` to `<ns_dir>/<filename>` and return the path."""
    path = ns_dir / filename
    path.write_text(body, encoding="utf-8")
    return path


class TestNumericOrdering:
    """Acceptance criterion: a new migration with a higher numeric version
    always applies after lower versions, regardless of filename width."""

    def test_applies_in_numeric_order_across_width_boundary(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        # Mix widths deliberately. Lexicographic sort would rank 10 before 2
        # (pre-CR3-MIG-001 bug).
        _write_migration(ns_dir, "001_first.sql", "CREATE TEMP TABLE _cr3_first (n INT)")
        _write_migration(ns_dir, "002_second.sql", "CREATE TEMP TABLE _cr3_second (n INT)")
        _write_migration(ns_dir, "10_tenth.sql", "CREATE TEMP TABLE _cr3_tenth (n INT)")

        discovered = db_migrations._discover(namespace)
        versions = [m.version for m in discovered]
        assert versions == [1, 2, 10], (
            f"Migrations must be ordered by parsed integer, got {versions}"
        )

    def test_strictly_monotonic_apply_order(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_a.sql", "SELECT 1")
        _write_migration(ns_dir, "002_b.sql", "SELECT 1")
        _write_migration(ns_dir, "100_z.sql", "SELECT 1")

        applied = db_migrations.apply_pending(conn, namespaces=[namespace])
        assert [a.rsplit("/", 1)[1] for a in applied] == [
            "001_a",
            "002_b",
            "100_z",
        ]


class TestMalformedFilenameFailsFast:
    """Acceptance criterion: malformed filename → deterministic startup error."""

    def test_non_matching_filename_raises(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_valid.sql", "SELECT 1")
        # Typo: missing numeric prefix. Pre-CR3-MIG-001 this was silently
        # skipped with a warning — that is exactly the bug we're closing.
        _write_migration(ns_dir, "init_without_version.sql", "SELECT 1")

        with pytest.raises(MigrationError, match="Malformed migration filename"):
            db_migrations.apply_pending(conn, namespaces=[namespace])

    def test_version_collision_raises(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_first.sql", "SELECT 1")
        # Same numeric version, different name — the runner must refuse.
        _write_migration(ns_dir, "1_also_first.sql", "SELECT 1")

        with pytest.raises(MigrationError, match="Version collision"):
            db_migrations.apply_pending(conn, namespaces=[namespace])


class TestHistoryDriftDetection:
    """Acceptance criterion: missing-on-disk / retroactive-insertion drift
    must raise a deterministic startup failure."""

    def test_applied_migration_missing_from_disk_raises(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        # Apply 001 normally.
        _write_migration(ns_dir, "001_first.sql", "SELECT 1")
        db_migrations.apply_pending(conn, namespaces=[namespace])

        # Now delete the file from disk (simulating a rename/delete drift).
        (ns_dir / "001_first.sql").unlink()

        with pytest.raises(MigrationError, match="history drift"):
            db_migrations.apply_pending(conn, namespaces=[namespace])

    def test_retroactive_insertion_below_highest_applied_raises(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_first.sql", "SELECT 1")
        _write_migration(ns_dir, "005_fifth.sql", "SELECT 1")
        db_migrations.apply_pending(conn, namespaces=[namespace])

        # Now back-fill a migration below the fence. This is the scenario
        # where an engineer merges an older branch with a new migration that
        # was numbered before the team's current head — allowing it to run
        # would skip 005's assumed predecessor state.
        _write_migration(ns_dir, "003_backfill.sql", "SELECT 1")

        with pytest.raises(MigrationError, match="Retroactive migration insertion"):
            db_migrations.apply_pending(conn, namespaces=[namespace])

    def test_new_migration_above_highest_applied_is_allowed(self, isolated_migration_env):
        """Control: legitimate forward migrations must still apply."""
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_first.sql", "SELECT 1")
        db_migrations.apply_pending(conn, namespaces=[namespace])

        _write_migration(ns_dir, "002_forward.sql", "SELECT 1")
        applied = db_migrations.apply_pending(conn, namespaces=[namespace])

        assert len(applied) == 1
        assert applied[0].endswith("/002_forward")


class TestApplyTransactionality:
    """A migration that raises mid-SQL must leave no tracking row and no
    partial schema changes."""

    def test_failing_migration_rolls_back_and_raises(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(
            ns_dir,
            "001_explodes.sql",
            # Deliberately broken SQL.
            "CREATE TEMP TABLE _cr3_good (n INT); SELECT * FROM __does_not_exist__;",
        )

        with pytest.raises(psycopg2.Error):
            db_migrations.apply_pending(conn, namespaces=[namespace])

        # No tracking row should exist — the migration must not be marked applied.
        row = conn.execute(
            "SELECT version FROM schema_migrations WHERE version = %s",
            [f"{namespace}/001_explodes"],
        ).fetchone()
        assert row is None, (
            "Failed migration must not leave a schema_migrations row behind"
        )

    def test_idempotent_rerun_no_ops(self, isolated_migration_env):
        conn, migrations_root, namespace = isolated_migration_env
        ns_dir = migrations_root / namespace

        _write_migration(ns_dir, "001_a.sql", "SELECT 1")
        first = db_migrations.apply_pending(conn, namespaces=[namespace])
        second = db_migrations.apply_pending(conn, namespaces=[namespace])

        assert len(first) == 1
        assert second == [], "Second apply must be a no-op"
