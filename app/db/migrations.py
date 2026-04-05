"""Versioned SQL migration runner (CR2-OPS-004, hardened in CR3-MIG-001).

Tracks applied migrations in a `schema_migrations` table and applies any
pending `.sql` files from the on-disk migrations directory in **numeric**
version order. Each file is applied inside an explicit transaction and
recorded as a single row so reruns are safe.

Why roll our own instead of Alembic / Yoyo:
- The project already owns PostgreSQL connection lifecycle via
  `app.db.connection.ConnectionWrapper` and autocommit semantics. A full ORM
  migration tool would need to be taught the same conventions or fight them.
- We only need forward-only migrations. There is no "down" path — rollbacks
  ship as new forward migrations.
- Startup must be able to run this synchronously before the app accepts
  traffic, with zero external config files.

Layout:
    migrations/
        prediction/
            001_schema_code_backfill.sql
            002_feature_snapshot_column.sql
            ...

Filename grammar: `<numeric version><sep><name>.sql`, where the version is
a bare integer (width is not part of the identity — `1`, `01`, `001` all
resolve to the same logical version 1) and the separator is `_` or `-`.
Application order is the parsed integer, not the string — CR3-MIG-001
regression against the prior lexicographic sort which misranked 10 before 2
when widths drift.

Validation policy (fail-fast, all at startup):
  1. **Malformed filenames**: any `*.sql` file that doesn't match the pattern
     is a `MigrationError`, NOT a skipped warning. A typo like `01a.sql` that
     silently no-ops in production is exactly the failure mode we want to
     prevent.
  2. **Version collisions**: two files with the same integer version in the
     same namespace are a `MigrationError`.
  3. **History drift**: an applied migration that no longer appears on disk
     (rename, delete, moved namespace) is a `MigrationError`. An operator
     must either restore the file or manually remove the `schema_migrations`
     row to acknowledge the drift.
  4. **Retroactive insertion**: a new on-disk migration whose numeric
     version is lower than the highest-applied version triggers a
     `MigrationError`. Allowing a "back-filled" migration to run after newer
     ones would skip its dependencies and silently corrupt schema history.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)

# Repo root → migrations/<namespace>/*.sql
_REPO_ROOT = Path(__file__).resolve().parents[2]
_MIGRATIONS_ROOT = _REPO_ROOT / "migrations"

# Accept any positive integer prefix. Width is informational, not identity —
# see numeric ordering comment in the module docstring.
_VERSION_RE = re.compile(r"^(\d+)[_\-](.+)\.sql$")


class MigrationError(RuntimeError):
    """Raised on any migration-runner invariant violation.

    Distinct from `RuntimeError` so callers (startup, tests) can catch
    migration-specific failures without swallowing unrelated errors.
    """


@dataclass(frozen=True)
class Migration:
    namespace: str
    version: int
    name: str
    path: Path

    @property
    def key(self) -> str:
        """Stable identifier stored in `schema_migrations.version`.

        The key embeds the *original* filename width so that migrations
        already recorded in `schema_migrations` under the pre-CR3-MIG-001
        key format (e.g. `prediction/001_schema_code_backfill`) continue to
        match after this rewrite. New migrations follow the same
        zero-padded convention on disk and in the DB.
        """
        return f"{self.namespace}/{self.path.stem}"


def _ensure_tracking_table(conn: ConnectionWrapper) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     VARCHAR PRIMARY KEY,
            applied_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)


def _discover(namespace: str) -> list[Migration]:
    """Parse every `.sql` file under the namespace directory.

    Returns migrations sorted by numeric version. Raises `MigrationError` on
    any malformed filename or duplicate version — there is no silent skip
    path (CR3-MIG-001).
    """
    ns_dir = _MIGRATIONS_ROOT / namespace
    if not ns_dir.is_dir():
        return []

    migrations: list[Migration] = []
    by_version: dict[int, Migration] = {}
    for path in sorted(ns_dir.iterdir()):
        if not path.is_file() or path.suffix != ".sql":
            continue
        match = _VERSION_RE.match(path.name)
        if not match:
            raise MigrationError(
                f"Malformed migration filename {path.name!r} in namespace "
                f"{namespace!r}. Expected `<version>_<name>.sql` where "
                f"<version> is a non-negative integer (e.g. 001_init.sql). "
                "Rename or remove the file and re-run."
            )
        version = int(match.group(1))
        name = match.group(2)
        migration = Migration(
            namespace=namespace,
            version=version,
            name=name,
            path=path,
        )
        existing = by_version.get(version)
        if existing is not None:
            raise MigrationError(
                f"Version collision in namespace {namespace!r}: both "
                f"{existing.path.name!r} and {path.name!r} share numeric "
                f"version {version}. Each migration must have a unique "
                "integer prefix."
            )
        by_version[version] = migration
        migrations.append(migration)

    # Sort by parsed numeric version, not filesystem/string order. This is
    # the fix for CR3-MIG-001 — lexicographic sort ranked 10 before 2 once
    # the width exceeded 9.
    migrations.sort(key=lambda m: m.version)
    return migrations


def _applied_versions(conn: ConnectionWrapper) -> set[str]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {row[0] for row in rows}


def _validate_history(
    namespace: str,
    discovered: list[Migration],
    applied: set[str],
) -> None:
    """Assert that `applied` is a prefix of `discovered` (CR3-MIG-001).

    Two drift classes we catch:
      * **Missing on disk**: a migration recorded in `schema_migrations` no
        longer exists in the discovered set. Almost always means a
        rename/delete that also wipes the tracking row would be required.
      * **Retroactive insertion**: a new on-disk migration has a version
        lower than the highest applied version. Allowing that to run would
        skip its assumed predecessors.
    """
    discovered_keys = {m.key for m in discovered}

    # Applied-but-missing check, scoped to this namespace so other namespaces
    # don't false-positive each other.
    namespace_prefix = f"{namespace}/"
    applied_in_ns = {k for k in applied if k.startswith(namespace_prefix)}
    missing = applied_in_ns - discovered_keys
    if missing:
        raise MigrationError(
            f"Applied migration history drift in namespace {namespace!r}: "
            f"{sorted(missing)!r} recorded in schema_migrations but no "
            "matching file on disk. Restore the file or manually delete the "
            "schema_migrations row after confirming the DB state is consistent."
        )

    # Retroactive-insertion check: the highest applied version establishes a
    # floor for new migrations in the discovered set.
    if not applied_in_ns:
        return
    applied_versions: list[int] = []
    for key in applied_in_ns:
        # key = "<namespace>/<version>_<name>"; parse the version back out.
        tail = key[len(namespace_prefix):]
        m = _VERSION_RE.match(tail + ".sql")
        if m:
            applied_versions.append(int(m.group(1)))
    if not applied_versions:
        return
    highest_applied = max(applied_versions)
    pending_below_floor = [
        m for m in discovered
        if m.key not in applied and m.version < highest_applied
    ]
    if pending_below_floor:
        raise MigrationError(
            f"Retroactive migration insertion detected in namespace "
            f"{namespace!r}: pending {[m.path.name for m in pending_below_floor]} "
            f"has a version below the highest applied version ({highest_applied}). "
            "Back-filled migrations would skip newer predecessors. Re-number "
            "the new migration to a version greater than the highest applied."
        )


def apply_pending(conn: ConnectionWrapper, namespaces: Iterable[str] = ("prediction",)) -> list[str]:
    """Apply every migration in `namespaces` that is not yet recorded.

    Runs each migration file inside an explicit transaction (autocommit is
    temporarily suspended on the raw psycopg2 connection). If the migration
    SQL fails or validation inside its body raises, the transaction rolls
    back and startup fails loudly — partial schema states are never committed.

    Returns the list of `Migration.key` strings that were applied this call.
    Idempotent: calling again with no new migration files is a no-op.

    Raises `MigrationError` on any drift/ordering/filename violation.
    """
    _ensure_tracking_table(conn)
    applied = _applied_versions(conn)

    newly_applied: list[str] = []
    for namespace in namespaces:
        discovered = _discover(namespace)
        _validate_history(namespace, discovered, applied)

        # Sanity-check that our discovered list is strictly monotonic in
        # version number. `_discover` already sorts numerically; this is a
        # belt-and-suspenders guard in case the sort key is tampered with.
        last_version = -1
        for migration in discovered:
            if migration.version <= last_version:
                raise MigrationError(
                    f"Internal migration order violation in namespace "
                    f"{namespace!r}: version {migration.version} is not "
                    f"strictly greater than prior version {last_version}. "
                    "This is a runner bug — report it."
                )
            last_version = migration.version

        for migration in discovered:
            if migration.key in applied:
                continue
            sql = migration.path.read_text(encoding="utf-8")
            logger.info(
                "migration_applying",
                extra={
                    "event": "migration_applying",
                    "migration": migration.key,
                    "path": str(migration.path),
                },
            )
            raw = conn.raw
            previous_autocommit = raw.autocommit
            raw.autocommit = False
            try:
                with raw.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s) "
                        "ON CONFLICT (version) DO NOTHING",
                        [migration.key],
                    )
                raw.commit()
            except Exception:
                raw.rollback()
                logger.error(
                    "migration_failed",
                    extra={"event": "migration_failed", "migration": migration.key},
                    exc_info=True,
                )
                raise
            finally:
                raw.autocommit = previous_autocommit
            newly_applied.append(migration.key)
            logger.info(
                "migration_applied",
                extra={"event": "migration_applied", "migration": migration.key},
            )

    return newly_applied
