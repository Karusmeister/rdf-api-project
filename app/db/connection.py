"""
Shared PostgreSQL connection manager.

Single connection lifecycle for the entire application. Both scraper
and prediction modules use this shared connection to the same database.

Provides a ConnectionWrapper that preserves the DuckDB-style
conn.execute(sql, params).fetchone() / .fetchall() API so that
callers require minimal changes.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool

from app.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[pool.ThreadedConnectionPool] = None
_conn: Optional[ConnectionWrapper] = None  # type: ignore[assignment]


class ConnectionWrapper:
    """Wraps a psycopg2 connection to match DuckDB's execute().fetchone() API.

    DuckDB: conn.execute(sql, params) returns a result set with .fetchone()/.fetchall().
    psycopg2: conn.cursor().execute(sql, params) returns None; results are on the cursor.

    This wrapper bridges the gap so call-sites like:
        row = conn.execute("SELECT ... WHERE x = %s", [val]).fetchone()
    continue to work without modification.
    """

    def __init__(self, conn: psycopg2.extensions.connection):
        self._conn = conn

    def execute(self, sql: str, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    @property
    def closed(self):
        return self._conn.closed

    @property
    def raw(self):
        """Access the underlying psycopg2 connection."""
        return self._conn


def connect() -> ConnectionWrapper:
    """Open the shared PostgreSQL connection. Idempotent — returns existing if already open."""
    global _conn
    if _conn is not None and not _conn.closed:
        return _conn
    dsn = settings.database_url
    raw = psycopg2.connect(dsn)
    raw.autocommit = True  # match DuckDB's per-statement autocommit behavior
    _conn = ConnectionWrapper(raw)
    logger.info("db_connected", extra={"event": "db_connected", "dsn": dsn.split("@")[-1]})
    return _conn


def get_conn() -> ConnectionWrapper:
    """Return the shared connection. Raises if not yet opened."""
    if _conn is None or _conn.closed:
        raise RuntimeError("DB not connected - call app.db.connection.connect() first")
    return _conn


def close() -> None:
    """Close the shared connection."""
    global _conn
    if _conn is not None and not _conn.closed:
        _conn.close()
        _conn = None
        logger.info("db_closed", extra={"event": "db_closed"})


def reset() -> None:
    """Force-clear the connection reference (for test isolation)."""
    global _conn
    _conn = None


@contextmanager
def get_db():
    """Context manager that gets a connection from the pool, commits on success, rolls back on error."""
    if _pool is None:
        raise RuntimeError("Connection pool not initialized")
    raw = _pool.getconn()
    try:
        yield ConnectionWrapper(raw)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        _pool.putconn(raw)


def make_connection(dsn: str) -> ConnectionWrapper:
    """Create a standalone PostgreSQL connection (for batch workers)."""
    raw = psycopg2.connect(dsn)
    raw.autocommit = True
    return ConnectionWrapper(raw)
