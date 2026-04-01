"""
Shared PostgreSQL connection manager.

Single connection lifecycle for the entire application. Both scraper
and prediction modules use this shared connection to the same database.

Provides a ConnectionWrapper around psycopg2 with a convenient
conn.execute(sql, params).fetchone() / .fetchall() API.

Request-scoped connections are managed via a ContextVar and middleware,
ensuring each request gets its own pooled connection that is returned
on completion. Scripts and tests fall back to the shared connection.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool

from app.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[pool.ThreadedConnectionPool] = None
_conn: Optional[ConnectionWrapper] = None  # type: ignore[assignment]
_request_conn: ContextVar[Optional[ConnectionWrapper]] = ContextVar("_request_conn", default=None)


def init_pool(dsn: str, minconn: int, maxconn: int) -> None:
    """Initialize the ThreadedConnectionPool for concurrent access."""
    global _pool
    if _pool is not None:
        return
    _pool = pool.ThreadedConnectionPool(minconn, maxconn, dsn)
    logger.info("pool_initialized", extra={"event": "pool_initialized", "min": minconn, "max": maxconn})


def close_pool() -> None:
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("pool_closed", extra={"event": "pool_closed"})


class ConnectionWrapper:
    """Wraps a psycopg2 connection with a convenient execute().fetchone() API.

    psycopg2's cursor-based API requires separate execute() and fetch calls.
    This wrapper lets call-sites use:
        row = conn.execute("SELECT ... WHERE x = %s", [val]).fetchone()
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
    raw.autocommit = True
    _conn = ConnectionWrapper(raw)
    logger.info("db_connected", extra={"event": "db_connected", "dsn": dsn.split("@")[-1]})
    return _conn


def get_conn() -> ConnectionWrapper:
    """Return the best available connection.

    Priority:
    1. Request-scoped pooled connection (set by middleware during HTTP requests)
    2. Shared global connection (for CLI scripts, jobs, tests)
    """
    req = _request_conn.get()
    if req is not None:
        return req
    if _conn is None or _conn.closed:
        raise RuntimeError("DB not connected - call app.db.connection.connect() first")
    return _conn


# ---------------------------------------------------------------------------
# Request-scoped connection lifecycle (used by middleware)
# ---------------------------------------------------------------------------

def acquire_request_conn() -> None:
    """Acquire a pooled connection and bind it to the current request context."""
    if _pool is None:
        return  # no pool → fall back to shared conn via get_conn()
    raw = _pool.getconn()
    raw.autocommit = True
    _request_conn.set(ConnectionWrapper(raw))


def release_request_conn() -> None:
    """Return the request-scoped connection to the pool."""
    if _pool is None:
        return
    wrapper = _request_conn.get()
    if wrapper is not None:
        _pool.putconn(wrapper.raw)
        _request_conn.set(None)


# ---------------------------------------------------------------------------
# Standalone helpers
# ---------------------------------------------------------------------------

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
    _request_conn.set(None)


@contextmanager
def get_db():
    """Context manager: per-request pooled connection. Autocommit for consistency with shared conn.

    Usage from FastAPI endpoints that need concurrency-safe DB access:
        with get_db() as conn:
            conn.execute(...)
    Falls back to the shared connection if pool is not initialized (tests, CLI scripts).
    """
    if _pool is None:
        yield get_conn()
        return
    raw = _pool.getconn()
    raw.autocommit = True
    try:
        yield ConnectionWrapper(raw)
    finally:
        _pool.putconn(raw)


def make_connection(dsn: str) -> ConnectionWrapper:
    """Create a standalone PostgreSQL connection (for batch workers)."""
    raw = psycopg2.connect(dsn)
    raw.autocommit = True
    return ConnectionWrapper(raw)
