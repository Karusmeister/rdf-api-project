"""
Shared DuckDB connection manager.

Single connection lifecycle for the entire application. Both scraper
and prediction modules use this shared connection to the same DB file.
"""

from __future__ import annotations

import os
from typing import Optional

import duckdb

from app.config import settings

_conn: Optional[duckdb.DuckDBPyConnection] = None


def connect() -> duckdb.DuckDBPyConnection:
    """Open the shared DuckDB connection. Idempotent — returns existing if already open."""
    global _conn
    if _conn is not None:
        return _conn
    db_path = settings.scraper_db_path
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    _conn = duckdb.connect(db_path)
    return _conn


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return the shared connection. Raises if not yet opened."""
    if _conn is None:
        raise RuntimeError("DB not connected - call app.db.connection.connect() first")
    return _conn


def close() -> None:
    """Close the shared connection."""
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None


def reset() -> None:
    """Force-clear the connection reference (for test isolation)."""
    global _conn
    _conn = None
