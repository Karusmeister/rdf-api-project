"""Pipeline connection helpers.

Opens independent psycopg2 connections to the two databases:
    - scraper_dsn  (rdf-postgres, read-only access from pipeline code)
    - pipeline_dsn (rdf-pipeline, read/write)

When the pipeline runs under the FastAPI lifespan (for tests, API-triggered
runs), the normal `app.db.connection.get_conn()` and
`app.db.pipeline_db.get_conn()` are used instead — those may yield
request-scoped pooled connections.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import psycopg2

from app.config import settings
from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)


@dataclass
class DualConns:
    scraper: ConnectionWrapper
    pipeline: ConnectionWrapper


@contextmanager
def open_dual_connections(
    scraper_dsn: str | None = None,
    pipeline_dsn: str | None = None,
) -> Iterator[DualConns]:
    """Open fresh connections to both DBs for the duration of a pipeline run.

    Autocommit = True on both so statements commit immediately (matches the
    rest of the codebase's pattern).
    """
    scraper_dsn = scraper_dsn or settings.database_url
    pipeline_dsn = pipeline_dsn or settings.pipeline_database_url

    scraper_raw = psycopg2.connect(scraper_dsn)
    scraper_raw.autocommit = True
    scraper_raw.set_session(readonly=True)

    pipeline_raw = psycopg2.connect(pipeline_dsn)
    pipeline_raw.autocommit = True

    try:
        yield DualConns(
            scraper=ConnectionWrapper(scraper_raw),
            pipeline=ConnectionWrapper(pipeline_raw),
        )
    finally:
        try:
            scraper_raw.close()
        except Exception:
            pass
        try:
            pipeline_raw.close()
        except Exception:
            pass
