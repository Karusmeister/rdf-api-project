"""
Pipeline database connection manager.

A completely separate connection layer from app.db.connection / app.db.prediction_db.
Points at the PIPELINE_DATABASE_URL (a dedicated Cloud SQL instance in production,
or a second local Postgres on port 5433 for dev).

The pipeline database holds analytical/prediction state:
    - companies, financial_reports, raw_financial_data, financial_line_items
    - feature_definitions, feature_sets, feature_set_members, computed_features
    - model_registry, prediction_runs, predictions
    - bankruptcy_events, etl_attempts
    - pipeline_queue, pipeline_runs, population_stats  (pipeline-only)

The scraper database (rdf-postgres) is NEVER written to by the pipeline. This
module exposes independent connect()/get_conn()/init_pool()/close() so the
existing connection layer is untouched.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional

import psycopg2
from psycopg2 import pool

from app.config import settings
from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)

_pool: Optional[pool.ThreadedConnectionPool] = None
_conn: Optional[ConnectionWrapper] = None
_request_conn: ContextVar[Optional[ConnectionWrapper]] = ContextVar(
    "_pipeline_request_conn", default=None
)
_schema_initialized = False


def connect() -> ConnectionWrapper:
    """Open the shared pipeline DB connection and ensure schema. Idempotent."""
    global _conn
    if _conn is not None and not _conn.closed:
        _ensure_schema()
        return _conn
    raw = psycopg2.connect(settings.pipeline_database_url)
    raw.autocommit = True
    _conn = ConnectionWrapper(raw)
    logger.info(
        "pipeline_db_connected",
        extra={"event": "pipeline_db_connected",
               "dsn": settings.pipeline_database_url.split("@")[-1]},
    )
    _ensure_schema()
    return _conn


def init_pool(minconn: Optional[int] = None, maxconn: Optional[int] = None) -> None:
    global _pool
    if _pool is not None:
        return
    _pool = pool.ThreadedConnectionPool(
        minconn or settings.pipeline_db_pool_min,
        maxconn or settings.pipeline_db_pool_max,
        settings.pipeline_database_url,
    )
    logger.info("pipeline_pool_initialized", extra={"event": "pipeline_pool_initialized"})


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


def close() -> None:
    global _conn
    if _conn is not None and not _conn.closed:
        _conn.close()
    _conn = None
    close_pool()


def get_conn() -> ConnectionWrapper:
    """Return request-scoped pooled conn if set, else the shared singleton."""
    req = _request_conn.get()
    if req is not None:
        return req
    if _conn is None or _conn.closed:
        raise RuntimeError(
            "Pipeline DB not connected — call app.db.pipeline_db.connect() first"
        )
    return _conn


# ---- request-scoped helpers (mirror connection.py) -----------------------

def acquire_request_conn() -> None:
    if _pool is None:
        return
    raw = _pool.getconn()
    raw.autocommit = True
    _request_conn.set(ConnectionWrapper(raw))


def release_request_conn() -> None:
    if _pool is None:
        return
    wrapper = _request_conn.get()
    if wrapper is not None:
        _pool.putconn(wrapper.raw)
        _request_conn.set(None)


@contextmanager
def get_db():
    if _pool is None:
        yield get_conn()
        return
    raw = _pool.getconn()
    raw.autocommit = True
    try:
        yield ConnectionWrapper(raw)
    finally:
        _pool.putconn(raw)


def reset() -> None:
    """Force-clear state (tests)."""
    global _conn, _schema_initialized
    _conn = None
    _schema_initialized = False
    _request_conn.set(None)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    _init_schema()
    _schema_initialized = True


def _init_schema() -> None:
    """Create all pipeline tables. Idempotent. Auth tables are NOT included —
    those live in the scraper/rdf-postgres database."""
    conn = get_conn()

    # ---- Layer 1: Core Entity Registry ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            krs             VARCHAR(10) PRIMARY KEY,
            nip             VARCHAR(13),
            regon           VARCHAR(14),
            pkd_code        VARCHAR(10),
            incorporation_date DATE,
            voivodeship     VARCHAR(100),
            updated_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # ---- Layer 2: Financial Data ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial_reports (
            id              VARCHAR PRIMARY KEY,
            logical_key     VARCHAR NOT NULL,
            report_version  INTEGER NOT NULL DEFAULT 1,
            supersedes_report_id VARCHAR,
            krs             VARCHAR(10) NOT NULL,
            data_source_id  VARCHAR NOT NULL DEFAULT 'KRS',
            report_type     VARCHAR(20) NOT NULL DEFAULT 'annual',
            fiscal_year     INTEGER NOT NULL,
            period_start    DATE NOT NULL,
            period_end      DATE NOT NULL,
            taxonomy_version VARCHAR(50),
            source_document_id VARCHAR,
            source_file_path   VARCHAR,
            schema_code     VARCHAR(10),
            ingestion_status VARCHAR(20) DEFAULT 'pending',
            ingestion_error VARCHAR,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(logical_key, report_version)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_financial_data (
            report_id       VARCHAR NOT NULL,
            section         VARCHAR(30) NOT NULL,
            extraction_version INTEGER NOT NULL DEFAULT 1,
            data_json       JSON NOT NULL,
            taxonomy_version VARCHAR(50),
            created_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY(report_id, section, extraction_version)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial_line_items (
            report_id       VARCHAR NOT NULL,
            section         VARCHAR(30) NOT NULL,
            tag_path        VARCHAR(200) NOT NULL,
            extraction_version INTEGER NOT NULL DEFAULT 1,
            label_pl        VARCHAR(500),
            value_current   DOUBLE PRECISION,
            value_previous  DOUBLE PRECISION,
            currency        VARCHAR(3) DEFAULT 'PLN',
            schema_code     VARCHAR(10),
            PRIMARY KEY(report_id, section, tag_path, extraction_version)
        )
    """)

    # ---- Layer 3: Feature Engineering ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_definitions (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            description     VARCHAR,
            category        VARCHAR(50),
            formula_description VARCHAR,
            formula_numerator   VARCHAR(200),
            formula_denominator VARCHAR(200),
            required_tags   JSON,
            computation_logic VARCHAR(20) DEFAULT 'ratio',
            version         INTEGER DEFAULT 1,
            is_active       BOOLEAN DEFAULT true,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_sets (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            description     VARCHAR,
            is_active       BOOLEAN DEFAULT true,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_set_members (
            feature_set_id      VARCHAR NOT NULL,
            feature_definition_id VARCHAR NOT NULL,
            ordinal             INTEGER NOT NULL,
            PRIMARY KEY(feature_set_id, feature_definition_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS computed_features (
            report_id               VARCHAR NOT NULL,
            feature_definition_id   VARCHAR NOT NULL,
            krs                     VARCHAR(10) NOT NULL,
            fiscal_year             INTEGER NOT NULL,
            value                   DOUBLE PRECISION,
            is_valid                BOOLEAN DEFAULT true,
            error_message           VARCHAR,
            source_extraction_version INTEGER NOT NULL DEFAULT 1,
            computation_version     INTEGER DEFAULT 1,
            computed_at             TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY(report_id, feature_definition_id, computation_version)
        )
    """)

    # ---- Layer 4: Models & Predictions ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_registry (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            model_type      VARCHAR(50) NOT NULL,
            version         VARCHAR(20) NOT NULL,
            feature_set_id  VARCHAR,
            description     VARCHAR,
            hyperparameters JSON,
            training_metrics JSON,
            training_date   TIMESTAMP,
            training_data_spec JSON,
            artifact_path   VARCHAR,
            is_active       BOOLEAN DEFAULT true,
            is_baseline     BOOLEAN DEFAULT false,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(name, version)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS prediction_runs (
            id              VARCHAR PRIMARY KEY,
            model_id        VARCHAR NOT NULL,
            run_date        TIMESTAMP DEFAULT current_timestamp,
            parameters      JSON,
            companies_scored INTEGER,
            status          VARCHAR(20) DEFAULT 'running',
            error_message   VARCHAR,
            duration_seconds DOUBLE PRECISION,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              VARCHAR PRIMARY KEY,
            prediction_run_id VARCHAR NOT NULL,
            krs             VARCHAR(10) NOT NULL,
            report_id       VARCHAR NOT NULL,
            raw_score       DOUBLE PRECISION,
            probability     DOUBLE PRECISION,
            classification  SMALLINT,
            risk_category   VARCHAR(20),
            feature_contributions JSON,
            feature_snapshot JSON,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)
    # Parity with app/db/prediction_db.py on main (CR-PZN / multi-year
    # predictions): feature_snapshot was added after the initial pipeline_db
    # schema. Backfill the column for any pre-existing pipeline DB.
    conn.execute("""
        ALTER TABLE predictions
            ADD COLUMN IF NOT EXISTS feature_snapshot JSON
    """)

    # ---- Layer 5: Ground Truth ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bankruptcy_events (
            id              VARCHAR PRIMARY KEY,
            krs             VARCHAR(10) NOT NULL,
            event_type      VARCHAR(30) NOT NULL,
            event_date      DATE NOT NULL,
            data_source_id  VARCHAR,
            court_case_ref  VARCHAR(200),
            announcement_id VARCHAR(200),
            is_confirmed    BOOLEAN DEFAULT false,
            notes           VARCHAR,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(krs, event_type, event_date)
        )
    """)

    # ---- ETL attempts ----
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_etl_attempts START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_attempts (
            attempt_id       BIGINT PRIMARY KEY DEFAULT nextval('seq_etl_attempts'),
            document_id      VARCHAR NOT NULL,
            krs              VARCHAR(10),
            started_at       TIMESTAMP NOT NULL DEFAULT current_timestamp,
            finished_at      TIMESTAMP,
            status           VARCHAR NOT NULL,
            reason_code      VARCHAR,
            error_message    VARCHAR,
            xml_path         VARCHAR,
            report_id        VARCHAR,
            extraction_version INTEGER
        )
    """)

    # ---- Pipeline orchestration ----
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_queue (
            krs             VARCHAR(10) NOT NULL,
            document_key    VARCHAR NOT NULL DEFAULT '__none__',
            trigger_reason  VARCHAR(50) NOT NULL,
            document_id     VARCHAR,
            queued_at       TIMESTAMP DEFAULT now(),
            status          VARCHAR(20) DEFAULT 'pending',
            pipeline_run_id INTEGER,
            completed_at    TIMESTAMP,
            error_message   VARCHAR,
            PRIMARY KEY (krs, document_key)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_queue_pending
            ON pipeline_queue(status) WHERE status = 'pending'
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_queue_run
            ON pipeline_queue(pipeline_run_id)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id              SERIAL PRIMARY KEY,
            started_at          TIMESTAMP NOT NULL DEFAULT now(),
            finished_at         TIMESTAMP,
            status              VARCHAR(20) NOT NULL DEFAULT 'running',
            trigger             VARCHAR(50),
            krs_queued          INTEGER DEFAULT 0,
            krs_processed       INTEGER DEFAULT 0,
            krs_failed          INTEGER DEFAULT 0,
            etl_docs_parsed     INTEGER DEFAULT 0,
            etl_line_items_written INTEGER DEFAULT 0,
            etl_duration_seconds REAL,
            features_computed   INTEGER DEFAULT 0,
            features_failed     INTEGER DEFAULT 0,
            features_duration_seconds REAL,
            predictions_written INTEGER DEFAULT 0,
            predictions_duration_seconds REAL,
            bq_sync_rows        INTEGER DEFAULT 0,
            bq_sync_duration_seconds REAL,
            stats_refreshed     BOOLEAN DEFAULT FALSE,
            stats_duration_seconds REAL,
            total_duration_seconds REAL,
            error_message       VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS population_stats (
            pkd_code        VARCHAR(10),
            tenure_bucket   VARCHAR(20),
            model_id        VARCHAR NOT NULL,
            mean_score      DOUBLE PRECISION,
            stddev_score    DOUBLE PRECISION,
            p25             DOUBLE PRECISION,
            p50             DOUBLE PRECISION,
            p75             DOUBLE PRECISION,
            p90             DOUBLE PRECISION,
            p95             DOUBLE PRECISION,
            sample_size     INTEGER,
            computed_at     TIMESTAMP DEFAULT now(),
            PRIMARY KEY (pkd_code, tenure_bucket, model_id)
        )
    """)

    # ---- Indexes ----
    existing = {
        row[0] for row in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        ).fetchall()
    }
    for name, sql in [
        ("idx_pipeline_companies_pkd",
         "CREATE INDEX idx_pipeline_companies_pkd ON companies(pkd_code)"),
        ("idx_pipeline_line_items_tag",
         "CREATE INDEX idx_pipeline_line_items_tag ON financial_line_items(tag_path)"),
        ("idx_pipeline_reports_krs",
         "CREATE INDEX idx_pipeline_reports_krs ON financial_reports(krs)"),
        ("idx_pipeline_reports_year",
         "CREATE INDEX idx_pipeline_reports_year ON financial_reports(fiscal_year)"),
        ("idx_pipeline_reports_logical",
         "CREATE INDEX idx_pipeline_reports_logical ON financial_reports(logical_key, report_version)"),
        ("idx_pipeline_features_krs",
         "CREATE INDEX idx_pipeline_features_krs ON computed_features(krs)"),
        ("idx_pipeline_features_year",
         "CREATE INDEX idx_pipeline_features_year ON computed_features(fiscal_year)"),
        ("idx_pipeline_predictions_krs",
         "CREATE INDEX idx_pipeline_predictions_krs ON predictions(krs)"),
        ("idx_pipeline_predictions_run",
         "CREATE INDEX idx_pipeline_predictions_run ON predictions(prediction_run_id)"),
    ]:
        if name not in existing:
            try:
                conn.execute(sql)
            except Exception:
                pass

    # ---- Latest-version views (matching prediction_db) ----
    conn.execute("""
        CREATE OR REPLACE VIEW latest_financial_reports AS
        SELECT id, logical_key, report_version, supersedes_report_id, krs,
               data_source_id, report_type, fiscal_year, period_start, period_end,
               taxonomy_version, source_document_id, source_file_path,
               ingestion_status, ingestion_error, created_at, schema_code
        FROM (
            SELECT fr.*, row_number() OVER (
                PARTITION BY fr.logical_key
                ORDER BY fr.report_version DESC, fr.created_at DESC, fr.id DESC
            ) AS version_rank
            FROM financial_reports fr
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_financial_line_items AS
        SELECT report_id, section, tag_path, extraction_version, label_pl,
               value_current, value_previous, currency, schema_code
        FROM (
            SELECT fli.*, row_number() OVER (
                PARTITION BY fli.report_id, fli.section, fli.tag_path
                ORDER BY fli.extraction_version DESC
            ) AS version_rank
            FROM financial_line_items fli
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_computed_features AS
        SELECT report_id, feature_definition_id, krs, fiscal_year, value,
               is_valid, error_message, source_extraction_version,
               computation_version, computed_at
        FROM (
            SELECT cf.*, row_number() OVER (
                PARTITION BY cf.report_id, cf.feature_definition_id
                ORDER BY cf.computation_version DESC, cf.computed_at DESC
            ) AS version_rank
            FROM computed_features cf
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_successful_financial_reports AS
        SELECT id, logical_key, report_version, supersedes_report_id, krs,
               data_source_id, report_type, fiscal_year, period_start, period_end,
               taxonomy_version, source_document_id, source_file_path,
               ingestion_status, ingestion_error, created_at, schema_code
        FROM (
            SELECT fr.*, row_number() OVER (
                PARTITION BY fr.logical_key
                ORDER BY fr.report_version DESC, fr.created_at DESC, fr.id DESC
            ) AS rn
            FROM financial_reports fr
            WHERE fr.ingestion_status = 'completed'
        ) ranked
        WHERE rn = 1
    """)
