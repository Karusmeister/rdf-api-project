from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from app.db import connection as shared_conn

_schema_initialized = False
logger = logging.getLogger(__name__)


def _cursor_to_dicts(cursor) -> list[dict]:
    """Convert cursor rows to list of dicts using cursor.description column names."""
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def connect() -> None:
    """Ensure shared connection is open and prediction schema exists."""
    shared_conn.connect()
    _ensure_schema()


def close() -> None:
    """No-op. Connection lifecycle is managed by app.db.connection."""
    pass


def get_conn():
    """Return the shared database connection."""
    return shared_conn.get_conn()


def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    _init_schema()
    _schema_initialized = True


def _init_schema() -> None:
    """Create all prediction engine tables if they don't exist. Idempotent."""
    conn = get_conn()

    # ----- Layer 1: Core Entity Registry -----

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

    # ----- Layer 2: Financial Data -----

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
            ingestion_status VARCHAR(20) DEFAULT 'pending',
            ingestion_error  VARCHAR,
            schema_code     VARCHAR(10),
            created_at       TIMESTAMP DEFAULT current_timestamp,
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

    # ----- Layer 3: Feature Engineering -----

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

    # ----- Layer 4: Model Registry and Predictions -----

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

    # ----- Layer 5: Ground Truth -----

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

    # ----- ETL Attempts -----

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_etl_attempts START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_attempts (
            attempt_id            BIGINT PRIMARY KEY DEFAULT nextval('seq_etl_attempts'),
            document_id           VARCHAR NOT NULL,
            krs                   VARCHAR(10),
            started_at            TIMESTAMP NOT NULL DEFAULT current_timestamp,
            finished_at           TIMESTAMP,
            status                VARCHAR NOT NULL,
            reason_code           VARCHAR,
            error_message         VARCHAR,
            xml_path              VARCHAR,
            report_id             VARCHAR,
            extraction_version    INTEGER
        )
    """)

    # ----- Job tracking -----

    conn.execute("""
        CREATE TABLE IF NOT EXISTS assessment_jobs (
            id              VARCHAR PRIMARY KEY,
            krs             VARCHAR(10) NOT NULL,
            status          VARCHAR(30) DEFAULT 'pending',
            stage           VARCHAR(50),
            error_message   VARCHAR,
            result_json     JSON,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            updated_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # ----- Auth tables -----

    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id              VARCHAR PRIMARY KEY,
            email           VARCHAR NOT NULL UNIQUE,
            name            VARCHAR,
            auth_method     VARCHAR(20) NOT NULL,
            password_hash   VARCHAR,
            is_verified     BOOLEAN DEFAULT false,
            has_full_access BOOLEAN DEFAULT false,
            is_active       BOOLEAN DEFAULT true,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            last_login_at   TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS verification_codes (
            id              VARCHAR PRIMARY KEY,
            user_id         VARCHAR NOT NULL REFERENCES users(id),
            code            VARCHAR(6) NOT NULL,
            purpose         VARCHAR(20) NOT NULL,
            expires_at      TIMESTAMP NOT NULL,
            used_at         TIMESTAMP,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_krs_access (
            user_id         VARCHAR NOT NULL REFERENCES users(id),
            krs             VARCHAR(10) NOT NULL,
            granted_at      TIMESTAMP DEFAULT current_timestamp,
            granted_by      VARCHAR,
            PRIMARY KEY (user_id, krs)
        )
    """)

    # ----- Password Reset Tokens -----

    conn.execute("""
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id              VARCHAR PRIMARY KEY,
            user_id         VARCHAR NOT NULL REFERENCES users(id),
            token_hash      VARCHAR NOT NULL,
            expires_at      TIMESTAMP NOT NULL,
            used_at         TIMESTAMP,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    # Deduplicate any active reset-token hash collisions before enforcing unique index.
    # Keep newest active token per hash; mark older active duplicates as used.
    deduped = conn.execute("""
        WITH duplicate_hashes AS (
            SELECT token_hash
            FROM password_reset_tokens
            WHERE used_at IS NULL
            GROUP BY token_hash
            HAVING count(*) > 1
        ),
        ranked AS (
            SELECT
                prt.id,
                row_number() OVER (
                    PARTITION BY prt.token_hash
                    ORDER BY prt.created_at DESC, prt.id DESC
                ) AS rn
            FROM password_reset_tokens prt
            JOIN duplicate_hashes d ON d.token_hash = prt.token_hash
            WHERE prt.used_at IS NULL
        ),
        marked AS (
            UPDATE password_reset_tokens t
            SET used_at = current_timestamp
            FROM ranked r
            WHERE t.id = r.id
              AND r.rn > 1
            RETURNING t.id
        )
        SELECT count(*) FROM marked
    """).fetchone()
    deduped_count = int(deduped[0] or 0)
    if deduped_count > 0:
        logger.warning(
            "reset_token_hash_dedup_applied",
            extra={"event": "reset_token_hash_dedup_applied", "rows_marked_used": deduped_count},
        )

    # ----- Activity Log -----

    conn.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id          BIGSERIAL PRIMARY KEY,
            user_id     VARCHAR REFERENCES users(id),
            action      VARCHAR(50) NOT NULL,
            krs_number  VARCHAR(10),
            detail      JSONB,
            ip_address  INET,
            created_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # Indexes (check existing first to stay idempotent)
    existing_rows = conn.execute(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'"
    ).fetchall()
    existing = {row[0] for row in existing_rows}
    indexdef_by_name = {row[0]: row[1] for row in existing_rows}

    # Migrate old full unique index to partial unique index (active tokens only).
    reset_idx_def = indexdef_by_name.get("idx_reset_token_hash")
    if reset_idx_def and "WHERE (used_at IS NULL)" not in reset_idx_def:
        conn.execute("DROP INDEX idx_reset_token_hash")
        existing.discard("idx_reset_token_hash")
        logger.info(
            "reset_token_index_migrated",
            extra={"event": "reset_token_index_migrated", "index": "idx_reset_token_hash"},
        )

    index_defs = [
        ("idx_companies_pkd",       "CREATE INDEX idx_companies_pkd ON companies(pkd_code)"),
        ("idx_companies_nip",       "CREATE INDEX idx_companies_nip ON companies(nip)"),
        ("idx_line_items_tag",      "CREATE INDEX idx_line_items_tag ON financial_line_items(tag_path)"),
        ("idx_reports_logical",     "CREATE INDEX idx_reports_logical ON financial_reports(logical_key, report_version)"),
        ("idx_reports_krs",         "CREATE INDEX idx_reports_krs ON financial_reports(krs)"),
        ("idx_reports_year",        "CREATE INDEX idx_reports_year ON financial_reports(fiscal_year)"),
        ("idx_features_krs",        "CREATE INDEX idx_features_krs ON computed_features(krs)"),
        ("idx_features_year",       "CREATE INDEX idx_features_year ON computed_features(fiscal_year)"),
        ("idx_predictions_krs",     "CREATE INDEX idx_predictions_krs ON predictions(krs)"),
        ("idx_predictions_risk",    "CREATE INDEX idx_predictions_risk ON predictions(risk_category)"),
        ("idx_bankruptcy_krs",      "CREATE INDEX idx_bankruptcy_krs ON bankruptcy_events(krs)"),
        ("idx_bankruptcy_date",     "CREATE INDEX idx_bankruptcy_date ON bankruptcy_events(event_date)"),
        ("idx_assess_jobs_krs",     "CREATE INDEX idx_assess_jobs_krs ON assessment_jobs(krs)"),
        ("idx_etl_attempts_doc",    "CREATE INDEX idx_etl_attempts_doc ON etl_attempts(document_id)"),
        ("idx_etl_attempts_status", "CREATE INDEX idx_etl_attempts_status ON etl_attempts(status)"),
        ("idx_verification_user",   "CREATE INDEX idx_verification_user ON verification_codes(user_id, purpose)"),
        ("idx_user_krs",            "CREATE INDEX idx_user_krs ON user_krs_access(user_id)"),
        ("idx_activity_krs",        "CREATE INDEX idx_activity_krs ON activity_log(krs_number, created_at DESC)"),
        ("idx_activity_user",       "CREATE INDEX idx_activity_user ON activity_log(user_id, created_at DESC)"),
        ("idx_activity_time",       "CREATE INDEX idx_activity_time ON activity_log(created_at DESC)"),
        ("idx_reset_token_hash",    "CREATE UNIQUE INDEX idx_reset_token_hash ON password_reset_tokens(token_hash) WHERE used_at IS NULL"),
        ("idx_reset_token_lookup",  "CREATE INDEX idx_reset_token_lookup ON password_reset_tokens(token_hash, expires_at, created_at DESC) WHERE used_at IS NULL"),
        ("idx_reset_token_user",    "CREATE INDEX idx_reset_token_user ON password_reset_tokens(user_id, used_at, expires_at)"),
        # CR-PZN-003: supports `_find_unscored_reports` in model scorers. The
        # unscored-lookup query filters `computed_features` by
        # feature_definition_id with `is_valid = true`, so a partial index on
        # (feature_definition_id, report_id) keyed on the valid rows keeps that
        # path linear in the number of target features rather than scanning the
        # whole table.
        ("idx_features_valid_fid_report",
         "CREATE INDEX idx_features_valid_fid_report ON computed_features(feature_definition_id, report_id) WHERE is_valid = true"),
    ]

    for name, sql in index_defs:
        if name not in existing:
            conn.execute(sql)

    # CR2-OPS-004: ALTER TABLE + UPDATE backfills that used to run here now
    # live in migrations/prediction/*.sql and are applied via
    # `app.db.migrations.apply_pending()` after `_init_schema` completes. This
    # file is responsible only for idempotent CREATE operations that are safe
    # to re-run on every boot (tables, indexes, views).
    for idx_name, idx_sql in [
        ("idx_reports_schema", "CREATE INDEX idx_reports_schema ON financial_reports(schema_code)"),
        ("idx_line_items_schema", "CREATE INDEX idx_line_items_schema ON financial_line_items(schema_code)"),
    ]:
        if idx_name not in existing:
            conn.execute(idx_sql)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_financial_reports AS
        SELECT
            id,
            logical_key,
            report_version,
            supersedes_report_id,
            krs,
            data_source_id,
            report_type,
            fiscal_year,
            period_start,
            period_end,
            taxonomy_version,
            source_document_id,
            source_file_path,
            ingestion_status,
            ingestion_error,
            created_at,
            schema_code
        FROM (
            SELECT
                fr.*,
                row_number() OVER (
                    PARTITION BY fr.logical_key
                    ORDER BY fr.report_version DESC, fr.created_at DESC, fr.id DESC
                ) AS version_rank
            FROM financial_reports fr
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_raw_financial_data AS
        SELECT
            report_id,
            section,
            extraction_version,
            data_json,
            taxonomy_version,
            created_at
        FROM (
            SELECT
                rfd.*,
                row_number() OVER (
                    PARTITION BY rfd.report_id, rfd.section
                    ORDER BY rfd.extraction_version DESC, rfd.created_at DESC
                ) AS version_rank
            FROM raw_financial_data rfd
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_financial_line_items AS
        SELECT
            report_id,
            section,
            tag_path,
            extraction_version,
            label_pl,
            value_current,
            value_previous,
            currency,
            schema_code
        FROM (
            SELECT
                fli.*,
                row_number() OVER (
                    PARTITION BY fli.report_id, fli.section, fli.tag_path
                    ORDER BY fli.extraction_version DESC
                ) AS version_rank
            FROM financial_line_items fli
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_computed_features AS
        SELECT
            report_id,
            feature_definition_id,
            krs,
            fiscal_year,
            value,
            is_valid,
            error_message,
            source_extraction_version,
            computation_version,
            computed_at
        FROM (
            SELECT
                cf.*,
                row_number() OVER (
                    PARTITION BY cf.report_id, cf.feature_definition_id
                    ORDER BY cf.computation_version DESC, cf.computed_at DESC
                ) AS version_rank
            FROM computed_features cf
        ) ranked
        WHERE version_rank = 1
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW latest_successful_financial_reports AS
        SELECT
            id, logical_key, report_version, supersedes_report_id, krs,
            data_source_id, report_type, fiscal_year, period_start, period_end,
            taxonomy_version, source_document_id, source_file_path,
            ingestion_status, ingestion_error, created_at, schema_code
        FROM (
            SELECT
                fr.*,
                row_number() OVER (
                    PARTITION BY fr.logical_key
                    ORDER BY fr.report_version DESC, fr.created_at DESC, fr.id DESC
                ) AS rn
            FROM financial_reports fr
            WHERE fr.ingestion_status = 'completed'
        ) ranked
        WHERE rn = 1
    """)


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------


def _build_logical_report_key(
    krs: str,
    data_source_id: str,
    report_type: str,
    fiscal_year: int,
    period_end: str,
) -> str:
    return f"{krs}|{data_source_id}|{report_type}|{fiscal_year}|{period_end}"


def get_latest_extraction_version(report_id: str) -> int:
    conn = get_conn()
    row = conn.execute("""
        WITH extraction_versions AS (
            SELECT coalesce(max(extraction_version), 0) AS version
            FROM raw_financial_data
            WHERE report_id = %s

            UNION ALL

            SELECT coalesce(max(extraction_version), 0) AS version
            FROM financial_line_items
            WHERE report_id = %s
        )
        SELECT coalesce(max(version), 0) FROM extraction_versions
    """, [report_id, report_id]).fetchone()
    return int(row[0] or 0)


def get_next_extraction_version(report_id: str) -> int:
    return get_latest_extraction_version(report_id) + 1

# --- companies ---

def upsert_company(krs: str, nip: Optional[str] = None, regon: Optional[str] = None,
                   pkd_code: Optional[str] = None, incorporation_date: Optional[str] = None,
                   voivodeship: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO companies (krs, nip, regon, pkd_code, incorporation_date, voivodeship, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (krs) DO UPDATE SET
            nip               = COALESCE(excluded.nip, companies.nip),
            regon             = COALESCE(excluded.regon, companies.regon),
            pkd_code          = COALESCE(excluded.pkd_code, companies.pkd_code),
            incorporation_date = COALESCE(excluded.incorporation_date, companies.incorporation_date),
            voivodeship       = COALESCE(excluded.voivodeship, companies.voivodeship),
            updated_at        = excluded.updated_at
    """, [krs, nip, regon, pkd_code, incorporation_date, voivodeship, now])


def get_company(krs: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT krs, nip, regon, pkd_code, incorporation_date, voivodeship FROM companies WHERE krs = %s", [krs]
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["krs", "nip", "regon", "pkd_code", "incorporation_date", "voivodeship"], row))


# --- financial_reports ---

def create_financial_report(report_id: str, krs: str, fiscal_year: int, period_start: str,
                             period_end: str, report_type: str = "annual",
                             data_source_id: str = "KRS",
                             taxonomy_version: Optional[str] = None,
                             source_document_id: Optional[str] = None,
                             source_file_path: Optional[str] = None,
                             schema_code: Optional[str] = None) -> Optional[str]:
    """Create a financial report and preserve correction history.

    Handles three cases:
    1. Same report_id (retry/re-ingest): refresh mutable ETL status fields in place.
    2. Different report_id, same business key (correction): insert a new report_version
       row and keep the earlier filing available for audit/history queries.
    3. Brand new report: insert report_version = 1.

    Returns the previous latest report_id for the same logical report, if any.
    """
    conn = get_conn()
    now = datetime.now(timezone.utc)
    logical_key = _build_logical_report_key(krs, data_source_id, report_type, fiscal_year, period_end)

    existing_by_id = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id
        FROM financial_reports
        WHERE id = %s
    """, [report_id]).fetchone()

    if existing_by_id is not None:
        conn.execute("""
            UPDATE financial_reports
            SET ingestion_status = 'pending',
                ingestion_error = NULL,
                source_file_path = %s,
                taxonomy_version = %s,
                source_document_id = coalesce(%s, source_document_id),
                schema_code = coalesce(%s, schema_code)
            WHERE id = %s
        """, [source_file_path, taxonomy_version, source_document_id, schema_code, report_id])
        return existing_by_id[3]

    previous_latest = conn.execute("""
        SELECT id, report_version
        FROM latest_financial_reports
        WHERE logical_key = %s
    """, [logical_key]).fetchone()

    superseded_id = previous_latest[0] if previous_latest is not None else None
    report_version = (int(previous_latest[1]) + 1) if previous_latest is not None else 1

    conn.execute("""
        INSERT INTO financial_reports
            (id, logical_key, report_version, supersedes_report_id, krs, data_source_id,
             report_type, fiscal_year, period_start, period_end, taxonomy_version,
             source_document_id, source_file_path, ingestion_status, created_at, schema_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, [report_id, logical_key, report_version, superseded_id, krs, data_source_id,
          report_type, fiscal_year, period_start, period_end, taxonomy_version,
          source_document_id, source_file_path, now, schema_code])

    return superseded_id


def update_report_status(report_id: str, status: str, error: Optional[str] = None) -> None:
    conn = get_conn()
    conn.execute("""
        UPDATE financial_reports SET ingestion_status = %s, ingestion_error = %s WHERE id = %s
    """, [status, error, report_id])


def get_financial_report(report_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id, krs, data_source_id,
               report_type, fiscal_year, period_start, period_end,
               ingestion_status, ingestion_error, source_document_id, schema_code
        FROM financial_reports WHERE id = %s
    """, [report_id]).fetchone()
    if row is None:
        return None
    cols = [
        "id",
        "logical_key",
        "report_version",
        "supersedes_report_id",
        "krs",
        "data_source_id",
        "report_type",
        "fiscal_year",
        "period_start",
        "period_end",
        "ingestion_status",
        "ingestion_error",
        "source_document_id",
        "schema_code",
    ]
    return dict(zip(cols, row))


def get_financial_reports_batch(report_ids: list[str]) -> dict[str, dict]:
    """Fetch many financial reports in a single round-trip keyed by id.

    CR-PZN-003: the per-report scoring loop previously issued one SELECT per
    report (see callers of `get_financial_report`). This helper lets
    `score_batch` load them in a single query.
    """
    if not report_ids:
        return {}
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id, krs, data_source_id,
               report_type, fiscal_year, period_start, period_end,
               ingestion_status, ingestion_error, source_document_id, schema_code
        FROM financial_reports WHERE id = ANY(%s)
    """, [list(report_ids)]).fetchall()
    cols = [
        "id", "logical_key", "report_version", "supersedes_report_id", "krs",
        "data_source_id", "report_type", "fiscal_year", "period_start", "period_end",
        "ingestion_status", "ingestion_error", "source_document_id", "schema_code",
    ]
    return {row[0]: dict(zip(cols, row)) for row in rows}


def get_reports_for_krs(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id,
               fiscal_year, period_start, period_end, report_type, ingestion_status
        FROM financial_reports
        WHERE krs = %s
        ORDER BY period_end DESC, report_version DESC, created_at DESC
    """, [krs]).fetchall()
    cols = [
        "id",
        "logical_key",
        "report_version",
        "supersedes_report_id",
        "fiscal_year",
        "period_start",
        "period_end",
        "report_type",
        "ingestion_status",
    ]
    return [dict(zip(cols, row)) for row in rows]


# --- raw_financial_data ---

def upsert_raw_financial_data(report_id: str, section: str, data: dict,
                               taxonomy_version: Optional[str] = None,
                               extraction_version: Optional[int] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    if extraction_version is None:
        extraction_version = get_next_extraction_version(report_id)
    conn.execute("""
        INSERT INTO raw_financial_data
            (report_id, section, extraction_version, data_json, taxonomy_version, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_id, section, extraction_version) DO NOTHING
    """, [report_id, section, extraction_version, json.dumps(data), taxonomy_version, now])


# --- financial_line_items ---

def batch_insert_line_items(items: list[dict], extraction_version: Optional[int] = None) -> None:
    """Insert line items in bulk. Each dict: report_id, section, tag_path, label_pl, value_current, value_previous, currency."""
    if not items:
        return

    conn = get_conn()
    report_ids = {item["report_id"] for item in items}
    if len(report_ids) != 1:
        raise ValueError("batch_insert_line_items expects items for a single report_id")

    report_id = next(iter(report_ids))
    if extraction_version is None:
        extraction_version = get_next_extraction_version(report_id)

    for item in items:
        conn.execute("""
            INSERT INTO financial_line_items
                (report_id, section, tag_path, extraction_version, label_pl, value_current, value_previous, currency, schema_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (report_id, section, tag_path, extraction_version) DO NOTHING
        """, [
            item["report_id"], item["section"], item["tag_path"],
            extraction_version,
            item.get("label_pl"), item.get("value_current"), item.get("value_previous"),
            item.get("currency", "PLN"), item.get("schema_code"),
        ])


def get_line_items(report_id: str, section: Optional[str] = None) -> list[dict]:
    conn = get_conn()
    if section:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, extraction_version,
                   label_pl, value_current, value_previous, currency, schema_code
            FROM latest_financial_line_items
            WHERE report_id = %s AND section = %s
        """, [report_id, section]).fetchall()
    else:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, extraction_version,
                   label_pl, value_current, value_previous, currency, schema_code
            FROM latest_financial_line_items
            WHERE report_id = %s
        """, [report_id]).fetchall()
    cols = ["report_id", "section", "tag_path", "extraction_version", "label_pl", "value_current", "value_previous", "currency", "schema_code"]
    return [dict(zip(cols, row)) for row in rows]


# --- feature_definitions ---

def upsert_feature_definition(feature_id: str, name: str, description: Optional[str] = None,
                               category: Optional[str] = None, formula_description: Optional[str] = None,
                               formula_numerator: Optional[str] = None, formula_denominator: Optional[str] = None,
                               required_tags: Optional[list] = None, computation_logic: str = "ratio",
                               version: int = 1) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO feature_definitions
            (id, name, description, category, formula_description, formula_numerator,
             formula_denominator, required_tags, computation_logic, version, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name                = excluded.name,
            description         = excluded.description,
            category            = excluded.category,
            formula_description = excluded.formula_description,
            formula_numerator   = excluded.formula_numerator,
            formula_denominator = excluded.formula_denominator,
            required_tags       = excluded.required_tags,
            computation_logic   = excluded.computation_logic,
            version             = excluded.version
    """, [feature_id, name, description, category, formula_description, formula_numerator,
          formula_denominator, json.dumps(required_tags) if required_tags else None,
          computation_logic, version, now])


def get_feature_definitions(active_only: bool = True) -> list[dict]:
    conn = get_conn()
    if active_only:
        rows = conn.execute("""
            SELECT id, name, description, category, formula_description,
                   formula_numerator, formula_denominator, required_tags,
                   computation_logic, version, is_active
            FROM feature_definitions WHERE is_active = true
            ORDER BY id
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, name, description, category, formula_description,
                   formula_numerator, formula_denominator, required_tags,
                   computation_logic, version, is_active
            FROM feature_definitions
            ORDER BY id
        """).fetchall()
    cols = ["id", "name", "description", "category", "formula_description",
            "formula_numerator", "formula_denominator", "required_tags",
            "computation_logic", "version", "is_active"]
    return [dict(zip(cols, row)) for row in rows]


# --- feature_sets ---

def upsert_feature_set(set_id: str, name: str, description: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO feature_sets (id, name, description, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET name = excluded.name, description = excluded.description
    """, [set_id, name, description, now])


def add_feature_set_member(set_id: str, feature_definition_id: str, ordinal: int) -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO feature_set_members (feature_set_id, feature_definition_id, ordinal)
        VALUES (%s, %s, %s)
        ON CONFLICT (feature_set_id, feature_definition_id) DO UPDATE SET ordinal = excluded.ordinal
    """, [set_id, feature_definition_id, ordinal])


def get_feature_set_members(set_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT fsm.feature_set_id, fsm.feature_definition_id, fsm.ordinal,
               fd.name, fd.computation_logic
        FROM feature_set_members fsm
        JOIN feature_definitions fd ON fd.id = fsm.feature_definition_id
        WHERE fsm.feature_set_id = %s
        ORDER BY fsm.ordinal
    """, [set_id]).fetchall()
    cols = ["feature_set_id", "feature_definition_id", "ordinal", "feature_name", "computation_logic"]
    return [dict(zip(cols, row)) for row in rows]


# --- computed_features ---

def upsert_computed_feature(report_id: str, feature_definition_id: str, krs: str,
                             fiscal_year: int, value: Optional[float],
                             is_valid: bool = True, error_message: Optional[str] = None,
                             computation_version: Optional[int] = None,
                             source_extraction_version: Optional[int] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    if computation_version is None:
        row = conn.execute("""
            SELECT coalesce(max(computation_version), 0)
            FROM computed_features
            WHERE report_id = %s AND feature_definition_id = %s
        """, [report_id, feature_definition_id]).fetchone()
        computation_version = int(row[0] or 0) + 1

    if source_extraction_version is None:
        source_extraction_version = get_latest_extraction_version(report_id)

    conn.execute("""
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, error_message, source_extraction_version, computation_version, computed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_id, feature_definition_id, computation_version) DO NOTHING
    """, [report_id, feature_definition_id, krs, fiscal_year, value,
          is_valid, error_message, source_extraction_version, computation_version, now])


def get_computed_features_for_report(report_id: str, valid_only: bool = True) -> list[dict]:
    conn = get_conn()
    if valid_only:
        rows = conn.execute("""
            SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid,
                   error_message, source_extraction_version, computation_version
            FROM latest_computed_features
            WHERE report_id = %s AND is_valid = true
            ORDER BY feature_definition_id
        """, [report_id]).fetchall()
    else:
        rows = conn.execute("""
            SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid,
                   error_message, source_extraction_version, computation_version
            FROM latest_computed_features
            WHERE report_id = %s
            ORDER BY feature_definition_id
        """, [report_id]).fetchall()
    cols = [
        "report_id",
        "feature_definition_id",
        "krs",
        "fiscal_year",
        "value",
        "is_valid",
        "error_message",
        "source_extraction_version",
        "computation_version",
    ]
    return [dict(zip(cols, row)) for row in rows]


def get_computed_features_for_reports_batch(
    report_ids: list[str],
    valid_only: bool = True,
) -> dict[str, list[dict]]:
    """Fetch computed features for many reports in a single query.

    CR-PZN-003: collapses the N+1 `get_computed_features_for_report` loop in
    model `score_batch` paths. Returns a map `{report_id: [feature_row, ...]}`
    with the same row shape as `get_computed_features_for_report`.
    """
    if not report_ids:
        return {}
    conn = get_conn()
    base_sql = """
        SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid,
               error_message, source_extraction_version, computation_version
        FROM latest_computed_features
        WHERE report_id = ANY(%s)
    """
    if valid_only:
        base_sql += " AND is_valid = true"
    base_sql += " ORDER BY report_id, feature_definition_id"

    rows = conn.execute(base_sql, [list(report_ids)]).fetchall()
    cols = [
        "report_id", "feature_definition_id", "krs", "fiscal_year", "value",
        "is_valid", "error_message", "source_extraction_version", "computation_version",
    ]
    grouped: dict[str, list[dict]] = {rid: [] for rid in report_ids}
    for row in rows:
        rec = dict(zip(cols, row))
        grouped.setdefault(rec["report_id"], []).append(rec)
    return grouped


def get_computed_features(krs: str, fiscal_year: Optional[int] = None) -> list[dict]:
    conn = get_conn()
    if fiscal_year is not None:
        rows = conn.execute("""
            SELECT cf.report_id, cf.feature_definition_id, cf.krs, cf.fiscal_year, cf.value,
                   cf.is_valid, cf.error_message, cf.source_extraction_version, cf.computation_version
            FROM latest_computed_features cf
            JOIN latest_financial_reports fr ON fr.id = cf.report_id
            WHERE cf.krs = %s AND cf.fiscal_year = %s AND cf.is_valid = true
            ORDER BY cf.report_id, cf.feature_definition_id
        """, [krs, fiscal_year]).fetchall()
    else:
        rows = conn.execute("""
            SELECT cf.report_id, cf.feature_definition_id, cf.krs, cf.fiscal_year, cf.value,
                   cf.is_valid, cf.error_message, cf.source_extraction_version, cf.computation_version
            FROM latest_computed_features cf
            JOIN latest_financial_reports fr ON fr.id = cf.report_id
            WHERE cf.krs = %s AND cf.is_valid = true
            ORDER BY cf.report_id, cf.feature_definition_id
        """, [krs]).fetchall()
    cols = [
        "report_id",
        "feature_definition_id",
        "krs",
        "fiscal_year",
        "value",
        "is_valid",
        "error_message",
        "source_extraction_version",
        "computation_version",
    ]
    return [dict(zip(cols, row)) for row in rows]


# --- model_registry ---

def register_model(model_id: str, name: str, model_type: str, version: str,
                   feature_set_id: Optional[str] = None, description: Optional[str] = None,
                   hyperparameters: Optional[dict] = None, training_metrics: Optional[dict] = None,
                   training_date: Optional[datetime] = None, training_data_spec: Optional[dict] = None,
                   artifact_path: Optional[str] = None, is_baseline: bool = False) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, description,
             hyperparameters, training_metrics, training_date, training_data_spec,
             artifact_path, is_baseline, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (name, version) DO UPDATE SET
            model_type       = excluded.model_type,
            feature_set_id   = excluded.feature_set_id,
            description      = excluded.description,
            hyperparameters  = excluded.hyperparameters,
            training_metrics = excluded.training_metrics,
            training_date    = excluded.training_date,
            artifact_path    = excluded.artifact_path,
            is_baseline      = excluded.is_baseline
    """, [model_id, name, model_type, version, feature_set_id, description,
          json.dumps(hyperparameters) if hyperparameters else None,
          json.dumps(training_metrics) if training_metrics else None,
          training_date or now,
          json.dumps(training_data_spec) if training_data_spec else None,
          artifact_path, is_baseline, now])


def get_active_models() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, model_type, version, feature_set_id, artifact_path, is_baseline, is_active
        FROM model_registry WHERE is_active = true ORDER BY created_at DESC
    """).fetchall()
    cols = ["id", "name", "model_type", "version", "feature_set_id", "artifact_path", "is_baseline", "is_active"]
    return [dict(zip(cols, row)) for row in rows]


# --- prediction_runs ---

def create_prediction_run(run_id: str, model_id: str, parameters: Optional[dict] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO prediction_runs (id, model_id, parameters, status, run_date, created_at)
        VALUES (%s, %s, %s, 'running', %s, %s)
    """, [run_id, model_id, json.dumps(parameters) if parameters else None, now, now])


def finish_prediction_run(run_id: str, status: str, companies_scored: int = 0,
                           duration_seconds: Optional[float] = None,
                           error_message: Optional[str] = None) -> None:
    conn = get_conn()
    conn.execute("""
        UPDATE prediction_runs SET
            status           = %s,
            companies_scored = %s,
            duration_seconds = %s,
            error_message    = %s
        WHERE id = %s
    """, [status, companies_scored, duration_seconds, error_message, run_id])


# --- predictions ---

def insert_prediction(prediction_id: str, prediction_run_id: str, krs: str, report_id: str,
                      raw_score: Optional[float], probability: Optional[float],
                      classification: Optional[int], risk_category: Optional[str],
                      feature_contributions: Optional[dict] = None,
                      feature_snapshot: Optional[dict] = None) -> None:
    """Persist a prediction row.

    `feature_snapshot` is an immutable map {feature_definition_id: computation_version}
    captured at scoring time. Read path uses it to fetch the exact feature values
    that fed the score, avoiding timestamp-based heuristics.
    """
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO predictions
            (id, prediction_run_id, krs, report_id, raw_score, probability,
             classification, risk_category, feature_contributions, feature_snapshot, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, [prediction_id, prediction_run_id, krs, report_id, raw_score, probability,
          classification, risk_category,
          json.dumps(feature_contributions) if feature_contributions else None,
          json.dumps(feature_snapshot) if feature_snapshot else None,
          now])


def insert_predictions_batch(rows: list[dict]) -> int:
    """Bulk-insert prediction rows.

    CR-PZN-003: replaces the per-row `insert_prediction` loop in `score_batch`
    with a single `executemany`. Each row dict must carry the same keys as
    `insert_prediction`'s kwargs (prediction_id, prediction_run_id, krs,
    report_id, raw_score, probability, classification, risk_category,
    feature_contributions, feature_snapshot). Returns the count of rows queued
    (not strictly the count inserted — rows conflicting on id are skipped).
    """
    if not rows:
        return 0
    conn = get_conn()
    now = datetime.now(timezone.utc)
    payload = [
        (
            r["prediction_id"],
            r["prediction_run_id"],
            r["krs"],
            r["report_id"],
            r.get("raw_score"),
            r.get("probability"),
            r.get("classification"),
            r.get("risk_category"),
            json.dumps(r["feature_contributions"]) if r.get("feature_contributions") else None,
            json.dumps(r["feature_snapshot"]) if r.get("feature_snapshot") else None,
            now,
        )
        for r in rows
    ]
    with conn.raw.cursor() as cur:  # psycopg2 executemany is fine for moderate batches
        cur.executemany("""
            INSERT INTO predictions
                (id, prediction_run_id, krs, report_id, raw_score, probability,
                 classification, risk_category, feature_contributions, feature_snapshot, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, payload)
    return len(rows)


def get_latest_prediction(krs: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT p.id, p.krs, p.report_id, p.raw_score, p.probability,
               p.classification, p.risk_category, p.created_at,
               mr.name AS model_name, mr.version AS model_version
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        JOIN model_registry mr ON mr.id = pr.model_id
        WHERE p.krs = %s
        ORDER BY p.created_at DESC
        LIMIT 1
    """, [krs]).fetchone()
    if row is None:
        return None
    cols = ["id", "krs", "report_id", "raw_score", "probability",
            "classification", "risk_category", "created_at", "model_name", "model_version"]
    result = dict(zip(cols, row))
    result["created_at"] = str(result["created_at"])
    return result


def get_prediction_history(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.id, p.raw_score, p.probability, p.classification, p.risk_category, p.created_at,
               mr.name AS model_name, mr.version AS model_version, fr.fiscal_year
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        JOIN model_registry mr ON mr.id = pr.model_id
        JOIN financial_reports fr ON fr.id = p.report_id
        WHERE p.krs = %s
        ORDER BY p.created_at DESC
    """, [krs]).fetchall()
    cols = ["id", "raw_score", "probability", "classification", "risk_category",
            "created_at", "model_name", "model_version", "fiscal_year"]
    results = [dict(zip(cols, row)) for row in rows]
    for r in results:
        r["created_at"] = str(r["created_at"])
    return results


# --- Fat queries for predictions API (PKR-64) ---

def get_predictions_fat(krs: str) -> list[dict]:
    """Single JOINed query returning all prediction data for a KRS number.

    Ordered by fiscal_year DESC so callers taking the first row per model
    get the most recent reporting year (not merely the most recently scored).
    Ties are broken deterministically: latest scoring time first, then the
    higher report_version (correction filings win over originals), then
    prediction id — so equal timestamps still yield a stable winner.
    """
    conn = get_conn()
    cur = conn.execute("""
        SELECT
            p.raw_score, p.probability, p.classification, p.risk_category,
            p.feature_contributions, p.feature_snapshot, p.created_at AS scored_at,
            mr.id AS model_id, mr.name AS model_name, mr.model_type,
            mr.version AS model_version, mr.is_baseline, mr.description AS model_description,
            mr.hyperparameters, mr.feature_set_id,
            fr.id AS report_id, fr.fiscal_year, fr.period_start, fr.period_end,
            fr.report_version, fr.data_source_id, fr.created_at AS ingested_at,
            fr.schema_code
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        JOIN model_registry mr ON mr.id = pr.model_id
        JOIN financial_reports fr ON fr.id = p.report_id
        WHERE p.krs = %s AND mr.is_active = true
        ORDER BY fr.fiscal_year DESC, p.created_at DESC, fr.report_version DESC, p.id DESC
    """, [krs])
    results = _cursor_to_dicts(cur)
    for d in results:
        for key in ("scored_at", "ingested_at", "period_start", "period_end"):
            d[key] = str(d[key]) if d[key] else None
        if isinstance(d["feature_contributions"], str):
            d["feature_contributions"] = json.loads(d["feature_contributions"])
        if isinstance(d.get("feature_snapshot"), str):
            d["feature_snapshot"] = json.loads(d["feature_snapshot"])
        if isinstance(d["hyperparameters"], str):
            d["hyperparameters"] = json.loads(d["hyperparameters"])
    return results


def get_features_for_report(report_id: str, feature_set_id: str) -> list[dict]:
    """Get computed features with definitions and source line items for a report."""
    conn = get_conn()
    cur = conn.execute("""
        SELECT
            cf.feature_definition_id, cf.value,
            fd.name, fd.category, fd.formula_description, fd.required_tags,
            fd.computation_logic
        FROM latest_computed_features cf
        JOIN feature_definitions fd ON fd.id = cf.feature_definition_id
        JOIN feature_set_members fsm ON fsm.feature_definition_id = fd.id
        WHERE cf.report_id = %s AND fsm.feature_set_id = %s AND cf.is_valid = true
        ORDER BY fsm.ordinal
    """, [report_id, feature_set_id])
    results = _cursor_to_dicts(cur)
    for d in results:
        if isinstance(d["required_tags"], str):
            d["required_tags"] = json.loads(d["required_tags"])
    return results


# Bounded batch size for large VALUES lists to stay well under PostgreSQL's
# 32767-parameter limit even for multi-column tuples.
_BATCH_CHUNK_SIZE = 800


def _chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def get_features_for_predictions_batch(
    requests: list[dict],
) -> dict[str, list[dict]]:
    """Batched feature loader: O(1) round-trips for many predictions.

    Each request dict must include a caller-supplied `request_id` that uniquely
    identifies the prediction (not the report/feature_set pair) — two distinct
    predictions using the same (report_id, feature_set_id) but different
    snapshots / scored_at must carry different request_ids. Shape:

        {"request_id": str,            # REQUIRED, unique per prediction
         "report_id": str,
         "feature_set_id": str,
         "feature_snapshot": dict | None,  # {feature_id: computation_version}
         "scored_at": str | None,
         # Optional metadata for warning logs on partial-snapshot fallback:
         "model_id": str | None,
         "fiscal_year": int | None}

    Returns `{request_id: [feature_row, ...]}`.

    Resolution:
      1. Exact snapshot — rows are fetched at the immutable
         (report_id, feature_definition_id, computation_version) triple,
         constrained to `is_valid = true` and membership in the requested
         feature_set_id. If any expected snapshot key is missing from the
         returned rows (partial/corrupted snapshot), the request is demoted
         to the batched fallback and a structured warning is emitted.
      2. Fallback — a single SQL round-trip using a request CTE and window
         function picks the latest valid computed value per feature in the
         requested feature_set for that request, with `computed_at <= scored_at`
         when scored_at is provided, else the latest overall. The window
         partitions by request_id so two requests with the same report and
         feature_set but different scored_at are resolved independently.
    """
    if not requests:
        return {}

    # Defensive: enforce unique request_ids and require them to be present.
    seen_ids: set[str] = set()
    for req in requests:
        rid = req.get("request_id")
        if not rid:
            raise ValueError("get_features_for_predictions_batch: every request must carry a request_id")
        if rid in seen_ids:
            raise ValueError(f"get_features_for_predictions_batch: duplicate request_id {rid!r}")
        seen_ids.add(rid)

    conn = get_conn()
    result_map: dict[str, list[dict]] = {}

    # ---- Phase 1: exact snapshot ----
    exact_requests: list[dict] = []
    fallback_requests: list[dict] = []
    for req in requests:
        snap = req.get("feature_snapshot")
        if isinstance(snap, dict) and snap and all(isinstance(v, int) for v in snap.values()):
            exact_requests.append(req)
        else:
            fallback_requests.append(req)

    if exact_requests:
        # Tuples: (request_id, report_id, feature_set_id, feature_definition_id, computation_version)
        # Dedupe within a single request's snapshot in case the caller passed
        # duplicate feature ids. Different requests keep distinct rows because
        # request_id is part of the tuple.
        quad_set: set[tuple[str, str, str, str, int]] = set()
        for req in exact_requests:
            for fid, version in req["feature_snapshot"].items():
                quad_set.add((
                    req["request_id"], req["report_id"], req["feature_set_id"],
                    fid, version,
                ))
        quads = list(quad_set)

        exact_rows_by_request: dict[str, list[dict]] = {}
        for chunk in _chunks(quads, _BATCH_CHUNK_SIZE):
            placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(chunk))
            flat: list = []
            for t in chunk:
                flat.extend(t)
            cur = conn.execute(f"""
                WITH requested(request_id, report_id, feature_set_id,
                               feature_definition_id, computation_version)
                AS (VALUES {placeholders})
                SELECT
                    r.request_id, r.report_id, r.feature_set_id,
                    cf.feature_definition_id, cf.value, cf.computation_version,
                    cf.source_extraction_version,
                    fd.name, fd.category, fd.formula_description, fd.required_tags,
                    fd.computation_logic, fsm.ordinal
                FROM requested r
                JOIN computed_features cf
                    ON cf.report_id = r.report_id
                   AND cf.feature_definition_id = r.feature_definition_id
                   AND cf.computation_version = r.computation_version
                   AND cf.is_valid = true
                JOIN feature_set_members fsm
                    ON fsm.feature_set_id = r.feature_set_id
                   AND fsm.feature_definition_id = r.feature_definition_id
                JOIN feature_definitions fd
                    ON fd.id = cf.feature_definition_id
            """, flat)
            for d in _cursor_to_dicts(cur):
                if isinstance(d["required_tags"], str):
                    d["required_tags"] = json.loads(d["required_tags"])
                exact_rows_by_request.setdefault(d["request_id"], []).append(d)

        # Validate completeness — every snapshot key must have a returned row.
        for req in exact_requests:
            rid = req["request_id"]
            expected = set(req["feature_snapshot"].keys())
            returned = {r["feature_definition_id"] for r in exact_rows_by_request.get(rid, [])}
            missing = expected - returned
            if not missing and returned:
                rows = sorted(exact_rows_by_request[rid], key=lambda r: (r.get("ordinal") or 0))
                # Drop the request_id marker from output rows.
                for r in rows:
                    r.pop("request_id", None)
                result_map[rid] = rows
            else:
                logger.warning(
                    "feature_snapshot_incomplete_fallback",
                    extra={
                        "event": "feature_snapshot_incomplete_fallback",
                        "request_id": rid,
                        "report_id": req["report_id"],
                        "feature_set_id": req["feature_set_id"],
                        "model_id": req.get("model_id"),
                        "fiscal_year": req.get("fiscal_year"),
                        "expected": sorted(expected),
                        "missing": sorted(missing),
                    },
                )
                fallback_requests.append(req)

    # ---- Phase 2: batched fallback ----
    unresolved = [req for req in fallback_requests if req["request_id"] not in result_map]
    if unresolved:
        fallback_rows = _load_features_fallback_batch(conn, unresolved)
        for rid, rows in fallback_rows.items():
            result_map[rid] = rows
        for req in unresolved:
            result_map.setdefault(req["request_id"], [])

    return result_map


def _load_features_fallback_batch(
    conn,
    requests: list[dict],
) -> dict[str, list[dict]]:
    """Single batched SQL fallback for predictions without a usable snapshot.

    Keyed by `request_id` (not by report_id/feature_set_id), so two requests
    with the same report and feature set but different scored_at are resolved
    independently and never share rows. Picks the latest valid computed feature
    per request+feature_definition_id where computed_at <= scored_at (NULL
    scored_at resolves to the latest overall). Chunked to stay under the
    PostgreSQL parameter limit.
    """
    if not requests:
        return {}

    rows_by_request: dict[str, list[dict]] = {}
    for chunk in _chunks(requests, _BATCH_CHUNK_SIZE):
        placeholders = ", ".join(["(%s, %s, %s, %s::timestamptz)"] * len(chunk))
        flat: list = []
        for req in chunk:
            flat.extend([
                req["request_id"], req["report_id"], req["feature_set_id"],
                req.get("scored_at"),
            ])
        cur = conn.execute(f"""
            WITH requested(request_id, report_id, feature_set_id, scored_at) AS (
                VALUES {placeholders}
            ),
            ranked AS (
                SELECT
                    r.request_id,
                    r.report_id,
                    r.feature_set_id,
                    cf.feature_definition_id,
                    cf.value,
                    cf.source_extraction_version,
                    cf.computation_version,
                    fsm.ordinal,
                    row_number() OVER (
                        PARTITION BY r.request_id, cf.feature_definition_id
                        ORDER BY cf.computed_at DESC, cf.computation_version DESC
                    ) AS rn
                FROM requested r
                JOIN feature_set_members fsm
                    ON fsm.feature_set_id = r.feature_set_id
                JOIN computed_features cf
                    ON cf.report_id = r.report_id
                   AND cf.feature_definition_id = fsm.feature_definition_id
                   AND cf.is_valid = true
                   AND cf.computed_at <= coalesce(r.scored_at, 'infinity'::timestamptz)
            )
            SELECT
                ranked.request_id,
                ranked.feature_definition_id, ranked.value,
                ranked.source_extraction_version, ranked.computation_version,
                ranked.ordinal,
                fd.name, fd.category, fd.formula_description, fd.required_tags,
                fd.computation_logic
            FROM ranked
            JOIN feature_definitions fd ON fd.id = ranked.feature_definition_id
            WHERE ranked.rn = 1
            ORDER BY ranked.request_id, ranked.ordinal
        """, flat)
        for d in _cursor_to_dicts(cur):
            if isinstance(d["required_tags"], str):
                d["required_tags"] = json.loads(d["required_tags"])
            req_id = d.pop("request_id")
            rows_by_request.setdefault(req_id, []).append(d)

    # Second pass: if the "computed_at <= scored_at" window returned nothing
    # for a request but scored_at was set, retry that request with scored_at=None.
    # This mirrors the legacy per-request two-phase behavior for pre-computed_at rows.
    retry_requests = [
        req for req in requests
        if req.get("scored_at") is not None and not rows_by_request.get(req["request_id"])
    ]
    if retry_requests:
        retry_rows = _load_features_fallback_batch(
            conn,
            [{**req, "scored_at": None} for req in retry_requests],
        )
        for req_id, rows in retry_rows.items():
            rows_by_request[req_id] = rows

    return rows_by_request


def get_features_for_prediction(
    report_id: str,
    feature_set_id: str,
    scored_at: Optional[str] = None,
) -> list[dict]:
    """Get feature snapshot valid at scoring time for a prediction.

    For each feature in the model's feature set, picks the latest valid
    computed value where computed_at <= scored_at. If scored_at is None,
    this resolves to the latest valid computed value overall.
    """
    conn = get_conn()
    cur = conn.execute("""
        SELECT
            ranked.feature_definition_id, ranked.value, ranked.source_extraction_version,
            fd.name, fd.category, fd.formula_description, fd.required_tags,
            fd.computation_logic
        FROM (
            SELECT
                cf.feature_definition_id,
                cf.value,
                cf.source_extraction_version,
                fsm.ordinal,
                row_number() OVER (
                    PARTITION BY cf.feature_definition_id
                    ORDER BY cf.computed_at DESC, cf.computation_version DESC
                ) AS rn
            FROM computed_features cf
            JOIN feature_set_members fsm
                ON fsm.feature_definition_id = cf.feature_definition_id
               AND fsm.feature_set_id = %s
            WHERE cf.report_id = %s
              AND cf.is_valid = true
              AND cf.computed_at <= coalesce(%s::timestamptz, 'infinity'::timestamptz)
        ) ranked
        JOIN feature_definitions fd ON fd.id = ranked.feature_definition_id
        WHERE ranked.rn = 1
        ORDER BY ranked.ordinal
    """, [feature_set_id, report_id, scored_at])
    results = _cursor_to_dicts(cur)
    for d in results:
        if isinstance(d["required_tags"], str):
            d["required_tags"] = json.loads(d["required_tags"])
    return results


def get_source_line_items_for_reports_batch(
    requests: list[tuple[str, list[str]]],
) -> dict[str, list[dict]]:
    """Batched source-item loader: one DB round-trip for many (report_id, tags) pairs.

    Returns {report_id: [source_item, ...]} with label_pl / value_current /
    value_previous / section / schema_code per tag_path. value_previous is
    resolved from the immediately prior fiscal year's latest completed report
    for the same company, constrained by data_source_id and report_type.
    """
    if not requests:
        return {}

    # Build a single CTE that unions all report/tag pairs then joins line items.
    report_ids = [r[0] for r in requests]
    report_ids_unique = list(dict.fromkeys(report_ids))

    # De-dup (report_id, tag_path) request pairs.
    pairs: list[tuple[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for report_id, tags in requests:
        for tp in tags:
            key = (report_id, tp)
            if key not in seen_pairs:
                seen_pairs.add(key)
                pairs.append(key)

    if not pairs:
        return {rid: [] for rid in report_ids_unique}

    conn = get_conn()
    result: dict[str, list[dict]] = {rid: [] for rid in report_ids_unique}

    # Group pairs by report_id so each chunk stays self-contained and its
    # report_ids_unique slice is small.
    pairs_by_rid: dict[str, list[str]] = {}
    for rid, tp in pairs:
        pairs_by_rid.setdefault(rid, []).append(tp)

    # Chunk reports to bound both pair and report_id parameter counts.
    chunk_reports: list[str] = []
    chunk_pairs: list[tuple[str, str]] = []
    chunks: list[tuple[list[str], list[tuple[str, str]]]] = []
    for rid in report_ids_unique:
        tags = pairs_by_rid.get(rid, [])
        if chunk_pairs and (len(chunk_pairs) + len(tags) > _BATCH_CHUNK_SIZE):
            chunks.append((chunk_reports, chunk_pairs))
            chunk_reports, chunk_pairs = [], []
        chunk_reports.append(rid)
        chunk_pairs.extend((rid, tp) for tp in tags)
    if chunk_pairs:
        chunks.append((chunk_reports, chunk_pairs))

    for rids_chunk, pairs_chunk in chunks:
        pair_placeholders = ", ".join(["(%s, %s)"] * len(pairs_chunk))
        rid_placeholders = ", ".join(["%s"] * len(rids_chunk))
        flat_pairs: list = []
        for pair in pairs_chunk:
            flat_pairs.extend(pair)

        cur = conn.execute(f"""
            WITH requested(report_id, tag_path) AS (
                VALUES {pair_placeholders}
            ),
            current_reports AS (
                SELECT id, krs, fiscal_year, data_source_id, report_type, schema_code
                FROM financial_reports
                WHERE id IN ({rid_placeholders})
            ),
            previous_reports AS (
                SELECT DISTINCT ON (cr.id)
                    cr.id AS current_report_id,
                    fr_prev.id AS previous_report_id
                FROM current_reports cr
                JOIN financial_reports fr_prev
                  ON fr_prev.krs = cr.krs
                 AND fr_prev.fiscal_year = cr.fiscal_year - 1
                 AND fr_prev.data_source_id = cr.data_source_id
                 AND fr_prev.report_type = cr.report_type
                 AND fr_prev.ingestion_status = 'completed'
                ORDER BY cr.id, fr_prev.report_version DESC, fr_prev.created_at DESC, fr_prev.id DESC
            ),
            current_items AS (
                SELECT li.report_id, li.tag_path, li.label_pl, li.value_current,
                       li.section, li.schema_code
                FROM latest_financial_line_items li
                WHERE li.report_id IN ({rid_placeholders})
            ),
            previous_items AS (
                SELECT pr.current_report_id AS report_id, li.tag_path, li.label_pl,
                       li.value_current AS value_previous, li.section
                FROM previous_reports pr
                JOIN latest_financial_line_items li ON li.report_id = pr.previous_report_id
            )
            SELECT
                r.report_id,
                r.tag_path,
                coalesce(ci.label_pl, pi.label_pl) AS label_pl,
                ci.value_current AS value_current,
                pi.value_previous AS value_previous,
                coalesce(ci.section, pi.section) AS section,
                coalesce(ci.schema_code, cr.schema_code) AS schema_code
            FROM requested r
            LEFT JOIN current_items ci ON ci.report_id = r.report_id AND ci.tag_path = r.tag_path
            LEFT JOIN previous_items pi ON pi.report_id = r.report_id AND pi.tag_path = r.tag_path
            LEFT JOIN current_reports cr ON cr.id = r.report_id
        """, flat_pairs + rids_chunk + rids_chunk)

        for row in _cursor_to_dicts(cur):
            result.setdefault(row["report_id"], []).append(row)

    if len(chunks) > 1:
        logger.info(
            "source_items_batch_chunked",
            extra={
                "event": "source_items_batch_chunked",
                "chunks": len(chunks),
                "reports": len(report_ids_unique),
                "pairs": len(pairs),
            },
        )

    return result


def get_source_line_items_for_report(report_id: str, tag_paths: list[str]) -> list[dict]:
    """Get financial line items for specific tags with prior-year values.

    Returns one row per requested tag_path. value_previous is resolved from the
    immediately prior fiscal year's latest completed report for the same company.
    """
    if not tag_paths:
        return []
    unique_tag_paths = list(dict.fromkeys(tag_paths))
    conn = get_conn()
    placeholders = ", ".join(["(%s)"] * len(unique_tag_paths))
    cur = conn.execute(f"""
        WITH requested_tags(tag_path) AS (
            VALUES {placeholders}
        ),
        current_report AS (
            SELECT id, krs, fiscal_year, data_source_id, report_type
            FROM financial_reports
            WHERE id = %s
            LIMIT 1
        ),
        previous_report AS (
            SELECT fr_prev.id
            FROM financial_reports fr_prev
            JOIN current_report cr ON fr_prev.krs = cr.krs
            WHERE fr_prev.fiscal_year = cr.fiscal_year - 1
              AND fr_prev.data_source_id = cr.data_source_id
              AND fr_prev.report_type = cr.report_type
              AND fr_prev.ingestion_status = 'completed'
            ORDER BY fr_prev.report_version DESC, fr_prev.created_at DESC, fr_prev.id DESC
            LIMIT 1
        ),
        current_items AS (
            SELECT tag_path, label_pl, value_current, section
            FROM latest_financial_line_items
            WHERE report_id = %s
        ),
        previous_items AS (
            SELECT li.tag_path, li.label_pl, li.value_current AS value_previous, li.section
            FROM latest_financial_line_items li
            JOIN previous_report pr ON pr.id = li.report_id
        )
        SELECT
            rt.tag_path,
            coalesce(ci.label_pl, pi.label_pl) AS label_pl,
            ci.value_current AS value_current,
            pi.value_previous AS value_previous,
            coalesce(ci.section, pi.section) AS section
        FROM requested_tags rt
        LEFT JOIN current_items ci ON ci.tag_path = rt.tag_path
        LEFT JOIN previous_items pi ON pi.tag_path = rt.tag_path
    """, unique_tag_paths + [report_id, report_id])
    return _cursor_to_dicts(cur)


def get_models_with_details() -> list[dict]:
    """Get all active models with full metadata."""
    conn = get_conn()
    cur = conn.execute("""
        SELECT id, name, model_type, version, feature_set_id, description,
               hyperparameters, is_baseline, is_active, created_at
        FROM model_registry
        WHERE is_active = true
        ORDER BY is_baseline DESC, created_at DESC
    """)
    results = _cursor_to_dicts(cur)
    for d in results:
        d["created_at"] = str(d["created_at"]) if d["created_at"] else None
        if isinstance(d["hyperparameters"], str):
            d["hyperparameters"] = json.loads(d["hyperparameters"])
    return results


def get_prediction_history_fat(krs: str, model_id: str | None = None) -> list[dict]:
    """Get prediction history with report context for charting.

    Returns one row per (model, fiscal_year) — the most recently scored
    prediction wins when a year has been rescored multiple times. Ordered
    by fiscal_year ASC so the frontend can plot the time series directly.

    The business rule is one valid filing per fiscal year; when corrections
    exist, the most recent filing (highest report_version) is preferred.
    Same-timestamp ties fall back to prediction id for deterministic output.
    """
    conn = get_conn()
    params: list = [krs]
    model_filter = ""
    if model_id:
        model_filter = "AND mr.id = %s"
        params.append(model_id)
    cur = conn.execute(f"""
        SELECT
            raw_score, probability, classification, risk_category,
            feature_contributions, scored_at,
            model_id, model_name, model_version,
            fiscal_year, period_start, period_end
        FROM (
            SELECT
                p.raw_score, p.probability, p.classification, p.risk_category,
                p.feature_contributions, p.created_at AS scored_at,
                mr.id AS model_id, mr.name AS model_name, mr.version AS model_version,
                fr.fiscal_year, fr.period_start, fr.period_end,
                row_number() OVER (
                    PARTITION BY mr.id, fr.fiscal_year
                    ORDER BY p.created_at DESC, fr.report_version DESC, p.id DESC
                ) AS rn
            FROM predictions p
            JOIN prediction_runs pr ON pr.id = p.prediction_run_id
            JOIN model_registry mr ON mr.id = pr.model_id
            JOIN financial_reports fr ON fr.id = p.report_id
            WHERE p.krs = %s AND mr.is_active = true {model_filter}
        ) ranked
        WHERE rn = 1
        ORDER BY fiscal_year ASC
    """, params)
    results = _cursor_to_dicts(cur)
    for d in results:
        for key in ("scored_at", "period_start", "period_end"):
            d[key] = str(d[key]) if d[key] else None
        if isinstance(d["feature_contributions"], str):
            d["feature_contributions"] = json.loads(d["feature_contributions"])
    return results


# --- bankruptcy_events ---

def insert_bankruptcy_event(event_id: str, krs: str, event_type: str, event_date: str,
                             data_source_id: Optional[str] = None, court_case_ref: Optional[str] = None,
                             announcement_id: Optional[str] = None, is_confirmed: bool = False,
                             notes: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO bankruptcy_events
            (id, krs, event_type, event_date, data_source_id, court_case_ref,
             announcement_id, is_confirmed, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (krs, event_type, event_date) DO NOTHING
    """, [event_id, krs, event_type, event_date, data_source_id, court_case_ref,
          announcement_id, is_confirmed, notes, now])


def get_bankruptcy_events(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, krs, event_type, event_date, is_confirmed, court_case_ref, notes
        FROM bankruptcy_events WHERE krs = %s ORDER BY event_date DESC
    """, [krs]).fetchall()
    cols = ["id", "krs", "event_type", "event_date", "is_confirmed", "court_case_ref", "notes"]
    return [dict(zip(cols, row)) for row in rows]


# --- assessment_jobs ---

def create_assessment_job(job_id: str, krs: str) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO assessment_jobs (id, krs, status, stage, created_at, updated_at)
        VALUES (%s, %s, 'pending', NULL, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, [job_id, krs, now, now])


def update_assessment_job(job_id: str, status: str, stage: Optional[str] = None,
                           error_message: Optional[str] = None,
                           result: Optional[dict] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        UPDATE assessment_jobs SET
            status        = %s,
            stage         = %s,
            error_message = %s,
            result_json   = %s,
            updated_at    = %s
        WHERE id = %s
    """, [status, stage, error_message,
          json.dumps(result) if result else None, now, job_id])


def get_assessment_job(job_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id, krs, status, stage, error_message, result_json, created_at, updated_at
        FROM assessment_jobs WHERE id = %s
    """, [job_id]).fetchone()
    if row is None:
        return None
    cols = ["id", "krs", "status", "stage", "error_message", "result_json", "created_at", "updated_at"]
    result = dict(zip(cols, row))
    result["created_at"] = str(result["created_at"])
    result["updated_at"] = str(result["updated_at"])
    return result


def get_running_assessment_for_krs(krs: str) -> Optional[dict]:
    """Find an in-progress assessment job for deduplication."""
    conn = get_conn()
    row = conn.execute("""
        SELECT id, krs, status, stage, error_message, result_json, created_at, updated_at
        FROM assessment_jobs
        WHERE krs = %s AND status IN ('pending', 'running')
        ORDER BY created_at DESC LIMIT 1
    """, [krs]).fetchone()
    if row is None:
        return None
    cols = ["id", "krs", "status", "stage", "error_message", "result_json", "created_at", "updated_at"]
    result = dict(zip(cols, row))
    result["created_at"] = str(result["created_at"])
    result["updated_at"] = str(result["updated_at"])
    return result


def update_assessment_progress(job_id: str, progress: dict) -> None:
    """Update only the progress portion of result_json without overwriting other keys."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    # Read existing result_json, merge progress, write back
    row = conn.execute(
        "SELECT result_json FROM assessment_jobs WHERE id = %s", [job_id]
    ).fetchone()
    existing = json.loads(row[0]) if row and row[0] else {}
    existing["progress"] = progress
    conn.execute("""
        UPDATE assessment_jobs SET result_json = %s, updated_at = %s WHERE id = %s
    """, [json.dumps(existing), now, job_id])


def get_ingested_report_ids_for_krs(krs: str) -> set[str]:
    """Return set of source_document_ids that have been ingested for this KRS."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT source_document_id FROM financial_reports
        WHERE krs = %s AND ingestion_status = 'completed'
    """, [krs]).fetchall()
    return {row[0] for row in rows if row[0]}


# --- users ---

_USER_COLS = ["id", "email", "name", "auth_method", "password_hash", "is_verified",
              "has_full_access", "is_active", "created_at", "last_login_at"]
_USER_SELECT = "SELECT " + ", ".join(_USER_COLS) + " FROM users"


def _row_to_user(row) -> Optional[dict]:
    if row is None:
        return None
    result = dict(zip(_USER_COLS, row))
    result["created_at"] = str(result["created_at"]) if result["created_at"] else None
    result["last_login_at"] = str(result["last_login_at"]) if result["last_login_at"] else None
    return result


def create_user(user_id: str, email: str, name: Optional[str], auth_method: str,
                password_hash: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO users (id, email, name, auth_method, password_hash, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, [user_id, email, name, auth_method, password_hash, now])


def get_user_by_email(email: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(f"{_USER_SELECT} WHERE email = %s", [email]).fetchone()
    return _row_to_user(row)


def get_user_by_id(user_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(f"{_USER_SELECT} WHERE id = %s", [user_id]).fetchone()
    return _row_to_user(row)


def update_last_login(user_id: str) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("UPDATE users SET last_login_at = %s WHERE id = %s", [now, user_id])


def verify_user(user_id: str) -> None:
    conn = get_conn()
    conn.execute("UPDATE users SET is_verified = true WHERE id = %s", [user_id])


def delete_unverified_user(user_id: str) -> None:
    """Remove an unverified user and associated verification codes (signup compensation)."""
    conn = get_conn()
    conn.execute("DELETE FROM verification_codes WHERE user_id = %s", [user_id])
    conn.execute("DELETE FROM users WHERE id = %s AND is_verified = false", [user_id])


# --- verification_codes ---

def create_verification_code(user_id: str, code: str, purpose: str, expires_at) -> str:
    import uuid as _uuid
    code_id = str(_uuid.uuid4())
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO verification_codes (id, user_id, code, purpose, expires_at, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, [code_id, user_id, code, purpose, expires_at, now])
    return code_id


def consume_verification_code(user_id: str, code: str, purpose: str) -> bool:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    row = conn.execute("""
        UPDATE verification_codes
        SET used_at = %s
        WHERE id = (
            SELECT id FROM verification_codes
            WHERE user_id = %s AND code = %s AND purpose = %s
              AND used_at IS NULL AND expires_at > %s
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id
    """, [now, user_id, code, purpose, now]).fetchone()
    return row is not None


# --- user_krs_access ---

def get_user_krs_access(user_id: str) -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT krs FROM user_krs_access WHERE user_id = %s ORDER BY krs", [user_id]
    ).fetchall()
    return [r[0] for r in rows]


def grant_krs_access(user_id: str, krs: str, granted_by: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO user_krs_access (user_id, krs, granted_at, granted_by)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id, krs) DO NOTHING
    """, [user_id, krs, now, granted_by])


def check_krs_access(user_id: str, krs: str) -> bool:
    conn = get_conn()
    # Check has_full_access first
    row = conn.execute(
        "SELECT has_full_access FROM users WHERE id = %s", [user_id]
    ).fetchone()
    if row and row[0]:
        return True
    # Check specific KRS grant
    row = conn.execute(
        "SELECT 1 FROM user_krs_access WHERE user_id = %s AND krs = %s", [user_id, krs]
    ).fetchone()
    return row is not None


# --- password_reset_tokens ---

def create_password_reset_token(user_id: str, token_hash: str, expires_at) -> str:
    """Store a hashed password reset token. Returns the row ID."""
    import uuid as _uuid
    token_id = str(_uuid.uuid4())
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO password_reset_tokens (id, user_id, token_hash, expires_at, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, [token_id, user_id, token_hash, expires_at, now])
    return token_id


def consume_password_reset_token(token_hash: str) -> Optional[str]:
    """Consume a valid reset token. Returns user_id on success, None if invalid/expired/used."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    row = conn.execute("""
        UPDATE password_reset_tokens
        SET used_at = %s
        WHERE id = (
            SELECT id FROM password_reset_tokens
            WHERE token_hash = %s
              AND used_at IS NULL
              AND expires_at > %s
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING user_id
    """, [now, token_hash, now]).fetchone()
    return row[0] if row else None


def reset_password_atomic(token_hash: str, new_password_hash: str) -> Optional[str]:
    """Atomically consume a reset token, update password, and revoke all other tokens for the user.

    Returns user_id on success, None if token is invalid/expired/used.
    Uses a CTE to ensure all three operations happen in a single statement:
    1. Consume the matching token (set used_at)
    2. Update the user's password hash
    3. Revoke all other unused tokens for the same user
    """
    conn = get_conn()
    row = conn.execute("""
        WITH consume AS (
            UPDATE password_reset_tokens
            SET used_at = current_timestamp
            WHERE id = (
                SELECT id FROM password_reset_tokens
                WHERE token_hash = %s
                  AND used_at IS NULL
                  AND expires_at > current_timestamp
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, user_id
        ),
        update_pw AS (
            UPDATE users
            SET password_hash = %s
            WHERE id = (SELECT user_id FROM consume)
            RETURNING id
        ),
        revoke_others AS (
            UPDATE password_reset_tokens
            SET used_at = current_timestamp
            WHERE user_id = (SELECT user_id FROM consume)
              AND used_at IS NULL
              AND id != (SELECT id FROM consume)
        )
        SELECT user_id FROM consume
    """, [token_hash, new_password_hash]).fetchone()
    return row[0] if row else None


def update_password(user_id: str, password_hash: str) -> None:
    """Update a user's password hash."""
    conn = get_conn()
    conn.execute(
        "UPDATE users SET password_hash = %s WHERE id = %s",
        [password_hash, user_id],
    )
