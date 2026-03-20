from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import duckdb

from app.db import connection as shared_conn

_schema_initialized = False


def connect() -> None:
    """Ensure shared connection is open and prediction schema exists."""
    shared_conn.connect()
    _ensure_schema()


def close() -> None:
    """No-op. Connection lifecycle is managed by app.db.connection."""
    pass


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB connection."""
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

    # ----- Layer 1: Core Entity & Data Source Registry -----

    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_sources (
            id              VARCHAR PRIMARY KEY,
            name            VARCHAR NOT NULL,
            description     VARCHAR,
            base_url        VARCHAR,
            is_active       BOOLEAN DEFAULT true,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        INSERT INTO data_sources (id, name, description, base_url)
        VALUES ('KRS', 'Krajowy Rejestr Sadowy', 'Court registry - financial statements',
                'https://rdf-przegladarka.ms.gov.pl')
        ON CONFLICT (id) DO NOTHING
    """)

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

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_company_identifiers START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS company_identifiers (
            id              INTEGER PRIMARY KEY,
            krs             VARCHAR(10) NOT NULL,
            data_source_id  VARCHAR NOT NULL,
            identifier_type VARCHAR(20) NOT NULL,
            identifier_value VARCHAR(50) NOT NULL,
            valid_from      DATE,
            valid_to        DATE,
            created_at      TIMESTAMP DEFAULT current_timestamp,
            UNIQUE(data_source_id, identifier_type, identifier_value)
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
            value_current   DOUBLE,
            value_previous  DOUBLE,
            currency        VARCHAR(3) DEFAULT 'PLN',
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
            value                   DOUBLE,
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
            duration_seconds DOUBLE,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              VARCHAR PRIMARY KEY,
            prediction_run_id VARCHAR NOT NULL,
            krs             VARCHAR(10) NOT NULL,
            report_id       VARCHAR NOT NULL,
            raw_score       DOUBLE,
            probability     DOUBLE,
            classification  SMALLINT,
            risk_category   VARCHAR(20),
            feature_contributions JSON,
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

    # Indexes (check existing first to stay idempotent)
    existing = {
        row[0]
        for row in conn.execute("SELECT index_name FROM duckdb_indexes()").fetchall()
    }

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
    ]

    for name, sql in index_defs:
        if name not in existing:
            conn.execute(sql)

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
            created_at
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
            currency
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
            WHERE report_id = ?

            UNION ALL

            SELECT coalesce(max(extraction_version), 0) AS version
            FROM financial_line_items
            WHERE report_id = ?
        )
        SELECT coalesce(max(version), 0) FROM extraction_versions
    """, [report_id, report_id]).fetchone()
    return int(row[0] or 0)


def get_next_extraction_version(report_id: str) -> int:
    return get_latest_extraction_version(report_id) + 1

# --- data_sources ---

def get_data_sources() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT id, name, description, base_url, is_active FROM data_sources").fetchall()
    cols = ["id", "name", "description", "base_url", "is_active"]
    return [dict(zip(cols, row)) for row in rows]


# --- companies ---

def upsert_company(krs: str, nip: Optional[str] = None, regon: Optional[str] = None,
                   pkd_code: Optional[str] = None, incorporation_date: Optional[str] = None,
                   voivodeship: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO companies (krs, nip, regon, pkd_code, incorporation_date, voivodeship, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
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
        "SELECT krs, nip, regon, pkd_code, incorporation_date, voivodeship FROM companies WHERE krs = ?", [krs]
    ).fetchone()
    if row is None:
        return None
    return dict(zip(["krs", "nip", "regon", "pkd_code", "incorporation_date", "voivodeship"], row))


# --- company_identifiers ---

def add_company_identifier(krs: str, data_source_id: str, identifier_type: str,
                            identifier_value: str, valid_from: Optional[str] = None,
                            valid_to: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    next_id = conn.execute("SELECT nextval('seq_company_identifiers')").fetchone()[0]
    conn.execute("""
        INSERT INTO company_identifiers
            (id, krs, data_source_id, identifier_type, identifier_value, valid_from, valid_to, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (data_source_id, identifier_type, identifier_value) DO NOTHING
    """, [next_id, krs, data_source_id, identifier_type, identifier_value, valid_from, valid_to, now])


# --- financial_reports ---

def create_financial_report(report_id: str, krs: str, fiscal_year: int, period_start: str,
                             period_end: str, report_type: str = "annual",
                             data_source_id: str = "KRS",
                             taxonomy_version: Optional[str] = None,
                             source_document_id: Optional[str] = None,
                             source_file_path: Optional[str] = None) -> Optional[str]:
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
        WHERE id = ?
    """, [report_id]).fetchone()

    if existing_by_id is not None:
        conn.execute("""
            UPDATE financial_reports
            SET ingestion_status = 'pending',
                ingestion_error = NULL,
                source_file_path = ?,
                taxonomy_version = ?,
                source_document_id = coalesce(?, source_document_id)
            WHERE id = ?
        """, [source_file_path, taxonomy_version, source_document_id, report_id])
        return existing_by_id[3]

    previous_latest = conn.execute("""
        SELECT id, report_version
        FROM latest_financial_reports
        WHERE logical_key = ?
    """, [logical_key]).fetchone()

    superseded_id = previous_latest[0] if previous_latest is not None else None
    report_version = (int(previous_latest[1]) + 1) if previous_latest is not None else 1

    conn.execute("""
        INSERT INTO financial_reports
            (id, logical_key, report_version, supersedes_report_id, krs, data_source_id,
             report_type, fiscal_year, period_start, period_end, taxonomy_version,
             source_document_id, source_file_path, ingestion_status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        ON CONFLICT (id) DO NOTHING
    """, [report_id, logical_key, report_version, superseded_id, krs, data_source_id,
          report_type, fiscal_year, period_start, period_end, taxonomy_version,
          source_document_id, source_file_path, now])

    return superseded_id


def update_report_status(report_id: str, status: str, error: Optional[str] = None) -> None:
    conn = get_conn()
    conn.execute("""
        UPDATE financial_reports SET ingestion_status = ?, ingestion_error = ? WHERE id = ?
    """, [status, error, report_id])


def get_financial_report(report_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id, krs, data_source_id,
               report_type, fiscal_year, period_start, period_end,
               ingestion_status, ingestion_error, source_document_id
        FROM financial_reports WHERE id = ?
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
    ]
    return dict(zip(cols, row))


def get_reports_for_krs(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, logical_key, report_version, supersedes_report_id,
               fiscal_year, period_start, period_end, report_type, ingestion_status
        FROM financial_reports
        WHERE krs = ?
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
        VALUES (?, ?, ?, ?, ?, ?)
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
                (report_id, section, tag_path, extraction_version, label_pl, value_current, value_previous, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (report_id, section, tag_path, extraction_version) DO NOTHING
        """, [
            item["report_id"], item["section"], item["tag_path"],
            extraction_version,
            item.get("label_pl"), item.get("value_current"), item.get("value_previous"),
            item.get("currency", "PLN"),
        ])


def get_line_items(report_id: str, section: Optional[str] = None) -> list[dict]:
    conn = get_conn()
    if section:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, extraction_version,
                   label_pl, value_current, value_previous, currency
            FROM latest_financial_line_items
            WHERE report_id = ? AND section = ?
        """, [report_id, section]).fetchall()
    else:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, extraction_version,
                   label_pl, value_current, value_previous, currency
            FROM latest_financial_line_items
            WHERE report_id = ?
        """, [report_id]).fetchall()
    cols = ["report_id", "section", "tag_path", "extraction_version", "label_pl", "value_current", "value_previous", "currency"]
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    where = "WHERE is_active = true" if active_only else ""
    rows = conn.execute(f"""
        SELECT id, name, description, category, formula_description,
               formula_numerator, formula_denominator, required_tags,
               computation_logic, version, is_active
        FROM feature_definitions {where}
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
        VALUES (?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET name = excluded.name, description = excluded.description
    """, [set_id, name, description, now])


def add_feature_set_member(set_id: str, feature_definition_id: str, ordinal: int) -> None:
    conn = get_conn()
    conn.execute("""
        INSERT INTO feature_set_members (feature_set_id, feature_definition_id, ordinal)
        VALUES (?, ?, ?)
        ON CONFLICT (feature_set_id, feature_definition_id) DO UPDATE SET ordinal = excluded.ordinal
    """, [set_id, feature_definition_id, ordinal])


def get_feature_set_members(set_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT fsm.feature_set_id, fsm.feature_definition_id, fsm.ordinal,
               fd.name, fd.computation_logic
        FROM feature_set_members fsm
        JOIN feature_definitions fd ON fd.id = fsm.feature_definition_id
        WHERE fsm.feature_set_id = ?
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
            WHERE report_id = ? AND feature_definition_id = ?
        """, [report_id, feature_definition_id]).fetchone()
        computation_version = int(row[0] or 0) + 1

    if source_extraction_version is None:
        source_extraction_version = get_latest_extraction_version(report_id)

    conn.execute("""
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, error_message, source_extraction_version, computation_version, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (report_id, feature_definition_id, computation_version) DO NOTHING
    """, [report_id, feature_definition_id, krs, fiscal_year, value,
          is_valid, error_message, source_extraction_version, computation_version, now])


def get_computed_features_for_report(report_id: str, valid_only: bool = True) -> list[dict]:
    conn = get_conn()
    where = "WHERE report_id = ?" if not valid_only else "WHERE report_id = ? AND is_valid = true"
    rows = conn.execute(f"""
        SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid,
               error_message, source_extraction_version, computation_version
        FROM latest_computed_features
        {where}
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


def get_computed_features(krs: str, fiscal_year: Optional[int] = None) -> list[dict]:
    conn = get_conn()
    if fiscal_year is not None:
        rows = conn.execute("""
            SELECT cf.report_id, cf.feature_definition_id, cf.krs, cf.fiscal_year, cf.value,
                   cf.is_valid, cf.error_message, cf.source_extraction_version, cf.computation_version
            FROM latest_computed_features cf
            JOIN latest_financial_reports fr ON fr.id = cf.report_id
            WHERE cf.krs = ? AND cf.fiscal_year = ? AND cf.is_valid = true
            ORDER BY cf.report_id, cf.feature_definition_id
        """, [krs, fiscal_year]).fetchall()
    else:
        rows = conn.execute("""
            SELECT cf.report_id, cf.feature_definition_id, cf.krs, cf.fiscal_year, cf.value,
                   cf.is_valid, cf.error_message, cf.source_extraction_version, cf.computation_version
            FROM latest_computed_features cf
            JOIN latest_financial_reports fr ON fr.id = cf.report_id
            WHERE cf.krs = ? AND cf.is_valid = true
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        VALUES (?, ?, ?, 'running', ?, ?)
    """, [run_id, model_id, json.dumps(parameters) if parameters else None, now, now])


def finish_prediction_run(run_id: str, status: str, companies_scored: int = 0,
                           duration_seconds: Optional[float] = None,
                           error_message: Optional[str] = None) -> None:
    conn = get_conn()
    conn.execute("""
        UPDATE prediction_runs SET
            status           = ?,
            companies_scored = ?,
            duration_seconds = ?,
            error_message    = ?
        WHERE id = ?
    """, [status, companies_scored, duration_seconds, error_message, run_id])


# --- predictions ---

def insert_prediction(prediction_id: str, prediction_run_id: str, krs: str, report_id: str,
                      raw_score: Optional[float], probability: Optional[float],
                      classification: Optional[int], risk_category: Optional[str],
                      feature_contributions: Optional[dict] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO predictions
            (id, prediction_run_id, krs, report_id, raw_score, probability,
             classification, risk_category, feature_contributions, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [prediction_id, prediction_run_id, krs, report_id, raw_score, probability,
          classification, risk_category,
          json.dumps(feature_contributions) if feature_contributions else None, now])


def get_latest_prediction(krs: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT p.id, p.krs, p.report_id, p.raw_score, p.probability,
               p.classification, p.risk_category, p.created_at,
               mr.name AS model_name, mr.version AS model_version
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        JOIN model_registry mr ON mr.id = pr.model_id
        WHERE p.krs = ?
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
        WHERE p.krs = ?
        ORDER BY p.created_at DESC
    """, [krs]).fetchall()
    cols = ["id", "raw_score", "probability", "classification", "risk_category",
            "created_at", "model_name", "model_version", "fiscal_year"]
    results = [dict(zip(cols, row)) for row in rows]
    for r in results:
        r["created_at"] = str(r["created_at"])
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (krs, event_type, event_date) DO NOTHING
    """, [event_id, krs, event_type, event_date, data_source_id, court_case_ref,
          announcement_id, is_confirmed, notes, now])


def get_bankruptcy_events(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, krs, event_type, event_date, is_confirmed, court_case_ref, notes
        FROM bankruptcy_events WHERE krs = ? ORDER BY event_date DESC
    """, [krs]).fetchall()
    cols = ["id", "krs", "event_type", "event_date", "is_confirmed", "court_case_ref", "notes"]
    return [dict(zip(cols, row)) for row in rows]


# --- assessment_jobs ---

def create_assessment_job(job_id: str, krs: str) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO assessment_jobs (id, krs, status, stage, created_at, updated_at)
        VALUES (?, ?, 'pending', NULL, ?, ?)
        ON CONFLICT (id) DO NOTHING
    """, [job_id, krs, now, now])


def update_assessment_job(job_id: str, status: str, stage: Optional[str] = None,
                           error_message: Optional[str] = None,
                           result: Optional[dict] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        UPDATE assessment_jobs SET
            status        = ?,
            stage         = ?,
            error_message = ?,
            result_json   = ?,
            updated_at    = ?
        WHERE id = ?
    """, [status, stage, error_message,
          json.dumps(result) if result else None, now, job_id])


def get_assessment_job(job_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id, krs, status, stage, error_message, result_json, created_at, updated_at
        FROM assessment_jobs WHERE id = ?
    """, [job_id]).fetchone()
    if row is None:
        return None
    cols = ["id", "krs", "status", "stage", "error_message", "result_json", "created_at", "updated_at"]
    result = dict(zip(cols, row))
    result["created_at"] = str(result["created_at"])
    result["updated_at"] = str(result["updated_at"])
    return result
