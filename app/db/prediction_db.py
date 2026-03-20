from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

import duckdb

from app.config import settings

_conn: Optional[duckdb.DuckDBPyConnection] = None


def connect() -> None:
    """Open DB connection and ensure prediction schema exists. Call once at startup."""
    global _conn
    if _conn is not None:
        return
    db_path = settings.scraper_db_path
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    _conn = duckdb.connect(db_path)
    _init_schema()


def close() -> None:
    """Close DB connection."""
    global _conn
    if _conn:
        _conn.close()
        _conn = None


def get_conn() -> duckdb.DuckDBPyConnection:  # type: ignore[return]
    if _conn is None:
        raise RuntimeError("Prediction DB not connected - call connect() first")
    return _conn


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
            UNIQUE(krs, data_source_id, fiscal_year, period_end, report_type)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_financial_data (
            report_id       VARCHAR NOT NULL,
            section         VARCHAR(30) NOT NULL,
            data_json       JSON NOT NULL,
            taxonomy_version VARCHAR(50),
            created_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY(report_id, section)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS financial_line_items (
            report_id       VARCHAR NOT NULL,
            section         VARCHAR(30) NOT NULL,
            tag_path        VARCHAR(200) NOT NULL,
            label_pl        VARCHAR(500),
            value_current   DOUBLE,
            value_previous  DOUBLE,
            currency        VARCHAR(3) DEFAULT 'PLN',
            PRIMARY KEY(report_id, section, tag_path)
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


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------

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
                             source_file_path: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO financial_reports
            (id, krs, data_source_id, report_type, fiscal_year, period_start, period_end,
             taxonomy_version, source_document_id, source_file_path, ingestion_status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        ON CONFLICT (krs, data_source_id, fiscal_year, period_end, report_type) DO NOTHING
    """, [report_id, krs, data_source_id, report_type, fiscal_year, period_start, period_end,
          taxonomy_version, source_document_id, source_file_path, now])


def update_report_status(report_id: str, status: str, error: Optional[str] = None) -> None:
    conn = get_conn()
    conn.execute("""
        UPDATE financial_reports SET ingestion_status = ?, ingestion_error = ? WHERE id = ?
    """, [status, error, report_id])


def get_financial_report(report_id: str) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id, krs, data_source_id, report_type, fiscal_year, period_start, period_end,
               ingestion_status, ingestion_error, source_document_id
        FROM financial_reports WHERE id = ?
    """, [report_id]).fetchone()
    if row is None:
        return None
    cols = ["id", "krs", "data_source_id", "report_type", "fiscal_year", "period_start",
            "period_end", "ingestion_status", "ingestion_error", "source_document_id"]
    return dict(zip(cols, row))


def get_reports_for_krs(krs: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, fiscal_year, period_start, period_end, report_type, ingestion_status
        FROM financial_reports WHERE krs = ? ORDER BY fiscal_year DESC
    """, [krs]).fetchall()
    cols = ["id", "fiscal_year", "period_start", "period_end", "report_type", "ingestion_status"]
    return [dict(zip(cols, row)) for row in rows]


# --- raw_financial_data ---

def upsert_raw_financial_data(report_id: str, section: str, data: dict,
                               taxonomy_version: Optional[str] = None) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO raw_financial_data (report_id, section, data_json, taxonomy_version, created_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (report_id, section) DO UPDATE SET
            data_json        = excluded.data_json,
            taxonomy_version = excluded.taxonomy_version
    """, [report_id, section, json.dumps(data), taxonomy_version, now])


# --- financial_line_items ---

def batch_insert_line_items(items: list[dict]) -> None:
    """Insert line items in bulk. Each dict: report_id, section, tag_path, label_pl, value_current, value_previous, currency."""
    conn = get_conn()
    for item in items:
        conn.execute("""
            INSERT INTO financial_line_items
                (report_id, section, tag_path, label_pl, value_current, value_previous, currency)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (report_id, section, tag_path) DO UPDATE SET
                label_pl       = excluded.label_pl,
                value_current  = excluded.value_current,
                value_previous = excluded.value_previous
        """, [
            item["report_id"], item["section"], item["tag_path"],
            item.get("label_pl"), item.get("value_current"), item.get("value_previous"),
            item.get("currency", "PLN"),
        ])


def get_line_items(report_id: str, section: Optional[str] = None) -> list[dict]:
    conn = get_conn()
    if section:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, label_pl, value_current, value_previous, currency
            FROM financial_line_items WHERE report_id = ? AND section = ?
        """, [report_id, section]).fetchall()
    else:
        rows = conn.execute("""
            SELECT report_id, section, tag_path, label_pl, value_current, value_previous, currency
            FROM financial_line_items WHERE report_id = ?
        """, [report_id]).fetchall()
    cols = ["report_id", "section", "tag_path", "label_pl", "value_current", "value_previous", "currency"]
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
                             computation_version: int = 1) -> None:
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, error_message, computation_version, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (report_id, feature_definition_id, computation_version) DO UPDATE SET
            value           = excluded.value,
            is_valid        = excluded.is_valid,
            error_message   = excluded.error_message,
            computed_at     = excluded.computed_at
    """, [report_id, feature_definition_id, krs, fiscal_year, value,
          is_valid, error_message, computation_version, now])


def get_computed_features(krs: str, fiscal_year: Optional[int] = None) -> list[dict]:
    conn = get_conn()
    if fiscal_year is not None:
        rows = conn.execute("""
            SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid, error_message
            FROM computed_features WHERE krs = ? AND fiscal_year = ? AND is_valid = true
        """, [krs, fiscal_year]).fetchall()
    else:
        rows = conn.execute("""
            SELECT report_id, feature_definition_id, krs, fiscal_year, value, is_valid, error_message
            FROM computed_features WHERE krs = ? AND is_valid = true
        """, [krs]).fetchall()
    cols = ["report_id", "feature_definition_id", "krs", "fiscal_year", "value", "is_valid", "error_message"]
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
