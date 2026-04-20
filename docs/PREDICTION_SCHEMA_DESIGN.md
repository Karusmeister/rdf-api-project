# Database Schema Design: Bankruptcy Prediction Engine

## Design Principles

1. **Data-source agnostic** - KRS today, GUS/CEIDG/GPW tomorrow. Every piece of data tracks its origin.
2. **Model agnostic** - Feature definitions are metadata, not hardcoded columns. Adding a new model = inserting rows, not ALTER TABLE.
3. **EAV for features, wide-format via PIVOT for training** - Store flexibly, query efficiently.
4. **Separate raw from computed** - Raw XML extractions preserved immutably; computed features are reproducible and versioned.
5. **PostgreSQL throughout** - All tables live in the shared PostgreSQL database alongside scraper tables. Per-request pooled connections via `ContextVar` middleware.
6. **Append-only analytical facts** - Source filings, ETL extractions, and computed features are insert-only. Current-state reads come from “latest” views, not destructive overwrites.

## Relationship to Existing System

> **Schema state (2026-04-19, v1.2.x):** The dedupe cutover collapsed `krs_entity_versions`/`krs_registry` → `krs_companies` and split `krs_document_versions` → `krs_documents` (immutable discovery) + `krs_document_downloads` (mutable download state). Sections below that still say `krs_registry` describe the pre-cutover design; the current code joins to `krs_companies.krs`.

The shared PostgreSQL database has `krs_companies`, `krs_documents`, `krs_document_downloads`, and `scraper_runs`. We extend this same database with new tables for the prediction pipeline. `krs_companies.krs` is the natural join key between entity metadata and prediction layers.

```
[RDF API] --> [Scraper] --> [krs_companies + krs_documents + krs_document_downloads]
                                     |
                                     v
                            [ETL: XML Parser] --> [Prediction tables in PostgreSQL]
                                                        |
                                                        v
                                                [Feature Engine] --> [Model Training/Scoring]
                                                                            |
                                                                            v
                                                                    [Predictions API] <-- [Auth: JWT + KRS access control]
```

`krs_companies` and `scraper_runs` remain operational control-plane tables for scheduling and monitoring. The append-only requirement applies to analytical source data and derived data, where auditability and reproducibility matter most.

---

## Schema

### Layer 1: Core Entity Registry

> **Note (2026-03):** `data_sources` and `company_identifiers` were removed as part of
> the multi-source simplification. No GUS/GPW integration is planned in the near term.
> ETL failure tracking moved to a dedicated `etl_attempts` table.

```sql
-- Extended company data beyond what krs_companies stores
-- krs_companies is the canonical entity table; this adds ML-relevant fields
CREATE TABLE IF NOT EXISTS companies (
    krs             VARCHAR(10) PRIMARY KEY,       -- FK concept to krs_companies.krs
    nip             VARCHAR(13),
    regon           VARCHAR(14),
    pkd_code        VARCHAR(10),                   -- primary PKD code - key for sector-based models
    incorporation_date DATE,
    voivodeship     VARCHAR(100),                  -- region, useful for geographic risk features
    updated_at      TIMESTAMP DEFAULT current_timestamp
);

-- Tracks every ETL ingestion attempt (completed/failed/skipped)
CREATE TABLE IF NOT EXISTS etl_attempts (
    attempt_id            BIGINT PRIMARY KEY DEFAULT nextval('seq_etl_attempts'),
    document_id           VARCHAR NOT NULL,
    krs                   VARCHAR(10),
    started_at            TIMESTAMP NOT NULL DEFAULT current_timestamp,
    finished_at           TIMESTAMP,
    status                VARCHAR NOT NULL,        -- 'running', 'completed', 'failed', 'skipped'
    reason_code           VARCHAR,                 -- 'no_xml_found', 'parse_error', 'not_downloaded', etc.
    error_message         VARCHAR,
    xml_path              VARCHAR,
    report_id             VARCHAR,
    extraction_version    INTEGER
);
```

### Layer 2: Financial Data (Raw + Structured Extraction)

```sql
-- Metadata for each ingested financial report.
-- Multiple filings for the same business period are preserved via report_version.
CREATE TABLE IF NOT EXISTS financial_reports (
    id              VARCHAR PRIMARY KEY,            -- source filing id, usually krs_documents.document_id
    logical_key     VARCHAR NOT NULL,               -- stable business key: krs + source + type + fiscal period
    report_version  INTEGER NOT NULL DEFAULT 1,     -- 1 = first seen filing, 2+ = corrections/amendments
    supersedes_report_id VARCHAR,                   -- previous latest filing for the same logical_key
    krs             VARCHAR(10) NOT NULL,
    data_source_id  VARCHAR NOT NULL DEFAULT 'KRS',
    report_type     VARCHAR(20) NOT NULL DEFAULT 'annual',
    fiscal_year     INTEGER NOT NULL,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    taxonomy_version VARCHAR(50),
    source_document_id VARCHAR,                     -- krs_documents.document_id
    source_file_path   VARCHAR,                     -- path to raw XML on disk
    ingestion_status VARCHAR(20) DEFAULT 'pending', -- pending, processing, completed, failed
    ingestion_error  VARCHAR,
    created_at       TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(logical_key, report_version)
);

-- Raw JSON storage - one row per report section per ETL extraction_version.
CREATE TABLE IF NOT EXISTS raw_financial_data (
    report_id       VARCHAR NOT NULL,               -- FK to financial_reports.id
    section         VARCHAR(30) NOT NULL,            -- 'balance_sheet', 'income_statement', 'cash_flow'
    extraction_version INTEGER NOT NULL DEFAULT 1,   -- ETL/parser reruns append new versions
    data_json       JSON NOT NULL,                   -- full parsed tree as nested dict
    taxonomy_version VARCHAR(50),
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(report_id, section, extraction_version)
);

-- Flattened line items - one row per tag per report per extraction_version.
CREATE TABLE IF NOT EXISTS financial_line_items (
    report_id       VARCHAR NOT NULL,               -- FK to financial_reports.id
    section         VARCHAR(30) NOT NULL,            -- 'Bilans', 'RZiS', 'CF'
    tag_path        VARCHAR(200) NOT NULL,           -- hierarchical: 'Bilans.Aktywa.A.I'
    extraction_version INTEGER NOT NULL DEFAULT 1,
    label_pl        VARCHAR(500),                    -- Polish label from TAG_LABELS
    value_current   DOUBLE,                          -- kwota_a (current period)
    value_previous  DOUBLE,                          -- kwota_b (previous period)
    currency        VARCHAR(3) DEFAULT 'PLN',
    PRIMARY KEY(report_id, section, tag_path, extraction_version)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_line_items_tag ON financial_line_items(tag_path);
CREATE INDEX IF NOT EXISTS idx_reports_logical ON financial_reports(logical_key, report_version);
CREATE INDEX IF NOT EXISTS idx_reports_krs ON financial_reports(krs);
CREATE INDEX IF NOT EXISTS idx_reports_year ON financial_reports(fiscal_year);
```

### Layer 3: Feature Engineering

```sql
-- Registry of all computable features (metadata - not the values themselves)
CREATE TABLE IF NOT EXISTS feature_definitions (
    id              VARCHAR PRIMARY KEY,             -- short code: 'roa', 'current_ratio', 'x1_maczynska'
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    category        VARCHAR(50),                     -- 'profitability', 'liquidity', 'leverage', 'activity', 'size'
    formula_description VARCHAR,                     -- human-readable: 'Net Profit / Total Assets'
    formula_numerator   VARCHAR(200),                -- tag_path for numerator: 'RZiS.L'
    formula_denominator VARCHAR(200),                -- tag_path for denominator: 'Bilans.Aktywa'
    required_tags   JSON,                            -- ['RZiS.L', 'Bilans.Aktywa'] - dependency tracking
    computation_logic VARCHAR(20) DEFAULT 'ratio',   -- 'ratio', 'difference', 'custom', 'raw_value'
    version         INTEGER DEFAULT 1,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Named groups of features used by specific models
CREATE TABLE IF NOT EXISTS feature_sets (
    id              VARCHAR PRIMARY KEY,              -- 'maczynska_6', 'zieba_64', 'basic_15'
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Which features belong to which set (many-to-many with ordering)
CREATE TABLE IF NOT EXISTS feature_set_members (
    feature_set_id      VARCHAR NOT NULL,             -- FK to feature_sets.id
    feature_definition_id VARCHAR NOT NULL,            -- FK to feature_definitions.id
    ordinal             INTEGER NOT NULL,              -- position in feature vector
    PRIMARY KEY(feature_set_id, feature_definition_id)
);

-- Computed feature values (EAV pattern - one row per feature per report per computation_version).
-- Each record also remembers which extraction_version it was computed from.
CREATE TABLE IF NOT EXISTS computed_features (
    report_id               VARCHAR NOT NULL,          -- FK to financial_reports.id
    feature_definition_id   VARCHAR NOT NULL,           -- FK to feature_definitions.id
    krs                     VARCHAR(10) NOT NULL,       -- denormalized for fast queries
    fiscal_year             INTEGER NOT NULL,            -- denormalized
    value                   DOUBLE,                      -- NULL if computation failed
    is_valid                BOOLEAN DEFAULT true,
    error_message           VARCHAR,                     -- 'division_by_zero', 'missing_tag', etc.
    source_extraction_version INTEGER NOT NULL DEFAULT 1,
    computation_version     INTEGER DEFAULT 1,
    computed_at             TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(report_id, feature_definition_id, computation_version)
);

CREATE INDEX IF NOT EXISTS idx_features_krs ON computed_features(krs);
CREATE INDEX IF NOT EXISTS idx_features_year ON computed_features(fiscal_year);
```

### Current-state views

```sql
CREATE OR REPLACE VIEW latest_financial_reports AS
SELECT *
FROM (
    SELECT
        fr.*,
        row_number() OVER (
            PARTITION BY fr.logical_key
            ORDER BY fr.report_version DESC, fr.created_at DESC
        ) AS version_rank
    FROM financial_reports fr
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW latest_financial_line_items AS
SELECT *
FROM (
    SELECT
        fli.*,
        row_number() OVER (
            PARTITION BY fli.report_id, fli.section, fli.tag_path
            ORDER BY fli.extraction_version DESC
        ) AS version_rank
    FROM financial_line_items fli
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW latest_computed_features AS
SELECT *
FROM (
    SELECT
        cf.*,
        row_number() OVER (
            PARTITION BY cf.report_id, cf.feature_definition_id
            ORDER BY cf.computation_version DESC, cf.computed_at DESC
        ) AS version_rank
    FROM computed_features cf
) ranked
WHERE version_rank = 1;
```

These views keep API/query ergonomics simple while preserving the full audit trail in the base tables.

### Layer 4: Model Registry and Predictions

```sql
-- Trained model metadata
CREATE TABLE IF NOT EXISTS model_registry (
    id              VARCHAR PRIMARY KEY,              -- 'maczynska_v1', 'xgboost_v2.1'
    name            VARCHAR NOT NULL,
    model_type      VARCHAR(50) NOT NULL,             -- 'discriminant', 'random_forest', 'xgboost', 'logistic'
    version         VARCHAR(20) NOT NULL,
    feature_set_id  VARCHAR,                          -- FK to feature_sets.id
    description     VARCHAR,
    hyperparameters JSON,                             -- full config for reproducibility
    training_metrics JSON,                            -- {'auc': 0.92, 'f1': 0.85, 'recall': 0.78}
    training_date   TIMESTAMP,
    training_data_spec JSON,                          -- what data was used (date range, filters, etc.)
    artifact_path   VARCHAR,                          -- path to serialized model (.pkl, .json)
    is_active       BOOLEAN DEFAULT true,
    is_baseline     BOOLEAN DEFAULT false,            -- marks Maczynska as the reference model
    created_at      TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(name, version)
);

-- Batch scoring runs
CREATE TABLE IF NOT EXISTS prediction_runs (
    id              VARCHAR PRIMARY KEY,
    model_id        VARCHAR NOT NULL,                 -- FK to model_registry.id
    run_date        TIMESTAMP DEFAULT current_timestamp,
    parameters      JSON,                             -- filters, thresholds
    companies_scored INTEGER,
    status          VARCHAR(20) DEFAULT 'running',    -- running, completed, failed
    error_message   VARCHAR,
    duration_seconds DOUBLE,
    created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Individual predictions
CREATE TABLE IF NOT EXISTS predictions (
    id              VARCHAR PRIMARY KEY,
    prediction_run_id VARCHAR NOT NULL,               -- FK to prediction_runs.id
    krs             VARCHAR(10) NOT NULL,
    report_id       VARCHAR NOT NULL,                 -- FK to financial_reports.id
    raw_score       DOUBLE,                           -- model output (Z-score, probability)
    probability     DOUBLE,                           -- calibrated [0,1]
    classification  SMALLINT,                         -- 0 = healthy, 1 = high risk
    risk_category   VARCHAR(20),                      -- 'low', 'medium', 'high', 'critical'
    feature_contributions JSON,                       -- SHAP values or similar
    feature_snapshot JSON,                            -- {feature_id: computation_version} map captured at scoring time
                                                      -- Immutable reference used by the API read path to fetch the exact
                                                      -- feature values that fed this score. Added via ALTER TABLE migration.
    created_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_predictions_krs ON predictions(krs);
CREATE INDEX IF NOT EXISTS idx_predictions_risk ON predictions(risk_category);
```

### Layer 5: Ground Truth (Bankruptcy Labels)

```sql
-- Historical bankruptcy/restructuring events - the training target
CREATE TABLE IF NOT EXISTS bankruptcy_events (
    id              VARCHAR PRIMARY KEY,
    krs             VARCHAR(10) NOT NULL,
    event_type      VARCHAR(30) NOT NULL,             -- 'bankruptcy', 'restructuring', 'liquidation', 'deregistration'
    event_date      DATE NOT NULL,
    data_source_id  VARCHAR,                          -- FK to data_sources.id
    court_case_ref  VARCHAR(200),                     -- sygnatura akt
    announcement_id VARCHAR(200),                     -- MSiG reference
    is_confirmed    BOOLEAN DEFAULT false,
    notes           VARCHAR,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(krs, event_type, event_date)
);

CREATE INDEX IF NOT EXISTS idx_bankruptcy_krs ON bankruptcy_events(krs);
CREATE INDEX IF NOT EXISTS idx_bankruptcy_date ON bankruptcy_events(event_date);
```

### Layer 6: Auth & Access Control

```sql
-- User accounts (local email/password or Google SSO)
CREATE TABLE IF NOT EXISTS users (
    id              VARCHAR PRIMARY KEY,
    email           VARCHAR NOT NULL UNIQUE,
    name            VARCHAR,
    auth_method     VARCHAR(20) NOT NULL,       -- 'local' or 'google'
    password_hash   VARCHAR,                    -- bcrypt, NULL for Google SSO users
    is_verified     BOOLEAN DEFAULT false,
    has_full_access BOOLEAN DEFAULT false,      -- admin: bypasses per-KRS checks
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    last_login_at   TIMESTAMP
);

-- 6-digit email verification codes with expiry
CREATE TABLE IF NOT EXISTS verification_codes (
    id              VARCHAR PRIMARY KEY,
    user_id         VARCHAR NOT NULL REFERENCES users(id),
    code            VARCHAR(6) NOT NULL,
    purpose         VARCHAR(20) NOT NULL,       -- 'email_verify'
    expires_at      TIMESTAMP NOT NULL,
    used_at         TIMESTAMP,                  -- atomic consume via UPDATE...RETURNING
    created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Per-user KRS access grants (predictions API gate)
CREATE TABLE IF NOT EXISTS user_krs_access (
    user_id         VARCHAR NOT NULL REFERENCES users(id),
    krs             VARCHAR(10) NOT NULL,
    granted_at      TIMESTAMP DEFAULT current_timestamp,
    granted_by      VARCHAR,                    -- admin user_id who granted
    PRIMARY KEY (user_id, krs)
);

CREATE INDEX IF NOT EXISTS idx_verification_user ON verification_codes(user_id, purpose);
CREATE INDEX IF NOT EXISTS idx_user_krs ON user_krs_access(user_id);
```

---

## Key Queries for ML Consumption

### Wide-format training data (pivot query)

```sql
-- Build feature matrix: one row per (krs, fiscal_year), columns = feature codes
-- Uses crosstab or application-level pivot (pandas) for wide format
WITH feature_data AS (
    SELECT
        cf.krs,
        cf.fiscal_year,
        cf.feature_definition_id AS feature_code,
        cf.value
    FROM computed_features cf
    WHERE cf.is_valid = true
)
PIVOT feature_data
ON feature_code
USING first(value)
GROUP BY krs, fiscal_year;

-- Join with labels for supervised learning
SELECT
    fd.*,
    c.pkd_code,
    c.incorporation_date,
    fr.period_end,
    CASE WHEN EXISTS (
        SELECT 1 FROM bankruptcy_events be
        WHERE be.krs = fd.krs
        AND be.event_date BETWEEN fr.period_end AND fr.period_end + INTERVAL 2 YEAR
    ) THEN 1 ELSE 0 END AS is_bankrupt_within_2y
FROM (/* pivoted feature_data CTE above */) fd
JOIN companies c ON c.krs = fd.krs
JOIN financial_reports fr ON fr.krs = fd.krs AND fr.fiscal_year = fd.fiscal_year;
```

### Export to pandas (from Python)

```python
import duckdb

conn = duckdb.connect("data/scraper.duckdb")
df = conn.execute("""
    SELECT cf.krs, cf.fiscal_year, fd.id AS feature, cf.value
    FROM computed_features cf
    JOIN feature_definitions fd ON fd.id = cf.feature_definition_id
    WHERE cf.is_valid = true
""").df()

# Pivot in pandas
training_matrix = df.pivot_table(
    index=['krs', 'fiscal_year'],
    columns='feature',
    values='value'
)
```

---

## Data Flow Summary

```
1. Scraper downloads ZIP (existing) --> raw XML files on disk
   [krs_companies, krs_documents, krs_document_downloads tables - already exist]

2. ETL picks up downloaded XMLs --> parses via xml_parser --> inserts into:
   - companies (extended metadata: PKD, NIP, incorporation_date)
   - financial_reports (metadata per statement)
   - raw_financial_data (JSON preservation)
   - financial_line_items (flattened tag/value pairs)

3. Feature Engine reads line_items + feature_definitions --> computes --> inserts into:
   - computed_features (cached values, one row per feature per report)

4. Training pipeline reads computed_features (pivoted wide) + bankruptcy_events --> trains model:
   - model_registry (metadata + artifact path)

5. Scoring pipeline loads model + features --> writes to:
   - prediction_runs + predictions
```

## Extensibility Notes

**Adding GUS as a data source:**
1. INSERT into data_sources (id='GUS')
2. Map GUS identifiers via company_identifiers (REGON-based)
3. GUS data goes into financial_line_items with section='GUS_employment' or similar
4. New feature_definitions for GUS-specific ratios (e.g. revenue_per_employee)
5. Existing models keep working; new models can use the expanded feature set

**Adding GPW (stock exchange) data:**
1. INSERT into data_sources (id='GPW')
2. Add a `market_data` table (krs, date, close_price, volume, market_cap) - extends schema without touching existing tables
3. New feature_definitions for market-based features (price_to_book, volatility)
4. company_identifiers maps ISIN to KRS

**Why DuckDB works well here:**
- Columnar storage is perfect for "scan all values of tag X across 10k companies"
- Native JSON support for raw_financial_data queries
- PIVOT operator for building wide training matrices
- Direct export to pandas DataFrames via .df()
- Single file, no server process - keeps the PoC simple
- Can handle millions of rows for this use case (Polish companies ~ 500k in KRS)
