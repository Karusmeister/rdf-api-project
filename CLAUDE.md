# RDF API Proxy + Bankruptcy Prediction Engine

## What this is

PoC backend proxy for Repozytorium Dokumentow Finansowych (rdf-przegladarka.ms.gov.pl).
FastAPI service that handles KRS encryption, proxies requests to the government API,
scrapes financial statements, and runs bankruptcy prediction models.

## Key context files - READ THESE FIRST

- `docs/RDF_API_DOCUMENTATION.md` - Full upstream API docs (endpoints, payloads, responses)
- `docs/AGENT_INSTRUCTIONS.md` - Step-by-step build guide with architecture decisions
- `docs/LOVABLE_UI_SPEC.md` - Frontend spec (for understanding what the API serves)
- `docs/PREDICTION_SCHEMA_DESIGN.md` - Database schema for the prediction engine (5 layers: entities, financial data, features, models, predictions)
- `docs/SCRAPER_ARCHITECTURE.md` - Scraper design and DuckDB schema

## Tech stack

- Python 3.12, FastAPI, uvicorn, httpx (async), pycryptodome, pydantic v2
- DuckDB for all persistence (scraper + prediction tables in same DB file)
- scikit-learn, xgboost, optuna for ML models
- NO requests library - everything async with httpx
- NO manual threading - use async + uvicorn --workers
- NO PostgreSQL - DuckDB only

## Critical: KRS encryption

The `/dokumenty/wyszukiwanie` endpoint requires AES-128-CBC encrypted KRS token.
Full algorithm is in `docs/RDF_API_DOCUMENTATION.md` section 3 and `docs/AGENT_INSTRUCTIONS.md` step 3.
Short version:

```
plaintext = krs.zfill(10) + now.strftime("%Y-%m-%d-%H-%M-%S")
key = iv = now.strftime("%Y-%m-%d-%H").rjust(16, "1")
token = base64(AES-CBC(plaintext, key, iv, PKCS7))
```

Generate fresh token for EVERY request. Never cache it.

## Project structure

```
app/
  main.py              - FastAPI app, lifespan, CORS, exception handlers
  config.py            - pydantic-settings (env vars)
  crypto.py            - encrypt_nrkrs()
  rdf_client.py        - httpx.AsyncClient wrapper (singleton, created in lifespan)
  routers/
    rdf/
      podmiot.py       - /api/podmiot/* (entity lookup)
      dokumenty.py     - /api/dokumenty/* (search, metadata, download)
      schemas.py       - Pydantic models for RDF endpoints
    analysis/
      routes.py        - /api/analysis/* (statement parsing, comparison, time-series)
      schemas.py       - Pydantic models for analysis
    scraper/
      routes.py        - /api/scraper/* (status dashboard)
    assess/
      routes.py        - /api/assess/* (prediction orchestration - see below)
  services/
    xml_parser.py      - e-Sprawozdanie XML parser (~1300 TAG_LABELS for Bilans, RZiS, CF)
    etl.py             - XML-to-DuckDB ingestion pipeline
    feature_engine.py  - Computes financial ratios from line items
    scoring.py         - Generic model scoring service
    training_data.py   - Assembles wide-format training datasets
    assessment_pipeline.py - Async orchestrator (download -> parse -> compute -> score)
  db/
    connection.py      - Shared DuckDB connection manager (single lifecycle)
    prediction_db.py   - DuckDB schema init + CRUD for prediction tables
  models/
    maczynska.py       - Maczynska MDA discriminant model (baseline)
    random_forest.py   - sklearn RandomForest pipeline
    xgboost_model.py   - XGBoost with Optuna tuning + SHAP
  scraper/
    db.py              - DuckDB schema + CRUD for scraper tables (existing)
    job.py             - Scraper job logic
    storage.py         - Document storage abstraction
scripts/
  seed_features.py     - Populate feature_definitions and feature_sets
  train_rf.py          - Train Random Forest model
  train_xgboost.py     - Train XGBoost model
tests/
  test_crypto.py
  test_endpoints.py
  test_prediction_db.py
  test_scraper_db.py
  test_storage.py
data/
  scraper.duckdb       - Single DuckDB file for ALL tables (scraper + prediction)
  models/              - Serialized model artifacts (.pkl, .json)
```

## Database: DuckDB tables

### Scraper tables (existing - app/scraper/db.py)
- `krs_registry` - KRS master list, scraper priority/scheduling
- `krs_documents` - Documents per KRS, download status, storage paths
- `scraper_runs` - Scraper run history

### Prediction tables (app/db/prediction_db.py)
Full DDL in `docs/PREDICTION_SCHEMA_DESIGN.md`. Summary:

**Layer 1 - Entities:**
- `data_sources` - Registry of data origins (KRS, GUS, CEIDG, GPW). PK = short code.
- `companies` - Extended company data. PK = krs VARCHAR(10). Joins to krs_registry.krs.
- `company_identifiers` - Cross-reference across data sources.

**Layer 2 - Financial data:**
- `financial_reports` - One row per ingested statement. Links to krs_documents.
- `raw_financial_data` - JSON per section (balance_sheet, income_statement, cash_flow).
- `financial_line_items` - THE WORKHORSE. Flattened tag/value pairs. PK = (report_id, section, tag_path).
  - tag_path uses dot notation matching TAG_LABELS: 'Bilans.Aktywa.A.I', 'RZiS.A'

**Layer 3 - Features:**
- `feature_definitions` - Feature metadata (formula, required tags). PK = short code ('roa', 'current_ratio').
- `feature_sets` - Named groups ('maczynska_6', 'basic_20').
- `feature_set_members` - Many-to-many with ordinal.
- `computed_features` - Cached feature values (EAV pattern). PK = (report_id, feature_definition_id, version).

**Layer 4 - Models & Predictions:**
- `model_registry` - Trained model metadata + artifact paths.
- `prediction_runs` - Batch scoring runs.
- `predictions` - Individual scores with risk_category and SHAP explanations.

**Layer 5 - Ground Truth:**
- `bankruptcy_events` - Historical bankruptcy/restructuring events (training labels).

**Job tracking:**
- `assessment_jobs` - Tracks async pipeline status for the UI polling pattern.

## Key patterns

### DuckDB connection pattern
One shared DuckDB connection managed by `app/db/connection.py`:
- `app/db/connection.py` owns the connection lifecycle (connect/close/reset)
- `app/scraper/db.py` and `app/db/prediction_db.py` delegate to the shared connection
- Each module has `connect()` (ensures schema), `get_conn()` (returns shared conn), `close()` (no-op)
- `app/main.py` lifespan calls `db_conn.connect()` + both schema inits at startup
- `db_conn.close()` at shutdown closes the single shared connection
- Plain SQL with parameterized queries, no ORM

### XML parsing
`app/services/xml_parser.py` has ~1300 TAG_LABELS mapping XML tags to Polish labels.
It parses e-Sprawozdania into hierarchical trees with kwota_a (current) and kwota_b (previous).
The ETL flattens these trees into financial_line_items using dot-notation tag_paths.

### Feature computation
Features are defined as metadata rows in feature_definitions, not hardcoded columns.
Adding a new feature = INSERT into feature_definitions + re-run compute. No schema changes.
computation_logic types: 'ratio' (num/denom), 'difference', 'raw_value', 'custom' (Python function).

### Async assessment pipeline
The main UI integration point is `POST /api/assess/{krs}`:
1. Check if predictions exist (cached) -> return immediately
2. If not, create assessment_job, kick off background pipeline via asyncio.create_task()
3. UI polls `GET /api/assess/status/{job_id}` for progress
4. Pipeline stages: downloading -> parsing -> computing_features -> scoring -> completed
5. When done, `GET /api/assess/{krs}/details` returns full prediction profile

## Commands

```bash
# Install
pip install -r requirements.txt

# Run dev
uvicorn app.main:app --reload --port 8000

# Run prod
uvicorn app.main:app --workers 4 --port 8000

# Test
pytest tests/ -v

# Test single module
pytest tests/test_crypto.py -v

# Seed feature definitions
python scripts/seed_features.py

# Train models
python scripts/train_rf.py
python scripts/train_xgboost.py
```

## API endpoints

### RDF proxy (existing)
| Method | Path | Upstream | Notes |
|--------|------|----------|-------|
| POST | /api/podmiot/lookup | dane-podstawowe | Plain KRS |
| POST | /api/podmiot/document-types | rodzajeDokWyszukiwanie | Plain KRS |
| POST | /api/dokumenty/search | wyszukiwanie | ENCRYPTED KRS |
| GET | /api/dokumenty/metadata/{id} | dokumenty/{id} | URL-encode Base64 ID |
| POST | /api/dokumenty/download | dokumenty/tresc | Returns ZIP |
| GET | /health | - | Simple healthcheck |

### Analysis (existing)
| Method | Path | Notes |
|--------|------|-------|
| POST | /api/analysis/statement | Parse single statement |
| POST | /api/analysis/compare | Compare two periods |
| POST | /api/analysis/time-series | Track fields across years |
| GET | /api/analysis/available-periods/{krs} | List statement periods |

### Assessment (prediction engine)
| Method | Path | Notes |
|--------|------|-------|
| POST | /api/assess/{krs} | Start assessment or return cached results |
| GET | /api/assess/status/{job_id} | Poll pipeline progress |
| GET | /api/assess/{krs}/details | Full prediction profile |
| POST | /api/predictions/score | Score specific companies with specific model |
| GET | /api/predictions/{krs} | Latest prediction |
| GET | /api/predictions/{krs}/history | Prediction history |
| POST | /api/etl/ingest | Trigger document ingestion |
| GET | /api/training/dataset-stats | Training data quality summary |

## Linear project: Bankruptcy Prediction Engine

All tasks are tracked in Linear under project "Bankruptcy Prediction Engine".
Execute in dependency order: PKR-5 -> 6 -> 7 -> 8 -> 9 -> 10 -> 11 -> 12 -> 13 -> 14 -> 15 -> 16 -> 17 -> 18 -> 19.
Each issue has full requirements, acceptance criteria, and explicit depends-on references.

## Gotchas

1. Document IDs are Base64 with `=`, `+`, `/` - must URL-encode in path params
2. The `nrKRS` field name differs between endpoints (numerKRS vs nrKRS)
3. Download endpoint needs Accept: application/octet-stream header override
4. Use StreamingResponse for download endpoint
5. CORS must be enabled (frontend runs on different port)
6. KRS is VARCHAR(10) everywhere - the natural join key across all tables
7. DuckDB JSON type (not JSONB) - use json_extract() for queries
8. Feature store uses EAV pattern - pivot to wide format for ML training
9. Assessment pipeline must be non-blocking - use asyncio.create_task(), never block the API
10. Generate fresh encryption token for EVERY search request - never cache
