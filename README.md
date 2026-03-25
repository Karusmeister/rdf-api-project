# RDF API Project

FastAPI service for working with the Polish Ministry of Justice financial document registry (`rdf-przegladarka.ms.gov.pl`).

The repository currently contains four connected pieces:

1. An RDF proxy API that hides the upstream KRS encryption and exposes a cleaner HTTP interface.
2. Analysis endpoints that download and parse Polish GAAP XML statements server-side.
3. A bulk scraper that keeps a local DuckDB inventory of KRS entities and downloaded documents.
4. An ETL and feature-engineering foundation for building a bankruptcy-prediction pipeline.

## Current State

Implemented today:

- RDF lookup, document search, metadata, and ZIP download endpoints
- Financial statement parsing, period discovery, year-over-year comparison, and time-series analysis
- Shared DuckDB persistence for scraper and prediction tables
- Local extracted-file storage for downloaded RDF documents
- Scraper CLI for importing KRS numbers and downloading document corpora
- ETL ingestion from extracted XML into analytical tables
- Feature definitions and feature computation services in Python

Not implemented or not exposed yet:

- Authentication or authorization
- A frontend application in this repository
- Public HTTP endpoints for feature computation, model training, or model scoring
- A working GCS storage backend (`STORAGE_BACKEND=gcs` currently raises `NotImplementedError`)

## Architecture

```text
Client / CLI
    |
    v
FastAPI app
  - /api/podmiot
  - /api/dokumenty
  - /api/analysis
  - /api/scraper
  - /api/etl
    |
    +--> app/rdf_client.py --> RDF upstream API
    |
    +--> app/services/xml_parser.py --> parsed financial statements
    |
    +--> app/scraper/job.py --> extracted files on disk
    |
    +--> app/services/etl.py --> prediction tables in DuckDB

Shared persistence
  - DuckDB: data/scraper.duckdb
  - Files:  data/documents/krs/<krs>/<document_id_safe>/
```

Key design choices:

- One shared DuckDB connection lifecycle, managed in [`app/db/connection.py`](/Users/piotrkraus/piotr/rdf-api-project/app/db/connection.py)
- Scraper tables and prediction/ETL tables live in the same database file
- Downloaded RDF ZIPs are extracted immediately and stored as raw files plus `manifest.json`
- KRS encryption is handled internally by [`app/crypto.py`](/Users/piotrkraus/piotr/rdf-api-project/app/crypto.py); clients never need to reproduce it

## Repository Layout

```text
app/
  main.py                 FastAPI app and startup/shutdown lifecycle
  config.py               Environment-driven settings
  crypto.py               RDF KRS encryption
  rdf_client.py           Async HTTP client for the upstream RDF API
  db/
    connection.py         Shared DuckDB connection manager
    prediction_db.py      Prediction/ETL schema and CRUD helpers
  routers/
    rdf/                  Proxy endpoints for the upstream RDF API
    analysis/             XML parsing and comparison endpoints
    scraper/              Read-only scraper status endpoint
    etl/                  ETL trigger endpoint
  scraper/
    cli.py                Scraper CLI
    db.py                 Scraper schema and CRUD helpers
    job.py                Bulk scraping job
    storage.py            Local extracted-file storage
  services/
    xml_parser.py         Statement parsing and comparison logic
    etl.py                XML -> DuckDB ingestion
    feature_engine.py     Ratio and feature computation
scripts/
  seed_features.py        Seeds feature definitions and feature sets
tests/
  unit/integration tests plus optional networked e2e coverage
docs/
  RDF_API_DOCUMENTATION.md    Reverse-engineered upstream RDF API reference
  PREDICTION_SCHEMA_DESIGN.md Prediction-layer schema reference
```

## Quick Start

### Prerequisites

- Python 3.12
- Network access to `rdf-przegladarka.ms.gov.pl` if you want to call the live upstream API

### Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

### Run the API

```bash
uvicorn app.main:app --reload --port 8000
```

API docs are then available at:

- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

### Run with Docker

```bash
docker build -t rdf-api-project .
docker run --rm -p 8000:8000 --env-file .env rdf-api-project
```

## Configuration

Most useful settings from [`.env.example`](/Users/piotrkraus/piotr/rdf-api-project/.env.example):

| Variable | Default | Purpose |
| --- | --- | --- |
| `RDF_BASE_URL` | official RDF service URL | Upstream API base URL |
| `REQUEST_TIMEOUT` | `30` | Upstream request timeout in seconds |
| `MAX_CONNECTIONS` | `20` | Connection limit for the shared `httpx` client |
| `CORS_ORIGINS` | localhost frontend origins | Allowed CORS origins |
| `WORKERS` | `4` | Uvicorn worker count in Docker/prod |
| `SCRAPER_DB_PATH` | `data/scraper.duckdb` | Shared DuckDB file |
| `STORAGE_BACKEND` | `local` | Storage backend; only `local` works today |
| `STORAGE_LOCAL_PATH` | `data/documents` | Extracted document root |
| `SCRAPER_ORDER_STRATEGY` | `priority_then_oldest` | KRS scheduling strategy |
| `SCRAPER_MAX_KRS_PER_RUN` | `0` | `0` means unlimited |

## Data Layout

DuckDB tables are split by responsibility but share one database file:

- Scraper control-plane tables: `krs_registry`, `krs_documents`, `scraper_runs`
- Prediction/ETL tables: `financial_reports`, `raw_financial_data`, `financial_line_items`, `computed_features`, and related metadata tables

Downloaded documents are stored under `data/documents` like this:

```text
data/documents/
  krs/
    0000694720/
      ZgsX8Fsncb1PFW07-T4XoQ/
        statement.xml
        manifest.json
```

The directory name is derived from the RDF `document_id` and made filesystem-safe in [`app/scraper/storage.py`](/Users/piotrkraus/piotr/rdf-api-project/app/scraper/storage.py).

## API Overview

Base URL in local development: `http://localhost:8000`

All KRS inputs accept `1-10` digits. The service zero-pads them when needed.

### Health

| Method | Path | Notes |
| --- | --- | --- |
| `GET` | `/health` | Liveness check |

Example:

```bash
curl http://localhost:8000/health
```

### RDF Proxy Endpoints

| Method | Path | Body | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/podmiot/lookup` | `{"krs":"694720"}` | Validate a KRS and return entity details |
| `POST` | `/api/podmiot/document-types` | `{"krs":"694720"}` | List available document categories |
| `POST` | `/api/dokumenty/search` | `{"krs":"694720","page":0,"page_size":10}` | Paginated document listing |
| `GET` | `/api/dokumenty/metadata/{doc_id}` | none | Raw metadata for a single document |
| `POST` | `/api/dokumenty/download` | `{"document_ids":["..."]}` | Download one or more documents as a ZIP |

Notes:

- `/api/dokumenty/search` accepts optional `sort_field` and `sort_dir`
- `sort_dir` is `MALEJACO` or `ROSNACO`
- `document_ids` accepts between 1 and 20 IDs per call
- `doc_id` is Base64-like and must stay URL-encoded when used in the path

Examples:

```bash
curl -X POST http://localhost:8000/api/podmiot/lookup \
  -H 'Content-Type: application/json' \
  -d '{"krs":"694720"}'
```

```bash
curl -X POST http://localhost:8000/api/dokumenty/search \
  -H 'Content-Type: application/json' \
  -d '{"krs":"694720","page":0,"page_size":10,"sort_dir":"MALEJACO"}'
```

```bash
curl -X POST http://localhost:8000/api/dokumenty/download \
  -H 'Content-Type: application/json' \
  -d '{"document_ids":["ZgsX8Fsncb1PFW07-T4XoQ=="]}' \
  -o documents.zip
```

### Analysis Endpoints

These endpoints work only on Polish GAAP statements that the parser understands. IFRS filings are skipped during period discovery.

| Method | Path | Body | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/analysis/available-periods/{krs}` | none | List parseable statement periods |
| `POST` | `/api/analysis/statement` | `{"krs":"694720","period_end":"2024-12-31"}` | Parse one statement into a tree |
| `POST` | `/api/analysis/compare` | `{"krs":"694720","period_end_current":"2024-12-31","period_end_previous":"2023-12-31"}` | Compare two periods |
| `POST` | `/api/analysis/time-series` | `{"krs":"694720","fields":["Aktywa","Pasywa_A","RZiS.A","RZiS.L"]}` | Track selected tags across periods |

For `time-series`, `fields` are parser tag names, for example:

- `Aktywa`
- `Aktywa_B`
- `Pasywa_A`
- `Pasywa_B_III`
- `RZiS.A`
- `RZiS.L`
- `CF.D`

Example:

```bash
curl -X POST http://localhost:8000/api/analysis/statement \
  -H 'Content-Type: application/json' \
  -d '{"krs":"694720"}'
```

### Scraper and ETL Endpoints

| Method | Path | Body | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/scraper/status` | none | Return aggregate scraper stats and last run |
| `POST` | `/api/etl/ingest` | `{}` or `{"document_id":"..."}` | Ingest all pending documents or one specific document |

Example:

```bash
curl http://localhost:8000/api/scraper/status
```

```bash
curl -X POST http://localhost:8000/api/etl/ingest \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## Bulk Scraping Workflow

The scraper is CLI-driven. Typical flow:

1. Import KRS numbers into DuckDB.
2. Run the scraper to discover and download documents.
3. Check status via CLI or `/api/scraper/status`.
4. Run ETL to ingest downloaded XML into analytical tables.

CLI entrypoint:

```bash
python -m app.scraper.cli --help
```

Useful commands:

```bash
python -m app.scraper.cli import-krs --file companies.csv --column krs
python -m app.scraper.cli import-range --start 1 --end 1000
python -m app.scraper.cli run --mode full_scan --max-krs 100
python -m app.scraper.cli status
```

Supported scraper modes:

- `full_scan`
- `new_only`
- `retry_errors`
- `specific_krs` when `--krs` is passed

## ETL and Feature Pipeline

Current pipeline after documents are on disk:

1. [`app/services/etl.py`](/Users/piotrkraus/piotr/rdf-api-project/app/services/etl.py) parses extracted XML and writes:
   - `financial_reports`
   - `raw_financial_data`
   - `financial_line_items`
2. [`scripts/seed_features.py`](/Users/piotrkraus/piotr/rdf-api-project/scripts/seed_features.py) seeds feature metadata
3. [`app/services/feature_engine.py`](/Users/piotrkraus/piotr/rdf-api-project/app/services/feature_engine.py) computes ratios and derived features

There is currently no public API endpoint for feature computation or prediction scoring. Those steps are available as Python services inside the repository.

## Testing

Run the regular test suite:

```bash
pytest tests/ -v
```

Run networked end-to-end tests against the live RDF service:

```bash
pytest tests/ -v --e2e
```

Without `--e2e`, those tests are skipped by default.

Run the dedicated live regression suite for the KRS Open API integration and RDF endpoints:

```bash
./scripts/run_regression_tests.sh
```

Those tests live under `tests/regression/` and are kept separate from the regular unit/integration suite.

## Further Reading

- [`docs/RDF_API_DOCUMENTATION.md`](/Users/piotrkraus/piotr/rdf-api-project/docs/RDF_API_DOCUMENTATION.md) for the upstream RDF API contract
- [`docs/PREDICTION_SCHEMA_DESIGN.md`](/Users/piotrkraus/piotr/rdf-api-project/docs/PREDICTION_SCHEMA_DESIGN.md) for the detailed prediction schema and lineage model
