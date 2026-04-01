# RDF API Project

FastAPI service for working with the Polish Ministry of Justice financial document registry (`rdf-przegladarka.ms.gov.pl`).

The repository currently contains five connected pieces:

1. An RDF proxy API that hides the upstream KRS encryption and exposes a cleaner HTTP interface.
2. Analysis endpoints that download and parse Polish GAAP XML statements server-side.
3. A bulk scraper that keeps a PostgreSQL inventory of KRS entities and downloaded documents.
4. An ETL and feature-engineering foundation for building a bankruptcy-prediction pipeline.
5. Auth (JWT + Google SSO) and predictions API with per-KRS access control.

## Architecture

```mermaid
graph TB
    Client["Client / Frontend"] -->|HTTPS| API

    subgraph GCP["GCP — europe-central2"]
        API["Cloud Run<br/>FastAPI"] -->|Auth Proxy| DB[("Cloud SQL<br/>PostgreSQL 16")]
        API -->|httpx| RDF

        subgraph VM["GCE VM — rdf-batch-vm"]
            Scanner["KRS Scanner<br/>7 workers + VPN"]
            Worker["RDF Worker<br/>5 workers, concurrency=5<br/>--skip-metadata for speed"]
            MetaBackfill["Metadata Backfill<br/>3 workers, concurrency=10"]
        end

        Scanner -->|probes| KRS
        Scanner -->|writes| DB
        Worker -->|discovery + download| RDF
        Worker -->|writes| DB
        Worker -->|uploads| GCS[("GCS Bucket<br/>rdf-project-documents")]
        MetaBackfill -->|metadata GET| RDF
        MetaBackfill -->|writes| DB
        Secrets["Secret Manager"] -.->|env vars| API
        Secrets -.->|.env| VM
    end

    subgraph Gov["Polish Government APIs"]
        KRS["KRS Open API<br/>api-krs.ms.gov.pl"]
        RDF["RDF Registry<br/>rdf-przegladarka.ms.gov.pl"]
    end
```

See [`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md) for the full architecture diagram and assessment.

Key design choices:

- **Two deployment targets**: Cloud Run for the API (stateless, scales to zero), GCE VM for batch scrapers (long-running, VPN-routed)
- **Git-based deploys** to the batch VM — `git pull` + `systemctl restart`
- Per-request pooled PostgreSQL connections via `ContextVar` middleware, with shared fallback for scripts/tests
- Scraper tables, prediction/ETL tables, and auth tables live in the same PostgreSQL database
- Downloaded RDF ZIPs are extracted immediately and stored in GCS (prod) or local disk (dev)
- KRS encryption is handled internally by [`app/crypto.py`](app/crypto.py); clients never need to reproduce it
- JWT-based auth with per-KRS access grants; Google SSO creates auto-verified users
- Secrets in GCP Secret Manager, not in env files or code

## Repository Layout

```text
app/
  main.py                 FastAPI app and startup/shutdown lifecycle
  config.py               Environment-driven settings
  crypto.py               RDF KRS encryption
  rdf_client.py           Async HTTP client for the upstream RDF API
  db/
    connection.py         Shared PostgreSQL connection manager
    prediction_db.py      Prediction/ETL schema and CRUD helpers
  auth.py                 JWT utilities, get_current_user dependency, access control
  rate_limit.py           Shared slowapi Limiter instance
  routers/
    rdf/                  Proxy endpoints for the upstream RDF API
    analysis/             XML parsing and comparison endpoints
    scraper/              Read-only scraper status endpoint
    etl/                  ETL trigger endpoint
    auth/                 Signup, login, verify, Google SSO, admin grant endpoints
    predictions/          Per-KRS prediction scores, history, model catalog
  scraper/
    cli.py                Scraper CLI
    db.py                 Scraper schema and CRUD helpers
    job.py                Bulk scraping job
    storage.py            Local extracted-file storage
  services/
    xml_parser.py         Statement parsing and comparison logic
    etl.py                XML -> PostgreSQL ingestion
    feature_engine.py     Ratio and feature computation
    predictions.py        Scoring, caching, response assembly for predictions API
scripts/
  seed_features.py        Seeds feature definitions and feature sets
  seed_admin.py           Bootstrap admin user (--password or --google)
  quick_scan.py           Find N valid KRS entities, scrape docs, run ETL
tests/
  unit/integration tests plus optional networked e2e coverage
docs/
  RDF_API_DOCUMENTATION.md    Reverse-engineered upstream RDF API reference
  PREDICTION_SCHEMA_DESIGN.md Prediction-layer schema reference
```

## Quick Start

### Prerequisites

- Python 3.12
- Docker (for PostgreSQL)
- Network access to `rdf-przegladarka.ms.gov.pl` if you want to call the live upstream API

### Local setup

```bash
# Start PostgreSQL
docker compose up -d

# Python environment
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
| `DATABASE_URL` | `postgresql://rdf:rdf_dev@localhost:5432/rdf` | PostgreSQL connection string |
| `STORAGE_BACKEND` | `local` | `local` (dev) or `gcs` (prod — requires GCP credentials) |
| `STORAGE_LOCAL_PATH` | `data/documents` | Extracted document root |
| `SCRAPER_ORDER_STRATEGY` | `priority_then_oldest` | KRS scheduling strategy |
| `SCRAPER_MAX_KRS_PER_RUN` | `0` | `0` means unlimited |
| `ENVIRONMENT` | `local` | `local`, `staging`, or `production` |
| `JWT_SECRET` | `change-me-in-production` | **Required** in staging/prod (>=32 bytes). Generate: `python -c "import secrets; print(secrets.token_hex(32))"` |
| `JWT_EXPIRE_MINUTES` | `1440` | JWT token lifetime (24h default) |
| `GOOGLE_CLIENT_ID` | _(empty)_ | Google OAuth2 client ID for SSO |
| `VERIFICATION_EMAIL_MODE` | `log` | `log` (dev, prints code) or `smtp` (sends real email) |

## Data Layout

PostgreSQL tables are split by responsibility but share one database:

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

### Auth Endpoints

All auth endpoints are under `/api/auth`. Signup and verify are rate-limited.

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/api/auth/signup` | none | Register with email + password (min 8 chars). Returns `user_id` for verification |
| `POST` | `/api/auth/verify` | none | Submit 6-digit code to verify email. Returns JWT |
| `POST` | `/api/auth/login` | none | Email + password login. Returns JWT |
| `POST` | `/api/auth/google` | none | Exchange Google OAuth2 ID token for JWT. Auto-creates verified user |
| `GET` | `/api/auth/me` | Bearer JWT | Current user profile and KRS access list |
| `POST` | `/api/auth/admin/grant-access` | Bearer JWT (admin) | Grant a user access to a specific KRS number |

Examples:

```bash
# Sign up
curl -X POST http://localhost:8000/api/auth/signup \
  -H 'Content-Type: application/json' \
  -d '{"email":"user@example.com","password":"securepass123"}'

# Verify (code is logged in dev mode)
curl -X POST http://localhost:8000/api/auth/verify \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"<uuid>","code":"123456"}'

# Login
curl -X POST http://localhost:8000/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"user@example.com","password":"securepass123"}'

# Get profile
curl http://localhost:8000/api/auth/me \
  -H 'Authorization: Bearer <token>'
```

### Predictions Endpoints

Predictions require authentication. Access to individual KRS numbers is gated by `user_krs_access` grants (or `has_full_access` for admins).

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `GET` | `/api/predictions/models` | none | List active models with interpretation thresholds |
| `GET` | `/api/predictions/{krs}` | Bearer JWT + KRS access | Full prediction detail: scores, features with source tags, history |
| `GET` | `/api/predictions/{krs}/history` | Bearer JWT + KRS access | Score timeline per model (optional `?model_id=` filter) |
| `POST` | `/api/predictions/cache/invalidate` | Bearer JWT (admin) | Flush model and feature definition caches |

Examples:

```bash
# List models (no auth required)
curl http://localhost:8000/api/predictions/models

# Get predictions for a KRS (requires auth + access)
curl http://localhost:8000/api/predictions/0000694720 \
  -H 'Authorization: Bearer <token>'

# Get prediction history
curl 'http://localhost:8000/api/predictions/0000694720/history?model_id=maczynska_1994_v1' \
  -H 'Authorization: Bearer <token>'
```

## Bulk Scraping Workflow

The scraper is CLI-driven. Typical flow:

1. Import KRS numbers into the database.
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

## Admin Setup

Bootstrap an admin user to access predictions and grant KRS access to others:

```bash
# With password (for local auth)
python scripts/seed_admin.py admin@example.com --password "S3cureP@ss!" "Admin Name"

# For Google SSO admin (no password needed)
python scripts/seed_admin.py admin@example.com --google "Admin Name"
```

## ETL and Feature Pipeline

Current pipeline after documents are on disk:

1. [`app/services/etl.py`](/Users/piotrkraus/piotr/rdf-api-project/app/services/etl.py) parses extracted XML and writes:
   - `financial_reports`
   - `raw_financial_data`
   - `financial_line_items`
2. [`scripts/seed_features.py`](/Users/piotrkraus/piotr/rdf-api-project/scripts/seed_features.py) seeds feature metadata
3. [`app/services/feature_engine.py`](/Users/piotrkraus/piotr/rdf-api-project/app/services/feature_engine.py) computes ratios and derived features

Prediction scores are exposed via `/api/predictions/{krs}` (see Auth and Predictions sections above). Scores are pre-computed by `app/services/maczynska.py` and stored in the `predictions` table.

## Testing

Tests are organized by app layer:

```bash
# Run all unit/integration tests
pytest tests/ -v

# Run by category
pytest tests/db/ -v          # DB schema, CRUD, versioning
pytest tests/api/ -v         # FastAPI endpoints
pytest tests/batch/ -v       # Batch scanner/worker
pytest tests/services/ -v    # ETL, feature engine, crypto
pytest tests/krs/ -v         # KRS adapter, client, pipeline
pytest tests/scraper/ -v     # Scraper integration, storage
```

Run end-to-end tests against live APIs (RDF + KRS):

```bash
pytest tests/e2e/ -v --e2e
```

Without `--e2e`, those tests are skipped by default.

Run the dedicated live regression suite for the KRS Open API integration and RDF endpoints:

```bash
./scripts/run_regression_tests.sh
```

Those tests live under `tests/regression/` and are kept separate from the regular unit/integration suite.

## Cloud Deployment

The project runs on GCP (`rdf-api-project`, `europe-central2`). Two components:

| Component | Runs on | Deploy method |
|-----------|---------|---------------|
| **FastAPI app** | Cloud Run | `gcloud run deploy rdf-api --source . --region europe-central2` |
| **Batch scrapers** | GCE VM (`rdf-batch-vm`) | `git pull` + `systemctl restart` |

Shared persistence:
- **Cloud SQL** (`rdf-postgres`) — PostgreSQL 16, all tables
- **GCS** (`rdf-project-documents`) — extracted document files
- **Secret Manager** — `database-url`, `jwt-secret`, `nordvpn-*`

### Deploying the API (Cloud Run)

```bash
gcloud run deploy rdf-api --source . --region europe-central2
```

Cloud Build reads the Dockerfile, builds the image, and updates the service. Takes 2-4 minutes.

### Deploying batch workers (GCE VM)

```bash
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --project=rdf-api-project
cd /opt/rdf-api-project
sudo -u worker git pull origin main
sudo -u worker .venv/bin/pip install -r requirements.txt  # only if deps changed
sudo systemctl restart rdf-worker         # RDF document discovery + download
sudo systemctl restart krs-scanner        # KRS integer scanner (if changed)
sudo systemctl restart metadata-backfill  # Metadata backfill (if changed)
```

See [`docs/CLOUD_DEPLOYMENT.md`](docs/CLOUD_DEPLOYMENT.md) for full details: architecture, secrets, VM setup, .env config, monitoring, and costs.

## Further Reading

- [`docs/CLOUD_DEPLOYMENT.md`](docs/CLOUD_DEPLOYMENT.md) — GCP architecture, deployment, secrets, and operations
- [`docs/ARCHITECTURE_REVIEW.md`](docs/ARCHITECTURE_REVIEW.md) — Full architecture review with diagrams, assessment, and recommendations
- [`docs/RDF_API_DOCUMENTATION.md`](docs/RDF_API_DOCUMENTATION.md) — Upstream RDF API contract
- [`docs/PREDICTION_SCHEMA_DESIGN.md`](docs/PREDICTION_SCHEMA_DESIGN.md) — Prediction schema and lineage model
