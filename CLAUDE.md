# RDF API Project

## What this is

FastAPI service around Repozytorium Dokumentow Finansowych (`rdf-przegladarka.ms.gov.pl`).
The repository currently contains:

- RDF proxy endpoints for entity lookup, document search, metadata, and ZIP download
- Financial statement analysis endpoints
- A bulk scraper that stores data in PostgreSQL (migrated from DuckDB)
- ETL and feature-engineering building blocks for a prediction pipeline

## Agent autonomy

Agents should operate autonomously without blocking on user approvals. Permissions are pre-configured in `.claude/settings.json` (shared) and `.claude/settings.local.json` (personal).

### Boundaries

- **Stay inside the project folder.** Never read, write, or execute outside `/Users/piotrkraus/piotr/rdf-api-project/` except for the Claude memory directory.
- **Never delete files** without explicit user request. Use `git mv` for renames, not rm + create.
- **Never write secrets** (.env, API keys, credentials) to tracked files. `.env` is gitignored; keep it that way.
- **No force-push, hard-reset, or clean -fd.** These are denied in settings.json.
- **Commit often, push only when asked.**

### Decision logging

When making non-trivial implementation decisions, document them so the user can review asynchronously:

1. **Issue-specific decisions** (e.g., "chose approach A over B for PKR-12"): add a comment on the Linear issue via MCP (`save_comment`).
2. **Architectural or cross-cutting decisions** (e.g., "switched from EAV to wide table for X"): save to Claude memory as a `project` type memory file.
3. **Session progress**: update `project_backlog_state.md` memory at end of session.

Decision comments should be short: what was decided, why, and what alternatives were rejected.

### Session workflow

1. **Start**: read Claude memory + check Linear backlog (via MCP) to orient.
2. **Plan**: if the task maps to a Linear issue, read the full issue spec with `get_issue`. If multi-step, break into sub-tasks.
3. **Execute**: implement, test, commit. Log decisions per the rules above.
4. **Close**: update Linear issue status, update memory, update CLAUDE.md/README if project surface changed.

## Key context files - read these first

- `README.md` - current project overview, setup, architecture, and API summary
- `docs/RDF_API_DOCUMENTATION.md` - reverse-engineered upstream RDF API contract
- `docs/PREDICTION_SCHEMA_DESIGN.md` - prediction schema and lineage design (Layer 1 updated: `data_sources`/`company_identifiers` removed)
- `docs/KRS_OPEN_API.md` - official MS KRS Open API reference (endpoints, response structure, GDPR)
- `docs/KRS_SYNC_RUNBOOK.md` - operational runbook for KRS sync pipeline

## Tech stack

- Python 3.12, FastAPI, uvicorn, httpx (async), pycryptodome, pydantic v2
- PostgreSQL for all persistence (migrated from DuckDB)
- psycopg2-binary for PostgreSQL connections with ConnectionWrapper (preserves DuckDB-style API)
- NO requests library - everything async with httpx
- NO manual threading - use async + uvicorn --workers

## Critical: KRS encryption

The `/dokumenty/wyszukiwanie` endpoint requires AES-128-CBC encrypted KRS token.
Full algorithm is in `docs/RDF_API_DOCUMENTATION.md` and implemented in `app/crypto.py`.
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
  rdf_client.py        - httpx.AsyncClient wrapper for RDF upstream (singleton, created in lifespan)
  krs_client.py        - Resilient httpx client for KRS Open API (retry, backoff, polite pacing)
  adapters/
    base.py            - KrsSourceAdapter Protocol (get_entity, search, health_check)
    models.py          - KrsEntity, SearchResult, SearchResponse, AdapterHealth
    registry.py        - Adapter registry keyed by source name
    ms_gov.py          - MsGovKrsAdapter — concrete adapter for api-krs.ms.gov.pl
    exceptions.py      - Common adapter exceptions (AdapterError, EntityNotFoundError, etc.)
  jobs/
    krs_sync.py        - Scheduled KRS entity sync job (discovery + re-enrichment)
    krs_scanner.py     - Resumable sequential KRS integer scanner (probes 1,2,3…)
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
    jobs/
      routes.py        - /jobs/krs-sync/* (status, trigger)
    etl/
      routes.py        - /api/etl/ingest
  services/
    xml_parser.py      - e-Sprawozdanie XML parser (~1300 TAG_LABELS for Bilans, RZiS, CF)
    etl.py             - XML-to-DuckDB ingestion pipeline
    feature_engine.py  - Computes financial ratios from line items
  monitoring/
    metrics.py         - Per-call metrics ring buffer, record_api_call(), get_stats()
  repositories/
    krs_repo.py        - DuckDB CRUD for krs_entities + krs_sync_log tables
  db/
    connection.py      - Shared DuckDB connection manager (single lifecycle)
    prediction_db.py   - DuckDB schema init + CRUD for prediction tables
  scraper/
    cli.py             - Scraper CLI (import-krs, import-range, run, status)
    db.py              - DuckDB schema + CRUD for scraper tables (existing)
    job.py             - Scraper job logic
    storage.py         - Document storage abstraction
batch/
  __init__.py          - Package marker
  __main__.py          - `python -m batch` entrypoint
  connections.py       - Connection dataclass + NordVPN SOCKS5 pool builder
  progress.py          - DuckDB-backed progress store (batch_progress table)
  entity_store.py      - Append-only entity store for batch scanner
  rdf_document_store.py - Append-only document store for batch RDF worker
  rdf_progress.py      - RDF document discovery progress store
  worker.py            - Async worker loop with stride partitioning + backoff
  rdf_worker.py        - RDF document discovery + download worker
  runner.py            - Multiprocessing orchestrator + argparse CLI
  rdf_runner.py        - RDF document batch orchestrator
scripts/
  seed_features.py     - Populate feature_definitions and feature_sets
  run_db_migration.py  - Run schema migrations (e.g., append-only backfill)
  db_migrations/       - Migration scripts (001_append_only_backfill.py, etc.)
tests/
  api/                 - FastAPI endpoint tests (endpoints, jobs routes)
  db/                  - DB schema + CRUD tests (prediction_db, krs_repo, scraper_db,
                         append-only versioning, document versions, migrations)
  batch/               - Batch processing tests (worker, runner, progress, connections,
                         rdf_worker, rdf_progress, rdf_runner)
  services/            - ETL, feature engine, crypto, code review fixes
  scraper/             - Scraper integration, storage
  krs/                 - KRS adapter, client, sync pipeline, scanner, monitoring
  e2e/                 - End-to-end tests against live APIs (--e2e flag required)
  regression/          - Live API regression tests (--e2e flag required)
data/
  scraper.duckdb       - Legacy DuckDB file (pre-migration backup, read-only analytics)
  documents/           - Extracted RDF files + manifest.json
```

## Database: PostgreSQL tables

### Scraper tables (app/scraper/db.py)
- `krs_registry` - KRS master list, scraper priority/scheduling
- `krs_documents` - Documents per KRS (legacy cache; reads via `krs_documents_current` view)
- `krs_document_versions` - Append-only document history. PK = version_id. UNIQUE(document_id, version_no).
- `scraper_runs` - Scraper run history

### KRS entity tables (app/repositories/krs_repo.py)
- `krs_entities` - Legacy cache of KRS entity data. PK = krs VARCHAR(10). Reads via `krs_entities_current` view.
- `krs_entity_versions` - Append-only entity history. PK = version_id. Indexed on (krs, is_current).
- `krs_sync_log` - Sync run history (started_at, counts, status).
- `krs_scan_cursor` - Single-row table tracking next KRS integer to probe. PK = boolean TRUE.
- `krs_scan_runs` - One row per scanner invocation (krs_from/to, probed/valid/error counts, stopped_reason).

### Views
- `krs_entities_current` - Current entity snapshot from `krs_entity_versions` (read path for all entity queries).
- `krs_documents_current` - Current document snapshot from `krs_document_versions` (read path for all document queries).
- `latest_successful_financial_reports` - Latest completed report per logical key (used by feature engine).

### Batch scanner table (batch/progress.py)
- `batch_progress` - Tracks which KRS integers have been probed by the batch scanner. PK = krs BIGINT. Status: found/not_found/error.

### Prediction tables (app/db/prediction_db.py)
Full DDL in `docs/PREDICTION_SCHEMA_DESIGN.md`. Summary:

**Layer 1 - Entities:**
- `companies` - Extended company data. PK = krs VARCHAR(10). Joins to krs_registry.krs.
- `etl_attempts` - Tracks every ETL ingestion attempt (completed/failed/skipped). PK = attempt_id.

**Layer 2 - Financial data:**
- `financial_reports` - One row per ingested statement. Links to krs_documents.
- `raw_financial_data` - JSON per section (balance_sheet, income_statement, cash_flow).
- `financial_line_items` - THE WORKHORSE. Flattened tag/value pairs. PK = (report_id, section, tag_path).
  - tag_path examples: `Aktywa`, `Pasywa_A`, `RZiS.A`, `CF.D`

**Layer 3 - Features:**
- `feature_definitions` - Feature metadata (formula, required tags). PK = short code ('roa', 'current_ratio').
- `feature_sets` - Named groups ('maczynska_6', 'basic_20').
- `feature_set_members` - Many-to-many with ordinal.
- `computed_features` - Cached feature values (EAV pattern). PK = (report_id, feature_definition_id, computation_version).

**Layer 4 - Models & Predictions:**
- `model_registry` - Trained model metadata + artifact paths.
- `prediction_runs` - Batch scoring runs.
- `predictions` - Individual scores with risk_category and SHAP explanations.

**Layer 5 - Ground Truth:**
- `bankruptcy_events` - Historical bankruptcy/restructuring events (training labels).

**Job tracking:**
- `assessment_jobs` - Tracks async pipeline status for the UI polling pattern.

## Key patterns

### PostgreSQL connection pattern
One shared PostgreSQL connection managed by `app/db/connection.py`:
- `app/db/connection.py` owns the connection lifecycle (connect/close/reset) using psycopg2
- `ConnectionWrapper` wraps psycopg2 to preserve `conn.execute(sql, params).fetchone()` API
- Connections use `autocommit=True` to match DuckDB's per-statement commit behavior
- `app/scraper/db.py` and `app/db/prediction_db.py` delegate to the shared connection
- Each module has `connect()` (ensures schema), `get_conn()` (returns shared conn), `close()` (no-op)
- `app/main.py` lifespan calls `db_conn.connect()` + both schema inits at startup
- Batch workers use `make_connection(dsn)` for standalone connections (no retry/lock logic needed)
- Plain SQL with `%s` parameterized queries, no ORM
- Configuration via `DATABASE_URL` env var (default: `postgresql://rdf:rdf_dev@localhost:5432/rdf`)

### XML parsing
`app/services/xml_parser.py` has ~1300 TAG_LABELS mapping XML tags to Polish labels.
It parses e-Sprawozdania into hierarchical trees with kwota_a (current) and kwota_b (previous).
The ETL flattens these trees into financial_line_items using tag paths such as `Aktywa`, `Pasywa_A`, `RZiS.A`, and `CF.D`.

### Feature computation
Features are defined as metadata rows in feature_definitions, not hardcoded columns.
Adding a new feature = INSERT into feature_definitions + re-run compute. No schema changes.
computation_logic types: 'ratio' (num/denom), 'difference', 'raw_value', 'custom' (Python function).

## Commands

```bash
# Install
pip install -r requirements.txt

# Run dev
uvicorn app.main:app --reload --port 8000

# Run prod
uvicorn app.main:app --workers 4 --port 8000

# Test all unit tests
pytest tests/ -v

# Test by category
pytest tests/db/ -v
pytest tests/api/ -v
pytest tests/batch/ -v
pytest tests/services/ -v
pytest tests/krs/ -v

# E2E tests (hits live APIs)
pytest tests/e2e/ -v --e2e

# Seed feature definitions
python scripts/seed_features.py

# Batch KRS scanner (all flags optional, defaults from .env)
python -m batch.runner
python -m batch.runner --start 500000 --workers 3 --vpn
python -m batch.runner --start 1 --no-vpn --delay 2.0
```

## API endpoints

### RDF proxy (existing)
| Method | Path | Upstream | Notes |
|--------|------|----------|-------|
| POST | /api/podmiot/lookup | dane-podstawowe | Plain KRS |
| POST | /api/podmiot/document-types | rodzajeDokWyszukiwanie | Plain KRS |
| POST | /api/dokumenty/search | wyszukiwanie | Client sends plain KRS; service encrypts internally |
| GET | /api/dokumenty/metadata/{id} | dokumenty/{id} | URL-encode Base64 ID |
| POST | /api/dokumenty/download | dokumenty/tresc | Returns ZIP |
| GET | /health | - | Simple healthcheck |
| GET | /health/krs | - | KRS adapter health (200 or 503) |
| GET | /metrics/krs | - | Per-call stats: p50/p95 latency, error rate |

### Analysis (existing)
| Method | Path | Notes |
|--------|------|-------|
| POST | /api/analysis/statement | Parse single statement |
| POST | /api/analysis/compare | Compare two periods |
| POST | /api/analysis/time-series | Track fields across years |
| GET | /api/analysis/available-periods/{krs} | List statement periods |

### Scraper + ETL (existing)
| Method | Path | Notes |
|--------|------|-------|
| GET | /api/scraper/status | Aggregate scraper stats + last run |
| POST | /api/etl/ingest | Trigger document ingestion |

### KRS sync job
| Method | Path | Notes |
|--------|------|-------|
| GET | /jobs/krs-sync/status | Last sync run summary (time, counts, errors) |
| POST | /jobs/krs-sync/trigger | Queue a sync run (202 accepted, 409 if already running) |

### KRS sequential scanner
| Method | Path | Notes |
|--------|------|-------|
| GET | /jobs/krs-scan/status | Cursor position, is_running, last run stats, total entities |
| POST | /jobs/krs-scan/trigger | Fire scan in background (202 accepted, 409 if running) |
| POST | /jobs/krs-scan/stop | Signal running scan to stop after current probe |
| POST | /jobs/krs-scan/reset-cursor | Body: `{"next_krs_int": N}`. Rejected 409 if running |

## Gotchas

1. Document IDs are Base64 with `=`, `+`, `/` - must URL-encode in path params
2. The `nrKRS` field name differs between endpoints (numerKRS vs nrKRS)
3. Download endpoint needs Accept: application/octet-stream header override
4. Use StreamingResponse for download endpoint
5. CORS must be enabled (frontend runs on different port)
6. KRS is VARCHAR(10) everywhere - the natural join key across all tables
7. PostgreSQL JSON type — use json_extract_path_text() or ->> for queries
8. Feature store uses EAV pattern - pivot to wide format for ML training
9. `STORAGE_BACKEND=gcs` requires `google-cloud-storage` and valid GCP credentials (ADC or service account)
10. Generate fresh encryption token for EVERY search request - never cache

## Keeping docs current

After completing a Linear issue or making structural changes (new files, endpoints, tables, commands), update this file and the Claude memory system before finishing the conversation:

1. **This file (CLAUDE.md):** update project structure, endpoint tables, database tables, commands, or gotchas if any of those changed.
2. **README.md:** update if user-facing information changed (new endpoints, new CLI commands, new config options).
3. **Claude memory (`project_backlog_state.md`):** update which Linear issues are done vs in-progress.
4. **Linear issues:** move completed issues to Done, add implementation notes as comments.

Check the Linear backlog at the start of each session to orient on what's next.
