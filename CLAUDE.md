# RDF API Project

## What this is

FastAPI service around Repozytorium Dokumentow Finansowych (`rdf-przegladarka.ms.gov.pl`).
The repository currently contains:

- RDF proxy endpoints for entity lookup, document search, metadata, and ZIP download
- Financial statement analysis endpoints
- A bulk scraper that stores data in PostgreSQL
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
- `docs/download_speed_up_tasks.md` - download pipeline optimization design (--skip-metadata, async GCS, metadata backfill)

## Tech stack

- Python 3.12, FastAPI, uvicorn, httpx (async), pycryptodome, pydantic v2
- PostgreSQL for all persistence (local dev via docker-compose)
- psycopg2-binary for PostgreSQL connections with ConnectionWrapper
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
      routes.py        - /api/etl/ingest, /api/etl/training/dataset-stats
    auth/
      routes.py        - /api/auth/* (signup, login, verify, Google SSO, admin grant)
      schemas.py       - Auth request/response Pydantic models
    predictions/
      routes.py        - /api/predictions/* (per-KRS scores, history, models, cache)
      schemas.py       - Prediction response Pydantic models
  auth.py              - JWT create/decode, get_current_user dependency, require_admin, require_krs_access
  rate_limit.py        - Shared slowapi Limiter instance
  services/
    xml_parser.py      - e-Sprawozdanie XML parser (~1300 TAG_LABELS for Bilans, RZiS, CF)
    etl.py             - XML-to-PostgreSQL ingestion pipeline
    feature_engine.py  - Computes financial ratios from line items (incl. x1_maczynska custom)
    maczynska.py       - Maczynska 1994 discriminant model (baseline bankruptcy predictor)
    poznanski.py       - Poznanski 2004 (Hamrol/Czajka/Piechocki) 4-variable discriminant model with U-shape liquidity warning
    training_data.py   - EAV-to-wide pivot, bankruptcy label joining, dataset stats
    predictions.py     - Predictions service: scoring, caching, response assembly
  monitoring/
    metrics.py         - Per-call metrics ring buffer, record_api_call(), get_stats()
  repositories/
    krs_repo.py        - PostgreSQL CRUD for krs_entities + krs_sync_log tables
  db/
    connection.py      - PostgreSQL connection manager (shared conn + ThreadedConnectionPool + ContextVar per-request)
    prediction_db.py   - PostgreSQL schema init + CRUD for prediction + auth tables
  scraper/
    cli.py             - Scraper CLI (import-krs, import-range, run, status)
    db.py              - PostgreSQL schema + CRUD for scraper tables
    job.py             - Scraper job logic
    storage.py         - Document storage abstraction (Local + GCS, with async_save_extracted)
batch/
  __init__.py          - Package marker
  __main__.py          - `python -m batch` entrypoint
  connections.py       - Connection dataclass, NordVPN SOCKS5 pool builder, shared validate_vpn_config()
  progress.py          - PostgreSQL-backed progress store (batch_progress table)
  entity_store.py      - Append-only entity store for batch scanner
  rdf_document_store.py - Append-only document store for batch RDF worker
  rdf_progress.py      - RDF document discovery progress store
  worker.py            - Async worker loop with stride partitioning + backoff
  rdf_worker.py        - RDF document discovery + download worker (supports --skip-metadata)
  runner.py            - Multiprocessing orchestrator + argparse CLI
  rdf_runner.py        - RDF document batch orchestrator (supports --skip-metadata)
  metadata_backfill.py - Standalone metadata backfill worker for docs downloaded with --skip-metadata
  metadata_runner.py   - Multiprocessing orchestrator for metadata backfill
scripts/
  seed_features.py     - Populate feature_definitions and feature_sets
  seed_admin.py        - Bootstrap admin user (--password or --google)
  quick_scan.py        - Find N valid KRS entities, scrape docs, run ETL
  pull_cloud_data.sh   - Pull cloud DB data to local for KRS numbers in data/documents/krs/
tests/
  api/                 - FastAPI endpoint tests (endpoints, jobs, auth, predictions)
  db/                  - DB schema + CRUD tests (prediction_db, krs_repo, scraper_db,
                         append-only versioning, document versions)
  batch/               - Batch processing tests (worker, runner, progress, connections,
                         rdf_worker, rdf_progress, rdf_runner)
  services/            - ETL, feature engine, crypto, Maczynska model, training data, code review fixes
  scraper/             - Scraper integration, storage
  krs/                 - KRS adapter, client, sync pipeline, scanner, monitoring
  e2e/                 - End-to-end tests against live APIs (--e2e flag required)
  regression/          - Live API regression tests (--e2e flag required)
deploy/
  krs-scanner.service    - systemd unit for KRS batch scanner
  rdf-worker.service     - systemd unit for RDF document download worker
  metadata-backfill.service - systemd unit for metadata backfill worker
  rdf-backup.cron        - Cron job for RDF backup
data/
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
- `dead_proxies` - Global dead-proxy registry shared across all workers. PK = proxy_name TEXT. 6h TTL. Populated by ProxyRotator on eviction and preflight check.

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
- `predictions` - Individual scores with risk_category, feature_contributions, and `feature_snapshot` (JSON map `{feature_id: computation_version}` captured at scoring time so the read path can fetch the exact feature values that fed the score — immutable reference, no timestamp heuristics).

**Layer 5 - Ground Truth:**
- `bankruptcy_events` - Historical bankruptcy/restructuring events (training labels).

**Job tracking:**
- `assessment_jobs` - Tracks async pipeline status for the UI polling pattern.

**Auth tables (app/db/prediction_db.py):**
- `users` - User accounts. PK = id VARCHAR. UNIQUE(email). auth_method: 'local' or 'google'.
- `verification_codes` - 6-digit email verification codes with expiry. FK to users.
- `user_krs_access` - Per-user KRS access grants. PK = (user_id, krs).

## Key patterns

### PostgreSQL connection pattern
Managed by `app/db/connection.py` with two tiers:
- **Shared connection** (`connect()` / `get_conn()`): single global conn for startup, schema init, CLI scripts, tests
- **Per-request pooled connections** (`ThreadedConnectionPool` + `ContextVar`): middleware acquires a pooled conn at request start, binds it to `_request_conn` ContextVar, releases it after response. `get_conn()` prefers the request-scoped conn when available.
- `ConnectionWrapper` wraps psycopg2 with convenient `conn.execute(sql, params).fetchone()` API
- All connections use `autocommit=True` for per-statement commit behavior
- `app/scraper/db.py` and `app/db/prediction_db.py` delegate to `get_conn()` which routes to the right connection
- `app/main.py` lifespan calls `db_conn.connect()` + `db_conn.init_pool()` + both schema inits at startup
- Batch workers use `make_connection(dsn)` for standalone connections (no retry/lock logic needed)
- `get_db()` context manager is available for explicit pool usage outside middleware
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
Each `computed_features` row has a `(report_id, feature_definition_id, computation_version)` PK so scoring can pin an immutable snapshot — see the Predictions response assembly section.

### Predictions response assembly
`/api/predictions/{krs}` returns one `PredictionDetail` per `(model_id, fiscal_year)`, each with its own `features[]`, `source_tags[]`, and `scored_at`. Hot-path assembly lives in `app/services/predictions.py::get_predictions` and uses two batched DB round-trips regardless of the number of rows:

- `prediction_db.get_features_for_predictions_batch(requests)` — keyed by a caller-supplied unique `request_id` per prediction (the service uses `f"{model_id}::{fiscal_year}::{report_id}"`). Enforces `request_id` uniqueness; raises `ValueError` on duplicates. Exact-snapshot path joins `computed_features` by the immutable `(report_id, feature_definition_id, computation_version)` triple with `cf.is_valid = true` and `feature_set_members` membership. Partial/corrupted snapshots demote the request to a batched window-function fallback partitioned by `(request_id, feature_definition_id)` and emit a structured `feature_snapshot_incomplete_fallback` warning. Chunked at `_BATCH_CHUNK_SIZE = 800` tuples to stay under PostgreSQL's parameter limit.
- `prediction_db.get_source_line_items_for_reports_batch(requests)` — one query per N reports using a single CTE. `value_previous` is resolved from the immediately prior fiscal year's latest completed report for the same KRS, **constrained by `data_source_id` AND `report_type`** so deltas never cross filing types. Returns `schema_code` per item for label fallback.

`_assemble_features()` stitches pre-loaded data, resolves `label_pl` against the item's own `schema_code` first (then report-level, then global registry), and stamps `higher_is_better` from the explicit per-tag registry in `predictions.py::_TAG_SEMANTIC_REGISTRY` (exact tag matches only, unknowns → `None` — no prefix inheritance).

`history[]` on the response is a backwards-compatible subset of `predictions[]`. New consumers can ignore it.

Scoring writes `feature_snapshot = {feature_id: computation_version}` on the `predictions` row so reads stay deterministic after rescoring or feature-definition edits. The Mączyńska scorer (`app/services/maczynska.py::score_report`) captures it from `get_computed_features_for_report`.

Feature-definition drift: an idempotent migration in `_init_schema()` backfills `x1_maczynska.required_tags` to include `CF.A_II_1` (covers NULL and stale rows). Re-run `scripts/seed_features.py` on deploy to keep the seed authoritative.

## Commands

```bash
# Start PostgreSQL (Docker)
docker compose up -d

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

# Seed admin user
python scripts/seed_admin.py admin@example.com --password "S3cureP@ss!" "Admin Name"
python scripts/seed_admin.py admin@example.com --google  # Google SSO admin

# Quick scan: find entities, scrape docs, run ETL
python scripts/quick_scan.py --count 10 --start 1

# Batch KRS scanner (all flags optional, defaults from .env)
python -m batch.runner
python -m batch.runner --start 500000 --workers 3 --vpn
python -m batch.runner --start 1 --no-vpn --delay 2.0

# RDF document download (fast mode skips metadata, backfill later)
python -m batch.rdf_runner --skip-metadata --no-vpn
python -m batch.rdf_runner --workers 5 --concurrency 5

# Metadata backfill (for docs downloaded with --skip-metadata)
python -m batch.metadata_runner --no-vpn
python -m batch.metadata_runner --workers 3 --concurrency 10 --delay 0.2

# Pull cloud DB data to local (for KRS numbers in data/documents/krs/)
cloud-sql-proxy "rdf-api-project:europe-central2:rdf-postgres" --port 15432 &
bash scripts/pull_cloud_data.sh
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
| GET | /api/etl/training/dataset-stats | Training dataset quality summary (feature_set query param) |

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

### Auth
| Method | Path | Auth | Notes |
|--------|------|------|-------|
| POST | /api/auth/signup | - | Email+password signup. Rate limited 5/min. Returns user_id for verify step |
| POST | /api/auth/verify | - | 6-digit code verification. Rate limited 10/min |
| POST | /api/auth/login | - | Email+password login. Returns JWT + user profile |
| POST | /api/auth/google | - | Google OAuth2 ID token exchange. Auto-creates verified user |
| GET | /api/auth/me | Bearer JWT | Current user profile + KRS access list |
| POST | /api/auth/admin/grant-access | Bearer JWT (admin) | Grant KRS access to a user. Rate limited 20/min |

### Predictions
| Method | Path | Auth | Notes |
|--------|------|------|-------|
| GET | /api/predictions/models | - | List active models with interpretation thresholds |
| GET | /api/predictions/{krs} | Bearer JWT + KRS access | Full prediction detail: scores, features, source tags, history |
| GET | /api/predictions/{krs}/history | Bearer JWT + KRS access | Score timeline per model for charting |
| POST | /api/predictions/cache/invalidate | Bearer JWT (admin) | Flush model + feature definition caches |

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
11. JWT_SECRET must be >=32 bytes in staging/production — app refuses to start otherwise
12. Auth endpoints are rate-limited via slowapi (signup 5/min, verify 10/min, grant-access 20/min)
13. `get_conn()` returns per-request pooled connection during HTTP requests, shared connection in scripts/tests
14. `--skip-metadata` is off by default — download workers fetch metadata unless explicitly skipped. Use `metadata_runner` to backfill later
15. Metadata backfill uses keyset pagination (`(krs, document_id)` cursor) — memory stays O(batch_size) regardless of backlog
16. VPN is ON by default for `metadata_runner` unless `--no-vpn` is passed. VPN config is validated before spawning workers (fail-fast)
17. `async_save_extracted()` on storage classes runs sync I/O in `run_in_executor` — keeps the event loop unblocked during GCS uploads
18. `batch/connections.py` has shared `validate_vpn_config()` — used by both `rdf_runner` and `metadata_runner`
19. Cloud DB password is stored in GCP Secret Manager (`cloud-db-password`). Never hardcode it — always fetch via `gcloud secrets versions access latest --secret=cloud-db-password --project=rdf-api-project`
20. GCP secrets: `database-url` (full Cloud SQL connection string), `cloud-db-password` (postgres password only), `jwt-secret`, `nordvpn-username`, `nordvpn-password`
21. `/api/predictions/{krs}` returns N `PredictionDetail` rows (one per `(model_id, fiscal_year)`), not the latest year only. Each row carries its own `features[]` and `source_tags[]`. `history[]` is backwards-compat only.
22. Every request to `get_features_for_predictions_batch` must carry a unique `request_id` — sharing report+feature_set across different snapshots or scored_at would collapse rows. Service uses `f"{model_id}::{fiscal_year}::{report_id}"`.
23. ETL fails with `reason_code=invalid_period_end` when `period_end` is missing or unparseable — never coerces `fiscal_year` to 0.
24. `SourceTag.higher_is_better` uses an **exact-match** per-tag registry in `app/services/predictions.py::_TAG_SEMANTIC_REGISTRY`. Adding a tag is a deliberate domain decision; unknowns resolve to `None` (neutral). No prefix inheritance — sibling tags do not auto-inherit.
25. **Proxy pool configuration** — three env vars control proxy behavior:
    - `BATCH_USE_VPN=true` — enables proxy pool (NordVPN + public proxies)
    - `BATCH_USE_PUBLIC_PROXIES=true` — loads `proxies.json` into the pool (opt-in, default off)
    - `BATCH_REQUIRE_VPN_ONLY=true` — strict mode: no direct egress fallback. Job fails if all proxies are dead.
    - Pool order: NordVPN → public proxies (PL/DE/CZ/SK/SE/NL/FR/AT/ES priority) → direct (unless strict mode)
    - Preflight TCP-check runs at startup, removing unreachable proxies. Dead proxies stored in `dead_proxies` table (6h TTL).
    - Strict production: `BATCH_USE_VPN=true BATCH_USE_PUBLIC_PROXIES=true BATCH_REQUIRE_VPN_ONLY=true`
    - Permissive local: `BATCH_USE_VPN=false` (all workers use direct, no proxy pool loaded)

## Keeping docs current

After completing a Linear issue or making structural changes (new files, endpoints, tables, commands), update this file and the Claude memory system before finishing the conversation:

1. **This file (CLAUDE.md):** update project structure, endpoint tables, database tables, commands, or gotchas if any of those changed.
2. **README.md:** update if user-facing information changed (new endpoints, new CLI commands, new config options).
3. **Claude memory (`project_backlog_state.md`):** update which Linear issues are done vs in-progress.
4. **Linear issues:** move completed issues to Done, add implementation notes as comments.

Check the Linear backlog at the start of each session to orient on what's next.
