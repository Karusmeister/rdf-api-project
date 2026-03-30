# ~~DB Split Refactor: Separate Batch and Analytics Databases~~

**Status: SUPERSEDED by [postgres-migration.md](postgres-migration.md)** — migrating to PostgreSQL solves the concurrency problem at the root instead of working around DuckDB's file-level locking.

---

**Priority:** ~~High~~ N/A
**Reason:** DuckDB file-level locking creates severe contention between batch workers and the rest of the system

## The Problem

Everything shares one DuckDB file (`data/scraper.duckdb`). DuckDB uses **exclusive file-level locks** — only one process can write at a time. Currently running on the cloud VM:

- **4 KRS scanner workers** — short-lived connections, retry on lock (20 attempts)
- **4 RDF document workers** — short-lived connections, retry on lock (10 attempts)

That's 8 processes fighting for the same file. Observed in production:

```
ERROR worker=1 task_error IOException: Could not set lock on file "/data/scraper.duckdb":
Conflicting lock is held in python (PID 54630)
```

The KRS scanner dropped from thousands/hour to **144/hour** because it's starved by the RDF workers. The RDF workers are also slowing each other down — every `mark_downloaded()`, `insert_documents()`, and `update_metadata()` call opens a connection, grabs the lock, writes, closes. With 4 workers at concurrency 3, that's up to 12 concurrent lock attempts.

When we add FastAPI + ETL + feature engine to the mix (planned), it gets worse: those use a **long-lived shared connection with NO retry**, meaning any batch lock contention blocks API requests.

## Current Table Layout (all in one file)

```
data/scraper.duckdb
├── Batch control plane (high write frequency, batch workers)
│   ├── batch_progress          — 761K+ rows, written by KRS scanner
│   └── batch_rdf_progress      — 1.6K+ rows, written by RDF workers
│
├── Entity registry (medium write, batch + API)
│   ├── krs_entities            — 354K rows, legacy cache
│   ├── krs_entity_versions     — append-only history
│   └── krs_registry            — company metadata
│
├── Document registry (high write, batch + API)
│   ├── krs_documents           — 39K+ rows, legacy cache
│   ├── krs_document_versions   — append-only history
│   └── scraper_runs            — run history
│
├── Financial data (write during ETL, read by API)
│   ├── financial_reports
│   ├── raw_financial_data
│   └── financial_line_items    — will be the largest table (10M+ rows)
│
├── Feature store (write during compute, read by API)
│   ├── feature_definitions
│   ├── feature_sets / members
│   └── computed_features
│
└── Predictions (write during scoring, read by API)
    ├── model_registry
    ├── predictions
    └── bankruptcy_events
```

## Proposed Solution: Three Database Files

Split by **access pattern and lifecycle**, not by domain:

### DB 1: `data/batch.duckdb` — Batch Progress (hot writes, batch workers only)

```
batch_progress          — KRS scan tracking
batch_rdf_progress      — RDF discovery tracking
```

**Why separate:** These tables are hammered by 8 workers constantly. They have zero readers outside the batch process. No API endpoint needs them. Isolating them eliminates 80% of the lock contention.

**Access:** Only `batch/worker.py`, `batch/rdf_worker.py`, `batch/progress.py`, `batch/rdf_progress.py`.

### DB 2: `data/scraper.duckdb` — Entity & Document Registry (medium writes, shared)

```
krs_entities            — legacy cache
krs_entity_versions     — append-only
krs_registry            — company metadata
krs_documents           — legacy cache
krs_document_versions   — append-only
scraper_runs            — run history
krs_scan_cursor         — scanner position
krs_scan_runs           — scanner run history
```

**Why keep together:** These tables JOIN each other frequently. The batch workers write to them, but less frequently than progress tables (only on entity found / document discovered / document downloaded — not on every probe). The FastAPI app reads them. Acceptable contention.

### DB 3: `data/prediction.duckdb` — Analytics (bulk writes during ETL, read by API)

```
companies
financial_reports
raw_financial_data
financial_line_items
feature_definitions / sets / members
computed_features
etl_attempts
model_registry
prediction_runs
predictions
bankruptcy_events
assessment_jobs
```

**Why separate:** ETL ingestion does long bulk writes (hundreds of line items per document). Feature computation loops through reports doing upserts. These are expensive operations that currently block everything else via the shared connection. Isolating them means:
- ETL can run without starving batch workers
- Feature compute can run without blocking API reads on entity/document data
- API can read predictions without waiting for batch locks

## Access Pattern After Split

```
batch/worker.py        → batch.duckdb (progress) + scraper.duckdb (entities)
batch/rdf_worker.py    → batch.duckdb (progress) + scraper.duckdb (documents)
FastAPI routes         → scraper.duckdb (entities, docs) + prediction.duckdb (analytics)
ETL pipeline           → scraper.duckdb (read docs) + prediction.duckdb (write financials)
Feature engine         → prediction.duckdb only
```

**Lock contention after split:**
- `batch.duckdb`: 8 workers fight for progress writes only — small, fast operations. Acceptable.
- `scraper.duckdb`: 8 workers write entities/docs (infrequent) + FastAPI reads. Low contention.
- `prediction.duckdb`: ETL writes + feature compute + API reads. Sequential by design (ETL runs, then features compute, then API serves). No contention.

## Implementation Plan

### Phase 1: Create batch.duckdb (highest impact, simplest change)

1. Add `BATCH_PROGRESS_DB_PATH` config setting (default: `data/batch.duckdb`)
2. Update `batch/progress.py` to use `BATCH_PROGRESS_DB_PATH`
3. Update `batch/rdf_progress.py` to use `BATCH_PROGRESS_DB_PATH`
4. Update `batch/runner.py` and `batch/rdf_runner.py` to init both DB paths
5. Migrate existing `batch_progress` and `batch_rdf_progress` data:
   ```sql
   -- On the VM:
   ATTACH 'data/batch.duckdb' AS batch_db;
   CREATE TABLE batch_db.batch_progress AS SELECT * FROM batch_progress;
   CREATE TABLE batch_db.batch_rdf_progress AS SELECT * FROM batch_rdf_progress;
   ```
6. Update `batch/rdf_progress.py` `get_pending_krs()` — currently JOINs `batch_progress` with `batch_rdf_progress`. After split, it needs to read `batch_progress` from `batch.duckdb`. DuckDB supports `ATTACH` for cross-database queries, or we prefetch the KRS list from batch.duckdb and pass it in.

**Complexity note:** `get_pending_krs()` and `get_needs_download_krs()` do cross-table JOINs between `batch_progress` (moving to batch.duckdb) and `krs_document_versions` (staying in scraper.duckdb). Options:
- **Option A:** Use DuckDB `ATTACH` to query across files. Still one lock per query but no long holds.
- **Option B:** Prefetch the KRS list from batch.duckdb at worker startup (already done — `get_pending_krs()` runs once at init), then query scraper.duckdb separately. This is how it works today (prefetch then iterate).
- **Recommended:** Option B — the prefetch pattern already exists. Just split the queries.

### Phase 2: Create prediction.duckdb (medium impact)

1. Add `PREDICTION_DB_PATH` config setting (default: `data/prediction.duckdb`)
2. Update `app/db/prediction_db.py` to use its own connection (not shared with scraper)
3. Update `app/services/etl.py` to open both connections (read docs from scraper, write to prediction)
4. Update `app/services/feature_engine.py` to use prediction connection only
5. Migrate existing prediction tables
6. Update FastAPI routes that join across scraper + prediction data

### Phase 3: Optimize remaining contention

1. Reduce batch worker concurrency to 2-3 workers for entity/document writes
2. Add transaction boundaries in ETL (`BEGIN`/`COMMIT` per document batch)
3. Consider DuckDB WAL mode for scraper.duckdb if read contention remains

## Quick Win (do now, before full refactor)

**Stop the KRS scanner on the VM.** It's already at 354K entities — far more than the RDF workers can process. Stopping it frees half the lock contention immediately:

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project \
  --command="sudo systemctl stop krs-scanner"
```

Re-enable it later when the RDF backlog is smaller, or after Phase 1 of this refactor.

## Config Changes Summary

```bash
# New settings in .env
BATCH_PROGRESS_DB_PATH=data/batch.duckdb     # Phase 1
PREDICTION_DB_PATH=data/prediction.duckdb     # Phase 2

# Existing (unchanged)
SCRAPER_DB_PATH=data/scraper.duckdb
BATCH_DB_PATH=data/scraper.duckdb            # rename to SCRAPER_DB_PATH (alias)
```

## Files to Modify

**Phase 1:**
- `app/config.py` — add `batch_progress_db_path` setting
- `batch/progress.py` — use new setting
- `batch/rdf_progress.py` — use new setting, split cross-DB queries
- `batch/runner.py` — init both DBs
- `batch/rdf_runner.py` — init both DBs
- `scripts/migrate_batch_db.py` — new migration script
- `deploy/krs-scanner.service` — update env if needed
- `deploy/rdf-worker.service` — update env if needed

**Phase 2:**
- `app/config.py` — add `prediction_db_path` setting
- `app/db/prediction_db.py` — own connection lifecycle
- `app/db/connection.py` — may need refactor for multiple connections
- `app/services/etl.py` — dual connection (read scraper, write prediction)
- `app/services/feature_engine.py` — use prediction connection
- `app/main.py` — init both connections at startup
- `scripts/migrate_prediction_db.py` — new migration script

## Estimated Impact

| Metric | Before Split | After Phase 1 | After Phase 2 |
|--------|-------------|---------------|---------------|
| KRS scan rate | 144/hr (starved) | ~3,000/hr | ~3,000/hr |
| RDF discovery rate | 83 krs/hr | ~200 krs/hr | ~200 krs/hr |
| Doc download rate | 1,363/hr | ~3,000/hr | ~3,000/hr |
| API response (p95) | N/A (not running) | Better (no progress lock) | Best (isolated analytics) |
| Lock retries/hr | Thousands | ~100 | ~10 |
