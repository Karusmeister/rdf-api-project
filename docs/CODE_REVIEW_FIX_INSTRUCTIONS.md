# Codex Fix Instructions: Prediction Pipeline Code Review

## Goal

Use this file as the implementation brief for a Codex run that should fix the current prediction-pipeline review findings.

The scope is the bankruptcy-prediction work built on top of the scraper and ETL layers. Focus on the findings below, implement fixes in code, update docs where needed, and add regression coverage.

## Context

- Review date: 2026-03-20
- Repository: `/Users/piotrkraus/piotr/rdf-api-project`
- Relevant Linear background:
  - `PKR-5` through `PKR-11` are implemented and marked done.
  - `PKR-12` is still backlog.
- Important architectural conclusion:
  - Keep **one DuckDB file**.
  - Prefer **one default schema** for now.
  - Do **not** split scraper and prediction data into separate DuckDB databases.
  - The real problem is that the code currently uses **two independent module-level connection managers** pointed at the same DB file.

## Current Verified State

- Full test suite passed during review: `109 passed, 12 skipped`
- The issues below were found despite passing tests because the missing coverage is mainly around ETL integration and correction/retry edge cases.

## Implementation Priorities

Implement these in order.

### 1. Consolidate DuckDB access around one shared connection lifecycle

#### Problem

The docs consistently describe one DuckDB file shared by scraper and prediction data, but the implementation opens that same file from two separate modules:

- `app/scraper/db.py`
- `app/db/prediction_db.py`

That creates fragmented lifecycle management and already contributes to runtime bugs.

#### Required direction

- Keep one DuckDB file.
- Introduce one shared DB connection manager, for example `app/db/duckdb.py`.
- Move connection open/close ownership there.
- Keep scraper/prediction modules responsible for schema init and CRUD helpers, not global connection lifecycle.
- Remove route-level open/close behavior that can unexpectedly close shared global state.

#### Key refs

- `docs/PREDICTION_SCHEMA_DESIGN.md`
- `CLAUDE.md`
- `app/scraper/db.py`
- `app/db/prediction_db.py`
- `app/main.py`
- `app/routers/scraper/routes.py`

#### Acceptance criteria

- Scraper and prediction code use one shared DuckDB connection lifecycle.
- The API does not rely on two independent module-level connections to the same file.
- Code and docs align on one-DB design.

### 2. Make `/api/etl/ingest` work after clean API startup

#### Problem

`POST /api/etl/ingest` currently fails after a fresh FastAPI startup because the app lifespan opens only prediction DB access, while ETL reads scraper DB access directly.

This was reproduced during review as:

- request: `POST /api/etl/ingest {}`
- result: `500`
- detail: `Scraper DB not connected - call connect() first`

#### Required change

- Ensure ETL endpoints initialize the scraper-side DB access they depend on, or move ETL fully onto the shared connection manager from task 1.
- Make sure unrelated routes do not close shared DB state.
- Add endpoint-level regression coverage for `/api/etl/ingest` under normal app lifespan startup.

#### Key refs

- `app/main.py`
- `app/services/etl.py`
- `app/routers/etl/routes.py`

#### Acceptance criteria

- `/api/etl/ingest` succeeds or returns a domain error after clean startup.
- It never fails due to connection initialization.
- There is a regression test for this path.

### 3. Handle corrections and duplicate-period filings without orphaning ETL data

#### Problem

`financial_reports` uses a uniqueness rule on:

- `(krs, data_source_id, fiscal_year, period_end, report_type)`

and `create_financial_report()` uses `ON CONFLICT DO NOTHING`.

When a second filing for the same period arrives, such as a correction:

- the new `financial_reports` row is silently dropped
- ETL still writes `raw_financial_data` and `financial_line_items` under the new document id
- `update_report_status()` then updates no `financial_reports` row

That creates orphaned ETL data and conflicts with the documented rule that corrections should supersede originals.

#### Required change

- Define report identity and correction handling explicitly.
- Choose one of these approaches and implement it consistently:
  - represent each filing version separately and mark superseded filings, or
  - upsert/replace the existing logical report cleanly
- Do not allow orphan rows in:
  - `raw_financial_data`
  - `financial_line_items`
  - `computed_features`
- Review whether foreign keys or explicit existence checks should enforce this.

#### Key refs

- `app/db/prediction_db.py`
- `app/services/etl.py`
- `docs/API.md`

#### Acceptance criteria

- Original and corrected filings for the same period are handled intentionally and consistently.
- No ETL path can create child rows for a report id missing from `financial_reports`.
- Regression tests cover original plus correction for one period.

### 4. Let failed ETL documents re-enter bulk ingestion

#### Problem

The ETL failure path writes a `financial_reports` row with status `failed`, but `ingest_all_pending()` excludes any document whose `source_document_id` already exists in `financial_reports`.

That means failed documents are skipped forever by bulk ingestion.

#### Required change

- Make failed ETL documents retryable in the bulk workflow.
- Preserve deduplication for already-completed ingests.
- A simple acceptable implementation is to exclude only successfully completed documents from `ingest_all_pending()`.
- If a richer retry model is better, implement it cleanly and document it.

#### Key refs

- `app/services/etl.py`

#### Acceptance criteria

- Failed ETL documents can be retried in bulk.
- Completed documents remain protected from duplicate ingest.
- Tests cover failed -> fixed -> bulk retry.

### 5. Align scraper DDL with the documented schema

#### Problem

`docs/SCRAPER_ARCHITECTURE.md` documents:

- a foreign key from `krs_documents.krs` to `krs_registry.krs`
- index `idx_documents_not_downloaded`

but `app/scraper/db.py` currently omits both.

Right now code and docs disagree on the scraper storage contract.

#### Required change

- Either implement the documented FK/index or update the docs and build instructions to match the real chosen design.
- Make the final tradeoff explicit.
- Ensure tests reflect the final contract.

#### Key refs

- `docs/SCRAPER_ARCHITECTURE.md`
- `docs/SCRAPER_BUILD_INSTRUCTIONS.md`
- `app/scraper/db.py`

#### Acceptance criteria

- Scraper DDL in code and docs match.
- Intended integrity and performance tradeoffs are explicit.

## Testing Requirements

At minimum, run and keep passing:

```bash
./.venv/bin/pytest -q
```

Add focused regression tests for:

- ETL ingest route after clean startup
- correction/original filing handling for one reporting period
- bulk retry of failed ETL documents
- any DB lifecycle changes introduced by the shared connection manager

## Notes For Codex

- Do not introduce a second DuckDB database.
- Do not introduce multiple schemas unless there is a concrete, justified need.
- Prefer the smallest architecture change that fixes the lifecycle problem cleanly.
- If changing the schema contract, update the relevant docs in the same change.
- Preserve unrelated user changes in the working tree.
