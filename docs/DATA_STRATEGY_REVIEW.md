# Data Strategy Review

## Executive Summary

The project should stay on one DuckDB database file.

Using two databases here would add synchronization cost, duplicate keys, and harder lineage without giving a meaningful scalability benefit at the current stage. The better split is not "scraper DB" vs "prediction DB", but:

- mutable control-plane tables for scheduling and job status
- append-only analytical tables for filings, ETL outputs, features, and predictions

That keeps the operational path simple while preserving history where reproducibility matters.

## Current Issues

### 1. Two-database thinking leaks into the design

Even though the code already points both modules at the same file, the conceptual split still shows up in the schema and docs. That creates a risk of:

- duplicated company/report identifiers
- ETL orphan rows
- inconsistent lifecycle rules across scraper and prediction layers

### 2. Some important business data was modeled as mutable state

Before this change, the risky parts were:

- corrected filings could replace earlier filings for the same period
- re-ingest could delete previous ETL output
- recompute could overwrite feature values in place

That breaks auditability and makes it hard to answer basic questions like:

- what did we know on a given date?
- which parser/model version produced this value?
- which filing was later corrected?

## Recommended Target Model

### One physical database

Keep one DuckDB file and one shared connection lifecycle.

Why this is the right tradeoff now:

- joins across scraper, ETL, and ML stay local and cheap
- one backup/export path
- one migration story
- no cross-database consistency problem
- simpler local and GCP deployment

### Append-only analytical core

These entities should be history-preserving:

- source filings
- parsed raw sections
- flattened line items
- computed features
- predictions
- model registry

The implemented pattern is:

- base tables are append-only
- each mutable business concept gets a version column
- reads that need "current state" use `latest_*` views

### Mutable control-plane tables

These can remain mutable for now:

- `krs_registry`
- `scraper_runs`
- ETL/report status fields like `ingestion_status`

Reason:

- they are operational workflow state, not analytical source-of-truth
- deriving scheduler state from event logs on every run would add complexity and cost without improving lineage

If needed later, add event logs beside them rather than forcing everything into SCD2 immediately.

## Implemented Direction

### Filing history

`financial_reports` now keeps:

- `logical_key`
- `report_version`
- `supersedes_report_id`

Effect:

- the first filing for a period is version 1
- a correction becomes version 2, 3, etc.
- nothing is deleted
- `latest_financial_reports` gives the current filing per period

### ETL history

`raw_financial_data` and `financial_line_items` now use `extraction_version`.

Effect:

- re-ingest appends a new parser/extraction result
- latest reads still stay simple
- parser improvements no longer destroy prior outputs

### Feature history

`computed_features` now stores:

- `computation_version`
- `source_extraction_version`

Effect:

- recomputation appends instead of overwriting
- each feature value remains traceable to the ETL version it came from

## Performance Implications

### One DB vs two DBs

One DB is faster and simpler for this workload because:

- DuckDB is optimized for local analytical scans and joins
- scraper-to-ETL-to-feature joins stay in-process
- there is no network hop or replication layer
- the full lineage query can run in one SQL plan

Two DBs would only start to make sense if:

- ingestion and scoring became independently scaled services
- write contention became material
- you needed different storage engines for different workloads

That is not the current bottleneck.

### Append-only writes

Append-only increases storage footprint, but for this project that is the right trade:

- inserts are cheap
- history enables debugging, reproducibility, and future backfills
- DuckDB compresses repeated values well in columnar storage

The main cost is larger scans over historical rows. That is why latest-state views matter.

### Latest views

`latest_*` views add a window-function step, which is acceptable at current scale because:

- reads are analytical anyway
- DuckDB handles partitioned scans well
- the latest-state logic stays centralized and consistent

If current-state reads become a hotspot later, promote selected views to materialized snapshot tables refreshed by the pipeline.

## Extensibility Guidance

The next schema additions should follow these rules:

1. Add new sources by extending `data_sources` and reusing company/report logical keys.
2. Treat new external filings as new versions, never in-place replacements.
3. Keep derived data tied to the exact source/extraction version that produced it.
4. Add convenience views for "latest" instead of collapsing history.
5. Only keep mutable tables for workflow state or caches that can be rebuilt.

## Suggested Next Steps

1. Add a `company_versions` table if you want company attributes like NIP, PKD, or legal form to become fully historical as well.
2. Split feature metadata into append-only `feature_definition_versions` if formula history needs to be reproducible alongside computed values.
3. Add explicit ETL run metadata if you want parser version, code commit, or ingestion operator stored per extraction.
4. Add retention/compaction policy only after you have real evidence that history volume is a problem.
