# Database Redesign Proposal

**Date**: 2026-03-29
**Status**: Draft — for evaluation

---

## Current State

21 tables across 4 schema files, organized into 3 logical layers:

1. **Scraping** — discovery + download tracking (`krs_registry`, `krs_documents`, `scraper_runs`, `krs_scan_cursor`, `krs_scan_runs`, `batch_progress`)
2. **Extracted data** — financial reports + line items (`financial_reports`, `raw_financial_data`, `financial_line_items`)
3. **Analytical** — features, models, predictions (`companies`, `company_identifiers`, `data_sources`, `feature_definitions`, `feature_sets`, `feature_set_members`, `computed_features`, `model_registry`, `prediction_runs`, `predictions`, `bankruptcy_events`, `assessment_jobs`)

Supporting tables: `krs_entities`, `krs_sync_log`

---

## Core Problem: Triple Entity Redundancy

Three tables store the same company with `krs` as PK:

| Column | `krs_registry` | `krs_entities` | `companies` |
|---|---|---|---|
| name | `company_name` | `name` | — |
| legal_form | `legal_form` | `legal_form` | — |
| nip | — | `nip` | `nip` |
| regon | — | `regon` | `regon` |
| is_active | `is_active` | — | — |
| status | — | `status` | — |
| address | — | 3 cols | — |
| pkd_code | — | — | `pkd_code` |
| voivodeship | — | — | `voivodeship` |
| incorporation_date | — | — | `incorporation_date` |

`batch/entity_store.py` already writes to **both** `krs_registry` and `krs_entities` atomically, confirming they describe the same entity. `companies` adds ML-relevant columns but duplicates `nip`/`regon`.

**Impact**: Data can drift between tables. A name update in `krs_entities` doesn't propagate to `krs_registry.company_name`. NIP in `krs_entities` can differ from NIP in `companies`.

---

## Layer-by-Layer Assessment

### Layer 1 — Scraping

| # | Issue | Severity |
|---|---|---|
| A | `krs_registry` mixes entity identity (name, legal_form) with scraper operational state (check_priority, error_count, last_checked_at). These change at different rates and for different reasons. | Medium |
| B | `batch_progress` and `krs_scan_cursor`/`krs_scan_runs` both track KRS scanning. They serve different processes (multiprocess batch vs single async scanner) but represent the same logical operation. | Low |
| C | `scraper_runs` is standalone — no FK to any KRS or document. You can't tell which documents were downloaded in which run. | Low |

### Layer 2 — Extracted Data

**Well-designed.** The raw/structured separation, versioning via `report_version`, and `latest_*` views are solid. No redundancy. `financial_reports.source_document_id` cleanly bridges layer 1 to layer 2.

### Layer 3 — Analytical

| # | Issue | Severity |
|---|---|---|
| D | `companies` duplicates entity data already in `krs_registry` + `krs_entities`. Its unique columns (`pkd_code`, `voivodeship`, `incorporation_date`) could live on the entity table. | High |
| E | `company_identifiers` is speculative infrastructure for multi-source dedup (GUS, CEIDG, GPW). No production code reads it. NIP/REGON are already on `krs_entities`. | Medium |
| F | `data_sources` has exactly 1 row (`'KRS'`). Every FK to it is hardcoded `DEFAULT 'KRS'`. Dead abstraction until a second source is added. | Low |

---

## Recommendations

### 1. Merge 3 entity tables into `krs_registry`

**Priority**: High | **Effort**: Medium | **Value**: High

**What it achieves**: Single source of truth for entity data. Eliminates dual-write in `entity_store.py`, eliminates drift risk, simplifies all JOINs.

**Schema changes** — add columns to `krs_registry`:

```sql
ALTER TABLE krs_registry ADD COLUMN name VARCHAR;
ALTER TABLE krs_registry ADD COLUMN status VARCHAR;
ALTER TABLE krs_registry ADD COLUMN registered_at DATE;
ALTER TABLE krs_registry ADD COLUMN last_changed_at DATE;
ALTER TABLE krs_registry ADD COLUMN nip VARCHAR(13);
ALTER TABLE krs_registry ADD COLUMN regon VARCHAR(14);
ALTER TABLE krs_registry ADD COLUMN address_city VARCHAR;
ALTER TABLE krs_registry ADD COLUMN address_street VARCHAR;
ALTER TABLE krs_registry ADD COLUMN address_postal_code VARCHAR;
ALTER TABLE krs_registry ADD COLUMN raw JSON;
ALTER TABLE krs_registry ADD COLUMN source VARCHAR DEFAULT 'ms_gov';
ALTER TABLE krs_registry ADD COLUMN synced_at TIMESTAMP;
ALTER TABLE krs_registry ADD COLUMN pkd_code VARCHAR(10);
ALTER TABLE krs_registry ADD COLUMN incorporation_date DATE;
ALTER TABLE krs_registry ADD COLUMN voivodeship VARCHAR(100);
```

**Data migration**:

```sql
-- Backfill from krs_entities
UPDATE krs_registry SET
    name = e.name,
    status = e.status,
    registered_at = e.registered_at,
    last_changed_at = e.last_changed_at,
    nip = e.nip,
    regon = e.regon,
    address_city = e.address_city,
    address_street = e.address_street,
    address_postal_code = e.address_postal_code,
    raw = e.raw,
    source = e.source,
    synced_at = e.synced_at
FROM krs_entities e
WHERE krs_registry.krs = e.krs;

-- Backfill from companies
UPDATE krs_registry SET
    nip = COALESCE(krs_registry.nip, c.nip),
    regon = COALESCE(krs_registry.regon, c.regon),
    pkd_code = c.pkd_code,
    incorporation_date = c.incorporation_date,
    voivodeship = c.voivodeship
FROM companies c
WHERE krs_registry.krs = c.krs;
```

**Backward compatibility**: Create views during transition:

```sql
CREATE OR REPLACE VIEW krs_entities AS
SELECT krs, name, legal_form, status, registered_at, last_changed_at,
       nip, regon, address_city, address_street, address_postal_code,
       raw, source, synced_at
FROM krs_registry;

CREATE OR REPLACE VIEW companies AS
SELECT krs, nip, regon, pkd_code, incorporation_date, voivodeship
FROM krs_registry;
```

**Code changes required**:
- `app/repositories/krs_repo.py` — rewrite to target `krs_registry`
- `app/db/prediction_db.py` — drop `companies` CREATE, replace `upsert_company` with UPDATE on `krs_registry`
- `batch/entity_store.py` — simplify from dual-write to single upsert
- `app/jobs/krs_sync.py` — remove LEFT JOIN gap detection (no separate tables)
- `app/scraper/db.py` — add new columns to schema init

### 2. Extract scraper operational state into `scraper_schedule`

**Priority**: Low | **Effort**: Low | **Value**: Medium

**What it achieves**: Entity table stays clean (identity + enrichment). Scraper state is isolated — can be truncated without touching entity data. Different update frequencies don't cause row contention.

**Schema**:

```sql
CREATE TABLE scraper_schedule (
    krs                 VARCHAR(10) PRIMARY KEY,
    check_priority      INTEGER DEFAULT 0,
    check_error_count   INTEGER DEFAULT 0,
    last_error_message  VARCHAR,
    last_checked_at     TIMESTAMP,
    last_download_at    TIMESTAMP
);
```

**Migration**:

```sql
INSERT INTO scraper_schedule (krs, check_priority, check_error_count, last_error_message, last_checked_at, last_download_at)
SELECT krs, check_priority, check_error_count, last_error_message, last_checked_at, last_download_at
FROM krs_registry;

ALTER TABLE krs_registry DROP COLUMN check_priority;
ALTER TABLE krs_registry DROP COLUMN check_error_count;
ALTER TABLE krs_registry DROP COLUMN last_error_message;
ALTER TABLE krs_registry DROP COLUMN last_checked_at;
ALTER TABLE krs_registry DROP COLUMN last_download_at;
```

**Only worth doing alongside recommendation 1** — otherwise `krs_registry` already has these columns and the separation adds complexity without benefit.

### 3. Drop `company_identifiers` and `data_sources`

**Priority**: Low-Medium | **Effort**: Low | **Value**: Low-Medium

**What it achieves**: Removes speculative abstraction. When GUS/CEIDG integration actually happens, the right multi-source model will be clearer.

**Migration**:

```sql
-- Replace data_source_id FK columns with plain VARCHAR
-- (They already contain 'KRS' in every row)

-- financial_reports: data_source_id already defaults to 'KRS' — keep as VARCHAR, drop FK concept
-- bankruptcy_events: data_source_id — same treatment

DROP TABLE IF EXISTS company_identifiers;
DROP TABLE IF EXISTS data_sources;
```

**Code changes**:
- `app/db/prediction_db.py` — remove `data_sources` and `company_identifiers` CREATE statements, remove seed INSERT
- Keep `data_source_id` columns as plain VARCHAR for provenance tracking

### 4. Unify scanning progress

**Priority**: Low | **Effort**: Low | **Value**: Low

Two scanning systems exist:
- **Single async scanner**: `krs_scan_cursor` + `krs_scan_runs` (in-process, app/jobs/krs_scanner.py)
- **Multiprocess batch**: `batch_progress` (separate processes, batch/worker.py)

**Options**:
- **(a)** Keep both if both deployment modes are needed. Add a view that unions their results.
- **(b)** Settle on one scanner and drop the other's tables.
- **(c)** Merge: use `batch_progress` as the per-KRS ledger for both, keep `krs_scan_runs` for run metadata.

**Recommendation**: Defer until scanner consolidation is decided.

---

## Implementation Order

| Phase | Changes | Effort | Risk |
|---|---|---|---|
| 1 | Merge entity tables (rec. 1) | Medium | Medium — many files, but backward-compat views mitigate |
| 2 | Drop `company_identifiers` + `data_sources` (rec. 3) | Low | Very low — no production readers |
| 3 | Extract `scraper_schedule` (rec. 2) | Low | Low — isolated change |
| 4 | Unify scanners (rec. 4) | Low | Low — deferred |

Phase 1 is the highest-value change. Phases 2-4 are independent and can be done in any order.

---

## What NOT to change

- **Layer 2 (financial data)** — well-designed, no action needed
- **Feature store EAV pattern** — correct for the use case
- **`latest_*` views** — solid pattern for versioned data
- **DuckDB as the single store** — appropriate for the workload
