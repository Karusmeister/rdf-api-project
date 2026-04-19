# Schema Deduplication Plan

Follow-up to the 2026-04-17 cleanup that reclaimed 55% of the Cloud SQL DB (5651 → 2544 MB). This file lists the structural redundancies that survived, with migration sketches you can implement incrementally. Items are ordered by impact ÷ risk.

## Context — what's already been done

- Dropped `krs_documents` and `krs_entities` (legacy flat caches).
- Dropped 5 unused/redundant indexes on `krs_document_versions` and `krs_entity_versions`.
- Deleted 5.26M historical rows from `krs_document_versions` (kept `is_current`).
- `pg_repack`'d `krs_document_versions` to reclaim bloat.
- Patched `app/scraper/db.py` so the dropped indexes don't return on next deploy.

## Remaining redundancies

### 1. `krs_document_versions` — versioning that isn't versioning

**Problem.** The table holds 8.26M rows average → 2.26 versions per document, but a 50k sample of v1→v2 pairs showed:
- 0 changes in business fields (`rodzaj`, `nazwa`, `date_prepared`, `is_ifrs`, `is_correction`)
- 73% of diffs are `is_downloaded` / `storage_path` / `file_size_bytes` — i.e., state transitions, not content versions

The `change_reason` distribution confirms it: `discovery`, `downloaded`, `metadata_update`, `download_error`, `bootstrap_from_krs_documents`. None of these mean "the document's upstream metadata changed." Cost: 2.26× storage + 13.5M UPDATEs driving WAL and bloat. Even after today's DELETE+repack, 31k new historical rows appeared during the 3-minute repack window. It will grow back.

**Fix.** Split into two tables:

```sql
-- Immutable discovery record, 1 row per document forever
CREATE TABLE krs_documents (
    document_id      VARCHAR PRIMARY KEY,
    krs              VARCHAR(10) NOT NULL,
    rodzaj           SMALLINT NOT NULL,            -- was varchar with values 1..22
    nazwa            VARCHAR,
    okres_start      DATE,
    okres_end        DATE,
    filename         VARCHAR,
    is_ifrs          BOOLEAN,
    is_correction    BOOLEAN,
    date_filed       DATE,                         -- was varchar "YYYY-MM-DD"
    is_deleted       BOOLEAN NOT NULL DEFAULT false,  -- was varchar status NIEUSUNIETY/USUNIETY
    discovered_at    TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE INDEX idx_documents_krs ON krs_documents(krs);

-- Mutable download state, 1 row per document
CREATE TABLE krs_document_downloads (
    document_id       VARCHAR PRIMARY KEY REFERENCES krs_documents(document_id),
    is_downloaded     BOOLEAN NOT NULL DEFAULT false,
    downloaded_at     TIMESTAMP,
    storage_path      VARCHAR,
    file_size_bytes   BIGINT,
    file_count        INTEGER,
    file_type         VARCHAR(10),                 -- from migration 006
    download_error    VARCHAR,
    metadata_fetched_at TIMESTAMP,
    updated_at        TIMESTAMP NOT NULL DEFAULT current_timestamp
);
CREATE INDEX idx_downloads_pending ON krs_document_downloads(document_id) WHERE NOT is_downloaded;
```

**Migration sketch:**

```sql
BEGIN;

INSERT INTO krs_documents (document_id, krs, rodzaj, nazwa, okres_start, okres_end,
                           filename, is_ifrs, is_correction, date_filed, is_deleted, discovered_at)
SELECT DISTINCT ON (document_id)
    document_id, krs, rodzaj::smallint, nazwa, okres_start::date, okres_end::date,
    filename, is_ifrs, is_correction,
    NULLIF(date_filed, '')::date,
    (status = 'USUNIETY'),
    discovered_at
FROM krs_document_versions
WHERE is_current
ORDER BY document_id, version_no;

INSERT INTO krs_document_downloads
SELECT document_id, is_downloaded, downloaded_at, storage_path, file_size_bytes,
       file_count, file_type, download_error, metadata_fetched_at, observed_at
FROM krs_document_versions WHERE is_current;

COMMIT;

-- later, after code switchover:
DROP TABLE krs_document_versions CASCADE;  -- drops the krs_documents_current view too
CREATE OR REPLACE VIEW krs_documents_current AS
SELECT d.*, dl.is_downloaded, dl.storage_path, dl.file_size_bytes,
       dl.file_count, dl.file_type, dl.download_error, dl.downloaded_at,
       dl.metadata_fetched_at
FROM krs_documents d LEFT JOIN krs_document_downloads dl USING (document_id);
```

**Code touch points.** Search for `krs_document_versions` — ~40 references across:
- [app/scraper/db.py](../app/scraper/db.py) — DDL + `_DOC_SNAPSHOT_FIELDS`, `upsert_document_version`, `mark_downloaded`
- [batch/rdf_document_store.py](../batch/rdf_document_store.py) — parallel DDL + write path
- [batch/metadata_backfill.py](../batch/metadata_backfill.py) — metadata updater
- Any place that reads via `krs_documents_current` view keeps working.

**Impact.** Writes collapse ~10× (no more snapshot-hash-and-version machinery for state transitions). Storage on new table ~1 GB vs current 1.8 GB. WAL pressure drops noticeably.

**Risk.** Medium. Dual-write during the transition is recommended: write to both old and new tables for a week, compare, then switch readers via view.

**Removed columns worth mentioning** (all 100% NULL/empty verified on 3.7M rows):
- `run_id` — never populated
- `date_prepared` — never populated (upstream doesn't provide it)
- `zip_size_bytes` — only 31/2.9M rows match `file_size_bytes`; value already lives in each `manifest.json`; drop from DB
- `storage_backend` — only value in prod is `gcs`; drop and hardcode
- `change_reason` — operational telemetry; fold into a small audit table if you want it, or drop
- `snapshot_hash` — only makes sense if you keep versioning; drop in the split

---

### 2. Three parallel tables for the same 551k entities

**Problem.** `krs_entity_versions` (349 MB) has `current_rows = total_rows = 551098`. Zero historical rows ever. The versioning column machinery (`version_id`, `valid_from`, `valid_to`, `snapshot_hash`, `change_reason`, `observed_at`) is pure overhead. Meanwhile `krs_registry` (176 MB) holds 550,151 of the same entities with scraper scheduling fields. Verified: 551,095/551,098 names match; 100% of legal_forms match.

**Fix.** One table, no versioning, fold in the scheduler columns.

```sql
CREATE TABLE krs_companies (
    krs                 VARCHAR(10) PRIMARY KEY,
    name                VARCHAR NOT NULL,
    legal_form          VARCHAR,
    status              VARCHAR,                 -- keep or normalize to enum
    is_active           BOOLEAN NOT NULL DEFAULT true,
    registered_at       DATE,
    last_changed_at     DATE,
    nip                 VARCHAR(13),
    regon               VARCHAR(14),
    address_city        VARCHAR,
    address_street      VARCHAR,
    address_postal_code VARCHAR,
    source              VARCHAR NOT NULL DEFAULT 'ms_gov',
    synced_at           TIMESTAMP NOT NULL DEFAULT current_timestamp,

    -- scraper scheduling (from krs_registry)
    first_seen_at       TIMESTAMP,
    last_checked_at     TIMESTAMP,
    last_download_at    TIMESTAMP,
    check_priority      INTEGER NOT NULL DEFAULT 0,
    check_error_count   INTEGER NOT NULL DEFAULT 0,
    last_error_message  VARCHAR,
    total_documents     INTEGER NOT NULL DEFAULT 0,
    total_downloaded    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_companies_name_trgm ON krs_companies USING gin (name gin_trgm_ops);
CREATE INDEX idx_companies_last_checked ON krs_companies(last_checked_at);
CREATE INDEX idx_companies_priority ON krs_companies(check_priority DESC, last_checked_at);
```

**Drop** `krs_entity_versions.raw` (131 MB of JSON duplicating flat columns — verified: `raw.podmiot.nazwaPodmiotu == name`, `raw.podmiot.formaPrawna == legal_form`). If you still want to keep raw upstream payloads, make them JSONB and move to a separate `krs_entity_raw_snapshots` table keyed by `(krs, fetched_at)`.

**Migration sketch:**

```sql
INSERT INTO krs_companies
SELECT v.krs, v.name, v.legal_form, v.status, r.is_active,
       v.registered_at, v.last_changed_at, v.nip, v.regon,
       v.address_city, v.address_street, v.address_postal_code,
       v.source, v.observed_at,
       r.first_seen_at, r.last_checked_at, r.last_download_at,
       COALESCE(r.check_priority, 0), COALESCE(r.check_error_count, 0),
       r.last_error_message, COALESCE(r.total_documents, 0),
       COALESCE(r.total_downloaded, 0)
FROM krs_entity_versions v
LEFT JOIN krs_registry r USING (krs)
WHERE v.is_current;

-- 121 rows in registry not in versions; backfill them too:
INSERT INTO krs_companies (krs, name, legal_form, is_active, source,
                           first_seen_at, last_checked_at, last_download_at,
                           check_priority, check_error_count, last_error_message,
                           total_documents, total_downloaded)
SELECT r.krs, r.company_name, r.legal_form, r.is_active, 'ms_gov',
       r.first_seen_at, r.last_checked_at, r.last_download_at,
       r.check_priority, r.check_error_count, r.last_error_message,
       r.total_documents, r.total_downloaded
FROM krs_registry r
LEFT JOIN krs_entity_versions v ON v.krs = r.krs AND v.is_current
WHERE v.krs IS NULL;
```

**Code touch points.**
- [app/repositories/krs_repo.py](../app/repositories/krs_repo.py) — entity CRUD + `krs_entities_current` view + `upsert_entity_version`
- [batch/entity_store.py](../batch/entity_store.py) — parallel DDL + write path
- Any query reading `krs_entities_current`, `krs_entity_versions`, or `krs_registry`

**Impact.** ~600 MB reclaimed. Simpler reads (no join, no partial indexes with `WHERE is_current = true` on 100%-current data). Removes a whole category of "why is the entity table shaped like this?" confusion for future you.

**Risk.** Medium-low. Entities aren't changing (last write 2026-04-05). Do it when batch workers are idle. Fallback: keep `krs_entities_current` as a view over the new table for transition period.

---

### 3. Column-level type waste (cheap win)

These apply to `krs_document_versions` if you keep it, or bake into the split in #1.

| Column | Current | Should be | Why | Saves |
|---|---|---|---|---|
| `rodzaj` | `varchar` | `SMALLINT` | 22 distinct integer values | ~25 MB |
| `status` | `varchar` | `BOOLEAN is_deleted` | 2 values (`NIEUSUNIETY`, `USUNIETY`) | ~40 MB |
| `date_filed` | `varchar` | `DATE` | Always `YYYY-MM-DD` or empty | ~15 MB |
| `storage_backend` | `varchar` | drop | Only `gcs` in prod | ~25 MB |
| `file_types` | `varchar` | drop | Redundant with `file_type` from migration 006 | ~30 MB |
| `change_reason` | `varchar` | `SMALLINT` enum or drop | 5 values | ~40 MB |

Example (standalone, safe to run now if you don't do #1):

```sql
ALTER TABLE krs_document_versions
    ADD COLUMN rodzaj_int SMALLINT;
UPDATE krs_document_versions SET rodzaj_int = rodzaj::smallint;
ALTER TABLE krs_document_versions
    ALTER COLUMN rodzaj_int SET NOT NULL,
    DROP COLUMN rodzaj,
    RENAME COLUMN rodzaj_int TO rodzaj;
```

Easier to just do this work inside #1 during the split.

---

### 4. JSON → JSONB

Every JSON column in the DB is `json` (text), not `jsonb`. Text JSON re-parses on every read and can't be indexed with GIN. One-line ALTERs, zero risk:

```sql
ALTER TABLE krs_entity_versions    ALTER COLUMN raw TYPE jsonb USING raw::jsonb;
ALTER TABLE raw_financial_data     ALTER COLUMN data_json TYPE jsonb USING data_json::jsonb;
ALTER TABLE predictions            ALTER COLUMN feature_contributions TYPE jsonb USING feature_contributions::jsonb;
ALTER TABLE predictions            ALTER COLUMN feature_snapshot      TYPE jsonb USING feature_snapshot::jsonb;
ALTER TABLE model_registry         ALTER COLUMN hyperparameters    TYPE jsonb USING hyperparameters::jsonb;
ALTER TABLE model_registry         ALTER COLUMN training_data_spec TYPE jsonb USING training_data_spec::jsonb;
ALTER TABLE model_registry         ALTER COLUMN training_metrics   TYPE jsonb USING training_metrics::jsonb;
ALTER TABLE prediction_runs        ALTER COLUMN parameters TYPE jsonb USING parameters::jsonb;
ALTER TABLE feature_definitions    ALTER COLUMN required_tags TYPE jsonb USING required_tags::jsonb;
ALTER TABLE assessment_jobs        ALTER COLUMN result_json TYPE jsonb USING result_json::jsonb;
```

The `krs_entity_versions.raw` conversion is the only one that matters for size (~131 MB → ~100 MB and faster reads). The rest are ergonomic. Most go away entirely if you do #2 (drop `raw`).

---

### 5. Missing foreign keys

Only 7 FKs exist, all on auth/predictions. Adding these catches bad ETL data at write time instead of letting it rot:

```sql
ALTER TABLE financial_line_items
    ADD CONSTRAINT fk_line_items_report
    FOREIGN KEY (report_id) REFERENCES financial_reports(id) ON DELETE CASCADE;

ALTER TABLE raw_financial_data
    ADD CONSTRAINT fk_raw_report
    FOREIGN KEY (report_id) REFERENCES financial_reports(id) ON DELETE CASCADE;

ALTER TABLE computed_features
    ADD CONSTRAINT fk_features_report
    FOREIGN KEY (report_id) REFERENCES financial_reports(id) ON DELETE CASCADE;

-- These depend on #1 and #2:
ALTER TABLE financial_reports
    ADD CONSTRAINT fk_reports_document
    FOREIGN KEY (source_document_id) REFERENCES krs_documents(document_id);

ALTER TABLE etl_attempts
    ADD CONSTRAINT fk_etl_document
    FOREIGN KEY (document_id) REFERENCES krs_documents(document_id);
```

Add them after cleaning up orphans — which brings us to…

---

### 6. Garbage rows in `financial_reports`

24 rows have `fiscal_year=0, period_end=1970-01-01, ingestion_status=failed`. Epoch-date sentinel from a failed ETL path. Either constrain them out (`CHECK (fiscal_year BETWEEN 1990 AND 2100)`) or delete:

```sql
DELETE FROM financial_line_items  WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM raw_financial_data    WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM computed_features     WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM financial_reports     WHERE fiscal_year = 0;

ALTER TABLE financial_reports
    ADD CONSTRAINT chk_fiscal_year_sane CHECK (fiscal_year BETWEEN 1990 AND 2100);
```

[app/services/etl.py](../app/services/etl.py) should already be rejecting `invalid_period_end` (gotcha #23 in [CLAUDE.md](../CLAUDE.md)) — these are pre-fix leftovers.

---

### 7. `batch_progress` not_found rows (operational, optional)

2.24M rows total; 1.45M are `not_found` (KRS integers the scanner probed and got 404). Useful as a "don't re-probe this" set, but 194 MB is a lot for what could be a sparse range. Low-priority. If it grows to hurt, replace with a range table:

```sql
CREATE TABLE batch_progress_ranges (
    krs_from BIGINT NOT NULL,
    krs_to   BIGINT NOT NULL,
    status   VARCHAR NOT NULL,
    scanned_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (krs_from)
);
```

…then store contiguous `not_found` runs as intervals. ~100× smaller, but the scanner code would need a range-lookup helper. Defer until the table actually gets uncomfortable.

---

### 8. `raw_financial_data` — probably dead cache

317 rows, 3.6 MB, low access (8 seq scans, 415 idx scans all-time). Holds the same section-level JSON that's already expanded into `financial_line_items` (20,590 rows). If the pipeline can rebuild line items from scratch from the source XML, this table is redundant and can be dropped. If it's an intentional audit log, at least switch to `jsonb` and add the FK in #5. Cost of keeping it: negligible. Listed only for completeness.

---

## Suggested order

1. **JSON → JSONB** (#4) — trivial, safe, do anytime.
2. **Clean up garbage rows** (#6) — needed before FKs.
3. **Collapse entity tables** (#2) — biggest medium-risk win, ~600 MB, entities are idle.
4. **Add foreign keys** (#5) — after #2 and before #1 would be ideal.
5. **Split `krs_document_versions`** (#1) — largest refactor, fixes the write-amplification that will otherwise bloat the table back to 4 GB over time.
6. Type trimming (#3) — fold into the migration for #1; doing it standalone is wasted effort.
7. `batch_progress` ranges (#7) and `raw_financial_data` review (#8) — nice-to-haves.

## Operational notes

- `pg_repack` extension is installed on Cloud SQL. The client binary (PG16-compatible) is at [/Users/piotrkraus/piotr/bin/pg_repack](/Users/piotrkraus/piotr/bin/pg_repack). Run as `postgres` (app's `rdf_api` role isn't a superuser). Use `--no-superuser-check` (`-k`) on Cloud SQL.
- Any of these migrations that rewrite a large table: DELETE first, then `pg_repack -t <table> -k` — that was the pattern used in the 2026-04-17 cleanup.
- Write to [schema_migrations](../app/db/migrations.py) as you apply each one. Prefix these with `dedupe/` so they sit in their own namespace (e.g. `dedupe/001_entity_tables_collapse`).
- If you run any of these while batch workers are active, pg_repack's triggers will replay writes correctly but the worker may see brief "relation does not exist" flickers during the final swap. Pausing [deploy/rdf-worker.service](../../rdf-infra/deploy/rdf-worker.service) during the swap is the safest path.
