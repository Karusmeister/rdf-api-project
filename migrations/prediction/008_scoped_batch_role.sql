-- Step 3a of NAT_COST_REDUCTION_PLAN.md — scoped Postgres role for batch VM.
--
-- Replaces the batch worker's `postgres` superuser credential with a scoped
-- role that has SELECT/INSERT/UPDATE only on the tables the scraper actually
-- touches. The role has no DELETE on any table (scraper is append-only with
-- proxies removed), no DDL, and no ability to read auth/PII/prediction tables.
--
-- A compromised batch VM after this migration cannot:
--   - read the users / verification_codes / user_krs_access tables
--   - read or write the predictions / model_registry / financial_* tables
--   - drop or alter any table
--   - become superuser
--   - exhaust the connection pool (CONNECTION LIMIT 60)
--
-- The password is set via psql `\set` from the migration runner / manual
-- apply, not stored in the migration file. Apply this manually with:
--
--     \set batch_password '`openssl rand -base64 32`'
--     \i 008_scoped_batch_role.sql

-- CREATE ROLE has no IF NOT EXISTS. Drop first if rerunning. Idempotent in
-- the sense that the migration system records this as applied once and
-- never reruns; rerunning manually requires DROP ROLE first.
CREATE ROLE rdf_batch LOGIN PASSWORD :'batch_password' CONNECTION LIMIT 60;

GRANT CONNECT ON DATABASE rdf TO rdf_batch;
GRANT USAGE ON SCHEMA public TO rdf_batch;

-- Workers run idempotent `CREATE TABLE IF NOT EXISTS ...` at startup
-- (batch/progress.py, batch/entity_store.py, batch/rdf_progress.py,
-- batch/rdf_document_store.py). In production where the tables already
-- exist these are functionally no-ops, but PostgreSQL needs CREATE
-- privilege on the schema to even parse the statements.
-- TODO §11: refactor startup DDL into migrations, then REVOKE this.
GRANT CREATE ON SCHEMA public TO rdf_batch;

-- Append-only DML on batch-touched tables. No DELETE granted anywhere.
GRANT SELECT, INSERT, UPDATE ON
  batch_progress,
  batch_rdf_progress,
  krs_scan_cursor,
  krs_entity_versions,
  krs_registry,
  krs_document_versions
TO rdf_batch;

-- Sequences for SERIAL/BIGSERIAL primary keys on tables we INSERT into.
-- (Verified via `\ds public.*` on 2026-04-10.)
GRANT USAGE, SELECT ON
  seq_krs_entity_versions,
  seq_krs_document_versions
TO rdf_batch;

-- Belt-and-braces: explicit revokes on every sensitive table. A future
-- migration that creates a new sensitive table MUST also REVOKE it from
-- rdf_batch in the same migration (see guardrail rule §7.10 in
-- NAT_COST_REDUCTION_PLAN.md).
REVOKE ALL ON
  users,
  verification_codes,
  user_krs_access,
  password_reset_tokens,
  activity_log,
  predictions,
  prediction_runs,
  model_registry,
  computed_features,
  feature_definitions,
  feature_sets,
  feature_set_members,
  assessment_jobs,
  financial_reports,
  raw_financial_data,
  financial_line_items,
  companies,
  etl_attempts,
  bankruptcy_events,
  search_log,
  krs_sync_log,
  krs_scan_runs,
  scraper_runs
FROM rdf_batch;
