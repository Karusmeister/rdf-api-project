-- 001: multi-schema parser support (CR2-OPS-004).
--
-- Moved out of app/db/prediction_db.py `_init_schema` (ALTER TABLE + UPDATE
-- backfills no longer run on every startup). Adds `schema_code` to the report
-- and line-item tables and backfills existing NULL rows to the legacy default.
--
-- Safe to re-run: IF NOT EXISTS guards make the ALTERs idempotent, and the
-- backfills are keyed on `schema_code IS NULL` so they only touch rows that
-- pre-date this migration.

ALTER TABLE financial_reports
    ADD COLUMN IF NOT EXISTS schema_code VARCHAR(10);

ALTER TABLE financial_line_items
    ADD COLUMN IF NOT EXISTS schema_code VARCHAR(10);

UPDATE financial_reports
SET schema_code = 'SFJINZ'
WHERE schema_code IS NULL;

UPDATE financial_line_items
SET schema_code = 'SFJINZ'
WHERE schema_code IS NULL;
