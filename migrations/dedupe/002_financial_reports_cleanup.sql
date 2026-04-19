-- SCHEMA_DEDUPE_PLAN #6: purge fiscal_year=0 sentinel rows and lock it out.
--
-- These rows are leftovers from an ETL path that coerced invalid period_end
-- into 1970-01-01 / fiscal_year=0. The ETL now rejects those with
-- reason_code=invalid_period_end (gotcha #23); only pre-fix rows remain.
--
-- Done in-line (not via helper) so the ordering is explicit: children first
-- to avoid the CHECK firing mid-transaction or orphaning FKs we're about to
-- add in dedupe/006.
DELETE FROM financial_line_items
    WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM raw_financial_data
    WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM computed_features
    WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM predictions
    WHERE report_id IN (SELECT id FROM financial_reports WHERE fiscal_year = 0);
DELETE FROM financial_reports WHERE fiscal_year = 0;

-- Defensive range: matches app/services/etl.py invariants.
-- 1990 is before the earliest usable Polish GAAP filing; 2100 is a
-- far-enough upper bound that anything beyond it is malformed input.
ALTER TABLE financial_reports
    ADD CONSTRAINT chk_fiscal_year_sane CHECK (fiscal_year BETWEEN 1990 AND 2100);
