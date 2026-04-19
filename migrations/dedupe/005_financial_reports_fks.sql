-- SCHEMA_DEDUPE_PLAN #5: enforce relational integrity for the financial
-- data layer. The three child tables (line items, raw sections, computed
-- features) are meaningless without their parent financial_reports row,
-- so the cascade direction is clear.
--
-- We keep the krs_documents-bound FKs (fk_reports_document, fk_etl_document)
-- for the phase-3 migration that introduces the new krs_documents table.
-- Attaching them now would point at the soon-to-be-dropped
-- krs_document_versions and require rewiring again.
--
-- Fails loud on existing orphans — mirrors the CR2-DB-003 pattern in
-- prediction/004 so operators have to clean up explicitly rather than
-- silently losing rows.

DO $$
DECLARE
    orphan_line_items INTEGER;
    orphan_raw        INTEGER;
    orphan_features   INTEGER;
BEGIN
    SELECT count(*) INTO orphan_line_items
    FROM financial_line_items li
    LEFT JOIN financial_reports fr ON fr.id = li.report_id
    WHERE fr.id IS NULL;

    SELECT count(*) INTO orphan_raw
    FROM raw_financial_data r
    LEFT JOIN financial_reports fr ON fr.id = r.report_id
    WHERE fr.id IS NULL;

    SELECT count(*) INTO orphan_features
    FROM computed_features cf
    LEFT JOIN financial_reports fr ON fr.id = cf.report_id
    WHERE fr.id IS NULL;

    IF orphan_line_items > 0 OR orphan_raw > 0 OR orphan_features > 0 THEN
        RAISE EXCEPTION
            'dedupe/005 blocked by orphan rows: '
            'financial_line_items without a financial_reports match=%, '
            'raw_financial_data without a financial_reports match=%, '
            'computed_features without a financial_reports match=%. '
            'Resolve orphans and re-run migrations.',
            orphan_line_items, orphan_raw, orphan_features;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_line_items_report'
    ) THEN
        ALTER TABLE financial_line_items
            ADD CONSTRAINT fk_line_items_report
            FOREIGN KEY (report_id) REFERENCES financial_reports(id)
            ON DELETE CASCADE ON UPDATE CASCADE;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_raw_financial_data_report'
    ) THEN
        ALTER TABLE raw_financial_data
            ADD CONSTRAINT fk_raw_financial_data_report
            FOREIGN KEY (report_id) REFERENCES financial_reports(id)
            ON DELETE CASCADE ON UPDATE CASCADE;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_computed_features_report'
    ) THEN
        ALTER TABLE computed_features
            ADD CONSTRAINT fk_computed_features_report
            FOREIGN KEY (report_id) REFERENCES financial_reports(id)
            ON DELETE CASCADE ON UPDATE CASCADE;
    END IF;
END
$$;
