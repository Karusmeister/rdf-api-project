-- SCHEMA_DEDUPE_PLAN #1 (cutover): now that dedupe/006 has populated the
-- new split tables and all application code writes exclusively to them,
-- retire krs_document_versions and rebuild the krs_documents_current view
-- as a thin join over the new tables.
--
-- View shape is intentionally NOT a byte-for-byte alias of the old one:
--
--   * ``status`` (VARCHAR NIEUSUNIETY/USUNIETY) → gone.
--     Readers should use ``is_deleted`` (BOOLEAN).
--   * ``rodzaj`` is now SMALLINT, not VARCHAR.
--   * ``date_filed`` / ``okres_start`` / ``okres_end`` are DATE, not VARCHAR.
--   * ``date_prepared`` / ``storage_backend`` / ``zip_size_bytes`` /
--     ``file_types`` → dropped entirely (see plan).

DROP VIEW IF EXISTS krs_documents_current;
DROP TABLE IF EXISTS krs_document_versions;
DROP SEQUENCE IF EXISTS seq_krs_document_versions;

CREATE OR REPLACE VIEW krs_documents_current AS
SELECT
    d.document_id,
    d.krs,
    d.rodzaj,
    d.nazwa,
    d.okres_start,
    d.okres_end,
    d.filename,
    d.is_ifrs,
    d.is_correction,
    d.date_filed,
    d.is_deleted,
    d.discovered_at,
    dl.is_downloaded,
    dl.downloaded_at,
    dl.storage_path,
    dl.file_size_bytes,
    dl.file_count,
    dl.file_type,
    dl.download_error,
    dl.metadata_fetched_at
FROM krs_documents d
LEFT JOIN krs_document_downloads dl USING (document_id);

-- Deferred FKs from SCHEMA_DEDUPE_PLAN #5.
-- financial_reports.source_document_id → krs_documents(document_id)
-- etl_attempts.document_id            → krs_documents(document_id)
-- No cascade here — deleting a document should not silently wipe its
-- prediction lineage. Operators must prune in order.
DO $$
DECLARE
    orphan_reports INTEGER;
    orphan_etl     INTEGER;
BEGIN
    SELECT count(*) INTO orphan_reports
    FROM financial_reports fr
    WHERE fr.source_document_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM krs_documents d WHERE d.document_id = fr.source_document_id
      );

    SELECT count(*) INTO orphan_etl
    FROM etl_attempts e
    WHERE e.document_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM krs_documents d WHERE d.document_id = e.document_id
      );

    IF orphan_reports > 0 OR orphan_etl > 0 THEN
        RAISE EXCEPTION
            'dedupe/007 blocked by orphan rows: '
            'financial_reports.source_document_id without krs_documents match=%, '
            'etl_attempts.document_id without krs_documents match=%. '
            'Resolve orphans and re-run migrations.',
            orphan_reports, orphan_etl;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_financial_reports_document'
    ) THEN
        ALTER TABLE financial_reports
            ADD CONSTRAINT fk_financial_reports_document
            FOREIGN KEY (source_document_id) REFERENCES krs_documents(document_id)
            ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_etl_attempts_document'
    ) THEN
        ALTER TABLE etl_attempts
            ADD CONSTRAINT fk_etl_attempts_document
            FOREIGN KEY (document_id) REFERENCES krs_documents(document_id)
            ON DELETE RESTRICT ON UPDATE CASCADE;
    END IF;
END
$$;
