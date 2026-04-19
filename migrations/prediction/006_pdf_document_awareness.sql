-- PKR-130: PDF document awareness — track file_type per document.
--
-- NOTE (2026-04-19, SCHEMA_DEDUPE_PLAN #1): after dedupe/006+007, the
-- ``file_type`` column lives on krs_document_downloads and the view is
-- rebuilt. On a fresh DB krs_document_versions no longer exists, so this
-- migration becomes a no-op. Pre-dedupe clusters still have the old table
-- and need the column + view refresh exactly as before.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'krs_document_versions'
    ) THEN
        EXECUTE 'ALTER TABLE krs_document_versions '
             || 'ADD COLUMN IF NOT EXISTS file_type VARCHAR(10) DEFAULT ''unknown''';

        EXECUTE $q$
            UPDATE krs_document_versions SET file_type =
                CASE
                    WHEN file_types LIKE '%xml%' THEN 'xml'
                    WHEN file_types LIKE '%pdf%' THEN 'pdf'
                    WHEN nazwa LIKE '%.xml' THEN 'xml'
                    WHEN nazwa LIKE '%.pdf' THEN 'pdf'
                    WHEN file_types IS NOT NULL AND file_types != '' THEN 'other'
                    ELSE 'unknown'
                END
            WHERE file_type = 'unknown'
        $q$;

        EXECUTE $q$
            CREATE OR REPLACE VIEW krs_documents_current AS
            SELECT
                document_id, krs, rodzaj, status, nazwa, okres_start, okres_end,
                filename, is_ifrs, is_correction, date_filed, date_prepared,
                is_downloaded, downloaded_at, storage_path, storage_backend,
                file_size_bytes, zip_size_bytes, file_count, file_types,
                discovered_at, metadata_fetched_at, download_error, file_type
            FROM krs_document_versions
            WHERE is_current = true
        $q$;
    END IF;
END
$$;
