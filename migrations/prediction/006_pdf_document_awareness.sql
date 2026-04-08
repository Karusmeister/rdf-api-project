-- PKR-130: PDF document awareness — track file_type per document

-- Add file_type column to krs_document_versions
ALTER TABLE krs_document_versions
    ADD COLUMN IF NOT EXISTS file_type VARCHAR(10) DEFAULT 'unknown';

-- Backfill from existing file_types and filename fields
UPDATE krs_document_versions SET file_type =
    CASE
        WHEN file_types LIKE '%xml%' THEN 'xml'
        WHEN file_types LIKE '%pdf%' THEN 'pdf'
        WHEN nazwa LIKE '%.xml' THEN 'xml'
        WHEN nazwa LIKE '%.pdf' THEN 'pdf'
        WHEN file_types IS NOT NULL AND file_types != '' THEN 'other'
        ELSE 'unknown'
    END
WHERE file_type = 'unknown';

-- Recreate view to include file_type
CREATE OR REPLACE VIEW krs_documents_current AS
SELECT
    document_id, krs, rodzaj, status, nazwa, okres_start, okres_end,
    filename, is_ifrs, is_correction, date_filed, date_prepared,
    is_downloaded, downloaded_at, storage_path, storage_backend,
    file_size_bytes, zip_size_bytes, file_count, file_types,
    discovered_at, metadata_fetched_at, download_error, file_type
FROM krs_document_versions
WHERE is_current = true;
