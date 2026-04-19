-- SCHEMA_DEDUPE_PLAN #1 + #3: split krs_document_versions into an immutable
-- discovery record + a mutable download-state row per document, and drop
-- the versioning columns. The table was averaging 2.26 versions/document
-- despite 0% of those versions representing real upstream content changes
-- (the diffs were is_downloaded / storage_path / file_size state
-- transitions). Removing versioning collapses writes ~10× and stops the
-- WAL-driven bloat that would regrow even after DELETE+repack.
--
-- Dropped vs. krs_document_versions:
--   * run_id, date_prepared, zip_size_bytes, storage_backend    — never useful
--   * file_types (VARCHAR)                                      — redundant with file_type
--   * change_reason, snapshot_hash                              — versioning telemetry
--   * version_no, version_id, valid_from, valid_to,
--     is_current, observed_at                                   — versioning
--
-- Type changes baked in here (plan #3):
--   * rodzaj       VARCHAR → SMALLINT           (22 distinct integer values)
--   * status       VARCHAR → BOOLEAN is_deleted (NIEUSUNIETY / USUNIETY)
--   * date_filed   VARCHAR → DATE               (always YYYY-MM-DD or empty)
--   * okres_start  VARCHAR → DATE
--   * okres_end    VARCHAR → DATE

CREATE TABLE IF NOT EXISTS krs_documents (
    document_id      VARCHAR PRIMARY KEY,
    krs              VARCHAR(10) NOT NULL,
    rodzaj           SMALLINT NOT NULL,
    nazwa            VARCHAR,
    okres_start      DATE,
    okres_end        DATE,
    filename         VARCHAR,
    is_ifrs          BOOLEAN,
    is_correction    BOOLEAN,
    date_filed       DATE,
    is_deleted       BOOLEAN NOT NULL DEFAULT false,
    discovered_at    TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_documents_krs ON krs_documents(krs);

CREATE TABLE IF NOT EXISTS krs_document_downloads (
    document_id         VARCHAR PRIMARY KEY
                            REFERENCES krs_documents(document_id) ON DELETE CASCADE,
    is_downloaded       BOOLEAN NOT NULL DEFAULT false,
    downloaded_at       TIMESTAMP,
    storage_path        VARCHAR,
    file_size_bytes     BIGINT,
    file_count          INTEGER,
    file_type           VARCHAR(10),
    download_error      VARCHAR,
    metadata_fetched_at TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT current_timestamp
);

-- Partial index: "documents still to download" is the exact hot query for
-- the worker loop. Same shape as the old is_current partial index.
CREATE INDEX IF NOT EXISTS idx_downloads_pending
    ON krs_document_downloads(document_id)
    WHERE NOT is_downloaded;

-- Backfill from krs_document_versions if it still exists. Wrapped in a DO
-- block so fresh installs (where dedupe/007 later drops the table and the
-- bootstrap DDL no longer creates it) skip cleanly.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'krs_document_versions'
    ) THEN
        INSERT INTO krs_documents (
            document_id, krs, rodzaj, nazwa, okres_start, okres_end,
            filename, is_ifrs, is_correction, date_filed, is_deleted,
            discovered_at
        )
        SELECT DISTINCT ON (document_id)
            document_id, krs,
            NULLIF(rodzaj, '')::smallint,
            nazwa,
            NULLIF(okres_start, '')::date,
            NULLIF(okres_end, '')::date,
            filename, is_ifrs, is_correction,
            NULLIF(date_filed, '')::date,
            (status = 'USUNIETY'),
            COALESCE(discovered_at, current_timestamp)
        FROM krs_document_versions
        WHERE is_current
        ORDER BY document_id, version_no DESC
        ON CONFLICT (document_id) DO NOTHING;

        INSERT INTO krs_document_downloads (
            document_id, is_downloaded, downloaded_at, storage_path,
            file_size_bytes, file_count, file_type, download_error,
            metadata_fetched_at, updated_at
        )
        SELECT DISTINCT ON (document_id)
            document_id,
            COALESCE(is_downloaded, false),
            downloaded_at, storage_path,
            file_size_bytes, file_count, file_type, download_error,
            metadata_fetched_at,
            COALESCE(observed_at, current_timestamp)
        FROM krs_document_versions
        WHERE is_current
        ORDER BY document_id, version_no DESC
        ON CONFLICT (document_id) DO NOTHING;
    END IF;
END
$$;
