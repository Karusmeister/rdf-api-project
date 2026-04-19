-- SCHEMA_DEDUPE_PLAN #2: collapse krs_entity_versions + krs_registry into
-- a single non-versioned krs_companies table.
--
-- Entity data has never actually versioned in prod (current_rows == total_rows
-- on krs_entity_versions). The valid_from / valid_to / snapshot_hash /
-- is_current machinery was pure overhead. At the same time krs_registry
-- held the same 551k KRS with scraper-scheduling fields; 551,095/551,098
-- names matched and 100% of legal_forms matched. One row per company,
-- full stop.
--
-- The `companies` table in prediction_db is a different, supplementary
-- concern (pkd_code, incorporation_date, voivodeship). It lives on.

CREATE TABLE IF NOT EXISTS krs_companies (
    krs                 VARCHAR(10) PRIMARY KEY,
    name                VARCHAR NOT NULL,
    legal_form          VARCHAR,
    status              VARCHAR,
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

    -- scraper scheduling (folded in from krs_registry)
    first_seen_at       TIMESTAMP,
    last_checked_at     TIMESTAMP,
    last_download_at    TIMESTAMP,
    check_priority      INTEGER NOT NULL DEFAULT 0,
    check_error_count   INTEGER NOT NULL DEFAULT 0,
    last_error_message  VARCHAR,
    total_documents     INTEGER NOT NULL DEFAULT 0,
    total_downloaded    INTEGER NOT NULL DEFAULT 0
);

-- Trigram index so the company-search GIN from migration 005 keeps working
-- once reads cut over to krs_companies. Requires pg_trgm (created in 005).
CREATE INDEX IF NOT EXISTS idx_krs_companies_name_trgm
    ON krs_companies USING gin (name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_krs_companies_last_checked
    ON krs_companies(last_checked_at);

CREATE INDEX IF NOT EXISTS idx_krs_companies_priority
    ON krs_companies(check_priority DESC, last_checked_at);

-- Backfill from the currently-authoritative sources. Wrapped in a DO block
-- so fresh installs (where the legacy tables never got created because the
-- new code stopped declaring them) skip cleanly.
DO $$
DECLARE
    has_versions boolean;
    has_registry boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'krs_entity_versions'
    ) INTO has_versions;
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'krs_registry'
    ) INTO has_registry;

    IF has_versions THEN
        IF has_registry THEN
            INSERT INTO krs_companies (
                krs, name, legal_form, status, is_active,
                registered_at, last_changed_at, nip, regon,
                address_city, address_street, address_postal_code,
                source, synced_at,
                first_seen_at, last_checked_at, last_download_at,
                check_priority, check_error_count, last_error_message,
                total_documents, total_downloaded
            )
            SELECT
                v.krs, v.name, v.legal_form, v.status,
                COALESCE(r.is_active, true),
                v.registered_at, v.last_changed_at, v.nip, v.regon,
                v.address_city, v.address_street, v.address_postal_code,
                v.source, v.valid_from,
                r.first_seen_at, r.last_checked_at, r.last_download_at,
                COALESCE(r.check_priority, 0),
                COALESCE(r.check_error_count, 0),
                r.last_error_message,
                COALESCE(r.total_documents, 0),
                COALESCE(r.total_downloaded, 0)
            FROM krs_entity_versions v
            LEFT JOIN krs_registry r USING (krs)
            WHERE v.is_current
            ON CONFLICT (krs) DO NOTHING;
        ELSE
            INSERT INTO krs_companies (
                krs, name, legal_form, status,
                registered_at, last_changed_at, nip, regon,
                address_city, address_street, address_postal_code,
                source, synced_at
            )
            SELECT
                v.krs, v.name, v.legal_form, v.status,
                v.registered_at, v.last_changed_at, v.nip, v.regon,
                v.address_city, v.address_street, v.address_postal_code,
                v.source, v.valid_from
            FROM krs_entity_versions v
            WHERE v.is_current
            ON CONFLICT (krs) DO NOTHING;
        END IF;
    END IF;

    -- Registry entries without a matching entity version (the plan counted
    -- ~121 of these in prod). Preserves scraper scheduling history.
    IF has_registry THEN
        INSERT INTO krs_companies (
            krs, name, legal_form, is_active, source,
            first_seen_at, last_checked_at, last_download_at,
            check_priority, check_error_count, last_error_message,
            total_documents, total_downloaded
        )
        SELECT
            r.krs,
            COALESCE(r.company_name, ''),
            r.legal_form,
            COALESCE(r.is_active, true),
            'ms_gov',
            r.first_seen_at, r.last_checked_at, r.last_download_at,
            COALESCE(r.check_priority, 0),
            COALESCE(r.check_error_count, 0),
            r.last_error_message,
            COALESCE(r.total_documents, 0),
            COALESCE(r.total_downloaded, 0)
        FROM krs_registry r
        WHERE NOT EXISTS (
            SELECT 1 FROM krs_companies c WHERE c.krs = r.krs
        )
        ON CONFLICT (krs) DO NOTHING;
    END IF;
END $$;
