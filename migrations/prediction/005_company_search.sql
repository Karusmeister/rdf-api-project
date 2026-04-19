-- PKR-124: Company search with trigram indexes + search log.
--
-- NOTE (2026-04-19, SCHEMA_DEDUPE_PLAN #2): the trigram index used to be on
-- `krs_registry.company_name`. After dedupe, company search reads from
-- `krs_companies.name` and dedupe/003 creates that index
-- (idx_krs_companies_name_trgm). To keep this migration rerunnable on a
-- fresh DB where krs_registry no longer exists, the index creation is
-- guarded by table presence. On pre-dedupe clusters that still have
-- krs_registry the index is created as before.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'krs_registry'
    ) THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_krs_registry_name_trgm '
             || 'ON krs_registry USING gin (company_name gin_trgm_ops)';
    END IF;
END $$;

-- Search log table for tracking queries and clicked results
CREATE TABLE IF NOT EXISTS search_log (
    id              BIGSERIAL PRIMARY KEY,
    query           TEXT NOT NULL,
    result_count    INTEGER,
    clicked_krs     VARCHAR(10),
    created_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_search_log_clicked_krs
    ON search_log(clicked_krs) WHERE clicked_krs IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_search_log_created_at
    ON search_log(created_at);
