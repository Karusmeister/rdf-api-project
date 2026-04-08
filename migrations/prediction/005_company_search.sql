-- PKR-124: Company search with trigram indexes + search log

-- Enable trigram extension for fast partial text matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN trigram index on krs_registry.company_name for ILIKE searches
CREATE INDEX IF NOT EXISTS idx_krs_registry_name_trgm
    ON krs_registry USING gin (company_name gin_trgm_ops);

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
