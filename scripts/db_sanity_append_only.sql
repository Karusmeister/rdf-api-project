-- db_sanity_append_only.sql
-- Post-migration sanity checks. Each query should return 0 rows for a healthy DB.
-- Run with: python scripts/run_db_migration.py scripts/db_sanity_append_only.sql
-- Or manually: ./.venv/bin/python -c "import duckdb; ..."

-- 1. Max 1 current entity version per krs
SELECT 'multi_current_entities' AS check_name, krs, COUNT(*) AS cnt
FROM krs_entity_versions
WHERE is_current = true
GROUP BY krs
HAVING COUNT(*) > 1;

-- 2. Max 1 current document version per document_id
SELECT 'multi_current_documents' AS check_name, document_id, COUNT(*) AS cnt
FROM krs_document_versions
WHERE is_current = true
GROUP BY document_id
HAVING COUNT(*) > 1;

-- 3. No new sentinel financial_reports (fiscal_year=0, sentinel dates)
SELECT 'sentinel_reports' AS check_name, cnt
FROM (
    SELECT COUNT(*) AS cnt
    FROM financial_reports
    WHERE fiscal_year = 0
      AND period_start = '1970-01-01'
      AND period_end = '1970-01-01'
) t
WHERE cnt > 0;

-- 4. No orphan krs_entity_versions without a matching krs_entities row
SELECT 'orphan_entity_versions' AS check_name, kev.krs
FROM krs_entity_versions kev
LEFT JOIN krs_entities ke ON ke.krs = kev.krs
WHERE ke.krs IS NULL
  AND kev.is_current = true;

-- 5. No orphan krs_document_versions without a matching krs_documents row
SELECT 'orphan_document_versions' AS check_name, kdv.document_id
FROM krs_document_versions kdv
LEFT JOIN krs_documents kd ON kd.document_id = kdv.document_id
WHERE kd.document_id IS NULL
  AND kdv.is_current = true;
