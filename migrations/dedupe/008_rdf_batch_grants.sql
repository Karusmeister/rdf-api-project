-- SCHEMA_DEDUPE follow-up: grant rdf_batch the same append-only DML on the
-- new dedupe tables that migration prediction/008_scoped_batch_role granted
-- on the now-dropped legacy tables.
--
-- Dropped by dedupe/004 + dedupe/007:  krs_entity_versions, krs_registry,
--                                       krs_document_versions
-- Replaced by dedupe/003 + dedupe/006: krs_companies, krs_documents,
--                                       krs_document_downloads
--
-- rdf_api already has SELECT/INSERT/UPDATE/DELETE on the new tables because
-- they were created by the app owner. rdf_batch needs explicit GRANTs.
--
-- Matches the scoped-role posture: SELECT/INSERT/UPDATE only. No DELETE,
-- no DDL, no superuser. The scraper is append-only.

GRANT SELECT, INSERT, UPDATE ON
  krs_companies,
  krs_documents,
  krs_document_downloads
TO rdf_batch;

-- No sequences needed: krs_companies PK is krs VARCHAR, krs_documents PK is
-- document_id VARCHAR, krs_document_downloads PK is document_id VARCHAR (FK).
