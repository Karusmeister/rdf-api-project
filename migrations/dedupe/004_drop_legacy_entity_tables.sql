-- SCHEMA_DEDUPE_PLAN #2 (continued): drop the now-unused entity version
-- history and registry tables + their sequences and indexes.
--
-- Runs AFTER dedupe/003 has fully populated krs_companies and AFTER the
-- accompanying code change that stopped writing to either source.

DROP VIEW IF EXISTS krs_entities_current;

DROP TABLE IF EXISTS krs_entity_versions;
DROP SEQUENCE IF EXISTS seq_krs_entity_versions;

DROP TABLE IF EXISTS krs_registry;

-- The legacy ``krs_entities`` flat cache was dropped in DB-003 but some
-- clusters may still carry it; drop is a no-op where it's already gone.
DROP TABLE IF EXISTS krs_entities;
