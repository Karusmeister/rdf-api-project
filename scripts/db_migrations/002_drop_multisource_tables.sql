-- 002_drop_multisource_tables.sql
-- Remove speculatively-added multi-source tables (GUS/GPW not planned for 3-6 months).
-- Safe to re-run: IF EXISTS guards.

DROP TABLE IF EXISTS company_identifiers;
DROP SEQUENCE IF EXISTS seq_company_identifiers;
DROP TABLE IF EXISTS data_sources;
