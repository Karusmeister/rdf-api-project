-- Step 3b of NAT_COST_REDUCTION_PLAN.md — scoped Postgres role for the
-- Cloud Run API service.
--
-- Replaces the API's `postgres` superuser credential with a scoped role
-- that has DML on every existing table + CREATE on schema public (the
-- API runs idempotent CREATE TABLE IF NOT EXISTS at startup; refactoring
-- this into migration-only DDL is filed in §11 follow-up #1).
--
-- This is intentionally a defense-in-depth + operational-hygiene improvement,
-- not a hard isolation boundary. With DML on every table including `users`,
-- a SQLi in any prediction endpoint can still touch the auth table. True
-- privsep is filed in §11 follow-up #2.
--
-- Security wins over `postgres` superuser:
--   - Cannot CREATE EXTENSION, ALTER ROLE, CREATE ROLE, DROP ROLE
--   - Cannot bypass row-level security if ever enabled
--   - Cannot read pg_authid, pg_shadow, system catalogs
--   - Cannot ALTER TABLE OWNER on tables it doesn't own
--   - Rotational hygiene: rotating rdf_api doesn't need to rotate postgres
--   - Audit clarity: pg_stat_activity shows app vs human sessions
--
-- Apply manually with:
--     \set api_password '`openssl rand -base64 32`'
--     \i 009_scoped_api_role.sql

CREATE ROLE rdf_api LOGIN PASSWORD :'api_password' CONNECTION LIMIT 30;

GRANT CONNECT ON DATABASE rdf TO rdf_api;
GRANT USAGE ON SCHEMA public TO rdf_api;

-- The API runs idempotent CREATE TABLE IF NOT EXISTS / CREATE INDEX /
-- CREATE OR REPLACE VIEW at startup (app/main.py lifespan -> *.connect()).
-- In prod these are no-ops, but the role needs CREATE for the parser.
-- TODO §11 follow-up #1: refactor startup DDL into migrations, then REVOKE this.
GRANT CREATE ON SCHEMA public TO rdf_api;

-- DML on every existing table (the API touches all of them, verified by
-- static analysis of app/ on 2026-04-10).
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO rdf_api;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO rdf_api;

-- Future tables created by future migrations should also be accessible
-- without a separate GRANT step. NOTE: ALTER DEFAULT PRIVILEGES only
-- applies to tables created BY postgres in the future. Tables created by
-- other roles are unaffected. Since migrations and the startup DDL both
-- run as postgres, this covers both paths.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO rdf_api;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO rdf_api;
