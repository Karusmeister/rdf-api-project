-- Step 3b follow-up — reassign ownership of all objects in schema public
-- to rdf_api so the API's idempotent startup DDL works.
--
-- The API runs ALTER TABLE / CREATE OR REPLACE VIEW at startup. These DDL
-- statements require ownership of the target object, not just GRANT
-- privileges. Without this migration, the API crashes on the first
-- ALTER TABLE ADD COLUMN IF NOT EXISTS in scraper/db.py:_init_schema().
--
-- Security impact: ownership lets rdf_api do DDL on its own tables. It does
-- NOT make rdf_api a database superuser — the role still cannot:
--   - CREATE EXTENSION / CREATE ROLE / ALTER ROLE / DROP ROLE
--   - bypass row-level security
--   - read pg_authid / pg_shadow / system catalogs
--   - access other databases or schemas
--
-- The right long-term fix is §11 follow-up #1 (refactor startup DDL into
-- migrations, then we can REASSIGN OWNED back to postgres). This migration
-- is the pragmatic prerequisite.

DO $$
DECLARE r record;
BEGIN
  -- Tables
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO rdf_api', r.tablename);
  END LOOP;

  -- Views
  FOR r IN SELECT viewname FROM pg_views WHERE schemaname = 'public' LOOP
    EXECUTE format('ALTER VIEW public.%I OWNER TO rdf_api', r.viewname);
  END LOOP;

  -- Sequences (some tables use auto-named, some use seq_*)
  FOR r IN SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public' LOOP
    EXECUTE format('ALTER SEQUENCE public.%I OWNER TO rdf_api', r.sequence_name);
  END LOOP;
END
$$;

-- After reassigning ownership, postgres can no longer write to its own
-- tables (Cloud SQL postgres is not a true superuser — it can't bypass
-- table grants). Make postgres a member of rdf_api and rdf_batch so it
-- inherits their privileges and can continue running migrations and DBA
-- work.
GRANT rdf_api TO postgres;
GRANT rdf_batch TO postgres;

-- ALTER OWNER does NOT revoke existing GRANTs on the table — rdf_batch
-- keeps its SELECT/INSERT/UPDATE on the batch tables that it had from
-- migration 008.
