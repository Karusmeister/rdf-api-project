# Schema Dedupe Cutover Runbook

One-time runbook for cutting over from the legacy `krs_entity_versions` / `krs_registry` / `krs_document_versions` tables to the dedupe schema (`krs_companies`, `krs_documents`, `krs_document_downloads`).

Validated against prod data 2026-04-19 — see [SCHEMA_DEDUPE_PLAN.md](SCHEMA_DEDUPE_PLAN.md) for the design rationale and [migrations/dedupe/](../migrations/dedupe/) for the migrations themselves.

## Why this isn't a normal tag-and-push

The dedupe release contains 7 SQL migrations that, left to Cloud Run's startup runner, would take **5–9 minutes** (migration 006 alone SELECTs 4.47M rows in 72s, plus INSERT + index updates). Cloud Run's startup probe is configured for **155s** failure-threshold ([cloud_run.tf:startup_probe](../../rdf-infra/terraform/cloud_run.tf)) and won't wait. Worse, migrations 004 and 007 DROP tables that the currently-serving revision still reads from — so even if we extended the probe, the old revision would error for 5+ minutes while the new revision finishes migrating.

The fix: **pre-run the migrations manually** via Cloud SQL proxy before the deploy. Additive migrations first (safe alongside the old code), then a short coordinated window for the breaking ones.

## Prerequisites (verify each before starting)

```bash
# 1. Active gcloud account is contact@kraus.uk (memory rule)
gcloud config get-value account
# expect: contact@kraus.uk

# 2. Migration 001 DROP VIEW fix is committed (see git log for migrations/dedupe/001_json_to_jsonb.sql)
cd /Users/piotrkraus/piotr/rdf-project/rdf-api-project
git log -1 --oneline migrations/dedupe/001_json_to_jsonb.sql
# must show a commit containing "DROP VIEW IF EXISTS latest_raw_financial_data"

# 3. Branch feat/schema-dedupe merged into main (release is cut from main)
git branch --merged main | grep feat/schema-dedupe && echo "merged" || echo "NOT merged — merge first"

# 4. pg_repack binary available (used to reclaim bloat after DROP TABLE in 007)
ls /Users/piotrkraus/piotr/bin/pg_repack
# NOTE: pg_repack is optional — runbook does not use it. Plan only; run separately post-cutover if bloat matters.

# 5. Cloud SQL proxy installed
which cloud-sql-proxy
```

## One-time setup at the start of the session

Open **two terminals** — one runs the proxy, one runs commands.

### Terminal 1 — Cloud SQL proxy (leave running for the whole cutover)

```bash
# Use contact@kraus.uk access token — ADC is bound to a different account
cloud-sql-proxy "rdf-api-project:europe-central2:rdf-postgres" \
  --port 15432 \
  --token "$(gcloud auth print-access-token --account=contact@kraus.uk)"
# Wait for: "The proxy has started successfully and is ready for new connections!"
# Leave this running.
```

**If the token expires** (1h OAuth TTL) during a long migration, the proxy will error. Restart the proxy with a fresh token.

### Terminal 2 — env vars + prod DB handle

```bash
export DB_PASS="$(gcloud secrets versions access latest --secret=cloud-db-password --project=rdf-api-project --account=contact@kraus.uk)"
export PGPASSWORD="$DB_PASS"

# Smoke test
psql -h localhost -p 15432 -U postgres -d rdf -c "SELECT version FROM schema_migrations ORDER BY version" | head -15
# expect: prediction/001..010 + legacy/002_post_release_drop. No dedupe/* yet.
```

## Phase 0 — Re-verify prod preconditions

The queries below were clean as of 2026-04-19. If state has drifted (data loaded between validation and cutover), you want to know NOW.

```bash
psql -h localhost -p 15432 -U postgres -d rdf <<'SQL'
\pset pager off

-- Must all be 0:
SELECT 'orphan_etl_attempts' AS check,
       count(*) AS value
FROM etl_attempts e
WHERE e.document_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM krs_document_versions v WHERE v.document_id = e.document_id AND v.is_current)
UNION ALL
SELECT 'orphan_financial_reports',
       count(*)
FROM financial_reports fr
WHERE fr.source_document_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM krs_document_versions v WHERE v.document_id = fr.source_document_id AND v.is_current)
UNION ALL
SELECT 'legacy_tables_present',
       count(*)
FROM information_schema.tables
WHERE table_schema='public' AND table_name IN ('krs_documents','krs_entities');
SQL
```

**If any row returns non-zero, STOP and resolve before proceeding.** Orphans need `DELETE` (or reparenting); legacy tables need `DROP TABLE`.

---

## Phase A — Additive migrations (no user impact)

These four migrations create new tables and add constraints. The old code keeps working because nothing it queries is touched. Safe to run at any time.

**Estimated time: 2–3 minutes total.**

### A.1 — Migration 001: JSON → JSONB

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/001_json_to_jsonb.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/001_json_to_jsonb') ON CONFLICT (version) DO NOTHING;"
```

Verify:

```bash
psql -h localhost -p 15432 -U postgres -d rdf -c \
  "SELECT column_name, data_type FROM information_schema.columns
   WHERE table_name='raw_financial_data' AND column_name='data_json'"
# expect: jsonb
```

### A.2 — Migration 002: Purge `fiscal_year=0` garbage (24 rows)

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/002_financial_reports_cleanup.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/002_financial_reports_cleanup') ON CONFLICT (version) DO NOTHING;"
```

### A.3 — Migration 003: Create `krs_companies` and populate

This is the entity collapse. Reads from `krs_entity_versions` + `krs_registry` and inserts ~551k rows. **Old code keeps reading from the source tables** — no impact until Phase B drops them.

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/003_create_krs_companies.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/003_create_krs_companies') ON CONFLICT (version) DO NOTHING;"
```

Verify row counts match source:

```bash
psql -h localhost -p 15432 -U postgres -d rdf <<'SQL'
SELECT
  (SELECT count(*) FROM krs_companies)                            AS new_companies,
  (SELECT count(*) FROM krs_entity_versions WHERE is_current)     AS source_current_versions,
  (SELECT count(*) FROM krs_registry)                             AS source_registry;
-- new_companies should equal GREATEST(source_current_versions, source_registry) ≈ 551219
SQL
```

### A.4 — Migration 005: Financial-reports FKs

**NOTE:** Skip migration 004 here — it drops the legacy entity tables and belongs in Phase B.

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/005_financial_reports_fks.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/005_financial_reports_fks') ON CONFLICT (version) DO NOTHING;"
```

### Phase A checkpoint

```bash
psql -h localhost -p 15432 -U postgres -d rdf -c "SELECT version FROM schema_migrations WHERE version LIKE 'dedupe/%' ORDER BY version"
# expect: 001, 002, 003, 005 (no 004, 006, 007 yet)
```

**If anything above failed**, the transaction rolled back cleanly. Resolve and retry the specific migration. Old code is unaffected by any partial progress at this point. You can abort here without any cleanup — the new tables are dormant until code reads from them.

---

## Phase B — Breaking migrations + deploy (user-visible cutover)

This is where the old code breaks. Requires coordination. User said "project can be stopped during transition" — this is that window.

**Estimated time: 10–15 minutes end-to-end.**

### B.1 — Pause batch workers (SSH to batch VM via IAP)

```bash
gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a \
  --project=rdf-api-project \
  --tunnel-through-iap \
  --account=contact@kraus.uk \
  --command="sudo systemctl stop rdf-worker krs-scanner metadata-backfill; sudo systemctl status rdf-worker --no-pager | head -5"
```

Verify all three show `inactive (dead)`.

### B.2 — Scale Cloud Run to zero (or drain traffic)

Two options. Pick one based on your tolerance:

**Option B.2a — Hard stop (zero user impact during migrations, visible downtime):**

```bash
# Save current max_instances for restore
gcloud run services describe rdf-api --region=europe-central2 --account=contact@kraus.uk \
  --format="value(spec.template.spec.containerConcurrency,spec.template.metadata.annotations['autoscaling.knative.dev/maxScale'])"

# Scale to 0
gcloud run services update rdf-api \
  --region=europe-central2 \
  --account=contact@kraus.uk \
  --max-instances=0
# Users get 5xx errors from Cloud Run LB. ~30s to take effect.
```

**Option B.2b — Keep old revision serving; accept ~5min of DB errors on dropped tables:**

```bash
# Do nothing — old revision keeps serving until new revision deploys.
# Users will see 500s on endpoints that hit krs_entity_versions / krs_document_versions
# once migrations 004 and 007 run below.
```

Recommendation: **B.2a** — cleaner, user sees a single short outage rather than sporadic errors.

### B.3 — Run the breaking migrations

#### B.3.1 — Migration 004: Drop legacy entity tables

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/004_drop_legacy_entity_tables.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/004_drop_legacy_entity_tables') ON CONFLICT (version) DO NOTHING;"
```

After this runs, `krs_entity_versions`, `krs_registry`, and the `krs_entities_current` view are gone. Old code that reads from them will now error.

#### B.3.2 — Migration 006: Split `krs_document_versions` into documents + downloads

**This is the long one (~5 min).** Don't abort it mid-way.

```bash
# Run without --single-transaction so timing doesn't compound — the migration
# wraps its own DO block. Start this in a screen/tmux if your SSH could drop.
time psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  -c "\i $PWD/migrations/dedupe/006_create_documents_split.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/006_create_documents_split') ON CONFLICT (version) DO NOTHING;"
```

Verify:

```bash
psql -h localhost -p 15432 -U postgres -d rdf <<'SQL'
SELECT 'new_documents' AS t, count(*) FROM krs_documents
UNION ALL SELECT 'new_downloads', count(*) FROM krs_document_downloads
UNION ALL SELECT 'source_current', count(*) FROM krs_document_versions WHERE is_current;
-- new_documents ≈ new_downloads ≈ source_current (≈ 4.47M)
SQL
```

#### B.3.3 — Migration 007: Drop `krs_document_versions`, rebuild view, add FKs

```bash
psql -h localhost -p 15432 -U postgres -d rdf \
  -v ON_ERROR_STOP=1 \
  --single-transaction \
  -c "\i $PWD/migrations/dedupe/007_drop_document_versions.sql" \
  -c "INSERT INTO schema_migrations (version) VALUES ('dedupe/007_drop_document_versions') ON CONFLICT (version) DO NOTHING;"
```

### B.4 — Deploy the new code

```bash
# From rdf-api-project root
cd /Users/piotrkraus/piotr/rdf-project/rdf-api-project

# Cut the tag
git tag -a v1.2.0 -m "Schema dedupe: krs_companies + krs_documents/downloads split + JSONB + FKs"
git push origin v1.2.0
# GitHub Actions kicks in: build image + deploy to Cloud Run
# Watch: gh run watch
```

On Cloud Run startup, the migration runner will see all 7 dedupe migrations marked applied in `schema_migrations` and skip them. **Startup should complete in seconds, not minutes.**

### B.5 — Restore traffic (if B.2a chose to scale down)

```bash
gcloud run services update rdf-api \
  --region=europe-central2 \
  --account=contact@kraus.uk \
  --max-instances=2   # or whatever the saved value from B.2a was
```

### B.6 — Resume batch workers

```bash
gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a \
  --project=rdf-api-project \
  --tunnel-through-iap \
  --account=contact@kraus.uk \
  --command="sudo systemctl start rdf-worker krs-scanner metadata-backfill && sudo systemctl status rdf-worker --no-pager | head -5"
```

### B.7 — Apply the Cloud Run CORS cleanup

The earlier Lovable cleanup left Terraform state drifted on the Cloud Run service (CORS_ORIGINS env var). With the app now deploying successfully, TF can push the change:

```bash
cd /Users/piotrkraus/piotr/rdf-project/rdf-infra/terraform
terraform plan -var-file=../environments/prod.tfvars
# Expect: 1 Cloud Run service update (CORS_ORIGINS loses the 2 Lovable origins)

terraform apply -var-file=../environments/prod.tfvars
```

---

## Phase C — Verification

```bash
# Health endpoints (API)
API="https://rdf-api-448201086881.europe-central2.run.app"
curl -sf "$API/health" && echo " ✓"
curl -sf "$API/health/predictions" && echo " ✓"
curl -sf "$API/health/krs" && echo " ✓"

# A real prediction endpoint — pick any KRS that had predictions before
curl -sf "$API/api/predictions/0000000001" | jq '.predictions | length'

# Cloud Run logs — look for any startup errors in the last few minutes
gcloud run services logs read rdf-api \
  --region=europe-central2 \
  --account=contact@kraus.uk \
  --limit=50 --freshness=10m | grep -iE "error|traceback" | head -20

# Batch VM services
gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --tunnel-through-iap \
  --account=contact@kraus.uk \
  --command="sudo systemctl is-active rdf-worker krs-scanner metadata-backfill"
# expect: three lines of "active"
```

### Spot-check the new schema

```bash
psql -h localhost -p 15432 -U postgres -d rdf <<'SQL'
\pset pager off
-- The full post-cutover schema
SELECT version FROM schema_migrations WHERE version LIKE 'dedupe/%' ORDER BY version;
-- expect: 001..007

-- Row counts on the new tables
SELECT 'krs_companies' AS t, count(*) FROM krs_companies
UNION ALL SELECT 'krs_documents', count(*) FROM krs_documents
UNION ALL SELECT 'krs_document_downloads', count(*) FROM krs_document_downloads;

-- FK constraints added by 005 and 007
SELECT conname FROM pg_constraint
WHERE conname IN ('fk_line_items_report','fk_raw_report','fk_features_report',
                  'fk_financial_reports_document','fk_etl_attempts_document');

-- Legacy tables gone
SELECT table_name FROM information_schema.tables
WHERE table_schema='public' AND table_name IN
  ('krs_entity_versions','krs_registry','krs_document_versions','krs_documents','krs_entities')
  AND table_name NOT IN ('krs_documents');  -- krs_documents is the NEW table
-- expect: 0 rows
SQL
```

---

## Rollback procedures

### If Phase A fails
Each migration's transaction rolled back. Old schema fully intact. No action needed beyond debugging the migration and retrying. You can abort without touching anything.

### If Phase B.3 fails mid-way (e.g., 006 times out on Cloud SQL)
The failing migration rolls back. Earlier ones that succeeded stay committed. Options:
- **Fix and retry the failing migration** — run it again, it's idempotent up to the `schema_migrations` guard.
- **Roll back completely** — you need to restore from Cloud SQL backup (automated daily backups are on). See [CLOUD_DEPLOYMENT.md](CLOUD_DEPLOYMENT.md) for restore steps.

### If Phase B.4 deploy fails
The new Cloud Run revision fails its startup probe. Cloud Run keeps routing to the previous revision. BUT the previous revision's code queries `krs_entity_versions` / `krs_document_versions` which are now gone → it 500s.

Options:
- **Fix the app code issue, redeploy** — migrations are already applied, new code will start fast.
- **Emergency rollback** — harder, since the DB schema has changed. Restore Cloud SQL from the most recent automated backup (pre-cutover):
  ```bash
  gcloud sql backups list --instance=rdf-postgres --account=contact@kraus.uk | head -5
  gcloud sql backups restore <BACKUP_ID> --restore-instance=rdf-postgres --account=contact@kraus.uk
  ```
  Then route traffic back to `rdf-api-00043-p7t`. Expect 15–30 min of downtime for the restore.

### If Phase B.7 terraform apply fails
Same Cloud Run revision keeps serving. No user impact. Fix the TF issue (probably related to a new startup bug in the deployed image) and retry.

---

## Artifacts created by this cutover

- 7 entries in `schema_migrations` (`dedupe/001..007`) — sticky forever
- 3 new tables: `krs_companies`, `krs_documents`, `krs_document_downloads`
- 1 view rebuilt: `krs_documents_current` (now joins the new tables)
- 3 tables dropped: `krs_entity_versions`, `krs_registry`, `krs_document_versions`
- 2 sequences dropped: `seq_krs_entity_versions`, `seq_krs_document_versions`
- 5 new FKs: `fk_line_items_report`, `fk_raw_report`, `fk_features_report`, `fk_financial_reports_document`, `fk_etl_attempts_document`
- Reclaimed storage: ~600 MB from entity collapse + ~1–1.5 GB from document split (pre-repack)

## Post-cutover follow-ups (not blocking)

- Run `pg_repack` on `financial_reports`, `financial_line_items`, and any large table that had heavy DELETE/UPDATE traffic to reclaim bloat. See [SCHEMA_DEDUPE_PLAN.md §Operational notes](SCHEMA_DEDUPE_PLAN.md).
- Consider Cloud SQL tier upgrade (`db-f1-micro` → `db-g1-small`) — the 72s SELECT in the benchmark suggests I/O headroom is tight.
- Archive this runbook to `rdf-infra/docs/archive/` once cutover is verified stable (say 7 days post-deploy).
