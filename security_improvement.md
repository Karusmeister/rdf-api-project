# Security Improvement Tasks — GCP Pipeline Setup

Tasks derived from the security review of `docs/PIPELINE_GCP_SETUP.md`.
Ordered by priority. Each task lists the concrete change required, the file
or resource touched, and a verification step.

---

## Priority 1 — High severity

### [H4] Remove public IP from `rdf-pipeline` Cloud SQL instance
**Why:** current `gcloud sql instances create` at `docs/PIPELINE_GCP_SETUP.md:116` provisions a public IP with password auth — a large external attack surface.
**Change:** add the following flags to the create command (or run `gcloud sql instances patch` if already created):
```
--no-assign-ip
--network=projects/rdf-api-project/global/networks/default
--enable-google-private-path
--require-ssl
--database-flags=cloudsql.iam_authentication=on
```
**Verify:** `gcloud sql instances describe rdf-pipeline --format='value(settings.ipConfiguration.ipv4Enabled)'` returns `False`. Pipeline still connects via Cloud SQL Auth Proxy.

### [H1] Replace default Compute SA with dedicated per-workload service accounts
**Why:** every workload currently uses `448201086881-compute@developer.gserviceaccount.com`, which typically has `roles/editor`. Any compromise = project editor.
**Change:** create three dedicated SAs:
- `pipeline-runner@rdf-api-project.iam.gserviceaccount.com` — Cloud Run Job
- `rdf-api-runtime@rdf-api-project.iam.gserviceaccount.com` — API service
- `scheduler-invoker@rdf-api-project.iam.gserviceaccount.com` — Cloud Scheduler
Update Cloud Run Job, Cloud Run service, Cloud Scheduler, and every `gcloud secrets add-iam-policy-binding` to target the appropriate dedicated SA. Then remove `roles/editor` from the default compute SA.
**Verify:** `gcloud projects get-iam-policy rdf-api-project` shows the default compute SA has no project-level roles; each dedicated SA has only the roles listed in H2/H3/M8.

### [H2] Scope Storage IAM to the staging bucket only
**Why:** `roles/storage.objectAdmin` at project level (`docs/PIPELINE_GCP_SETUP.md:209`) allows delete on every bucket in the project.
**Change:** remove the project-level binding and grant on the bucket:
```
gcloud projects remove-iam-policy-binding rdf-api-project \
    --member=serviceAccount:<pipeline-runner-sa> \
    --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding gs://rdf-pipeline-staging \
    --member=serviceAccount:<pipeline-runner-sa> \
    --role=roles/storage.objectUser
```
**Verify:** `gcloud storage buckets get-iam-policy gs://rdf-pipeline-staging` lists the SA with `objectUser` only; `gcloud projects get-iam-policy …` no longer shows project-level storage roles for this SA.

### [H3] Scope BigQuery dataEditor to the `rdf_analytics` dataset
**Why:** project-level `roles/bigquery.dataEditor` at `docs/PIPELINE_GCP_SETUP.md:201` allows writes to every dataset.
**Change:** remove project-level binding; grant dataset-level role via `bq update --set_iam_policy` or:
```
bq add-iam-policy-binding \
    --member=serviceAccount:<pipeline-runner-sa> \
    --role=roles/bigquery.dataEditor \
    rdf-api-project:rdf_analytics
```
Keep `roles/bigquery.jobUser` at project level (job creation requires it).
**Verify:** `bq get-iam-policy rdf-api-project:rdf_analytics` shows the SA; project IAM no longer lists it as dataEditor.

### [H5] Enable Cloud SQL IAM DB authentication
**Why:** the flow depends on a static `postgres` superuser password. IAM DB auth removes long-lived credentials.
**Change:**
1. Enable `cloudsql.iam_authentication=on` (covered by H4).
2. Create IAM DB users:
   ```
   gcloud sql users create <pipeline-runner-sa> --instance=rdf-pipeline --type=cloud_iam_service_account
   gcloud sql users create <rdf-api-runtime-sa> --instance=rdf-pipeline --type=cloud_iam_service_account
   ```
3. Grant each user only the DB privileges it needs (see M1).
4. Update application DSN to use IAM auth via Cloud SQL Auth Proxy (no password).
5. Keep a break-glass `postgres` password in Secret Manager, rotated quarterly.
**Verify:** `psql` via Cloud SQL Auth Proxy with `--auto-iam-authn` succeeds without a password; `gcloud sql users list --instance=rdf-pipeline` shows the IAM users.

---

## Priority 2 — Medium severity

### [M2] Enable PITR, backup retention, and deletion protection on `rdf-pipeline`
**Change:** `gcloud sql instances patch rdf-pipeline --enable-point-in-time-recovery --retained-backups-count=30 --deletion-protection`
**Verify:** `gcloud sql instances describe rdf-pipeline --format='value(settings.backupConfiguration.pointInTimeRecoveryEnabled,settings.deletionProtectionEnabled)'` returns `True True`.

### [M1] Create least-privilege application DB roles
**Change:** connect to the `pipeline` database once as `postgres` and run:
```sql
CREATE ROLE pipeline_rw NOINHERIT;
GRANT USAGE, CREATE ON SCHEMA public TO pipeline_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pipeline_rw;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pipeline_rw;

CREATE ROLE pipeline_ro NOINHERIT;
GRANT USAGE ON SCHEMA public TO pipeline_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pipeline_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pipeline_ro;

GRANT pipeline_rw TO "<pipeline-runner-sa>";
GRANT pipeline_ro TO "<rdf-api-runtime-sa>";
```
Update `app/db/pipeline_db.py` reads to use the `pipeline_ro` DSN for the API service.
**Verify:** attempt `TRUNCATE financial_reports` as the API user — must fail.

### [M5] Harden the `rdf-pipeline-staging` GCS bucket
**Change:** recreate (or update) with:
```
gcloud storage buckets update gs://rdf-pipeline-staging \
    --uniform-bucket-level-access \
    --public-access-prevention \
    --lifecycle-file=deploy/gcs_lifecycle.json
```
Create `deploy/gcs_lifecycle.json` to delete parquet files older than 30 days. Consider CMEK (`--default-encryption-key`) if data sensitivity warrants it.
**Verify:** `gcloud storage buckets describe gs://rdf-pipeline-staging --format='value(iamConfiguration.uniformBucketLevelAccess.enabled,iamConfiguration.publicAccessPrevention)'` returns `True enforced`.

### [M4] Pin Secret Manager replication to `europe-central2`
**Why:** defaults use automatic replication, which may violate EU residency.
**Change:** recreate the pipeline secrets with user-managed replication:
```
gcloud secrets create pipeline-database-url \
    --replication-policy=user-managed \
    --locations=europe-central2 \
    --data-file=-
```
Add `--next-rotation-time` and `--rotation-period=7776000s` (90 days) for automated rotation with a Pub/Sub topic handler that rotates the DB password.
**Verify:** `gcloud secrets describe pipeline-database-url --format='value(replication)'` shows `europe-central2` only.

### [M8] Scope Cloud Scheduler to invoke `pipeline-runner` only
**Change:** instead of granting `roles/run.invoker` project-wide to the scheduler SA, grant it on the specific job:
```
gcloud run jobs add-iam-policy-binding pipeline-runner \
    --region=europe-central2 \
    --member=serviceAccount:<scheduler-invoker-sa> \
    --role=roles/run.invoker
```
**Verify:** scheduler SA cannot invoke any other Cloud Run Job.

### [M6] Dedicated Cloud Build service account
**Change:** create `cloudbuild-pipeline@rdf-api-project.iam.gserviceaccount.com`, grant `roles/artifactregistry.writer` on the `rdf-api` repo only and `roles/logging.logWriter` project-wide, then pass `--service-account` to `gcloud builds submit`. Restrict Cloud Build triggers to `etl_pipeline` / `main` branches.
**Verify:** build succeeds with the dedicated SA; the default Cloud Build SA is removed from the project.

### [M7] Immutable Artifact Registry tags + digest pinning
**Change:**
```
gcloud artifacts repositories update rdf-api \
    --location=europe-central2 \
    --immutable-tags
```
Update Cloud Run Job deploy commands to reference the image by digest (`@sha256:…`) instead of `:latest`. Enable Container Analysis and block deploy on CRITICAL CVEs.
**Verify:** attempting to re-push `:latest` for an existing tag fails; `gcloud run jobs describe pipeline-runner --format='value(spec.template.template.containers[0].image)'` shows a digest.

### [M3] Decide on HA for `rdf-pipeline`
**Change:** if analytics is business-critical, patch to regional HA:
```
gcloud sql instances patch rdf-pipeline --availability-type=REGIONAL
```
Otherwise document the zonal-only decision in `docs/PIPELINE_GCP_SETUP.md` with the RTO/RPO implications.
**Verify:** `gcloud sql instances describe rdf-pipeline --format='value(settings.availabilityType)'` matches the decision.

---

## Priority 3 — Low severity

### [L1] Tighten the agent IAM allowlist
**Why:** `Bash(gcloud projects add-iam-policy-binding:*)` at `docs/PIPELINE_GCP_SETUP.md:36` lets a compromised/injected agent grant itself any role.
**Change:** remove this entry from `.claude/settings.local.json` allow list. Require the human operator to run IAM binding commands manually, or add narrowly scoped matchers for the specific bindings in this runbook.
**Verify:** review `.claude/settings.local.json` — no wildcard IAM binding permission.

### [L2] Scope `gcloud secrets versions access` allowlist
**Change:** restrict to the two pipeline secrets if the harness supports narrower matchers, otherwise drop it from the allowlist. This prevents an injected agent from exfiltrating `jwt-secret`, `nordvpn-password`, `cloud-db-password`.
**Verify:** attempt to access an unrelated secret as the agent — must be blocked.

### [L3] Avoid the password in shell variables
**Change:** rewrite section 0.2/0.3 in `docs/PIPELINE_GCP_SETUP.md` to generate the password in a single subshell and feed it via stdin to both `gcloud sql users set-password --password-file=-` and `gcloud secrets create --data-file=-`, without ever assigning to a variable. Replace `openssl rand -base64 32 | tr -d '/+='` with a full-entropy generator (e.g. `openssl rand -base64 48`).
**Verify:** `history | grep PIPELINE_DB_PASSWORD` returns nothing after running the setup.

### [L4] Fix the `database-url` verify step
**Change:** `docs/PIPELINE_GCP_SETUP.md:229` references a secret this doc does not create. Either add a creation step for `database-url` upstream of Phase 0 or remove the verify line.
**Verify:** doc is internally consistent — every secret referenced in verify commands is created within the doc.

### [L5] Add alerting on Cloud Run Job failures
**Change:** create a log-based metric on `resource.type=cloud_run_job AND resource.labels.job_name=pipeline-runner AND severity>=ERROR` and an alert policy routed to email / Slack. Alternatively, alert on `pipeline_runs.status='failed'` rows via a scheduled query.
**Verify:** manually fail a job run and confirm the alert fires.

### [L6] Egress controls for Cloud Run Job
**Change:** add `--vpc-connector=<connector>` and `--vpc-egress=all-traffic` to `gcloud run jobs create pipeline-runner`. Configure VPC firewall egress rules to allow only Google APIs (`private.googleapis.com`) and deny the rest. Add VPC Service Controls if the project handles sensitive data.
**Verify:** inside the job, `curl https://example.com` fails; `curl https://bigquery.googleapis.com` succeeds.

### [L7] Export admin audit logs to a retention-locked bucket
**Change:** create a log sink routing `logName:"cloudaudit.googleapis.com/activity"` to a GCS bucket with bucket retention lock (e.g. 400 days). Grants auditors immutable evidence independent of Cloud Logging defaults.
**Verify:** `gcloud logging sinks list` includes the sink; the target bucket has `retentionPolicy.isLocked=true`.

### [L8] Harden `Dockerfile.pipeline`
**Change:** review `Dockerfile.pipeline`:
- Pin base image by digest (`python:3.12-slim@sha256:…`).
- Add a non-root `USER pipeline` and `RUN useradd -m pipeline` before `ENTRYPOINT`.
- Remove `build-essential` from the final layer (use a multi-stage build).
**Verify:** `docker inspect pipeline-runner --format='{{.Config.User}}'` returns a non-root uid; `docker history` shows no compilers in the final layer.

---

## Suggested execution order

1. H4 — public IP removal (biggest external risk).
2. H1 → H2 → H3 → H5 — service-account and IAM scoping (blast radius).
3. M2 — backups and deletion protection (data durability).
4. L1 → L2 — agent allowlist hardening (prompt-injection safety).
5. M1 — application-level DB roles (defense in depth).
6. M5, M4, M8, M6, M7 — secondary hardening.
7. L3, L4 — doc hygiene.
8. L5, L6, L7, L8 — monitoring, egress, audit, container hardening.

## Out-of-scope / things already fine

- Secrets are mounted via `--set-secrets` (not baked into env).
- Deny-list in the agent allowlist blocks `delete` operations.
- API startup handles pipeline DB unavailability gracefully.
- All resources pinned to `europe-central2` (residency consistent).
- Rollback procedures clearly labeled destructive and human-only.
