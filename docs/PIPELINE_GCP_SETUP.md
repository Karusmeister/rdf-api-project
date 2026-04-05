# GCP Pipeline Setup — Step-by-Step

This file contains every `gcloud`, `gsutil`, `bq`, and `docker`/`gcloud builds`
command required to stand up the prediction pipeline infrastructure and
deploy the Cloud Run Job. Run commands in order; each phase is idempotent
where possible.

Project: `rdf-api-project`
Region:  `europe-central2`
Service account: `448201086881-compute@developer.gserviceaccount.com`

---

## 0. Permissions the coding agent needs to run this autonomously

If you want a coding agent (Claude Code) to execute this file end-to-end
without prompting, pre-authorize the following in
`.claude/settings.local.json` under `permissions.allow`. These are the
minimum shell commands touched below; `Bash(...)` matchers are prefix
matches.

```jsonc
{
  "permissions": {
    "allow": [
      // gcloud — enable APIs, create Cloud SQL, secrets, IAM, Cloud Run
      "Bash(gcloud services enable:*)",
      "Bash(gcloud sql instances create:*)",
      "Bash(gcloud sql instances describe:*)",
      "Bash(gcloud sql instances list:*)",
      "Bash(gcloud sql databases create:*)",
      "Bash(gcloud sql users set-password:*)",
      "Bash(gcloud secrets create:*)",
      "Bash(gcloud secrets versions access:*)",
      "Bash(gcloud secrets add-iam-policy-binding:*)",
      "Bash(gcloud projects add-iam-policy-binding:*)",
      "Bash(gcloud builds submit:*)",
      "Bash(gcloud run jobs create:*)",
      "Bash(gcloud run jobs update:*)",
      "Bash(gcloud run jobs execute:*)",
      "Bash(gcloud run services update:*)",
      "Bash(gcloud scheduler jobs create:*)",
      "Bash(gcloud scheduler jobs describe:*)",

      // GCS bucket
      "Bash(gsutil mb:*)",
      "Bash(gsutil ls:*)",

      // BigQuery dataset + schemas
      "Bash(bq mk:*)",
      "Bash(bq ls:*)",
      "Bash(bq show:*)",

      // Generating passwords locally (openssl) + piping secrets
      "Bash(openssl rand:*)",

      // Local DB smoke test (optional)
      "Bash(cloud-sql-proxy:*)"
    ],
    "deny": [
      // Keep these explicitly denied so the agent never accidentally removes
      // production resources.
      "Bash(gcloud sql instances delete:*)",
      "Bash(gcloud secrets delete:*)",
      "Bash(gcloud run services delete:*)",
      "Bash(gcloud run jobs delete:*)",
      "Bash(bq rm:*)",
      "Bash(gsutil rm:*)"
    ]
  }
}
```

Additionally, the **human operator** running the agent must already be
logged in:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project rdf-api-project
```

The agent should NOT be allowed to change the active project (`gcloud
config set project`) — that's left to the operator on purpose.

---

## Phase 0 — Core GCP Infrastructure

### 0.1 Enable required APIs

```bash
gcloud services enable \
    sqladmin.googleapis.com \
    bigquery.googleapis.com \
    bigquerydatatransfer.googleapis.com \
    storage.googleapis.com \
    cloudscheduler.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    --project=rdf-api-project
```

### 0.2 Create the `rdf-pipeline` Cloud SQL instance

Generate a strong password and create the instance + database. The password
is piped directly into `--password` — it is never written to disk
unencrypted.

```bash
# Generate once; save into a shell variable for the next few commands
PIPELINE_DB_PASSWORD="$(openssl rand -base64 32 | tr -d '/+=' | cut -c1-32)"

gcloud sql instances create rdf-pipeline \
    --database-version=POSTGRES_16 \
    --tier=db-custom-2-4096 \
    --region=europe-central2 \
    --storage-size=50 \
    --storage-auto-increase \
    --availability-type=zonal \
    --backup-start-time=04:00 \
    --project=rdf-api-project

gcloud sql databases create pipeline \
    --instance=rdf-pipeline \
    --project=rdf-api-project

gcloud sql users set-password postgres \
    --instance=rdf-pipeline \
    --password="${PIPELINE_DB_PASSWORD}" \
    --project=rdf-api-project
```

Verify the connection name:

```bash
gcloud sql instances describe rdf-pipeline \
    --format='value(connectionName)' \
    --project=rdf-api-project
# Expected: rdf-api-project:europe-central2:rdf-pipeline
```

### 0.3 Store the pipeline DATABASE_URL in Secret Manager

```bash
CONNECTION_NAME="rdf-api-project:europe-central2:rdf-pipeline"
PIPELINE_URL="postgresql://postgres:${PIPELINE_DB_PASSWORD}@/pipeline?host=/cloudsql/${CONNECTION_NAME}"

printf '%s' "${PIPELINE_URL}" | gcloud secrets create pipeline-database-url \
    --data-file=- \
    --project=rdf-api-project

# Also store just the password (useful for debug / cloud-sql-proxy)
printf '%s' "${PIPELINE_DB_PASSWORD}" | gcloud secrets create pipeline-db-password \
    --data-file=- \
    --project=rdf-api-project

# Grant access to the Compute Engine default service account (used by
# Cloud Run and Cloud Run Jobs).
gcloud secrets add-iam-policy-binding pipeline-database-url \
    --member="serviceAccount:448201086881-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --project=rdf-api-project

gcloud secrets add-iam-policy-binding pipeline-db-password \
    --member="serviceAccount:448201086881-compute@developer.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor" \
    --project=rdf-api-project

# Clear the password from the shell
unset PIPELINE_DB_PASSWORD PIPELINE_URL
```

### 0.4 Create the GCS staging bucket

```bash
gsutil mb -l europe-central2 -p rdf-api-project gs://rdf-pipeline-staging/
gsutil ls gs://rdf-pipeline-staging/  # sanity check
```

### 0.5 Create the BigQuery dataset

```bash
bq mk --location=europe-central2 --project_id=rdf-api-project rdf_analytics
bq ls --project_id=rdf-api-project  # sanity check — expect 'rdf_analytics'
```

> BigQuery tables are created by the pipeline at first run via
> `pipeline.bq_schema.ensure_tables()`. No `bq mk --table` calls are
> required here.

### 0.6 Grant BigQuery + Storage IAM to the service account

```bash
SA="448201086881-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding rdf-api-project \
    --member="serviceAccount:${SA}" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding rdf-api-project \
    --member="serviceAccount:${SA}" \
    --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding rdf-api-project \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding rdf-api-project \
    --member="serviceAccount:${SA}" \
    --role="roles/cloudsql.client"
```

### 0.7 Verify Phase 0

```bash
gcloud sql instances list --project=rdf-api-project
# Expect: rdf-postgres RUNNABLE, rdf-pipeline RUNNABLE

bq ls --project_id=rdf-api-project
# Expect: rdf_analytics

gsutil ls gs://rdf-pipeline-staging/
# Expect: no error

gcloud secrets versions access latest \
    --secret=database-url --project=rdf-api-project > /dev/null
gcloud secrets versions access latest \
    --secret=pipeline-database-url --project=rdf-api-project > /dev/null
# Both should succeed silently
```

---

## Phase 7 — Build and deploy the Cloud Run Job

### 7.1 Build the pipeline image

From the repo root (where `Dockerfile.pipeline` lives):

```bash
gcloud builds submit \
    --tag europe-central2-docker.pkg.dev/rdf-api-project/rdf-api/pipeline-runner:latest \
    --project=rdf-api-project \
    -f Dockerfile.pipeline \
    .
```

If the Artifact Registry repository `rdf-api` does not exist yet, create it:

```bash
gcloud artifacts repositories create rdf-api \
    --repository-format=docker \
    --location=europe-central2 \
    --project=rdf-api-project
```

Then re-run the `gcloud builds submit` command.

### 7.2 Create the Cloud Run Job

```bash
gcloud run jobs create pipeline-runner \
    --image europe-central2-docker.pkg.dev/rdf-api-project/rdf-api/pipeline-runner:latest \
    --region europe-central2 \
    --cpu 2 \
    --memory 4Gi \
    --max-retries 1 \
    --task-timeout 3600 \
    --set-env-vars PIPELINE_MODE=batch,LOG_LEVEL=INFO,GCP_PROJECT_ID=rdf-api-project,BQ_DATASET=rdf_analytics,BQ_LOCATION=europe-central2,PIPELINE_GCS_BUCKET=rdf-pipeline-staging \
    --set-secrets DATABASE_URL=database-url:latest,PIPELINE_DATABASE_URL=pipeline-database-url:latest \
    --set-cloudsql-instances rdf-api-project:europe-central2:rdf-postgres,rdf-api-project:europe-central2:rdf-pipeline \
    --args="--trigger,scheduled,--engine,bigquery" \
    --project=rdf-api-project
```

To update later (after a new image build) use `gcloud run jobs update` with
the same flags, or just:

```bash
gcloud run jobs update pipeline-runner \
    --image europe-central2-docker.pkg.dev/rdf-api-project/rdf-api/pipeline-runner:latest \
    --region europe-central2 \
    --project=rdf-api-project
```

### 7.3 Seed the pipeline database (one-off, first run only)

Run the seed script inside a one-off execution of the job. The simplest way
is to override the args:

```bash
gcloud run jobs execute pipeline-runner \
    --region europe-central2 \
    --project=rdf-api-project \
    --args="--trigger,manual,--limit,1,--skip-bq"
```

Alternatively, run `scripts/seed_pipeline_db.py` locally against the
cloud-sql-proxy:

```bash
cloud-sql-proxy "rdf-api-project:europe-central2:rdf-pipeline" --port 15433 &
PIPELINE_DATABASE_URL="postgresql://postgres:$(gcloud secrets versions access latest --secret=pipeline-db-password --project=rdf-api-project)@127.0.0.1:15433/pipeline" \
python scripts/seed_pipeline_db.py
```

### 7.4 Create the Cloud Scheduler trigger (daily 02:00 Europe/Warsaw)

```bash
gcloud scheduler jobs create http pipeline-daily \
    --location europe-central2 \
    --schedule "0 2 * * *" \
    --time-zone "Europe/Warsaw" \
    --uri "https://europe-central2-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/rdf-api-project/jobs/pipeline-runner:run" \
    --http-method POST \
    --oauth-service-account-email 448201086881-compute@developer.gserviceaccount.com \
    --project=rdf-api-project
```

Verify:

```bash
gcloud scheduler jobs describe pipeline-daily \
    --location europe-central2 \
    --project=rdf-api-project
```

### 7.5 Attach `rdf-pipeline` to the existing API Cloud Run service

The API needs to read predictions / peer-stats from the new database.

```bash
gcloud run services update rdf-api \
    --region europe-central2 \
    --set-cloudsql-instances rdf-api-project:europe-central2:rdf-postgres,rdf-api-project:europe-central2:rdf-pipeline \
    --update-secrets PIPELINE_DATABASE_URL=pipeline-database-url:latest \
    --project=rdf-api-project
```

> Note: `--set-cloudsql-instances` replaces the list, so both instances
> must be passed together. Omitting `rdf-postgres` here would detach the
> existing database.

### 7.6 First manual run (smoke test)

```bash
gcloud run jobs execute pipeline-runner \
    --region europe-central2 \
    --project=rdf-api-project \
    --args="--trigger,manual,--limit,10,--engine,bigquery"
```

Check logs:

```bash
gcloud logging read \
    "resource.type=cloud_run_job AND resource.labels.job_name=pipeline-runner" \
    --limit 50 \
    --project=rdf-api-project \
    --format="table(timestamp,textPayload)"
```

---

## Rollback notes

- The pipeline database is additive — the existing `rdf-postgres` is
  untouched, so disabling the pipeline is a matter of pausing the
  scheduler:

  ```bash
  gcloud scheduler jobs pause pipeline-daily \
      --location europe-central2 \
      --project=rdf-api-project
  ```

- The API will continue serving from `rdf-postgres` even if `rdf-pipeline`
  is unreachable — `app/main.py` catches `pipeline_db.connect()` failures
  and logs them without crashing startup.

- To fully tear down (DESTRUCTIVE — not in the agent's allowlist):

  ```bash
  # Run by a human operator only
  gcloud scheduler jobs delete pipeline-daily --location europe-central2
  gcloud run jobs delete pipeline-runner --region europe-central2
  gcloud sql instances delete rdf-pipeline
  bq rm -r -d rdf-api-project:rdf_analytics
  gsutil rm -r gs://rdf-pipeline-staging
  gcloud secrets delete pipeline-database-url
  gcloud secrets delete pipeline-db-password
  ```

---

## Cost estimate (EU central2, on-demand)

| Resource | Spec | Monthly |
|---|---|---|
| Cloud SQL `rdf-pipeline` | db-custom-2-4096, 50 GB SSD | ~$70 |
| BigQuery `rdf_analytics` | On-demand | ~$5–15 |
| GCS `rdf-pipeline-staging` | Standard | ~$2–3 |
| Cloud Run Job `pipeline-runner` | 2 vCPU / 4 GiB, daily 1h cap | ~$3–5 |
| Cloud Scheduler | 1 job | Free |
| Secret Manager | 2 secrets | Free |
| **Total new spend** | | **~$80–95** |
