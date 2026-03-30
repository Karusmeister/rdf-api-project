# PostgreSQL Migration: DuckDB → PostgreSQL

**Priority:** High
**Estimated effort:** 2-3 days (code changes + testing + cloud deploy)
**Why:** DuckDB file-level locking creates severe contention with 8 concurrent batch workers. PostgreSQL has row-level locking — the contention problem disappears entirely.

---

## Architecture Before & After

```
BEFORE (DuckDB)                           AFTER (PostgreSQL)
─────────────────                         ──────────────────
data/scraper.duckdb                       Cloud SQL (europe-central2)
  ├── 8 workers fight for file lock         ├── 8 workers write concurrently
  ├── retry-on-lock with backoff            ├── connection pooling (no retries)
  ├── KRS scanner starved (144/hr)          ├── KRS scanner: thousands/hr
  └── single file, no monitoring            └── managed backups, monitoring

Local dev                                 Local dev
  data/scraper.duckdb                       docker run postgres (port 5432)
  (stays as read-only analytics tool)       + same schema, same connection code
```

## What Changes, What Doesn't

**Changes:**
- Connection layer: `duckdb.connect(path)` → `psycopg2.connect(dsn)` with connection pool
- Parameter placeholders: `?` → `%s` (every SQL query in the project)
- Batch worker retry logic: remove DuckDB lock retry loops (no longer needed)
- A few DuckDB-specific functions: `duckdb_indexes()`, `DESCRIBE`, `INSERT OR IGNORE`
- Data type: `DOUBLE` → `DOUBLE PRECISION`

**Stays the same (already PostgreSQL-compatible):**
- All `INSERT ... ON CONFLICT` upsert patterns
- All `RETURNING` clauses
- All window functions and views (`row_number() OVER PARTITION BY`)
- All transaction control (`BEGIN`/`COMMIT`/`ROLLBACK`)
- All aggregate `FILTER` clauses
- All JSON storage patterns
- All date/time functions (`NOW()`, `current_timestamp`)

## Cost

| Resource | Spec | Monthly Cost |
|----------|------|-------------|
| Cloud SQL (dev tier) | `db-f1-micro`, 10GB SSD, europe-central2 | ~$10 |
| Cloud SQL (if you need more later) | `db-g1-small`, 20GB SSD | ~$27 |
| Automated backups | Included with Cloud SQL | $0 |
| **Local dev** | `docker run postgres` | $0 |

---

## Phase 0: Preparation (local, no code changes yet)

### Task 0.1: Set up local PostgreSQL

```bash
# Start PostgreSQL via Docker
docker run -d \
  --name rdf-postgres \
  -e POSTGRES_USER=rdf \
  -e POSTGRES_PASSWORD=rdf_dev \
  -e POSTGRES_DB=rdf \
  -p 5432:5432 \
  -v rdf-pgdata:/var/lib/postgresql/data \
  postgres:16

# Verify
psql postgresql://rdf:rdf_dev@localhost:5432/rdf -c "SELECT version();"
```

### Task 0.2: Snapshot the current DuckDB

Before any migration, create a backup of the current state.

**Locally:**
```bash
cp data/scraper.duckdb data/scraper.duckdb.pre-migration
```

**On the cloud VM:**
```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project \
  --command="sudo systemctl stop krs-scanner rdf-worker && \
    cp /data/scraper.duckdb /data/scraper.duckdb.pre-migration && \
    gsutil cp /data/scraper.duckdb gs://rdf-project-data/backups/scraper-pre-migration.duckdb"
```

### Task 0.3: Export DuckDB data to CSV/Parquet for migration

Create a script that exports all tables from DuckDB to a portable format. This becomes the migration payload.

**Create `scripts/export_duckdb.py`:**
```python
"""Export all DuckDB tables to Parquet files for PostgreSQL migration."""
import duckdb
import sys
from pathlib import Path

db_path = sys.argv[1] if len(sys.argv) > 1 else "data/scraper.duckdb"
out_dir = Path("data/export")
out_dir.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(db_path, read_only=True)

tables = [r[0] for r in conn.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
).fetchall()]

for table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    conn.execute(f"COPY {table} TO '{out_dir}/{table}.parquet' (FORMAT PARQUET)")
    print(f"  {table}: {count:,} rows → {table}.parquet")

conn.close()
print(f"\nExported {len(tables)} tables to {out_dir}/")
```

---

## Phase 1: Connection Layer Refactor (biggest change)

### Task 1.1: Add `psycopg2` dependency

```
# Add to requirements.txt
psycopg2-binary>=2.9
```

`psycopg2-binary` includes pre-built binaries — no system-level `libpq` needed. Use `psycopg2` (non-binary) in production Docker images for better performance.

### Task 1.2: Add PostgreSQL config settings

**File: `app/config.py`**

Add alongside existing settings (keep DuckDB settings for backwards compatibility during migration):

```python
# PostgreSQL
database_url: str = "postgresql://rdf:rdf_dev@localhost:5432/rdf"
db_pool_min: int = 2
db_pool_max: int = 10
```

**`.env` for local dev:**
```bash
DATABASE_URL=postgresql://rdf:rdf_dev@localhost:5432/rdf
```

**`.env` for cloud VM:**
```bash
DATABASE_URL=postgresql://rdf:PASSWORD@CLOUD_SQL_IP:5432/rdf
```

### Task 1.3: Rewrite `app/db/connection.py`

Replace the DuckDB singleton with a PostgreSQL connection pool.

**Current pattern (DuckDB):**
```python
_conn = None
def connect():
    global _conn
    _conn = duckdb.connect(settings.scraper_db_path)
def get_conn():
    return _conn
```

**New pattern (PostgreSQL):**
```python
import psycopg2
from psycopg2 import pool

_pool: pool.ThreadedConnectionPool | None = None

def connect():
    global _pool
    if _pool is not None:
        return
    _pool = pool.ThreadedConnectionPool(
        minconn=settings.db_pool_min,
        maxconn=settings.db_pool_max,
        dsn=settings.database_url,
    )

def get_conn():
    if _pool is None:
        raise RuntimeError("Database not connected")
    return _pool.getconn()

def put_conn(conn):
    """Return connection to pool."""
    if _pool is not None:
        _pool.putconn(conn)

def close():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
```

**Key difference:** Callers must now return connections to the pool after use. Use a context manager:

```python
from contextlib import contextmanager

@contextmanager
def get_db():
    conn = get_conn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        put_conn(conn)
```

### Task 1.4: Update batch worker connection pattern

**Current pattern (batch/progress.py, entity_store.py, etc.):**
```python
def _with_conn(self, fn):
    for attempt in range(self._MAX_LOCK_RETRIES):
        try:
            conn = duckdb.connect(self._db_path)
            result = fn(conn)
            conn.close()
            return result
        except duckdb.IOException:
            time.sleep(backoff)
    raise RuntimeError("Lock retries exhausted")
```

**New pattern:**
```python
def _with_conn(self, fn):
    conn = psycopg2.connect(self._dsn)
    try:
        result = fn(conn)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

No retry loop. No backoff. No lock contention. PostgreSQL handles concurrent writes natively.

**Files to update:**
- `batch/progress.py` — remove `_MAX_LOCK_RETRIES`, `_BASE_LOCK_DELAY`, retry loop
- `batch/rdf_progress.py` — same
- `batch/entity_store.py` — same
- `batch/rdf_document_store.py` — same

### Task 1.5: Replace all `?` parameter placeholders with `%s`

This is a global find-and-replace across all SQL strings. Every `conn.execute("... ? ...", [...])` becomes `conn.execute("... %s ...", [...])`.

**Files to update (every file with SQL):**
- `app/scraper/db.py`
- `app/db/prediction_db.py`
- `app/repositories/krs_repo.py`
- `app/services/etl.py`
- `app/services/feature_engine.py`
- `batch/progress.py`
- `batch/rdf_progress.py`
- `batch/entity_store.py`
- `batch/rdf_document_store.py`

**Approach:** Use regex replace `execute\(.*?\?` but review each change — don't blindly replace `?` in comments or strings.

### Task 1.6: Replace `conn.execute().fetchone()` / `.fetchall()` with cursor pattern

DuckDB's `conn.execute()` returns results directly. psycopg2 uses cursors.

**Current (DuckDB):**
```python
result = conn.execute("SELECT COUNT(*) FROM table").fetchone()
rows = conn.execute("SELECT * FROM table WHERE x = ?", [val]).fetchall()
```

**New (psycopg2):**
```python
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM table")
    result = cur.fetchone()

with conn.cursor() as cur:
    cur.execute("SELECT * FROM table WHERE x = %s", [val])
    rows = cur.fetchall()
```

**Alternative:** Create a thin wrapper to minimize code changes:
```python
class ConnectionWrapper:
    """Wraps psycopg2 connection to match DuckDB's execute().fetchone() API."""
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        cur.execute(sql, params)
        return cur  # cursor has .fetchone(), .fetchall()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()
```

This wrapper approach means most call sites need zero changes beyond the `?` → `%s` swap. **Recommended to minimize migration risk.**

---

## Phase 2: Schema Translation

### Task 2.1: Translate CREATE TABLE statements

Most DuckDB SQL is standard and works in PostgreSQL. These need changes:

| DuckDB | PostgreSQL | Where |
|--------|-----------|-------|
| `DOUBLE` | `DOUBLE PRECISION` | prediction_db.py, scraper/db.py |
| `INSERT OR IGNORE INTO` | `INSERT INTO ... ON CONFLICT DO NOTHING` | krs_repo.py (krs_scan_cursor) |
| `duckdb_indexes()` | `pg_indexes` system catalog | scraper/db.py, krs_repo.py, prediction_db.py |
| `DESCRIBE table` | `information_schema.columns` | krs_repo.py |
| `INTERVAL (? \|\| ' hours')` | `? * INTERVAL '1 hour'` | scraper/db.py |

### Task 2.2: Create PostgreSQL schema initialization

**Create `scripts/init_postgres_schema.sql`** — a single SQL file with all CREATE TABLE/VIEW/INDEX/SEQUENCE statements translated to PostgreSQL syntax.

Extract from the existing Python `_ensure_schema()` functions in:
- `app/scraper/db.py`
- `app/db/prediction_db.py`
- `app/repositories/krs_repo.py`
- `batch/progress.py`
- `batch/rdf_progress.py`
- `batch/entity_store.py`
- `batch/rdf_document_store.py`

Consolidate into one file, in dependency order. This becomes the canonical schema definition.

### Task 2.3: Update schema init functions

The existing `_ensure_schema()` functions use `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`. These work in PostgreSQL with minor adjustments:

- Replace `CREATE SEQUENCE IF NOT EXISTS seq_x START 1` with PostgreSQL syntax (identical, but verify)
- Replace `BIGINT DEFAULT nextval('seq')` with `BIGINT GENERATED ALWAYS AS IDENTITY` where appropriate
- Ensure `CREATE INDEX IF NOT EXISTS` works (PostgreSQL supports this since v9.5)

### Task 2.4: Replace DuckDB metadata queries

**`duckdb_indexes()` → `pg_indexes`:**

```python
# Before (DuckDB)
existing = {r[0] for r in conn.execute(
    "SELECT index_name FROM duckdb_indexes()"
).fetchall()}

# After (PostgreSQL)
cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
existing = {r[0] for r in cur.fetchall()}
```

**`DESCRIBE` → `information_schema`:**

```python
# Before (DuckDB)
cols = {r[0] for r in conn.execute("DESCRIBE krs_entities").fetchall()}

# After (PostgreSQL)
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'krs_entities' AND table_schema = 'public'
""")
cols = {r[0] for r in cur.fetchall()}
```

**Files:** `app/scraper/db.py`, `app/repositories/krs_repo.py`, `app/db/prediction_db.py`, `batch/rdf_document_store.py`

---

## Phase 3: Data Migration

### Task 3.1: Create migration script

**Create `scripts/migrate_to_postgres.py`:**

This script reads from the Parquet exports (Task 0.3) and loads into PostgreSQL.

```python
"""Migrate data from DuckDB Parquet exports to PostgreSQL."""
import psycopg2
import duckdb
from pathlib import Path

# Use DuckDB to read Parquet and generate INSERT statements
# This is ironic but DuckDB is excellent at reading Parquet
src = duckdb.connect()
dst = psycopg2.connect("postgresql://rdf:rdf_dev@localhost:5432/rdf")

export_dir = Path("data/export")

TABLE_ORDER = [
    # Batch progress (no dependencies)
    "batch_progress",
    "batch_rdf_progress",
    # Entity registry
    "krs_registry",
    "krs_entities",
    "krs_entity_versions",
    # Document registry
    "krs_documents",
    "krs_document_versions",
    "scraper_runs",
    # Scanner state
    "krs_scan_cursor",
    "krs_scan_runs",
    # Prediction layer (dependencies: krs_registry)
    "companies",
    "financial_reports",
    "raw_financial_data",
    "financial_line_items",
    "etl_attempts",
    # Feature layer
    "feature_definitions",
    "feature_sets",
    "feature_set_members",
    "computed_features",
    # Model layer
    "model_registry",
    "prediction_runs",
    "predictions",
    "bankruptcy_events",
    "assessment_jobs",
]

for table in TABLE_ORDER:
    parquet_path = export_dir / f"{table}.parquet"
    if not parquet_path.exists():
        print(f"  SKIP {table} (no parquet)")
        continue

    df = src.execute(f"SELECT * FROM '{parquet_path}'").fetchdf()
    if df.empty:
        print(f"  SKIP {table} (empty)")
        continue

    # Use psycopg2 COPY for fast bulk loading
    # ... (implementation details)

    print(f"  {table}: {len(df):,} rows loaded")

dst.commit()
dst.close()
```

### Task 3.2: Validate row counts

After migration, compare counts:

```python
"""Validate migration: compare DuckDB vs PostgreSQL row counts."""
import duckdb, psycopg2

duck = duckdb.connect("data/scraper.duckdb", read_only=True)
pg = psycopg2.connect("postgresql://rdf:rdf_dev@localhost:5432/rdf")
pg_cur = pg.cursor()

tables = [r[0] for r in duck.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
).fetchall()]

all_ok = True
for table in sorted(tables):
    duck_count = duck.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    pg_cur.execute(f"SELECT COUNT(*) FROM {table}")
    pg_count = pg_cur.fetchone()[0]
    status = "OK" if duck_count == pg_count else "MISMATCH"
    if status == "MISMATCH":
        all_ok = False
    print(f"  {table:40s} duck={duck_count:>10,}  pg={pg_count:>10,}  {status}")

print(f"\n{'ALL TABLES MATCH' if all_ok else 'MISMATCHES FOUND — investigate'}")
```

### Task 3.3: Reset sequences after data load

PostgreSQL sequences need to be set to the max existing ID after bulk loading:

```sql
SELECT setval('seq_krs_entity_versions', (SELECT COALESCE(MAX(version_id), 0) FROM krs_entity_versions));
SELECT setval('seq_krs_document_versions', (SELECT COALESCE(MAX(version_id), 0) FROM krs_document_versions));
-- ... for all sequences
```

---

## Phase 4: Local Validation

### Task 4.1: Run all tests against PostgreSQL

```bash
# Set env to use local PostgreSQL
export DATABASE_URL=postgresql://rdf:rdf_dev@localhost:5432/rdf

# Run full test suite
pytest tests/ -v

# Run by category to isolate failures
pytest tests/db/ -v          # Schema, CRUD, versioning
pytest tests/batch/ -v       # Worker logic
pytest tests/api/ -v         # FastAPI endpoints
pytest tests/services/ -v    # ETL, feature engine
pytest tests/krs/ -v         # KRS adapter, sync
```

**Expected failures:** Tests that create in-memory DuckDB databases for isolation (e.g., `duckdb.connect(":memory:")` or temp file paths). These need to use a test PostgreSQL database instead.

### Task 4.2: Update test fixtures

**Current pattern (many tests):**
```python
@pytest.fixture
def db():
    conn = duckdb.connect(":memory:")
    # or
    conn = duckdb.connect(tmp_path / "test.duckdb")
    ensure_schema(conn)
    yield conn
    conn.close()
```

**New pattern:**
```python
@pytest.fixture
def db():
    conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    ensure_schema(conn)
    yield conn
    conn.rollback()  # rollback to reset state between tests
    conn.close()
```

Or use a test database per test run:
```python
@pytest.fixture(scope="session")
def pg_db():
    """Create a fresh test database, drop on teardown."""
    admin = psycopg2.connect("postgresql://rdf:rdf_dev@localhost:5432/postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS rdf_test")
    admin.cursor().execute("CREATE DATABASE rdf_test")
    admin.close()

    conn = psycopg2.connect("postgresql://rdf:rdf_dev@localhost:5432/rdf_test")
    init_all_schemas(conn)
    yield conn
    conn.close()

    admin = psycopg2.connect("postgresql://rdf:rdf_dev@localhost:5432/postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE rdf_test")
    admin.close()
```

### Task 4.3: Run batch workers locally against PostgreSQL

```bash
# Start local PostgreSQL + load seed data
export DATABASE_URL=postgresql://rdf:rdf_dev@localhost:5432/rdf
python scripts/migrate_to_postgres.py

# Run a short batch scan (10 KRS numbers) to validate
python -m batch.runner --start 1 --workers 2 --concurrency 1 --delay 1.0

# Run RDF discovery for a few entities
python -m batch.rdf_runner --workers 2 --concurrency 1 --delay 1.0
```

Verify: no lock errors, no retries, workers run smoothly.

### Task 4.4: Run FastAPI locally against PostgreSQL

```bash
export DATABASE_URL=postgresql://rdf:rdf_dev@localhost:5432/rdf
uvicorn app.main:app --reload --port 8000

# Test endpoints
curl http://localhost:8000/health
curl http://localhost:8000/api/scraper/status
curl -X POST http://localhost:8000/api/podmiot/lookup -H 'Content-Type: application/json' -d '{"krs":"694720"}'
```

---

## Phase 5: Cloud SQL Setup

### Task 5.1: Create Cloud SQL instance

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud sql instances create rdf-postgres \
  --project=rdf-api-project \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=europe-central2 \
  --storage-size=10GB \
  --storage-type=SSD \
  --storage-auto-increase \
  --backup-start-time=03:00 \
  --availability-type=zonal \
  --authorized-networks=0.0.0.0/0
```

Note: `--authorized-networks=0.0.0.0/0` allows connections from anywhere (including the VM and your local machine). For production, restrict to the VM's IP and your dev IP. Or use Cloud SQL Auth Proxy.

### Task 5.2: Create database and user

```bash
# Set password
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud sql users set-password postgres \
  --instance=rdf-postgres \
  --project=rdf-api-project \
  --password=CHOOSE_A_STRONG_PASSWORD

# Create application database
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud sql databases create rdf \
  --instance=rdf-postgres \
  --project=rdf-api-project

# Get the instance IP
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud sql instances describe rdf-postgres \
  --project=rdf-api-project \
  --format="value(ipAddresses[0].ipAddress)"
```

### Task 5.3: Initialize schema on Cloud SQL

```bash
# Get the Cloud SQL IP
CLOUD_SQL_IP=$(CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud sql instances describe rdf-postgres \
  --project=rdf-api-project --format="value(ipAddresses[0].ipAddress)")

# Run schema init
psql postgresql://postgres:PASSWORD@${CLOUD_SQL_IP}:5432/rdf -f scripts/init_postgres_schema.sql
```

### Task 5.4: Migrate data to Cloud SQL

**Option A: From local machine (if you have the latest data locally)**
```bash
python scripts/export_duckdb.py data/scraper.duckdb
DATABASE_URL=postgresql://postgres:PASSWORD@${CLOUD_SQL_IP}:5432/rdf python scripts/migrate_to_postgres.py
python scripts/validate_migration.py
```

**Option B: From the VM (has the latest data)**
```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project --command="
    cd /opt/rdf-api-project && source .venv/bin/activate
    python scripts/export_duckdb.py /data/scraper.duckdb
    DATABASE_URL=postgresql://postgres:PASSWORD@CLOUD_SQL_IP:5432/rdf python scripts/migrate_to_postgres.py
    DATABASE_URL=postgresql://postgres:PASSWORD@CLOUD_SQL_IP:5432/rdf python scripts/validate_migration.py
  "
```

### Task 5.5: Validate row counts on Cloud SQL

Same as Task 3.2, but against the cloud database.

---

## Phase 6: Cloud Deployment (cutover)

### Task 6.1: Stop workers

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project \
  --command="sudo systemctl stop krs-scanner rdf-worker"
```

### Task 6.2: Deploy updated code to VM

```bash
# Package and upload (same as before)
tar czf /tmp/rdf-project.tar.gz \
  --exclude='data' --exclude='.venv' --exclude='__pycache__' --exclude='.git' \
  --exclude='.env' --exclude='.envrc' --exclude='.claude' --exclude='.pytest_cache' \
  --exclude='*.pyc' -C /Users/piotrkraus/piotr rdf-api-project/

CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute scp \
  --zone=europe-central2-a --project=rdf-api-project \
  /tmp/rdf-project.tar.gz rdf-batch-vm:/tmp/rdf-project.tar.gz

CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project --command="
    cd /opt && sudo tar xzf /tmp/rdf-project.tar.gz --strip-components=0
    sudo chown -R worker:worker /opt/rdf-api-project
    cd /opt/rdf-api-project && sudo -u worker bash -c 'source .venv/bin/activate && pip install -q -r requirements.txt'
  "
```

### Task 6.3: Update VM `.env`

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project --command="
    # Add PostgreSQL connection string
    echo 'DATABASE_URL=postgresql://postgres:PASSWORD@CLOUD_SQL_IP:5432/rdf' >> /opt/rdf-api-project/.env
  "
```

### Task 6.4: Start workers

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project --command="
    sudo systemctl start rdf-worker krs-scanner
    sleep 10
    sudo systemctl status rdf-worker krs-scanner --no-pager
    sudo journalctl -u rdf-worker --no-pager -n 10
  "
```

### Task 6.5: Verify rates improved

Wait 30 minutes, then check:

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project --command='
    cp /data/scraper.duckdb /tmp/query.duckdb 2>/dev/null  # old DB, for comparison
    cd /opt/rdf-api-project && source .venv/bin/activate && python3 -c "
import psycopg2, os
conn = psycopg2.connect(os.environ[\"DATABASE_URL\"])
cur = conn.cursor()

cur.execute(\"SELECT COUNT(*) FROM batch_progress WHERE processed_at > now() - interval '30 minutes'\")
print(f\"KRS scanned last 30min: {cur.fetchone()[0]:,}\")

cur.execute(\"SELECT COUNT(*), COALESCE(SUM(documents_found),0) FROM batch_rdf_progress WHERE processed_at > now() - interval '30 minutes'\")
r = cur.fetchone()
print(f\"KRS discovered last 30min: {r[0]:,} ({int(r[1]):,} docs)\")

cur.execute(\"SELECT COUNT(*) FROM krs_document_versions WHERE is_downloaded=true AND observed_at > now() - interval '30 minutes'\")
print(f\"Docs downloaded last 30min: {cur.fetchone()[0]:,}\")

conn.close()
"
  '
```

**Expected:** KRS scan rate should jump from ~144/hr to thousands/hr. Document downloads from ~1,300/hr to ~3,000+/hr.

---

## Phase 7: Cleanup

### Task 7.1: Update backup cron

Replace the DuckDB gsutil backup with a Cloud SQL export (or rely on automated backups):

```bash
# Remove old cron
sudo rm /etc/cron.d/rdf-backup

# Cloud SQL automated backups are already enabled (--backup-start-time=03:00)
# No cron needed for database backups
```

### Task 7.2: Update monitoring script

Update `scripts/cloud_status.sh` to query PostgreSQL instead of DuckDB.

### Task 7.3: Update docs

- `CLAUDE.md` — replace DuckDB references with PostgreSQL
- `README.md` — update setup instructions, config table
- `docs/GCP_VS_LOCAL_SETUP.md` — update cloud config section
- `docs/GCP_AGENT_TASKS.md` — update task descriptions

### Task 7.4: Keep DuckDB for local analytics (optional)

DuckDB remains useful as a **read-only analytics tool**. You can export data from PostgreSQL to Parquet/CSV and analyze locally:

```bash
# Export from Cloud SQL to local Parquet (via DuckDB)
python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"INSTALL postgres; LOAD postgres;\")
conn.execute(\"ATTACH 'postgresql://postgres:PASSWORD@CLOUD_SQL_IP:5432/rdf' AS pg (TYPE postgres)\")
conn.execute(\"COPY pg.financial_line_items TO 'data/analytics/line_items.parquet' (FORMAT PARQUET)\")
"
```

### Task 7.5: Remove DuckDB file from VM

Once PostgreSQL is validated and running for a few days:

```bash
# Keep the pre-migration backup on GCS (already uploaded in Task 0.2)
# Remove the local DuckDB file
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm \
  --zone=europe-central2-a --project=rdf-api-project \
  --command="rm /data/scraper.duckdb"
```

---

## Rollback Plan

If PostgreSQL migration fails or performance is worse:

1. **Stop workers** on the VM
2. **Revert code:** `git checkout main` (or the pre-migration commit)
3. **Restore DuckDB:** `gsutil cp gs://rdf-project-data/backups/scraper-pre-migration.duckdb /data/scraper.duckdb`
4. **Restart workers** with old code

The DuckDB file is the complete source of truth. As long as we have the pre-migration backup, rollback is a 5-minute operation.

---

## Files to Modify (complete list)

### Connection layer (Phase 1)
- `app/config.py` — add `database_url`, `db_pool_min`, `db_pool_max`
- `app/db/connection.py` — rewrite: psycopg2 pool + context manager
- `app/main.py` — update lifespan to use new connection init
- `batch/progress.py` — remove retry loop, use psycopg2
- `batch/rdf_progress.py` — same
- `batch/entity_store.py` — same
- `batch/rdf_document_store.py` — same
- `batch/runner.py` — update DB init
- `batch/rdf_runner.py` — update DB init

### SQL changes (Phase 1-2)
- `app/scraper/db.py` — `?`→`%s`, metadata queries, INTERVAL syntax, DOUBLE type
- `app/db/prediction_db.py` — `?`→`%s`, metadata queries, DOUBLE type
- `app/repositories/krs_repo.py` — `?`→`%s`, INSERT OR IGNORE, DESCRIBE, metadata
- `app/services/etl.py` — `?`→`%s`, cursor pattern
- `app/services/feature_engine.py` — `?`→`%s`, cursor pattern

### New scripts (Phase 0, 3)
- `scripts/export_duckdb.py` — export to Parquet
- `scripts/init_postgres_schema.sql` — canonical PostgreSQL schema
- `scripts/migrate_to_postgres.py` — load data into PostgreSQL
- `scripts/validate_migration.py` — compare row counts

### Tests (Phase 4)
- `conftest.py` — add PostgreSQL test fixtures
- `tests/db/` — update all fixtures from DuckDB to PostgreSQL
- `tests/batch/` — update worker tests
- `tests/services/` — update ETL/feature tests

### Docs (Phase 7)
- `CLAUDE.md`
- `README.md`
- `docs/GCP_VS_LOCAL_SETUP.md`
- `docs/GCP_AGENT_TASKS.md`
- `todo/db-split-refactor.md` — mark as superseded

---

## Timeline

| Phase | What | Effort | Dependency |
|-------|------|--------|------------|
| Phase 0 | Prep: local PG, backup, export | 1 hour | None |
| Phase 1 | Connection layer + `?`→`%s` | 4-6 hours | Phase 0 |
| Phase 2 | Schema translation | 2 hours | Phase 1 |
| Phase 3 | Data migration script | 2 hours | Phase 2 |
| Phase 4 | Local validation (tests + manual) | 2-3 hours | Phase 3 |
| Phase 5 | Cloud SQL setup + data load | 1 hour | Phase 4 |
| Phase 6 | Cloud cutover + verify rates | 1 hour | Phase 5 |
| Phase 7 | Cleanup + docs | 1-2 hours | Phase 6 |
| **Total** | | **~2 days** | |
