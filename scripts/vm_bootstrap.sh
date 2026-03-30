#!/usr/bin/env bash
# Bootstrap script to run ON the GCE VM after first SSH.
# Sets up Python environment, downloads seed DB, installs systemd services.
#
# Usage (on the VM):
#   sudo bash /opt/rdf-api-project/scripts/vm_bootstrap.sh
#   sudo bash /opt/rdf-api-project/scripts/vm_bootstrap.sh --env-file /tmp/rdf.env
#   sudo bash /opt/rdf-api-project/scripts/vm_bootstrap.sh --repo-url git@github.com:user/repo.git

set -euo pipefail

REPO_DIR="/opt/rdf-api-project"
DATA_DIR="/data"
VENV_DIR="$REPO_DIR/.venv"
DATA_BUCKET="rdf-project-data"
DOC_BUCKET="rdf-project-documents"
REPO_URL=""
ENV_FILE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --repo-url) REPO_URL="$2"; shift 2;;
    --env-file) ENV_FILE="$2"; shift 2;;
    --data-bucket) DATA_BUCKET="$2"; shift 2;;
    *) echo "Unknown flag: $1"; exit 1;;
  esac
done

echo "=== RDF Batch VM Bootstrap ==="
echo ""

# --- Step 1: System dependencies ---
echo "--- Step 1: System dependencies ---"
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git > /dev/null
echo "  Done"

# --- Step 2: Data directory ---
echo "--- Step 2: Data directory ---"
mkdir -p "$DATA_DIR"
echo "  Created $DATA_DIR"

# --- Step 3: Clone or update repo ---
echo "--- Step 3: Repository ---"
if [[ -n "$REPO_URL" ]]; then
  if [[ -d "$REPO_DIR/.git" ]]; then
    echo "  Repo exists, pulling latest..."
    cd "$REPO_DIR" && git pull
  else
    echo "  Cloning $REPO_URL..."
    git clone "$REPO_URL" "$REPO_DIR"
  fi
else
  if [[ -d "$REPO_DIR/.git" ]]; then
    echo "  Repo exists at $REPO_DIR, pulling latest..."
    cd "$REPO_DIR" && git pull
  else
    echo "  WARNING: No --repo-url provided and no repo at $REPO_DIR"
    echo "  You'll need to clone manually or rsync the code."
  fi
fi

# --- Step 4: Python environment ---
echo "--- Step 4: Python environment ---"
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
  echo "  Created venv at $VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -q -r "$REPO_DIR/requirements.txt"
echo "  Dependencies installed"

# --- Step 5: Download seed DB ---
echo "--- Step 5: Seed database ---"
if [[ -f "$DATA_DIR/scraper.duckdb" ]]; then
  echo "  Database already exists at $DATA_DIR/scraper.duckdb"
  ls -lh "$DATA_DIR/scraper.duckdb"
else
  echo "  Downloading seed DB from gs://$DATA_BUCKET/seed/scraper.duckdb..."
  if gsutil cp "gs://$DATA_BUCKET/seed/scraper.duckdb" "$DATA_DIR/scraper.duckdb"; then
    echo "  Downloaded. Verifying..."
    python3 -c "
import duckdb
c = duckdb.connect('$DATA_DIR/scraper.duckdb', read_only=True)
for t in ['batch_progress', 'krs_entities', 'krs_documents']:
    try:
        r = c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()
        print(f'  {t}: {r[0]:,} rows')
    except Exception as e:
        print(f'  {t}: {e}')
c.close()
"
  else
    echo "  WARNING: No seed DB found at gs://$DATA_BUCKET/seed/scraper.duckdb"
    echo "  Upload it first: gsutil cp data/scraper.duckdb gs://$DATA_BUCKET/seed/scraper.duckdb"
  fi
fi

# --- Step 6: Environment file ---
echo "--- Step 6: Environment configuration ---"
ENV_PATH="$REPO_DIR/.env"
if [[ -n "$ENV_FILE" ]]; then
  cp "$ENV_FILE" "$ENV_PATH"
  echo "  Copied $ENV_FILE to $ENV_PATH"
elif [[ -f "$ENV_PATH" ]]; then
  echo "  .env already exists"
else
  cat > "$ENV_PATH" << 'ENVEOF'
SCRAPER_DB_PATH=/data/scraper.duckdb
BATCH_DB_PATH=/data/scraper.duckdb
STORAGE_BACKEND=gcs
STORAGE_GCS_BUCKET=rdf-project-documents
STORAGE_GCS_PREFIX=krs/
BATCH_USE_VPN=false
BATCH_WORKERS=4
BATCH_CONCURRENCY_PER_WORKER=3
BATCH_DELAY_SECONDS=2.0
RDF_BATCH_CONCURRENCY=3
RDF_BATCH_DELAY_SECONDS=2.0
RDF_BATCH_PAGE_SIZE=100
REQUEST_TIMEOUT=30
SCRAPER_DOWNLOAD_TIMEOUT=60
ENVEOF
  echo "  Created default .env at $ENV_PATH"
fi

# --- Step 7: Create worker user ---
echo "--- Step 7: Worker user ---"
if id worker &>/dev/null; then
  echo "  User 'worker' already exists"
else
  useradd -r -s /bin/false worker
  echo "  Created user 'worker'"
fi
chown -R worker:worker "$DATA_DIR"
chown -R worker:worker "$REPO_DIR"

# --- Step 8: Install systemd services ---
echo "--- Step 8: systemd services ---"
DEPLOY_DIR="$REPO_DIR/deploy"
if [[ -d "$DEPLOY_DIR" ]]; then
  cp "$DEPLOY_DIR/krs-scanner.service" /etc/systemd/system/
  cp "$DEPLOY_DIR/rdf-worker.service" /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable krs-scanner rdf-worker
  echo "  Installed and enabled krs-scanner + rdf-worker services"
else
  echo "  WARNING: $DEPLOY_DIR not found, skipping systemd setup"
fi

# --- Step 9: DB backup cron ---
echo "--- Step 9: Backup cron ---"
if [[ -f "$DEPLOY_DIR/rdf-backup.cron" ]]; then
  cp "$DEPLOY_DIR/rdf-backup.cron" /etc/cron.d/rdf-backup
  chmod 644 /etc/cron.d/rdf-backup
  echo "  Installed /etc/cron.d/rdf-backup (every 6 hours)"
else
  echo "  WARNING: $DEPLOY_DIR/rdf-backup.cron not found, skipping"
fi

# --- Summary ---
echo ""
echo "=== Bootstrap Complete ==="
echo ""
echo "To start workers:"
echo "  sudo systemctl start krs-scanner"
echo "  sudo systemctl start rdf-worker"
echo ""
echo "To check status:"
echo "  sudo systemctl status krs-scanner rdf-worker"
echo "  sudo journalctl -u krs-scanner -f"
echo "  sudo journalctl -u rdf-worker -f"
echo ""
echo "Or run the status script:"
echo "  bash $REPO_DIR/scripts/cloud_status.sh"
