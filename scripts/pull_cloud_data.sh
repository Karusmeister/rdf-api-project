#!/bin/bash
# Pull data from GCP Cloud SQL to local PostgreSQL for the KRS numbers
# that have documents in data/documents/krs/.
#
# Prerequisites:
#   - cloud-sql-proxy running on port 15432
#   - gcloud auth application-default login completed
#   - Local PostgreSQL running on port 5432
#
# Usage:
#   # Start proxy first:
#   cloud-sql-proxy "rdf-api-project:europe-central2:rdf-postgres" --port 15432 &
#
#   # Then run:
#   bash scripts/pull_cloud_data.sh
set -euo pipefail

CLOUD_HOST="127.0.0.1"
CLOUD_PORT="15432"
CLOUD_USER="postgres"
CLOUD_DB="rdf"

LOCAL_HOST="localhost"
LOCAL_PORT="5432"
LOCAL_USER="rdf"
LOCAL_PASS="rdf_dev"
LOCAL_DB="rdf"

EXPORT_DIR="/tmp/rdf-cloud-export"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/documents/krs"

# Fetch cloud DB password from GCP Secret Manager
echo "Fetching cloud DB password from GCP Secret Manager..."
CLOUD_PASS=$(gcloud secrets versions access latest --secret=cloud-db-password --project=rdf-api-project 2>/dev/null) || {
    echo "ERROR: Could not fetch cloud-db-password from Secret Manager."
    echo "Run: gcloud auth application-default login --project=rdf-api-project"
    exit 1
}

# Build KRS list from local data directory
if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: $DATA_DIR not found"
    exit 1
fi

# Only consider actual directories whose names are purely numeric (KRS numbers)
KRS_NUMBERS=$(find "$DATA_DIR" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | grep -E '^[0-9]+$' | sort)
KRS_COUNT=$(printf '%s\n' "$KRS_NUMBERS" | grep -c .)

if [ "$KRS_COUNT" -eq 0 ]; then
    echo "ERROR: No KRS directories found in $DATA_DIR"
    exit 1
fi

echo "Found $KRS_COUNT KRS numbers in $DATA_DIR"

# Build SQL IN clause
KRS_LIST=$(printf '%s\n' "$KRS_NUMBERS" | sed "s/^/'/;s/$/'/" | paste -sd ',' -)

mkdir -p "$EXPORT_DIR"

cloud_psql() {
    PGPASSWORD="$CLOUD_PASS" psql -h "$CLOUD_HOST" -p "$CLOUD_PORT" -U "$CLOUD_USER" -d "$CLOUD_DB" "$@"
}

local_psql() {
    PGPASSWORD="$LOCAL_PASS" psql -h "$LOCAL_HOST" -p "$LOCAL_PORT" -U "$LOCAL_USER" -d "$LOCAL_DB" "$@"
}

# Verify cloud connectivity
echo "Verifying cloud DB connection..."
cloud_psql -c "SELECT 1;" > /dev/null 2>&1 || {
    echo "ERROR: Cannot connect to cloud DB. Is cloud-sql-proxy running on port $CLOUD_PORT?"
    echo "Start it with: cloud-sql-proxy \"rdf-api-project:europe-central2:rdf-postgres\" --port 15432 &"
    exit 1
}
echo "Cloud DB connection OK"

export_filtered() {
    local table=$1
    local krs_col=${2:-krs}
    echo "  Exporting $table (filtered by $krs_col)..."
    cloud_psql -c "\COPY (SELECT * FROM $table WHERE $krs_col IN ($KRS_LIST)) TO '$EXPORT_DIR/${table}.csv' WITH (FORMAT csv, HEADER true)"
    local count=$(wc -l < "$EXPORT_DIR/${table}.csv")
    echo "    -> $((count - 1)) rows"
}

export_joined() {
    local table=$1
    local query=$2
    echo "  Exporting $table (joined)..."
    cloud_psql -c "\COPY ($query) TO '$EXPORT_DIR/${table}.csv' WITH (FORMAT csv, HEADER true)"
    local count=$(wc -l < "$EXPORT_DIR/${table}.csv")
    echo "    -> $((count - 1)) rows"
}

export_full() {
    local table=$1
    echo "  Exporting $table (full)..."
    cloud_psql -c "\COPY $table TO '$EXPORT_DIR/${table}.csv' WITH (FORMAT csv, HEADER true)"
    local count=$(wc -l < "$EXPORT_DIR/${table}.csv")
    echo "    -> $((count - 1)) rows"
}

import_table() {
    local table=$1
    local csv="$EXPORT_DIR/${table}.csv"
    if [ ! -f "$csv" ]; then
        echo "  SKIP $table (no CSV)"
        return
    fi
    local count=$(wc -l < "$csv")
    if [ "$count" -le 1 ]; then
        echo "  SKIP $table (0 rows)"
        return
    fi
    echo "  Importing $table..."
    local_psql -c "TRUNCATE $table CASCADE;"
    local_psql -c "\COPY $table FROM '$csv' WITH (FORMAT csv, HEADER true)"
    local imported=$(local_psql -t -c "SELECT count(*) FROM $table;")
    echo "    -> $imported rows imported"
}

echo ""
echo "=== STEP 1: Export from cloud ==="
echo ""
echo "--- KRS-filtered tables ---"
export_filtered krs_registry
export_filtered krs_documents
export_filtered krs_document_versions
export_filtered krs_entities
export_filtered krs_entity_versions
export_filtered companies
export_filtered financial_reports
export_filtered computed_features
export_filtered etl_attempts
export_filtered assessment_jobs
export_filtered user_krs_access
export_filtered bankruptcy_events
export_joined raw_financial_data "SELECT rfd.* FROM raw_financial_data rfd JOIN financial_reports fr ON rfd.report_id = fr.id WHERE fr.krs IN ($KRS_LIST)"
export_joined financial_line_items "SELECT fli.* FROM financial_line_items fli JOIN financial_reports fr ON fli.report_id = fr.id WHERE fr.krs IN ($KRS_LIST)"
export_filtered predictions

echo ""
echo "--- Global reference tables ---"
export_full feature_definitions
export_full feature_sets
export_full feature_set_members
export_full model_registry
export_full prediction_runs
export_full users
export_full verification_codes
export_full password_reset_tokens
export_full krs_sync_log
export_full krs_scan_cursor
export_full krs_scan_runs
export_full activity_log
export_full scraper_runs
export_full batch_rdf_progress

echo ""
echo "=== STEP 2: Import to local DB ==="
echo ""

echo "--- Global reference tables ---"
import_table feature_definitions
import_table feature_sets
import_table feature_set_members
import_table model_registry
import_table prediction_runs
import_table users
import_table verification_codes
import_table password_reset_tokens
import_table krs_sync_log
import_table krs_scan_cursor
import_table krs_scan_runs
import_table activity_log
import_table scraper_runs

echo ""
echo "--- KRS-filtered tables ---"
import_table krs_registry
import_table krs_entities
import_table krs_entity_versions
import_table krs_documents
import_table krs_document_versions
import_table companies
import_table financial_reports
import_table raw_financial_data
import_table financial_line_items
import_table computed_features
import_table etl_attempts
import_table assessment_jobs
import_table user_krs_access
import_table predictions
import_table bankruptcy_events
import_table batch_rdf_progress

# Cleanup
rm -rf "$EXPORT_DIR"

echo ""
echo "=== DONE ==="
echo "Local DB now has data for $KRS_COUNT KRS numbers from cloud."
