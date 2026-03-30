#!/bin/bash
set -euo pipefail

DIRECTION=""
BUCKET="rdf-project-data"

usage() {
    echo "Usage: $0 --direction <up|down> [--bucket <bucket-name>]"
    echo ""
    echo "  --direction up    Upload data/scraper.duckdb to GCS seed location"
    echo "  --direction down  Download latest cloud backup to data/scraper-cloud.duckdb"
    echo "  --bucket          GCS bucket name (default: rdf-project-data)"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --direction) DIRECTION="$2"; shift 2 ;;
        --bucket) BUCKET="$2"; shift 2 ;;
        *) usage ;;
    esac
done

if [[ -z "$DIRECTION" ]]; then
    usage
fi

print_row_counts() {
    local db_path="$1"
    echo ""
    echo "=== Row counts for $db_path ==="
    python3 -c "
import duckdb
c = duckdb.connect('$db_path', read_only=True)
tables = [r[0] for r in c.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name\").fetchall()]
for t in tables:
    try:
        count = c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        print(f'  {t}: {count:,}')
    except Exception as e:
        print(f'  {t}: error ({e})')
c.close()
"
}

case "$DIRECTION" in
    up)
        if [[ ! -f data/scraper.duckdb ]]; then
            echo "ERROR: data/scraper.duckdb not found"
            exit 1
        fi
        echo "Uploading data/scraper.duckdb to gs://$BUCKET/seed/scraper.duckdb ..."
        CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil cp data/scraper.duckdb "gs://$BUCKET/seed/scraper.duckdb"
        echo "Upload complete."
        print_row_counts "data/scraper.duckdb"
        ;;
    down)
        echo "Downloading gs://$BUCKET/backups/scraper-latest.duckdb to data/scraper-cloud.duckdb ..."
        CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil cp "gs://$BUCKET/backups/scraper-latest.duckdb" data/scraper-cloud.duckdb
        echo "Download complete. Saved as data/scraper-cloud.duckdb (local data/scraper.duckdb NOT overwritten)."
        print_row_counts "data/scraper-cloud.duckdb"
        ;;
    *)
        echo "ERROR: --direction must be 'up' or 'down'"
        usage
        ;;
esac
