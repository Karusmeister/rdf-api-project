#!/bin/bash
echo "=== Services ==="
systemctl is-active krs-scanner rdf-worker

echo -e "\n=== Disk ==="
df -h /data

echo -e "\n=== DuckDB Size ==="
ls -lh /data/scraper.duckdb

echo -e "\n=== GCS Documents ==="
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil du -s gs://rdf-project-documents/ 2>/dev/null || echo "bucket not accessible"

echo -e "\n=== DB Progress ==="
cd /opt/rdf-api-project && source .venv/bin/activate 2>/dev/null
python3 -c "
import duckdb
c = duckdb.connect('/data/scraper.duckdb', read_only=True)
print('batch_progress:')
for r in c.execute('SELECT status, COUNT(*) FROM batch_progress GROUP BY status ORDER BY status').fetchall():
    print(f'  {r[0]}: {r[1]:,}')
print('rdf_progress:')
for r in c.execute('SELECT status, COUNT(*), COALESCE(SUM(documents_found),0) FROM batch_rdf_progress GROUP BY status ORDER BY status').fetchall():
    print(f'  {r[0]}: {r[1]:,} krs, {int(r[2]):,} docs')
try:
    r = c.execute('SELECT COUNT(*) FROM krs_document_versions WHERE is_downloaded=true').fetchone()
    print(f'downloaded documents: {r[0]:,}')
except: pass
c.close()
"

echo -e "\n=== Recent Logs (scanner) ==="
journalctl -u krs-scanner --no-pager -n 5 2>/dev/null || tail -5 /var/log/krs-scanner.log 2>/dev/null

echo -e "\n=== Recent Logs (rdf-worker) ==="
journalctl -u rdf-worker --no-pager -n 5 2>/dev/null || tail -5 /var/log/rdf-worker.log 2>/dev/null
