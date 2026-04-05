"""Sync derived BigQuery tables back into the pipeline PostgreSQL database.

Right now the only thing we pull back is `population_stats`, because that's
what the API serves to users. Predictions themselves are NOT pulled back
from BigQuery — PostgreSQL is the source of truth for those.
"""
from __future__ import annotations

import io
import logging
from typing import Any

from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)


def sync_population_stats_to_pg(
    client: Any,
    dataset: str,
    pipeline_conn: ConnectionWrapper,
) -> int:
    """Read BQ `population_stats` and COPY into PostgreSQL, truncating first."""
    query = f"""
    SELECT pkd_code, tenure_bucket, model_id, mean_score, stddev_score,
           p25, p50, p75, p90, p95, sample_size, computed_at
    FROM `{client.project}.{dataset}.population_stats`
    """
    rows = list(client.query(query).result())
    if not rows:
        return 0

    pipeline_conn.execute("TRUNCATE TABLE population_stats")

    buf = io.StringIO()
    for r in rows:
        vals = [
            r.pkd_code or "",
            r.tenure_bucket or "",
            r.model_id,
            r.mean_score if r.mean_score is not None else "\\N",
            r.stddev_score if r.stddev_score is not None else "\\N",
            r.p25 if r.p25 is not None else "\\N",
            r.p50 if r.p50 is not None else "\\N",
            r.p75 if r.p75 is not None else "\\N",
            r.p90 if r.p90 is not None else "\\N",
            r.p95 if r.p95 is not None else "\\N",
            r.sample_size or 0,
            r.computed_at.isoformat() if r.computed_at else "\\N",
        ]
        buf.write("\t".join(str(v) for v in vals) + "\n")
    buf.seek(0)

    cur = pipeline_conn.raw.cursor()
    cur.copy_expert(
        """
        COPY population_stats
            (pkd_code, tenure_bucket, model_id, mean_score, stddev_score,
             p25, p50, p75, p90, p95, sample_size, computed_at)
        FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '\\N')
        """,
        buf,
    )
    logger.info("population_stats_synced_to_pg",
                extra={"event": "population_stats_synced_to_pg", "rows": len(rows)})
    return len(rows)
