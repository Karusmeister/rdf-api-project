"""BigQuery-side computations: population stats and (optionally) features.

Feature computation in BQ is not the reference path — PostgreSQL
feature_compute is the source of truth. The BQ version exists so that
ad-hoc analytics queries can reference `rdf_analytics.computed_features`
across millions of rows efficiently.
"""
from __future__ import annotations

import logging
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)


def compute_population_stats(client: Any, dataset: str) -> int:
    """Compute mean/stddev/percentiles per (pkd_code, tenure_bucket, model_id).

    Tenure bucket is derived from (fiscal_year - year(incorporation_date)):
        0-2 = early, 3-7 = growth, 8-15 = mature, 16+ = established
    """
    query = f"""
    CREATE OR REPLACE TABLE `{client.project}.{dataset}.population_stats` AS
    WITH labeled AS (
        SELECT
            p.model_id,
            c.pkd_code,
            CASE
                WHEN c.incorporation_date IS NULL THEN 'unknown'
                WHEN DATE_DIFF(CURRENT_DATE(), c.incorporation_date, YEAR) <= 2 THEN 'early'
                WHEN DATE_DIFF(CURRENT_DATE(), c.incorporation_date, YEAR) <= 7 THEN 'growth'
                WHEN DATE_DIFF(CURRENT_DATE(), c.incorporation_date, YEAR) <= 15 THEN 'mature'
                ELSE 'established'
            END AS tenure_bucket,
            p.raw_score
        FROM `{client.project}.{dataset}.predictions` p
        LEFT JOIN `{client.project}.{dataset}.companies` c USING (krs)
        WHERE p.raw_score IS NOT NULL
    )
    SELECT
        COALESCE(pkd_code, 'UNKNOWN') AS pkd_code,
        tenure_bucket,
        model_id,
        AVG(raw_score) AS mean_score,
        STDDEV(raw_score) AS stddev_score,
        APPROX_QUANTILES(raw_score, 100)[OFFSET(25)] AS p25,
        APPROX_QUANTILES(raw_score, 100)[OFFSET(50)] AS p50,
        APPROX_QUANTILES(raw_score, 100)[OFFSET(75)] AS p75,
        APPROX_QUANTILES(raw_score, 100)[OFFSET(90)] AS p90,
        APPROX_QUANTILES(raw_score, 100)[OFFSET(95)] AS p95,
        COUNT(*) AS sample_size,
        CURRENT_TIMESTAMP() AS computed_at
    FROM labeled
    GROUP BY pkd_code, tenure_bucket, model_id
    """
    job = client.query(query)
    job.result()
    # Row count
    count_job = client.query(
        f"SELECT COUNT(*) AS c FROM `{client.project}.{dataset}.population_stats`"
    )
    row = list(count_job.result())[0]
    n = int(row.c)
    logger.info("bq_population_stats_computed",
                extra={"event": "bq_population_stats_computed", "rows": n})
    return n


def export_training_data(
    client: Any,
    dataset: str,
    feature_set_id: str,
    gcs_path: str,
) -> int:
    """Export wide-format training data (one row per report) to GCS Parquet."""
    export_query = f"""
    EXPORT DATA OPTIONS(
      uri='{gcs_path}/*.parquet',
      format='PARQUET',
      overwrite=true
    ) AS
    SELECT
        fr.report_id,
        fr.krs,
        fr.fiscal_year,
        c.pkd_code,
        MAX(IF(cf.feature_definition_id='x1_maczynska', cf.value, NULL)) AS x1_maczynska,
        MAX(IF(cf.feature_definition_id='x2_maczynska', cf.value, NULL)) AS x2_maczynska,
        MAX(IF(cf.feature_definition_id='x3_maczynska', cf.value, NULL)) AS x3_maczynska,
        MAX(IF(cf.feature_definition_id='x4_maczynska', cf.value, NULL)) AS x4_maczynska,
        MAX(IF(cf.feature_definition_id='x5_maczynska', cf.value, NULL)) AS x5_maczynska,
        MAX(IF(cf.feature_definition_id='x6_maczynska', cf.value, NULL)) AS x6_maczynska
    FROM `{client.project}.{dataset}.computed_features` cf
    JOIN (
        SELECT DISTINCT report_id, krs, fiscal_year FROM `{client.project}.{dataset}.computed_features`
    ) fr USING (report_id, krs, fiscal_year)
    LEFT JOIN `{client.project}.{dataset}.companies` c USING (krs)
    GROUP BY fr.report_id, fr.krs, fr.fiscal_year, c.pkd_code
    """
    job = client.query(export_query)
    job.result()
    logger.info("bq_training_exported",
                extra={"event": "bq_training_exported",
                       "feature_set": feature_set_id, "gcs_path": gcs_path})
    return 1
