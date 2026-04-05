"""BigQuery table schemas for the rdf_analytics dataset.

Importing this module has NO side effects and does NOT require the
google-cloud-bigquery library at import time — `ensure_tables()` is the only
function that actually talks to BigQuery.
"""
from __future__ import annotations

from typing import Any


def _sf(name: str, type_: str, mode: str = "NULLABLE") -> dict:
    return {"name": name, "type": type_, "mode": mode}


FINANCIAL_LINE_ITEMS = [
    _sf("report_id", "STRING", "REQUIRED"),
    _sf("krs", "STRING", "REQUIRED"),
    _sf("fiscal_year", "INT64", "REQUIRED"),
    _sf("section", "STRING"),
    _sf("tag_path", "STRING", "REQUIRED"),
    _sf("label_pl", "STRING"),
    _sf("value_current", "FLOAT64"),
    _sf("value_previous", "FLOAT64"),
    _sf("schema_code", "STRING"),
    _sf("extraction_version", "INT64"),
    _sf("exported_at", "TIMESTAMP"),
]

COMPUTED_FEATURES = [
    _sf("report_id", "STRING", "REQUIRED"),
    _sf("krs", "STRING", "REQUIRED"),
    _sf("fiscal_year", "INT64", "REQUIRED"),
    _sf("feature_definition_id", "STRING", "REQUIRED"),
    _sf("value", "FLOAT64"),
    _sf("is_valid", "BOOL"),
    _sf("computation_version", "INT64"),
    _sf("computed_at", "TIMESTAMP"),
]

PREDICTIONS = [
    _sf("id", "STRING", "REQUIRED"),
    _sf("prediction_run_id", "STRING"),
    _sf("krs", "STRING", "REQUIRED"),
    _sf("report_id", "STRING", "REQUIRED"),
    _sf("fiscal_year", "INT64", "REQUIRED"),
    _sf("model_id", "STRING", "REQUIRED"),
    _sf("raw_score", "FLOAT64"),
    _sf("probability", "FLOAT64"),
    _sf("classification", "INT64"),
    _sf("risk_category", "STRING"),
    _sf("created_at", "TIMESTAMP"),
]

COMPANIES = [
    _sf("krs", "STRING", "REQUIRED"),
    _sf("nip", "STRING"),
    _sf("regon", "STRING"),
    _sf("pkd_code", "STRING"),
    _sf("incorporation_date", "DATE"),
    _sf("voivodeship", "STRING"),
]

FEATURE_DEFINITIONS = [
    _sf("id", "STRING", "REQUIRED"),
    _sf("name", "STRING"),
    _sf("category", "STRING"),
    _sf("computation_logic", "STRING"),
    _sf("formula_numerator", "STRING"),
    _sf("formula_denominator", "STRING"),
]

POPULATION_STATS = [
    _sf("pkd_code", "STRING"),
    _sf("tenure_bucket", "STRING"),
    _sf("model_id", "STRING", "REQUIRED"),
    _sf("mean_score", "FLOAT64"),
    _sf("stddev_score", "FLOAT64"),
    _sf("p25", "FLOAT64"),
    _sf("p50", "FLOAT64"),
    _sf("p75", "FLOAT64"),
    _sf("p90", "FLOAT64"),
    _sf("p95", "FLOAT64"),
    _sf("sample_size", "INT64"),
    _sf("computed_at", "TIMESTAMP"),
]

TABLES = {
    "financial_line_items": {
        "schema": FINANCIAL_LINE_ITEMS,
        "partition": "fiscal_year",
        "cluster": ["krs"],
    },
    "computed_features": {
        "schema": COMPUTED_FEATURES,
        "partition": "fiscal_year",
        "cluster": ["krs"],
    },
    "predictions": {
        "schema": PREDICTIONS,
        "partition": "fiscal_year",
        "cluster": ["krs", "model_id"],
    },
    "companies": {"schema": COMPANIES, "partition": None, "cluster": None},
    "feature_definitions": {"schema": FEATURE_DEFINITIONS, "partition": None, "cluster": None},
    "population_stats": {"schema": POPULATION_STATS, "partition": None, "cluster": None},
}


def ensure_tables(client: Any, dataset: str) -> list[str]:
    """Create any missing tables in `dataset`. Returns list of created tables."""
    from google.cloud import bigquery  # imported lazily

    created: list[str] = []
    for table_name, spec in TABLES.items():
        table_ref = f"{client.project}.{dataset}.{table_name}"
        try:
            client.get_table(table_ref)
            continue
        except Exception:
            pass
        schema = [bigquery.SchemaField(**f) for f in spec["schema"]]
        table = bigquery.Table(table_ref, schema=schema)
        if spec.get("partition"):
            table.range_partitioning = bigquery.RangePartitioning(
                field=spec["partition"],
                range_=bigquery.PartitionRange(start=1990, end=2100, interval=1),
            )
        if spec.get("cluster"):
            table.clustering_fields = spec["cluster"]
        client.create_table(table)
        created.append(table_name)
    return created
