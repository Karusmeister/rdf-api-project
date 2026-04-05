"""Pipeline CLI entry point.

Examples:
    python -m pipeline --trigger scheduled
    python -m pipeline --trigger manual --limit 100 --skip-bq
    python -m pipeline --trigger manual --engine bigquery
"""
from __future__ import annotations

import argparse
import json
import sys

from pipeline.runner import run_pipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="Bankruptcy prediction pipeline")
    parser.add_argument("--trigger", default="manual",
                        choices=["scheduled", "manual", "model_deploy"])
    parser.add_argument("--limit", type=int, default=None, help="Max KRS to process")
    parser.add_argument("--skip-bq", action="store_true", help="Skip BigQuery sync")
    parser.add_argument("--engine", default="postgres",
                        choices=["postgres", "bigquery"])
    args = parser.parse_args()

    metrics = run_pipeline(
        trigger=args.trigger,
        limit=args.limit,
        skip_bq=args.skip_bq,
        engine=args.engine,
    )
    print(json.dumps({
        "run_id": metrics.run_id,
        "status": metrics.status,
        "krs_queued": metrics.krs_queued,
        "krs_processed": metrics.krs_processed,
        "krs_failed": metrics.krs_failed,
        "etl_docs": metrics.etl_docs,
        "features_computed": metrics.features_computed,
        "predictions_written": metrics.predictions_written,
        "total_seconds": metrics.total_seconds,
        "errors": metrics.errors[:10],
    }, default=str, indent=2))
    return 0 if metrics.status in ("completed", "completed_with_errors") else 1


if __name__ == "__main__":
    sys.exit(main())
