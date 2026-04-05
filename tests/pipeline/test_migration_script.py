"""Tests for scripts/migrate_predictions_to_pipeline.py.

Verifies the copy logic moves rows from the scraper (source) database to the
pipeline (destination) database, is idempotent under re-run, and respects
--dry-run.

We load the script dynamically via importlib because `scripts/` is not a
Python package.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import psycopg2
import pytest

from app.db import pipeline_db


def _load_migration_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "migrate_predictions_to_pipeline.py"
    spec = importlib.util.spec_from_file_location("_migrate_predictions", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _seed_source_rows(src_conn):
    """Seed a company + report + prediction in the scraper DB so the
    migration has something to copy."""
    cur = src_conn.cursor()
    cur.execute(
        """
        INSERT INTO companies (krs, nip, pkd_code)
        VALUES ('0000011111', '1111111111', '62.01.Z')
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO financial_reports
            (id, logical_key, report_version, krs, data_source_id, report_type,
             fiscal_year, period_start, period_end, ingestion_status)
        VALUES ('rpt-mig', '0000011111|KRS|annual|2023|2023-12-31', 1,
                '0000011111', 'KRS', 'annual', 2023, '2023-01-01', '2023-12-31',
                'completed')
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO financial_line_items
            (report_id, section, tag_path, extraction_version, value_current, currency)
        VALUES ('rpt-mig', 'Bilans', 'Aktywa', 1, 1000.0, 'PLN')
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO feature_definitions
            (id, name, category, computation_logic, is_active)
        VALUES ('roa', 'ROA', 'profitability', 'ratio', TRUE)
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO feature_sets (id, name, is_active)
        VALUES ('basic_1', 'basic', TRUE)
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO feature_set_members (feature_set_id, feature_definition_id, ordinal)
        VALUES ('basic_1', 'roa', 1)
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, computation_version)
        VALUES ('rpt-mig', 'roa', '0000011111', 2023, 0.05, TRUE, 1)
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, is_active, is_baseline)
        VALUES ('mig-model', 'mig', 'discriminant', '1.0', 'basic_1', TRUE, TRUE)
        ON CONFLICT (name, version) DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO prediction_runs (id, model_id, status)
        VALUES ('mig-pr', 'mig-model', 'completed')
        ON CONFLICT DO NOTHING
        """
    )
    cur.execute(
        """
        INSERT INTO predictions
            (id, prediction_run_id, krs, report_id, raw_score,
             classification, risk_category)
        VALUES ('mig-p', 'mig-pr', '0000011111', 'rpt-mig', 2.5, 0, 'low')
        ON CONFLICT DO NOTHING
        """
    )


def test_migrations_list_covers_expected_tables():
    """The MIGRATIONS list must include the core prediction tables."""
    mod = _load_migration_module()
    table_names = {t for t, _ in mod.MIGRATIONS}
    for required in (
        "companies", "financial_reports", "financial_line_items",
        "computed_features", "model_registry", "prediction_runs", "predictions",
        "feature_definitions", "feature_sets", "feature_set_members",
    ):
        assert required in table_names, f"{required} missing from MIGRATIONS"


def test_copy_table_moves_rows_to_pipeline(dual_db):
    """Run _copy_table for a representative table and verify rows land on the
    pipeline DB."""
    mod = _load_migration_module()

    src = psycopg2.connect(dual_db["pg_dsn"])
    src.autocommit = True
    _seed_source_rows(src)

    dst = psycopg2.connect(dual_db["pipeline_dsn"])
    dst.autocommit = True

    # Copy companies
    cols = next(c for t, c in mod.MIGRATIONS if t == "companies")
    n = mod._copy_table(src, dst, "companies", cols, batch_size=100, dry_run=False)
    assert n >= 1

    # Verify on pipeline side
    pconn = pipeline_db.get_conn()
    row = pconn.execute(
        "SELECT krs, nip, pkd_code FROM companies WHERE krs = '0000011111'"
    ).fetchone()
    assert row is not None
    assert row[1] == "1111111111"
    assert row[2] == "62.01.Z"

    src.close()
    dst.close()


def test_copy_table_is_idempotent(dual_db):
    """Running _copy_table twice must not error and must not duplicate rows
    (ON CONFLICT DO NOTHING)."""
    mod = _load_migration_module()

    src = psycopg2.connect(dual_db["pg_dsn"])
    src.autocommit = True
    _seed_source_rows(src)

    dst = psycopg2.connect(dual_db["pipeline_dsn"])
    dst.autocommit = True

    cols = next(c for t, c in mod.MIGRATIONS if t == "financial_reports")
    mod._copy_table(src, dst, "financial_reports", cols, batch_size=100, dry_run=False)
    mod._copy_table(src, dst, "financial_reports", cols, batch_size=100, dry_run=False)

    pconn = pipeline_db.get_conn()
    n = pconn.execute(
        "SELECT count(*) FROM financial_reports WHERE id = 'rpt-mig'"
    ).fetchone()[0]
    assert n == 1

    src.close()
    dst.close()


def test_copy_table_dry_run_does_not_write(dual_db):
    mod = _load_migration_module()

    src = psycopg2.connect(dual_db["pg_dsn"])
    src.autocommit = True
    _seed_source_rows(src)

    dst = psycopg2.connect(dual_db["pipeline_dsn"])
    dst.autocommit = True

    cols = next(c for t, c in mod.MIGRATIONS if t == "predictions")
    counted = mod._copy_table(
        src, dst, "predictions", cols, batch_size=100, dry_run=True
    )
    # Dry-run still counts rows read from source
    assert counted >= 1

    pconn = pipeline_db.get_conn()
    n = pconn.execute(
        "SELECT count(*) FROM predictions WHERE id = 'mig-p'"
    ).fetchone()[0]
    assert n == 0

    src.close()
    dst.close()


def test_copy_full_chain_preserves_row_counts(dual_db):
    """Copy every table in MIGRATIONS order and verify the destination row
    counts equal the source counts for each table."""
    mod = _load_migration_module()

    src = psycopg2.connect(dual_db["pg_dsn"])
    src.autocommit = True
    _seed_source_rows(src)

    dst = psycopg2.connect(dual_db["pipeline_dsn"])
    dst.autocommit = True

    for table, cols in mod.MIGRATIONS:
        mod._copy_table(src, dst, table, cols, batch_size=100, dry_run=False)

    pconn = pipeline_db.get_conn()
    src_cur = src.cursor()
    for table, _ in mod.MIGRATIONS:
        src_cur.execute(f"SELECT count(*) FROM {table}")
        src_count = src_cur.fetchone()[0]
        dst_count = pconn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        assert dst_count == src_count, (
            f"{table}: src={src_count} dst={dst_count}"
        )

    src.close()
    dst.close()
