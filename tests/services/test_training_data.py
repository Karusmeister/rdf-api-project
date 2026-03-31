"""Tests for the training data assembly pipeline."""

import uuid
from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, training_data
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS


@pytest.fixture
def isolated_db(pg_dsn, clean_pg):
    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "database_url", pg_dsn):
        scraper_db.connect()
        prediction_db.connect()
        yield
        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


@pytest.fixture
def seeded_db(isolated_db):
    for fdef in FEATURE_DEFINITIONS:
        prediction_db.upsert_feature_definition(
            feature_id=fdef["id"],
            name=fdef["name"],
            description=fdef.get("description"),
            category=fdef.get("category"),
            formula_description=fdef.get("formula_description"),
            formula_numerator=fdef.get("formula_numerator"),
            formula_denominator=fdef.get("formula_denominator"),
            required_tags=fdef.get("required_tags"),
            computation_logic=fdef.get("computation_logic", "ratio"),
        )
    for set_id, info in FEATURE_SETS.items():
        prediction_db.upsert_feature_set(set_id, info["name"], info.get("description"))
        for ordinal, member_id in enumerate(info["members"], start=1):
            prediction_db.add_feature_set_member(set_id, member_id, ordinal)
    return isolated_db


def _create_company_with_report(krs, report_id, fiscal_year, tag_values):
    """Helper: create company + report + line items + compute features."""
    prediction_db.upsert_company(krs=krs, pkd_code="62.01.Z")
    prediction_db.create_financial_report(
        report_id=report_id, krs=krs, fiscal_year=fiscal_year,
        period_start=f"{fiscal_year}-01-01", period_end=f"{fiscal_year}-12-31",
    )
    prediction_db.update_report_status(report_id, "completed")

    section_map = {
        "Aktywa": "Bilans", "Aktywa_B": "Bilans", "Aktywa_B_I": "Bilans",
        "Pasywa_A": "Bilans", "Pasywa_B": "Bilans", "Pasywa_B_III": "Bilans",
    }
    items = []
    for tag, value in tag_values.items():
        section = section_map.get(tag, "CF" if tag.startswith("CF.") else "RZiS")
        items.append({
            "report_id": report_id, "section": section,
            "tag_path": tag, "value_current": value,
        })
    prediction_db.batch_insert_line_items(items)
    feature_engine.compute_features_for_report(report_id, feature_set_id="maczynska_6")


STANDARD_TAGS = {
    "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
    "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
}


class TestBuildTrainingDataset:
    def test_basic_pivot(self, seeded_db):
        _create_company_with_report("0000400001", "train-rpt-1", 2023, STANDARD_TAGS)

        df = training_data.build_training_dataset("maczynska_6")
        assert len(df) == 1
        assert "x1_maczynska" in df.columns
        assert "x2_maczynska" in df.columns
        assert "krs" in df.columns
        assert df.iloc[0]["krs"] == "0000400001"

    def test_multiple_companies(self, seeded_db):
        _create_company_with_report("0000400002", "train-rpt-2", 2022, STANDARD_TAGS)
        _create_company_with_report("0000400003", "train-rpt-3", 2023, STANDARD_TAGS)

        df = training_data.build_training_dataset("maczynska_6")
        assert len(df) == 2

    def test_year_filter(self, seeded_db):
        _create_company_with_report("0000400004", "train-rpt-4", 2020, STANDARD_TAGS)
        _create_company_with_report("0000400005", "train-rpt-5", 2023, STANDARD_TAGS)

        df = training_data.build_training_dataset("maczynska_6", min_year=2022)
        assert len(df) == 1
        assert df.iloc[0]["fiscal_year"] == 2023

        df2 = training_data.build_training_dataset("maczynska_6", max_year=2021)
        assert len(df2) == 1
        assert df2.iloc[0]["fiscal_year"] == 2020

    def test_empty_dataset(self, seeded_db):
        df = training_data.build_training_dataset("maczynska_6")
        assert len(df) == 0

    def test_invalid_feature_set(self, seeded_db):
        with pytest.raises(ValueError, match="not found or empty"):
            training_data.build_training_dataset("nonexistent_set")

    def test_company_metadata_joined(self, seeded_db):
        _create_company_with_report("0000400006", "train-rpt-6", 2023, STANDARD_TAGS)

        df = training_data.build_training_dataset("maczynska_6")
        assert "pkd_code" in df.columns
        assert df.iloc[0]["pkd_code"] == "62.01.Z"

    def test_bankruptcy_label_default_zero(self, seeded_db):
        _create_company_with_report("0000400007", "train-rpt-7", 2023, STANDARD_TAGS)

        df = training_data.build_training_dataset("maczynska_6")
        assert "is_bankrupt_within_2y" in df.columns
        assert df.iloc[0]["is_bankrupt_within_2y"] == 0

    def test_bankruptcy_label_positive(self, seeded_db):
        _create_company_with_report("0000400008", "train-rpt-8", 2022, STANDARD_TAGS)

        # Insert bankruptcy event within 2 years of 2022-12-31
        prediction_db.insert_bankruptcy_event(
            event_id=str(uuid.uuid4()),
            krs="0000400008",
            event_type="bankruptcy",
            event_date="2024-06-15",
        )

        df = training_data.build_training_dataset("maczynska_6")
        assert df.iloc[0]["is_bankrupt_within_2y"] == 1

    def test_bankruptcy_outside_horizon(self, seeded_db):
        _create_company_with_report("0000400009", "train-rpt-9", 2020, STANDARD_TAGS)

        # Bankruptcy 3 years later — outside 2-year horizon
        prediction_db.insert_bankruptcy_event(
            event_id=str(uuid.uuid4()),
            krs="0000400009",
            event_type="bankruptcy",
            event_date="2024-01-15",
        )

        df = training_data.build_training_dataset("maczynska_6")
        assert df.iloc[0]["is_bankrupt_within_2y"] == 0

    def test_uses_latest_report_versions_only(self, seeded_db):
        # Original report (v1)
        old_tags = {
            "RZiS.I": 100000, "CF.A_II_1": 10000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        }
        _create_company_with_report("0000400015", "train-rpt-old", 2023, old_tags)

        # Correction report (v2, same logical period, different values)
        new_tags = {
            "RZiS.I": 300000, "CF.A_II_1": 10000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        }
        _create_company_with_report("0000400015", "train-rpt-new", 2023, new_tags)

        df = training_data.build_training_dataset("maczynska_6")

        assert len(df) == 1
        assert df.iloc[0]["report_id"] == "train-rpt-new"
        assert df.iloc[0]["x3_maczynska"] == pytest.approx(0.3, abs=1e-6)

    def test_failed_correction_falls_back_to_last_successful(self, seeded_db):
        """If the latest correction is pending/failed, use the last completed report."""
        # Successful v1
        _create_company_with_report("0000400016", "train-rpt-ok", 2023, STANDARD_TAGS)

        # Failed correction v2 — same company, same period, but ETL failed
        prediction_db.create_financial_report(
            report_id="train-rpt-fail", krs="0000400016", fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status("train-rpt-fail", "failed")

        df = training_data.build_training_dataset("maczynska_6")

        # Should still include the v1 completed report, not the failed v2
        assert len(df) == 1
        assert df.iloc[0]["report_id"] == "train-rpt-ok"


class TestExportToCsv:
    def test_export(self, seeded_db, tmp_path):
        _create_company_with_report("0000400010", "train-rpt-10", 2023, STANDARD_TAGS)

        path = str(tmp_path / "training.csv")
        count = training_data.export_to_csv("maczynska_6", path)
        assert count == 1

        import pandas as pd
        df = pd.read_csv(path)
        assert len(df) == 1
        assert "x1_maczynska" in df.columns


class TestGetDatasetStats:
    def test_empty_stats(self, seeded_db):
        stats = training_data.get_dataset_stats("maczynska_6")
        assert stats["row_count"] == 0
        assert stats["single_year_companies"] == 0
        assert stats["unique_companies"] == 0
        assert stats["year_range"] == []

    def test_stats_with_data(self, seeded_db):
        _create_company_with_report("0000400011", "stats-rpt-1", 2022, STANDARD_TAGS)
        _create_company_with_report("0000400012", "stats-rpt-2", 2023, STANDARD_TAGS)

        stats = training_data.get_dataset_stats("maczynska_6")
        assert stats["row_count"] == 2
        assert stats["feature_count"] == 6
        assert stats["unique_companies"] == 2
        assert "0" in stats["class_balance"]
        assert stats["single_year_companies"] == 2

    def test_missing_pct(self, seeded_db):
        # Create a report with only partial features
        prediction_db.upsert_company(krs="0000400013")
        prediction_db.create_financial_report(
            report_id="partial-rpt", krs="0000400013", fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status("partial-rpt", "completed")
        prediction_db.batch_insert_line_items([
            {"report_id": "partial-rpt", "section": "RZiS", "tag_path": "RZiS.I", "value_current": 100000},
            {"report_id": "partial-rpt", "section": "Bilans", "tag_path": "Aktywa", "value_current": 500000},
        ])
        feature_engine.compute_features_for_report("partial-rpt", feature_set_id="maczynska_6")

        # Also create a full report
        _create_company_with_report("0000400014", "full-rpt", 2023, STANDARD_TAGS)

        stats = training_data.get_dataset_stats("maczynska_6")
        assert stats["row_count"] == 2
        # x1_maczynska requires Pasywa_B which partial doesn't have
        assert stats["missing_pct"]["x1_maczynska"] > 0
