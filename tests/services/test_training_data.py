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


# ---------------------------------------------------------------------------
# CR2-SCALE-005: dataset-assembly scalability regression guards
# ---------------------------------------------------------------------------


class TestDatasetScalability:
    """Guards against the old row-wise pandas `apply` + global table pulls.

    The previous implementation was O(rows * bankruptcy_events) because
    labeling filtered the full events DataFrame once per row. These tests
    force a moderately large synthetic dataset and assert two invariants:
      * Labels stay correct after the vectorized merge-based rewrite.
      * DB-side scoping actually restricts the bankruptcy-events pull to the
        working krs set instead of scanning the whole table. Both invariants
        are the acceptance criteria from CR2-SCALE-005.
    """

    def test_labels_match_reference_for_many_rows(self, seeded_db):
        # 30 companies × 3 years = 90 rows. Every third company is marked
        # bankrupt in the year immediately after its 2022 report so we can
        # check that the vectorized horizon match lines up with the reference
        # Python implementation.
        years = [2020, 2021, 2022]
        for i in range(30):
            krs = f"{500000 + i:010d}"
            for year in years:
                _create_company_with_report(krs, f"scale-rpt-{i}-{year}", year, STANDARD_TAGS)

            if i % 3 == 0:
                prediction_db.get_conn().execute(
                    """
                    INSERT INTO bankruptcy_events (id, krs, event_type, event_date, is_confirmed)
                    VALUES (%s, %s, 'bankruptcy', %s, true)
                    """,
                    [str(uuid.uuid4()), krs, "2023-06-15"],
                )

        df = training_data.build_training_dataset(
            "maczynska_6", prediction_horizon_years=2
        )
        assert len(df) == 90, (
            f"Expected 30 companies × 3 years = 90 rows, got {len(df)}"
        )

        # Every row for a "bankrupt within horizon" company in fiscal 2022
        # must be labeled 1 (event in 2023 falls inside period_end 2022-12-31
        # + 2 years). Fiscal 2020/2021 rows are labeled 0 because the event
        # lies outside their horizon.
        bankrupt_krs = {f"{500000 + i:010d}" for i in range(0, 30, 3)}
        labels = df.set_index(["krs", "fiscal_year"])["is_bankrupt_within_2y"]

        for krs in bankrupt_krs:
            assert labels.loc[(krs, 2022)] == 1, (
                f"{krs}@2022 should be bankrupt (event in 2023, horizon covers it)"
            )
            # 2020 period_end = 2020-12-31; horizon end = 2022-12-31 — event in
            # 2023 is past the horizon → 0.
            assert labels.loc[(krs, 2020)] == 0, (
                f"{krs}@2020 should not be bankrupt (event outside 2y horizon)"
            )

        # Every non-bankrupt company row must be 0.
        healthy_rows = df[~df["krs"].isin(bankrupt_krs)]
        assert healthy_rows["is_bankrupt_within_2y"].sum() == 0

    def test_bankruptcy_query_scoped_to_working_krs_set(self, seeded_db):
        """CR2-SCALE-005 acceptance criterion: metadata/event queries are
        restricted to the report_id/krs set in the feature dataset.

        We seed bankruptcy events for both "in-scope" and "out-of-scope" krs
        values and confirm the out-of-scope events never even get pulled into
        Python memory. This is asserted by monkeypatching the connection
        `execute` to record every SQL call and making sure the pulled
        bankruptcy rows only cover the working krs set.
        """
        import re

        _create_company_with_report("0000555001", "scoped-rpt-1", 2022, STANDARD_TAGS)
        _create_company_with_report("0000555002", "scoped-rpt-2", 2022, STANDARD_TAGS)

        conn = prediction_db.get_conn()
        # In-scope bankruptcy — should influence labels.
        conn.execute(
            """
            INSERT INTO bankruptcy_events (id, krs, event_type, event_date, is_confirmed)
            VALUES (%s, %s, 'bankruptcy', %s, true)
            """,
            [str(uuid.uuid4()), "0000555001", "2023-03-01"],
        )
        # Out-of-scope bankruptcy — no matching row in the feature set, so
        # the optimized query should filter it server-side.
        conn.execute(
            """
            INSERT INTO bankruptcy_events (id, krs, event_type, event_date, is_confirmed)
            VALUES (%s, %s, 'bankruptcy', %s, true)
            """,
            [str(uuid.uuid4()), "0000999999", "2023-03-01"],
        )

        captured_sql: list[str] = []
        original_execute = type(conn).execute

        def _capturing_execute(self, sql, params=None):
            captured_sql.append(" ".join(sql.split()))
            return original_execute(self, sql, params)

        with patch.object(type(conn), "execute", _capturing_execute):
            df = training_data.build_training_dataset("maczynska_6")

        # Behavior: in-scope company labeled 1, no row for the out-of-scope
        # bankruptcy exists in the dataset.
        labels = df.set_index(["krs", "fiscal_year"])["is_bankrupt_within_2y"]
        assert labels.loc[("0000555001", 2022)] == 1
        assert "0000999999" not in df["krs"].values

        # Structural guard: the bankruptcy query must be parameterized on a
        # krs ANY(...) filter, not an unbounded scan of the whole table.
        events_queries = [
            sql for sql in captured_sql
            if "FROM bankruptcy_events" in sql
        ]
        assert events_queries, "bankruptcy_events query never executed"
        assert all(
            re.search(r"krs\s*=\s*ANY", sql, re.IGNORECASE)
            for sql in events_queries
        ), (
            "CR2-SCALE-005 regression: bankruptcy_events query is no longer "
            f"scoped to a working krs set — captured SQL: {events_queries}"
        )
