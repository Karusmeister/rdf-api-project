"""
Training data assembly pipeline.

Pivots computed_features from EAV to wide format, joins with bankruptcy labels,
and returns a clean DataFrame ready for sklearn/xgboost.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from app.db import prediction_db

logger = logging.getLogger(__name__)


def build_training_dataset(
    feature_set_id: str,
    min_year: Optional[int] = None,
    max_year: Optional[int] = None,
    prediction_horizon_years: int = 2,
) -> pd.DataFrame:
    """Assemble wide-format training data from the EAV feature store.

    Returns a DataFrame with columns:
    - krs, report_id, fiscal_year
    - one column per feature in the feature set
    - is_bankrupt_within_Ny (binary label)
    - company metadata: pkd_code, incorporation_date
    """
    conn = prediction_db.get_conn()

    # Get feature set members to know column names
    members = prediction_db.get_feature_set_members(feature_set_id)
    if not members:
        raise ValueError(f"Feature set '{feature_set_id}' not found or empty")
    feature_ids = [m["feature_definition_id"] for m in members]

    # Build year filter
    year_clauses = []
    params: list = [feature_set_id]
    if min_year is not None:
        year_clauses.append("cf.fiscal_year >= %s")
        params.append(min_year)
    if max_year is not None:
        year_clauses.append("cf.fiscal_year <= %s")
        params.append(max_year)
    year_filter = (" AND " + " AND ".join(year_clauses)) if year_clauses else ""

    # Query computed features for the feature set (EAV format)
    rows = conn.execute(f"""
        SELECT cf.report_id, cf.krs, cf.fiscal_year, cf.feature_definition_id, cf.value
        FROM latest_computed_features cf
        JOIN latest_successful_financial_reports fr ON fr.id = cf.report_id
        JOIN feature_set_members fsm ON fsm.feature_definition_id = cf.feature_definition_id
        WHERE fsm.feature_set_id = %s
          AND cf.is_valid = true
          {year_filter}
        ORDER BY cf.report_id, cf.feature_definition_id
    """, params).fetchall()

    if not rows:
        return pd.DataFrame()

    # Build EAV DataFrame and pivot to wide format
    eav_df = pd.DataFrame(rows, columns=["report_id", "krs", "fiscal_year", "feature_id", "value"])
    wide_df = eav_df.pivot_table(
        index=["report_id", "krs", "fiscal_year"],
        columns="feature_id",
        values="value",
        aggfunc="first",
    ).reset_index()
    wide_df.columns.name = None

    # Add company metadata
    company_rows = conn.execute("""
        SELECT krs, pkd_code, incorporation_date FROM companies
    """).fetchall()
    if company_rows:
        companies_df = pd.DataFrame(company_rows, columns=["krs", "pkd_code", "incorporation_date"])
        wide_df = wide_df.merge(companies_df, on="krs", how="left")

    # Add bankruptcy labels
    label_col = f"is_bankrupt_within_{prediction_horizon_years}y"
    bankruptcy_rows = conn.execute("""
        SELECT krs, event_date FROM bankruptcy_events
        WHERE is_confirmed = true OR event_type IN ('bankruptcy', 'restructuring')
    """).fetchall()

    if bankruptcy_rows:
        events_df = pd.DataFrame(bankruptcy_rows, columns=["krs", "event_date"])
        events_df["event_date"] = pd.to_datetime(events_df["event_date"])

        # For each report row, check if the company went bankrupt within N years of fiscal year end
        report_dates = conn.execute("""
            SELECT id, period_end FROM latest_successful_financial_reports
        """).fetchall()
        dates_df = pd.DataFrame(report_dates, columns=["report_id", "period_end"])
        dates_df["period_end"] = pd.to_datetime(dates_df["period_end"])

        wide_df = wide_df.merge(dates_df, on="report_id", how="left")

        def _has_bankruptcy(row):
            company_events = events_df[events_df["krs"] == row["krs"]]
            if company_events.empty or pd.isna(row.get("period_end")):
                return 0
            horizon_end = row["period_end"] + pd.DateOffset(years=prediction_horizon_years)
            return int(any(
                (row["period_end"] < evt) and (evt <= horizon_end)
                for evt in company_events["event_date"]
            ))

        wide_df[label_col] = wide_df.apply(_has_bankruptcy, axis=1)
        wide_df.drop(columns=["period_end"], inplace=True, errors="ignore")
    else:
        wide_df[label_col] = 0

    logger.info(
        "training_dataset_built",
        extra={
            "event": "training_dataset_built",
            "feature_set_id": feature_set_id,
            "rows": len(wide_df),
            "features": len(feature_ids),
            "label_col": label_col,
        },
    )

    return wide_df


def export_to_csv(feature_set_id: str, path: str, **kwargs) -> int:
    """Export training dataset to CSV. Returns row count."""
    df = build_training_dataset(feature_set_id, **kwargs)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("training_data_exported", extra={"event": "training_data_exported", "path": path, "rows": len(df)})
    return len(df)


def get_dataset_stats(feature_set_id: str, **kwargs) -> dict:
    """Return summary statistics about the training dataset."""
    df = build_training_dataset(feature_set_id, **kwargs)

    if df.empty:
        return {
            "feature_set_id": feature_set_id,
            "row_count": 0,
            "feature_count": 0,
            "class_balance": {},
            "missing_pct": {},
            "high_missing_features": [],
            "single_year_companies": 0,
            "unique_companies": 0,
            "year_range": [],
        }

    members = prediction_db.get_feature_set_members(feature_set_id)
    feature_ids = [m["feature_definition_id"] for m in members]
    present_features = [f for f in feature_ids if f in df.columns]

    # Class balance
    label_cols = [c for c in df.columns if c.startswith("is_bankrupt_within_")]
    class_balance = {}
    if label_cols:
        label = label_cols[0]
        counts = df[label].value_counts().to_dict()
        class_balance = {str(k): int(v) for k, v in counts.items()}

    # Missing value percentages per feature
    missing_pct = {}
    for f in present_features:
        pct = round(float(df[f].isna().mean()) * 100, 1)
        missing_pct[f] = pct

    high_missing = [f for f, pct in missing_pct.items() if pct > 50]

    # Companies with only 1 year of data
    single_year = int((df.groupby("krs")["fiscal_year"].nunique() == 1).sum())

    return {
        "feature_set_id": feature_set_id,
        "row_count": len(df),
        "feature_count": len(present_features),
        "class_balance": class_balance,
        "missing_pct": missing_pct,
        "high_missing_features": high_missing,
        "single_year_companies": single_year,
        "unique_companies": int(df["krs"].nunique()),
        "year_range": [int(df["fiscal_year"].min()), int(df["fiscal_year"].max())] if len(df) > 0 else [],
    }
