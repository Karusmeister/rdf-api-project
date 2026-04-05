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

    CR2-SCALE-005: every join/filter below is scoped to the krs/report set
    actually present in the working feature slice instead of loading all
    companies / all reports / all bankruptcy events globally and doing
    Python-side row filtering. The bankruptcy label is produced in a single
    vectorized merge + interval check instead of the old row-wise `apply`.

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

    # Query computed features for the feature set (EAV format). This is the
    # authoritative row-set: every downstream join must be scoped to the
    # (krs, report_id) values that land here.
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

    # The exact set of krs / report_id we need metadata for.
    working_krs = list(wide_df["krs"].unique())
    working_report_ids = list(wide_df["report_id"].unique())

    # CR2-SCALE-005: filter `companies` server-side on the working krs set
    # instead of loading the whole table and merging everything.
    if working_krs:
        company_rows = conn.execute(
            """
            SELECT krs, pkd_code, incorporation_date
            FROM companies
            WHERE krs = ANY(%s)
            """,
            [working_krs],
        ).fetchall()
        if company_rows:
            companies_df = pd.DataFrame(
                company_rows,
                columns=["krs", "pkd_code", "incorporation_date"],
            )
            wide_df = wide_df.merge(companies_df, on="krs", how="left")

    # Bankruptcy labels
    label_col = f"is_bankrupt_within_{prediction_horizon_years}y"

    # CR2-SCALE-005: only pull bankruptcy events for krs values that are in
    # the working dataset. Previously this query loaded every event in the
    # table and Python filtered row-by-row.
    bankruptcy_rows = (
        conn.execute(
            """
            SELECT krs, event_date
            FROM bankruptcy_events
            WHERE krs = ANY(%s)
              AND (is_confirmed = true OR event_type IN ('bankruptcy', 'restructuring'))
            """,
            [working_krs],
        ).fetchall()
        if working_krs
        else []
    )

    if bankruptcy_rows:
        events_df = pd.DataFrame(bankruptcy_rows, columns=["krs", "event_date"])
        events_df["event_date"] = pd.to_datetime(events_df["event_date"])

        # CR2-SCALE-005: pull period_end dates only for the reports we care
        # about — not the whole latest_successful_financial_reports view.
        report_dates_rows = conn.execute(
            """
            SELECT id, period_end
            FROM latest_successful_financial_reports
            WHERE id = ANY(%s)
            """,
            [working_report_ids],
        ).fetchall()
        dates_df = pd.DataFrame(report_dates_rows, columns=["report_id", "period_end"])
        dates_df["period_end"] = pd.to_datetime(dates_df["period_end"])
        wide_df = wide_df.merge(dates_df, on="report_id", how="left")

        # CR2-SCALE-005: vectorized label computation.
        # Old implementation: `wide_df.apply(_has_bankruptcy, axis=1)` — a
        # Python loop per row with a per-row filter of `events_df` (O(rows *
        # events)). For N rows and M events that is O(N * M) and the `apply`
        # overhead dominates for realistic datasets.
        #
        # New implementation:
        #   1. Inner-merge reports with events on krs so every (report, event)
        #      pair in the same company becomes one row — this is the only
        #      cartesian fan-out we need.
        #   2. Filter to events that fall in (period_end, period_end + horizon].
        #   3. Collapse to the distinct set of report_ids that had a hit.
        #   4. Left-merge that set back into wide_df as a 0/1 label.
        # Complexity is dominated by the merge (hash join, ~O(N + M))
        # regardless of dataset size.
        horizon = pd.DateOffset(years=prediction_horizon_years)
        reports_with_dates = wide_df[["report_id", "krs", "period_end"]].dropna(
            subset=["period_end"]
        )
        joined = reports_with_dates.merge(events_df, on="krs", how="inner")
        if not joined.empty:
            horizon_end = joined["period_end"] + horizon
            in_horizon = (joined["period_end"] < joined["event_date"]) & (
                joined["event_date"] <= horizon_end
            )
            positive_reports = set(joined.loc[in_horizon, "report_id"].unique())
        else:
            positive_reports = set()

        wide_df[label_col] = wide_df["report_id"].isin(positive_reports).astype(int)
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
