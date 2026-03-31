"""
Seed feature_definitions and feature_sets into the prediction database.

Usage:
    python scripts/seed_features.py

Idempotent — safe to run multiple times (uses upserts).
"""

from app.config import settings
from app.db import prediction_db

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------

# Tag paths match what the ETL writes to financial_line_items:
#   Bilans section:  Aktywa, Aktywa_A, Aktywa_B, Pasywa_A, Pasywa_B, Pasywa_B_III, etc.
#   RZiS section:    RZiS.A, RZiS.B, RZiS.C, RZiS.F, RZiS.L, etc.
#   CF section:      CF.A_III, CF.B_III, CF.C_III, CF.D, etc.

FEATURE_DEFINITIONS = [
    # --- Profitability ---
    {
        "id": "roa",
        "name": "Return on Assets",
        "description": "Net Profit / Total Assets",
        "category": "profitability",
        "formula_description": "Net Profit / Total Assets",
        "formula_numerator": "RZiS.L",
        "formula_denominator": "Aktywa",
        "required_tags": ["RZiS.L", "Aktywa"],
        "computation_logic": "ratio",
    },
    {
        "id": "roe",
        "name": "Return on Equity",
        "description": "Net Profit / Equity",
        "category": "profitability",
        "formula_description": "Net Profit / Equity",
        "formula_numerator": "RZiS.L",
        "formula_denominator": "Pasywa_A",
        "required_tags": ["RZiS.L", "Pasywa_A"],
        "computation_logic": "ratio",
    },
    {
        "id": "ros",
        "name": "Return on Sales",
        "description": "Net Profit / Revenue",
        "category": "profitability",
        "formula_description": "Net Profit / Revenue",
        "formula_numerator": "RZiS.L",
        "formula_denominator": "RZiS.A",
        "required_tags": ["RZiS.L", "RZiS.A"],
        "computation_logic": "ratio",
    },
    {
        "id": "operating_margin",
        "name": "Operating Margin",
        "description": "Operating Profit / Revenue",
        "category": "profitability",
        "formula_description": "Operating Profit / Revenue",
        "formula_numerator": "RZiS.F",
        "formula_denominator": "RZiS.A",
        "required_tags": ["RZiS.F", "RZiS.A"],
        "computation_logic": "ratio",
    },
    {
        "id": "gross_margin",
        "name": "Gross Margin",
        "description": "Gross Profit / Revenue",
        "category": "profitability",
        "formula_description": "Gross Profit / Revenue",
        "formula_numerator": "RZiS.C",
        "formula_denominator": "RZiS.A",
        "required_tags": ["RZiS.C", "RZiS.A"],
        "computation_logic": "ratio",
    },

    # --- Liquidity ---
    {
        "id": "current_ratio",
        "name": "Current Ratio",
        "description": "Current Assets / Short-term Liabilities",
        "category": "liquidity",
        "formula_description": "Current Assets / Short-term Liabilities",
        "formula_numerator": "Aktywa_B",
        "formula_denominator": "Pasywa_B_III",
        "required_tags": ["Aktywa_B", "Pasywa_B_III"],
        "computation_logic": "ratio",
    },
    {
        "id": "quick_ratio",
        "name": "Quick Ratio",
        "description": "(Current Assets - Inventory) / Short-term Liabilities",
        "category": "liquidity",
        "formula_description": "(Current Assets - Inventory) / Short-term Liabilities",
        "formula_numerator": "Aktywa_B",
        "formula_denominator": "Pasywa_B_III",
        "required_tags": ["Aktywa_B", "Aktywa_B_I", "Pasywa_B_III"],
        "computation_logic": "custom",
    },
    {
        "id": "cash_ratio",
        "name": "Cash Ratio",
        "description": "Cash / Short-term Liabilities",
        "category": "liquidity",
        "formula_description": "Cash / Short-term Liabilities",
        "formula_numerator": "Aktywa_B_III",
        "formula_denominator": "Pasywa_B_III",
        "required_tags": ["Aktywa_B_III", "Pasywa_B_III"],
        "computation_logic": "ratio",
    },

    # --- Leverage ---
    {
        "id": "debt_ratio",
        "name": "Debt Ratio",
        "description": "Total Liabilities / Total Assets",
        "category": "leverage",
        "formula_description": "Total Liabilities / Total Assets",
        "formula_numerator": "Pasywa_B",
        "formula_denominator": "Aktywa",
        "required_tags": ["Pasywa_B", "Aktywa"],
        "computation_logic": "ratio",
    },
    {
        "id": "equity_ratio",
        "name": "Equity Ratio",
        "description": "Equity / Total Assets",
        "category": "leverage",
        "formula_description": "Equity / Total Assets",
        "formula_numerator": "Pasywa_A",
        "formula_denominator": "Aktywa",
        "required_tags": ["Pasywa_A", "Aktywa"],
        "computation_logic": "ratio",
    },
    {
        "id": "debt_to_equity",
        "name": "Debt to Equity",
        "description": "Total Liabilities / Equity",
        "category": "leverage",
        "formula_description": "Total Liabilities / Equity",
        "formula_numerator": "Pasywa_B",
        "formula_denominator": "Pasywa_A",
        "required_tags": ["Pasywa_B", "Pasywa_A"],
        "computation_logic": "ratio",
    },

    # --- Activity ---
    {
        "id": "asset_turnover",
        "name": "Asset Turnover",
        "description": "Revenue / Total Assets",
        "category": "activity",
        "formula_description": "Revenue / Total Assets",
        "formula_numerator": "RZiS.A",
        "formula_denominator": "Aktywa",
        "required_tags": ["RZiS.A", "Aktywa"],
        "computation_logic": "ratio",
    },
    {
        "id": "receivables_turnover",
        "name": "Receivables Turnover",
        "description": "Revenue / Short-term Receivables",
        "category": "activity",
        "formula_description": "Revenue / Short-term Receivables",
        "formula_numerator": "RZiS.A",
        "formula_denominator": "Aktywa_B_II",
        "required_tags": ["RZiS.A", "Aktywa_B_II"],
        "computation_logic": "ratio",
    },
    {
        "id": "inventory_turnover",
        "name": "Inventory Turnover",
        "description": "Operating Costs / Inventory",
        "category": "activity",
        "formula_description": "Operating Costs / Inventory",
        "formula_numerator": "RZiS.B",
        "formula_denominator": "Aktywa_B_I",
        "required_tags": ["RZiS.B", "Aktywa_B_I"],
        "computation_logic": "ratio",
    },

    # --- Size ---
    {
        "id": "log_total_assets",
        "name": "Log Total Assets",
        "description": "ln(Total Assets)",
        "category": "size",
        "formula_description": "ln(Total Assets)",
        "formula_numerator": "Aktywa",
        "formula_denominator": None,
        "required_tags": ["Aktywa"],
        "computation_logic": "custom",
    },
    {
        "id": "log_revenue",
        "name": "Log Revenue",
        "description": "ln(Revenue)",
        "category": "size",
        "formula_description": "ln(Revenue)",
        "formula_numerator": "RZiS.A",
        "formula_denominator": None,
        "required_tags": ["RZiS.A"],
        "computation_logic": "custom",
    },

    # --- Maczynska 1994 Model (X1-X6) ---
    # Reference: Maczynska E., 1994. Ocena kondycji przedsiebiorstwa.
    # Zm = 1.5*X1 + 0.08*X2 + 10*X3 + 5*X4 + 0.3*X5 + 0.1*X6
    {
        "id": "x1_maczynska",
        "name": "Maczynska X1: cash generation to debt",
        "description": "(Gross profit + Depreciation) / Total liabilities",
        "category": "maczynska",
        "formula_description": "(RZiS.I + CF.A_II_1) / Pasywa_B",
        "formula_numerator": "RZiS.I",
        "formula_denominator": "Pasywa_B",
        "required_tags": ["RZiS.I", "Pasywa_B"],
        "computation_logic": "custom",
    },
    {
        "id": "x2_maczynska",
        "name": "Maczynska X2: asset coverage",
        "description": "Total assets / Total liabilities",
        "category": "maczynska",
        "formula_description": "Aktywa / Pasywa_B",
        "formula_numerator": "Aktywa",
        "formula_denominator": "Pasywa_B",
        "required_tags": ["Aktywa", "Pasywa_B"],
        "computation_logic": "ratio",
    },
    {
        "id": "x3_maczynska",
        "name": "Maczynska X3: pre-tax ROA",
        "description": "Pre-tax profit / Total assets",
        "category": "maczynska",
        "formula_description": "RZiS.I / Aktywa",
        "formula_numerator": "RZiS.I",
        "formula_denominator": "Aktywa",
        "required_tags": ["RZiS.I", "Aktywa"],
        "computation_logic": "ratio",
    },
    {
        "id": "x4_maczynska",
        "name": "Maczynska X4: pre-tax margin",
        "description": "Pre-tax profit / Revenue",
        "category": "maczynska",
        "formula_description": "RZiS.I / RZiS.A",
        "formula_numerator": "RZiS.I",
        "formula_denominator": "RZiS.A",
        "required_tags": ["RZiS.I", "RZiS.A"],
        "computation_logic": "ratio",
    },
    {
        "id": "x5_maczynska",
        "name": "Maczynska X5: inventory to revenue",
        "description": "Inventory / Revenue",
        "category": "maczynska",
        "formula_description": "Aktywa_B_I / RZiS.A",
        "formula_numerator": "Aktywa_B_I",
        "formula_denominator": "RZiS.A",
        "required_tags": ["Aktywa_B_I", "RZiS.A"],
        "computation_logic": "ratio",
    },
    {
        "id": "x6_maczynska",
        "name": "Maczynska X6: asset turnover",
        "description": "Revenue / Total assets",
        "category": "maczynska",
        "formula_description": "RZiS.A / Aktywa",
        "formula_numerator": "RZiS.A",
        "formula_denominator": "Aktywa",
        "required_tags": ["RZiS.A", "Aktywa"],
        "computation_logic": "ratio",
    },
]

# ---------------------------------------------------------------------------
# Feature sets
# ---------------------------------------------------------------------------

FEATURE_SETS = {
    "basic_20": {
        "name": "Basic 20 Financial Features",
        "description": "Standard set of 20 financial ratios covering profitability, liquidity, leverage, activity, and size",
        "members": [
            "roa", "roe", "ros", "operating_margin", "gross_margin",
            "current_ratio", "quick_ratio", "cash_ratio",
            "debt_ratio", "equity_ratio", "debt_to_equity",
            "asset_turnover", "receivables_turnover", "inventory_turnover",
            "log_total_assets", "log_revenue",
            "x1_maczynska", "x2_maczynska", "x3_maczynska", "x4_maczynska",
        ],
    },
    "maczynska_6": {
        "name": "Maczynska MDA Model (6 features)",
        "description": "Six features for the Maczynska Multiple Discriminant Analysis bankruptcy model",
        "members": [
            "x1_maczynska", "x2_maczynska", "x3_maczynska",
            "x4_maczynska", "x5_maczynska", "x6_maczynska",
        ],
    },
}


def seed():
    """Seed all feature definitions and feature sets. Idempotent."""
    prediction_db.connect()

    # Seed feature definitions
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

    print(f"Seeded {len(FEATURE_DEFINITIONS)} feature definitions")

    # Seed feature sets
    for set_id, set_info in FEATURE_SETS.items():
        prediction_db.upsert_feature_set(set_id, set_info["name"], set_info.get("description"))

        for ordinal, member_id in enumerate(set_info["members"], start=1):
            prediction_db.add_feature_set_member(set_id, member_id, ordinal)

        print(f"Seeded feature set '{set_id}' with {len(set_info['members'])} members")

    prediction_db.close()


if __name__ == "__main__":
    seed()
