"""Seed feature definitions, feature sets, and model registry into the pipeline DB.

Reuses the FEATURE_DEFINITIONS / FEATURE_SETS constants from seed_features.py
but writes them to the pipeline database via pipeline_db instead of
prediction_db.

Usage:
    python scripts/seed_pipeline_db.py
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

from app.db import pipeline_db


def _load_seed_features_module():
    """Load scripts/seed_features.py as a module without requiring `scripts`
    to be a package. The module top-level imports prediction_db but does not
    execute any DB writes until seed() is called — so importing is safe."""
    path = Path(__file__).parent / "seed_features.py"
    spec = importlib.util.spec_from_file_location("_seed_features", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _upsert_feature_definition(conn, fdef: dict) -> None:
    conn.execute(
        """
        INSERT INTO feature_definitions
            (id, name, description, category, formula_description,
             formula_numerator, formula_denominator, required_tags,
             computation_logic, version, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (id) DO UPDATE SET
            name                = excluded.name,
            description         = excluded.description,
            category            = excluded.category,
            formula_description = excluded.formula_description,
            formula_numerator   = excluded.formula_numerator,
            formula_denominator = excluded.formula_denominator,
            required_tags       = excluded.required_tags,
            computation_logic   = excluded.computation_logic,
            version             = excluded.version
        """,
        [
            fdef["id"], fdef["name"], fdef.get("description"),
            fdef.get("category"), fdef.get("formula_description"),
            fdef.get("formula_numerator"), fdef.get("formula_denominator"),
            json.dumps(fdef.get("required_tags")) if fdef.get("required_tags") else None,
            fdef.get("computation_logic", "ratio"),
            fdef.get("version", 1),
        ],
    )


def _upsert_feature_set(conn, set_id: str, set_info: dict) -> None:
    conn.execute(
        """
        INSERT INTO feature_sets (id, name, description, created_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (id) DO UPDATE SET
            name = excluded.name,
            description = excluded.description
        """,
        [set_id, set_info["name"], set_info.get("description")],
    )
    for ordinal, fid in enumerate(set_info.get("members", []), start=1):
        conn.execute(
            """
            INSERT INTO feature_set_members (feature_set_id, feature_definition_id, ordinal)
            VALUES (%s, %s, %s)
            ON CONFLICT (feature_set_id, feature_definition_id) DO UPDATE SET
                ordinal = excluded.ordinal
            """,
            [set_id, fid, ordinal],
        )


def _register_maczynska(conn) -> None:
    from app.services.maczynska import COEFFICIENTS
    conn.execute(
        """
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, description,
             hyperparameters, is_active, is_baseline, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, TRUE, now())
        ON CONFLICT (name, version) DO UPDATE SET
            description = excluded.description,
            hyperparameters = excluded.hyperparameters
        """,
        [
            "maczynska_1994_v1",
            "maczynska",
            "discriminant",
            "1994_v1",
            "maczynska_6",
            "Maczynska (1994) 6-variable discriminant model for Polish companies",
            json.dumps({"coefficients": COEFFICIENTS,
                        "cutoffs": {"critical": 0, "high": 1, "medium": 2}}),
        ],
    )


def main() -> int:
    seed_features = _load_seed_features_module()
    pipeline_db.connect()
    conn = pipeline_db.get_conn()

    fdefs = getattr(seed_features, "FEATURE_DEFINITIONS", [])
    fsets = getattr(seed_features, "FEATURE_SETS", {})

    for f in fdefs:
        _upsert_feature_definition(conn, f)
    print(f"Seeded {len(fdefs)} feature definitions")

    for set_id, set_info in fsets.items():
        _upsert_feature_set(conn, set_id, set_info)
    print(f"Seeded {len(fsets)} feature sets")

    _register_maczynska(conn)
    print("Registered maczynska_1994_v1 model")
    return 0


if __name__ == "__main__":
    sys.exit(main())
