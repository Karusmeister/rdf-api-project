"""Ensure pipeline DB has the built-in models + feature definitions.

The pipeline DB is separate from rdf-postgres, so the standard
`prediction_db.register_model` path (which writes to rdf-postgres) does not
populate `model_registry` here. This module provides an idempotent bootstrap
that mirrors `scripts/seed_pipeline_db.py` but runs inside the pipeline
runner so a Cloud Run Job on a fresh database still scores correctly.

Kept intentionally parallel to scripts/seed_pipeline_db.py so the two seed
paths share the exact same feature/model contents.
"""
from __future__ import annotations

import json
import logging

from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)


def _load_seed_features():
    """Lazy import of scripts/seed_features.py (not a package)."""
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "scripts" / "seed_features.py"
    spec = importlib.util.spec_from_file_location("_seed_features", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _upsert_feature_definition(conn: ConnectionWrapper, fdef: dict) -> None:
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


def _upsert_feature_set(conn: ConnectionWrapper, set_id: str, set_info: dict) -> None:
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


def _upsert_model(
    conn: ConnectionWrapper,
    model_id: str,
    name: str,
    version: str,
    feature_set_id: str,
    description: str,
    hyperparameters: dict,
    is_baseline: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, description,
             hyperparameters, is_active, is_baseline, created_at)
        VALUES (%s, %s, 'discriminant', %s, %s, %s, %s, TRUE, %s, now())
        ON CONFLICT (name, version) DO UPDATE SET
            description = excluded.description,
            hyperparameters = excluded.hyperparameters,
            feature_set_id = excluded.feature_set_id,
            is_active = excluded.is_active,
            is_baseline = excluded.is_baseline
        """,
        [
            model_id, name, version, feature_set_id, description,
            json.dumps(hyperparameters), is_baseline,
        ],
    )


def ensure_builtin_catalog(conn: ConnectionWrapper) -> dict:
    """Ensure every feature definition, feature set, and built-in model is
    present in the pipeline DB. Safe to call on every pipeline run.

    Returns counts of rows upserted (for metrics/logging).
    """
    seed_features = _load_seed_features()
    fdefs = getattr(seed_features, "FEATURE_DEFINITIONS", [])
    fsets = getattr(seed_features, "FEATURE_SETS", {})

    for f in fdefs:
        _upsert_feature_definition(conn, f)
    for set_id, set_info in fsets.items():
        _upsert_feature_set(conn, set_id, set_info)

    # Maczynska
    from app.services.maczynska import COEFFICIENTS as MACZ_COEFF
    _upsert_model(
        conn,
        model_id="maczynska_1994_v1",
        name="maczynska",
        version="1994_v1",
        feature_set_id="maczynska_6",
        description="Maczynska (1994) 6-variable discriminant model for Polish companies",
        hyperparameters={
            "coefficients": MACZ_COEFF,
            "cutoffs": {"critical": 0, "high": 1, "medium": 2},
        },
    )

    # Poznanski
    from app.services.poznanski import (
        COEFFICIENTS as POZ_COEFF,
        INTERCEPT as POZ_INTERCEPT,
        NON_LINEAR_LIQUIDITY_THRESHOLD as POZ_X2_THRESHOLD,
    )
    _upsert_model(
        conn,
        model_id="poznanski_2004_v1",
        name="poznanski",
        version="2004_v1",
        feature_set_id="poznanski_4",
        description=(
            "Poznanski (Hamrol, Czajka, Piechocki 2004) 4-variable "
            "discriminant model for Polish companies"
        ),
        hyperparameters={
            "coefficients": POZ_COEFF,
            "intercept": POZ_INTERCEPT,
            "cutoffs": {"critical": 0, "medium": 1},
            "non_linear_liquidity_threshold": POZ_X2_THRESHOLD,
        },
    )

    logger.info(
        "pipeline_catalog_ensured",
        extra={
            "event": "pipeline_catalog_ensured",
            "feature_definitions": len(fdefs),
            "feature_sets": len(fsets),
            "models": 2,
        },
    )
    return {
        "feature_definitions": len(fdefs),
        "feature_sets": len(fsets),
        "models": 2,
    }
