from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query

from app.auth import CurrentUser, require_krs_access, require_admin
from app.services import predictions as predictions_service

from .schemas import HistoryResponse, ModelsResponse, PredictionResponse

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


@router.get("/models", summary="List active prediction models")
def list_models() -> ModelsResponse:
    """Return all active models with their interpretation thresholds. No authentication required."""
    return predictions_service.get_models()


@router.get("/{krs}", summary="Get predictions for a company")
def get_predictions(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
) -> PredictionResponse:
    """Full prediction detail for a KRS number: scores, features with source financial data,
    interpretation thresholds, and score history. Requires JWT auth and KRS access."""
    require_krs_access(krs, user)
    result = predictions_service.get_predictions(krs)
    company = result["company"]
    has_company_data = any(company.get(k) is not None for k in ("nip", "pkd_code"))
    if not has_company_data and not result["predictions"] and not result["history"]:
        raise HTTPException(status_code=404, detail=f"No data found for KRS {krs}")
    return result


@router.get("/{krs}/history", summary="Get prediction score history")
def get_history(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    model_id: Annotated[str | None, Query()] = None,
) -> HistoryResponse:
    """Score timeline for a company, ordered by fiscal year. Optionally filter by model_id.
    Useful for charting score trends over time. Requires JWT auth and KRS access."""
    require_krs_access(krs, user)
    return predictions_service.get_history(krs, model_id=model_id)


@router.post("/cache/invalidate", tags=["admin"], summary="Flush prediction caches")
def invalidate_cache(user: CurrentUser):
    """Admin-only. Flush the in-memory model and feature definition caches.
    Use after seeding new models or feature definitions."""
    require_admin(user)
    predictions_service.invalidate_caches()
    return {"status": "caches_invalidated"}
