from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query

from app.auth import CurrentUser, require_krs_access, require_admin
from app.services import predictions as predictions_service

from .schemas import HistoryResponse, ModelsResponse, PredictionResponse

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


@router.get("/models")
def list_models() -> ModelsResponse:
    return predictions_service.get_models()


@router.get("/{krs}")
def get_predictions(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
) -> PredictionResponse:
    require_krs_access(krs, user)
    result = predictions_service.get_predictions(krs)
    company = result["company"]
    has_company_data = any(company.get(k) is not None for k in ("nip", "pkd_code"))
    if not has_company_data and not result["predictions"] and not result["history"]:
        raise HTTPException(status_code=404, detail=f"No data found for KRS {krs}")
    return result


@router.get("/{krs}/history")
def get_history(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    model_id: Annotated[str | None, Query()] = None,
) -> HistoryResponse:
    require_krs_access(krs, user)
    return predictions_service.get_history(krs, model_id=model_id)


@router.post("/cache/invalidate", tags=["admin"])
def invalidate_cache(user: CurrentUser):
    require_admin(user)
    predictions_service.invalidate_caches()
    return {"status": "caches_invalidated"}
