from fastapi import APIRouter

from app.routers.rdf.podmiot import router as podmiot_router
from app.routers.rdf.dokumenty import router as dokumenty_router

router = APIRouter()
router.include_router(podmiot_router)
router.include_router(dokumenty_router)
