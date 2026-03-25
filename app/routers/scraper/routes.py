import logging

from fastapi import APIRouter

from app.scraper import db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scraper", tags=["scraper"])


@router.get("/status")
async def scraper_status():
    """Return scraper dashboard stats. Read-only, fast."""
    logger.debug("scraper_status", extra={"event": "scraper_status"})
    stats = db.get_stats()
    last_run = db.get_last_run()
    return {
        **stats,
        "last_run": last_run,
    }
