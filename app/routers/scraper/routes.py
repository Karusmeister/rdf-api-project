from fastapi import APIRouter

from app.scraper import db

router = APIRouter(prefix="/api/scraper", tags=["scraper"])


@router.get("/status")
async def scraper_status():
    """Return scraper dashboard stats. Read-only, fast."""
    db.connect()
    stats = db.get_stats()
    last_run = db.get_last_run()
    db.close()
    return {
        **stats,
        "last_run": last_run,
    }
