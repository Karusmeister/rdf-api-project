"""Quick scan: find N valid KRS entities, discover+download docs, run ETL.

Usage:
    python scripts/quick_scan.py --count 10
"""
import argparse
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def scan_entities(target: int, start: int = 1) -> list[str]:
    """Probe sequential KRS integers via the MS Gov API, return list of valid KRS."""
    from app import krs_client
    from app.adapters.ms_gov import MsGovKrsAdapter
    from app.adapters.registry import register
    from app.db import connection as db_conn
    from app.repositories import krs_repo
    from app.scraper import db as scraper_db

    db_conn.connect()
    krs_repo._ensure_schema()
    scraper_db._ensure_schema()

    await krs_client.start()
    adapter = MsGovKrsAdapter()
    register("ms_gov", adapter)

    found: list[str] = []
    krs_int = start

    try:
        while len(found) < target:
            krs_str = str(krs_int).zfill(10)
            try:
                entity = await adapter.get_entity(krs_str)
                if entity is None:
                    krs_int += 1
                    await asyncio.sleep(0.5)
                    continue

                logger.info("FOUND KRS %s — %s", krs_str, entity.name)

                krs_repo.upsert_entity(
                    krs=entity.krs,
                    name=entity.name,
                    legal_form=entity.legal_form,
                    address_city=entity.address_city,
                    raw=entity.raw,
                )
                scraper_db.upsert_krs(krs_str, entity.name, entity.legal_form, True)
                found.append(krs_str)
            except Exception as e:
                err = type(e).__name__
                if "NotFound" in err:
                    pass  # expected — most integers are not valid KRS
                elif "RateLimit" in err:
                    logger.warning("Rate limited, backing off 30s...")
                    await asyncio.sleep(30)
                    continue  # retry same krs_int
                else:
                    logger.warning("KRS %s error: %s", krs_str, e)

            krs_int += 1
            await asyncio.sleep(0.5)  # polite pacing
    finally:
        await krs_client.stop()

    return found


async def discover_and_download(krs_list: list[str]) -> int:
    """Run the scraper for specific KRS numbers. Returns count of downloaded docs."""
    from app import rdf_client
    from app.scraper.job import run_scraper

    await rdf_client.start()
    try:
        result = await run_scraper(mode="specific_krs", specific_krs=krs_list)
        logger.info("Scraper result: %s", result)
        return result.get("documents_downloaded", 0)
    finally:
        await rdf_client.stop()


def run_etl() -> dict:
    """Ingest all pending downloaded documents."""
    from app.services.etl import ingest_all_pending
    result = ingest_all_pending()
    logger.info("ETL result: %s", result)
    return result


async def main():
    parser = argparse.ArgumentParser(description="Quick scan + scrape + ETL")
    parser.add_argument("--count", type=int, default=10, help="Number of KRS entities to find")
    parser.add_argument("--start", type=int, default=1, help="First KRS integer to probe")
    args = parser.parse_args()

    # Step 1: Find entities
    logger.info("=== Step 1: Scanning for %d valid KRS entities (starting at %d) ===", args.count, args.start)
    found = await scan_entities(args.count, args.start)
    logger.info("Found %d entities: %s", len(found), found)

    if not found:
        logger.error("No entities found, nothing to scrape.")
        return

    # Step 2: Discover and download documents
    logger.info("=== Step 2: Discovering and downloading documents ===")
    downloaded = await discover_and_download(found)
    logger.info("Downloaded %d documents", downloaded)

    # Step 3: Parse into database
    logger.info("=== Step 3: Running ETL (XML → PostgreSQL) ===")
    etl_result = run_etl()
    logger.info("=== Done! Completed: %d, Failed: %d ===",
                etl_result.get("completed", 0), etl_result.get("failed", 0))


if __name__ == "__main__":
    asyncio.run(main())
