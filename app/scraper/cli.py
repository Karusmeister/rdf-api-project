from __future__ import annotations

import asyncio
import csv
import logging

import click


@click.group()
def cli():
    """RDF Scraper - bulk KRS document collector."""
    pass


@cli.command()
@click.option("--mode", type=click.Choice(["full_scan", "new_only", "retry_errors"]), default="full_scan")
@click.option("--krs", multiple=True, help="Specific KRS numbers to process (overrides mode)")
@click.option("--max-krs", type=int, default=0, help="Max KRS to process (0=unlimited)")
@click.option("--verbose", "-v", is_flag=True)
def run(mode, krs, max_krs, verbose):
    """Run the scraper job."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if krs:
        mode = "specific_krs"

    from app.scraper.job import run_scraper
    stats = asyncio.run(run_scraper(
        mode=mode,
        specific_krs=list(krs) if krs else None,
        max_krs=max_krs,
    ))

    click.echo("\nRun completed:")
    for k, v in stats.items():
        click.echo(f"  {k}: {v}")


@cli.command("import-krs")
@click.option("--file", "filepath", required=True, type=click.Path(exists=True))
@click.option("--column", default="krs", help="Column name containing KRS numbers")
def import_krs(filepath, column):
    """Import KRS numbers from a CSV file."""
    from app.scraper import db

    db.connect()
    count = 0
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            krs_val = row[column].strip().zfill(10)
            if krs_val.isdigit() and len(krs_val) == 10:
                db.upsert_krs(krs_val, company_name=None, legal_form=None, is_active=True)
                count += 1
    db.close()
    click.echo(f"Imported {count} KRS numbers from {filepath}")


@cli.command("import-range")
@click.option("--start", "start_num", required=True, type=int)
@click.option("--end", "end_num", required=True, type=int)
def import_range(start_num, end_num):
    """Import a range of KRS numbers (will be validated during scan)."""
    from app.scraper import db

    db.connect()
    count = 0
    for n in range(start_num, end_num + 1):
        krs = str(n).zfill(10)
        db.upsert_krs(krs, company_name=None, legal_form=None, is_active=True)
        count += 1
        if count % 10000 == 0:
            click.echo(f"  ...{count} inserted")
    db.close()
    click.echo(f"Imported {count} KRS numbers (range {start_num}-{end_num})")


@cli.command()
def status():
    """Show scraper statistics."""
    from app.scraper import db

    db.connect()
    stats = db.get_stats()
    last_run = db.get_last_run()
    db.close()

    click.echo("=== Scraper Status ===")
    for k, v in stats.items():
        click.echo(f"  {k}: {v}")

    if last_run:
        click.echo(f"\nLast run: {last_run['started_at']} ({last_run['status']})")
        click.echo(f"  KRS checked: {last_run.get('krs_checked', '?')}")
        click.echo(f"  Docs downloaded: {last_run.get('documents_downloaded', '?')}")


if __name__ == "__main__":
    cli()
