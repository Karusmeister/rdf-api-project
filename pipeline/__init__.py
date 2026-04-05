"""Bankruptcy prediction pipeline package.

Runs as a standalone Cloud Run Job (see Dockerfile.pipeline). Reads changed
documents from the scraper database (rdf-postgres, read-only), ETLs them to
the pipeline database (rdf-pipeline), computes features, scores models, and
optionally syncs to BigQuery for cross-company analytics.
"""

__version__ = "0.1.0"
