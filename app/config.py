from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    rdf_base_url: str = (
        "https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych"
    )
    rdf_referer: str = "https://rdf-przegladarka.ms.gov.pl/wyszukaj-podmiot"
    rdf_origin: str = "https://rdf-przegladarka.ms.gov.pl"
    request_timeout: int = 30
    max_connections: int = 20
    cors_origins: List[str] = ["*"]
    workers: int = 4

    # --- Scraper ---
    scraper_db_path: str = "data/scraper.duckdb"

    # Storage
    storage_backend: str = "local"           # 'local' or 'gcs'
    storage_local_path: str = "data/documents"
    storage_gcs_bucket: str = ""
    storage_gcs_prefix: str = "krs/"

    # Scraper behavior
    scraper_order_strategy: str = "priority_then_oldest"
    scraper_delay_between_krs: float = 2.0
    scraper_delay_between_requests: float = 0.5
    scraper_max_krs_per_run: int = 0          # 0 = unlimited
    scraper_max_errors_before_skip: int = 3
    scraper_error_backoff_hours: int = 24
    scraper_download_timeout: int = 60


settings = Settings()
