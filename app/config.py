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
    log_level: str = "INFO"

    # --- KRS Open API ---
    krs_api_base_url: str = "https://api-krs.ms.gov.pl/api/krs"
    krs_request_timeout: int = 15
    krs_max_retries: int = 5
    krs_request_delay_ms: int = 3000  # polite delay between sequential requests

    # --- KRS Sync Job ---
    krs_sync_cron: str = "0 3 * * *"  # default 3am daily
    krs_sync_batch_size: int = 100     # max entities per run
    krs_sync_stale_hours: int = 168    # re-sync entities older than 7 days

    # --- KRS Sequential Scanner ---
    krs_scan_batch_size: int = 500     # probes per run
    krs_scan_cron: str = "0 1 * * *"  # 1am daily (separate from krs_sync_cron)
    krs_scan_checkpoint_interval: int = 100  # flush stats to DB every N probes
    krs_scan_rate_limit_backoff_s: int = 60  # pause when upstream rate-limits us
    krs_scan_max_consecutive_errors: int = 10  # stop scan after this many in a row

    # --- PostgreSQL ---
    database_url: str = "postgresql://rdf:rdf_dev@localhost:5432/rdf"
    db_pool_min: int = 2
    db_pool_max: int = 10

    # --- Batch runner ---
    batch_use_vpn: bool = False
    batch_workers: int = 4
    batch_start_krs: int = 1
    batch_concurrency_per_worker: int = 3
    batch_delay_seconds: float = 2.5
    # --- RDF Batch document discovery ---
    rdf_batch_concurrency: int = 3
    rdf_batch_delay_seconds: float = 1.5
    rdf_batch_page_size: int = 100        # max docs per page to minimize pagination

    # NordVPN SOCKS5 credentials (only used when batch_use_vpn=true)
    nordvpn_username: str = ""
    nordvpn_password: str = ""
    nordvpn_servers: list[str] = []

    # --- Scraper ---

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
