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
    cors_origins: List[str] = [
        # Local development
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000",
        # Lovable preview + production
        "https://id-preview--aaed77c9-b864-4814-8747-ad97402d1f70.lovable.app",
        "https://joy-forge-express.lovable.app",
        "https://aaed77c9-b864-4814-8747-ad97402d1f70.lovableproject.com",
    ]
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
    rdf_batch_concurrency: int = 5
    rdf_batch_delay_seconds: float = 1.5   # delay for discovery (encrypted search)
    rdf_batch_download_delay: float = 0.3  # delay for metadata + ZIP download (lighter endpoints)
    rdf_batch_page_size: int = 100         # max docs per page to minimize pagination
    rdf_batch_skip_metadata: bool = False  # skip metadata fetch during download (backfill later)
    metadata_backfill_fetch_batch_size: int = 500  # rows per keyset page in metadata backfill

    # NordVPN SOCKS5 credentials (only used when batch_use_vpn=true)
    nordvpn_username: str = ""
    nordvpn_password: str = ""
    nordvpn_servers: list[str] = []

    # Public proxy pool (opt-in, default off)
    batch_use_public_proxies: bool = False  # load proxies.json into the proxy pool
    batch_require_vpn_only: bool = False    # strict mode: never use direct egress

    # --- Auth ---
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440  # 24 hours
    google_client_id: str = ""
    verification_code_expire_minutes: int = 15
    verification_email_mode: str = "log"  # 'log' (dev) or 'smtp' (prod)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "noreply@example.com"
    recaptcha_secret_key: str = ""  # Google reCAPTCHA v3 secret; empty = skip verification (dev)
    auth_require_captcha_in_nonlocal: bool = True  # enforce reCAPTCHA in staging/production
    frontend_url: str = "http://localhost:5173"  # base URL for password-reset links
    environment: str = "local"  # 'local', 'staging', 'production'

    # --- Activity Logging ---
    activity_logging_enabled: bool = True

    def validate_jwt_secret(self) -> None:
        if self.environment != "local" and self.jwt_secret == "change-me-in-production":
            raise ValueError(
                "JWT_SECRET must be set to a non-default value outside local dev. "
                "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
            )
        if len(self.jwt_secret.encode()) < 32 and self.environment != "local":
            raise ValueError("JWT_SECRET must be at least 32 bytes for HMAC-SHA256")

    def validate_auth_security(self) -> None:
        """Enforce auth security invariants in non-local environments. Fail fast."""
        if self.environment == "local":
            return

        if self.auth_require_captcha_in_nonlocal and not self.recaptcha_secret_key:
            raise ValueError(
                "RECAPTCHA_SECRET_KEY must be set in staging/production. "
                "Get one at https://www.google.com/recaptcha/admin. "
                "Set AUTH_REQUIRE_CAPTCHA_IN_NONLOCAL=false only if you accept the risk."
            )

        if self.verification_email_mode == "log":
            raise ValueError(
                "VERIFICATION_EMAIL_MODE must not be 'log' in staging/production — "
                "verification codes would only appear in server logs, not reach users. "
                "Set VERIFICATION_EMAIL_MODE=smtp and configure SMTP_* variables."
            )

        if not self.frontend_url.startswith("https://"):
            raise ValueError(
                f"FRONTEND_URL must use https:// in staging/production (got: {self.frontend_url}). "
                "Password reset links are sent over email and must use a secure URL."
            )

        _lower = self.frontend_url.lower()
        if "localhost" in _lower or "127.0.0.1" in _lower:
            raise ValueError(
                f"FRONTEND_URL must not point to localhost in staging/production (got: {self.frontend_url})."
            )

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
