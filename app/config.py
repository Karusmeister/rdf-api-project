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


settings = Settings()
