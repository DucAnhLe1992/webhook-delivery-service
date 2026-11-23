from functools import lru_cache

from pydantic import HttpUrl, PositiveFloat, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Read from environment variables (or a local .env file), e.g. RECEIVER_URL."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: PostgresDsn = PostgresDsn(
        "postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks"
    )

    # The only destination. Producers never choose where a webhook goes, which keeps
    # the service from being used to make requests to arbitrary addresses (SSRF).
    receiver_url: HttpUrl = HttpUrl("http://127.0.0.1:9000/webhooks")
    delivery_timeout_seconds: PositiveFloat = 5.0

    # Worker: how long to wait before retrying a retryable failure, and how often to
    # look for due deliveries when there is nothing to do.
    retry_delay_seconds: PositiveFloat = 10.0
    poll_interval_seconds: PositiveFloat = 1.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
