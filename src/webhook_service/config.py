from functools import lru_cache

from pydantic import HttpUrl, PositiveFloat
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Read from environment variables (or a local .env file), e.g. RECEIVER_URL."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # The only destination. Producers never choose where a webhook goes, which keeps
    # the service from being used to make requests to arbitrary addresses (SSRF).
    receiver_url: HttpUrl = HttpUrl("http://127.0.0.1:9000/webhooks")
    delivery_timeout_seconds: PositiveFloat = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
