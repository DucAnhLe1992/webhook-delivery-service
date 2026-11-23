from functools import lru_cache
from typing import Self

from pydantic import HttpUrl, PositiveFloat, PositiveInt, PostgresDsn, model_validator
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

    # Worker: a delivery gets at most max_attempts attempts. After a retryable failure it
    # waits retry_base_delay_seconds, doubling each time up to retry_max_delay_seconds.
    max_attempts: PositiveInt = 8
    retry_base_delay_seconds: PositiveFloat = 10.0
    retry_max_delay_seconds: PositiveFloat = 3600.0
    # How long a claim lasts. If a worker has not saved its result by then, it is presumed
    # dead and another worker sends the delivery again.
    claim_timeout_seconds: PositiveFloat = 60.0
    poll_interval_seconds: PositiveFloat = 1.0

    @model_validator(mode="after")
    def claim_outlasts_an_attempt(self) -> Self:
        # The timeout applies separately to connecting, sending and reading, so one slow
        # attempt can take several times DELIVERY_TIMEOUT_SECONDS.
        if self.claim_timeout_seconds < 3 * self.delivery_timeout_seconds:
            raise ValueError(
                "CLAIM_TIMEOUT_SECONDS must be at least 3 x DELIVERY_TIMEOUT_SECONDS, "
                "or slow attempts would lose their claim"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
