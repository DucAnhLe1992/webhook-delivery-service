from functools import lru_cache
from typing import Annotated, Self

from pydantic import (
    HttpUrl,
    PositiveFloat,
    PositiveInt,
    PostgresDsn,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from webhook_service.signing import decode_secret


class Settings(BaseSettings):
    """Read from environment variables (or a local .env file), e.g. RECEIVER_URL."""

    # An invalid value is not repeated in the error message, because some values are secrets
    # (WEBHOOK_SECRETS, the password in DATABASE_URL) and error messages end up in logs.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    database_url: PostgresDsn = PostgresDsn(
        "postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks"
    )

    # API: the largest request body it reads, in bytes, which limits the size of an event.
    max_event_bytes: PositiveInt = 65536

    # The only destination. Producers never choose where a webhook goes, which keeps
    # the service from being used to make requests to arbitrary addresses (SSRF). Deliveries
    # waiting for a retry go to the URL a worker has when it sends them, so every worker
    # must use the same value.
    receiver_url: HttpUrl = HttpUrl("http://127.0.0.1:9000/webhooks")
    # How long each network step of an attempt may take; see claim_outlasts_an_attempt.
    delivery_timeout_seconds: PositiveFloat = 5.0
    # Worker: the secrets it signs webhooks with, separated by spaces. Each is whsec_ followed
    # by 24 to 64 random bytes in base64 (`webhook-admin new-webhook-secret` makes one). Every
    # secret signs every webhook, so a receiver can move to a new secret without missing any.
    webhook_secrets: Annotated[list[SecretStr], NoDecode] = []

    # Worker: a delivery gets at most max_attempts attempts. After a retryable failure it
    # waits retry_base_delay_seconds, doubling each time up to retry_max_delay_seconds.
    max_attempts: PositiveInt = 8
    retry_base_delay_seconds: PositiveFloat = 10.0
    retry_max_delay_seconds: PositiveFloat = 3600.0
    # How long a claim lasts. If a worker has not saved its result by then, it is presumed
    # dead and another worker sends the delivery again.
    claim_timeout_seconds: PositiveFloat = 60.0
    poll_interval_seconds: PositiveFloat = 1.0

    @field_validator("webhook_secrets", mode="before")
    @classmethod
    def split_on_spaces(cls, value: object) -> object:
        return value.split() if isinstance(value, str) else value

    @field_validator("webhook_secrets")
    @classmethod
    def secrets_are_valid(cls, secrets: list[SecretStr]) -> list[SecretStr]:
        for number, secret in enumerate(secrets, start=1):
            try:
                decode_secret(secret.get_secret_value())
            except ValueError as error:
                raise ValueError(f"secret {number} in WEBHOOK_SECRETS: {error}") from None
        return secrets

    @property
    def signing_keys(self) -> list[bytes]:
        return [decode_secret(secret.get_secret_value()) for secret in self.webhook_secrets]

    @model_validator(mode="after")
    def claim_outlasts_an_attempt(self) -> Self:
        # The timeout applies to each network step separately: connecting, sending the request
        # and waiting for the receiver's status line. A receiver that is slow at every step
        # takes about 3 x the timeout, so a claim must last at least that long. It is not a
        # hard limit: a receiver that sends its status line a few bytes at a time can outlast
        # any claim. Another worker then sends the delivery again, with the same webhook-id,
        # and the late result is discarded.
        if self.claim_timeout_seconds < 3 * self.delivery_timeout_seconds:
            raise ValueError(
                "CLAIM_TIMEOUT_SECONDS must be at least 3 x DELIVERY_TIMEOUT_SECONDS, so that "
                "an attempt that is slow at every step keeps its claim"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
