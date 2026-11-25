"""The vocabulary shared by the API, the worker and the database: delivery states and attempt
outcomes. It imports nothing else from the service, so every layer can depend on it."""

from enum import StrEnum

from pydantic import BaseModel


class DeliveryStatus(StrEnum):
    """pending -> processing -> delivered, back to pending to retry, or failed."""

    PENDING = "pending"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"


class AttemptOutcome(StrEnum):
    DELIVERED = "delivered"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"


class ErrorCategory(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    HTTP_ERROR = "http_error"
    # Recorded by the worker that takes over an expired claim, not by the sender: the
    # previous worker stopped before saving a result, so the receiver may have the event.
    CLAIM_EXPIRED = "claim_expired"


class AttemptResult(BaseModel):
    outcome: AttemptOutcome
    http_status: int | None = None
    error_category: ErrorCategory | None = None
    duration_ms: int
