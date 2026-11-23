"""Outbound HTTP: send one webhook attempt and classify what happened."""

import json
import time
from enum import StrEnum
from typing import Any

import httpx2
from pydantic import BaseModel


class AttemptOutcome(StrEnum):
    DELIVERED = "delivered"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"


class ErrorCategory(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION_ERROR = "connection_error"
    HTTP_ERROR = "http_error"


class AttemptResult(BaseModel):
    outcome: AttemptOutcome
    http_status: int | None = None
    error_category: ErrorCategory | None = None
    duration_ms: int


RETRYABLE_STATUS_CODES = {408, 429}


def classify_status(status_code: int) -> AttemptOutcome:
    if 200 <= status_code < 300:
        return AttemptOutcome.DELIVERED
    if status_code >= 500 or status_code in RETRYABLE_STATUS_CODES:
        return AttemptOutcome.RETRYABLE_FAILURE
    # Any other answer means the receiver rejected this message, so sending it again
    # will not help. Redirects (3xx) land here too because they are never followed.
    return AttemptOutcome.PERMANENT_FAILURE


def send_webhook(
    client: httpx2.Client, url: str, webhook_id: str, payload: dict[str, Any]
) -> AttemptResult:
    body = json.dumps(payload, separators=(",", ":")).encode()
    headers = {
        "content-type": "application/json",
        "webhook-id": webhook_id,
        # When this attempt was sent; the payload's timestamp is when the event happened.
        "webhook-timestamp": str(int(time.time())),
    }
    started = time.perf_counter()

    def elapsed_ms() -> int:
        return round((time.perf_counter() - started) * 1000)

    try:
        response = client.post(url, content=body, headers=headers)
    except httpx2.TimeoutException:
        # The receiver may already have processed the event; only its answer is missing.
        return AttemptResult(
            outcome=AttemptOutcome.RETRYABLE_FAILURE,
            error_category=ErrorCategory.TIMEOUT,
            duration_ms=elapsed_ms(),
        )
    except httpx2.TransportError:
        return AttemptResult(
            outcome=AttemptOutcome.RETRYABLE_FAILURE,
            error_category=ErrorCategory.CONNECTION_ERROR,
            duration_ms=elapsed_ms(),
        )

    outcome = classify_status(response.status_code)
    return AttemptResult(
        outcome=outcome,
        http_status=response.status_code,
        error_category=None if outcome is AttemptOutcome.DELIVERED else ErrorCategory.HTTP_ERROR,
        duration_ms=elapsed_ms(),
    )
