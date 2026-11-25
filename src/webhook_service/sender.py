"""Outbound HTTP: send one webhook attempt and classify what happened."""

import json
import time
from collections.abc import Sequence
from typing import Any

import httpx2

from webhook_service import signing
from webhook_service.domain import AttemptOutcome, AttemptResult, ErrorCategory

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
    client: httpx2.Client,
    url: str,
    webhook_id: str,
    payload: dict[str, Any],
    *,
    signing_keys: Sequence[bytes],
) -> AttemptResult:
    """Send one signed attempt and classify it by the receiver's status code alone.

    The response body is never read. The status line is the whole answer, and the receiver
    must not be able to make the worker download a large or slow body, or fail to decode one.
    """
    body = json.dumps(payload, separators=(",", ":")).encode()
    # When this attempt was sent; the payload's timestamp is when the event happened. Each
    # attempt is signed afresh, so a retry hours later still passes the receiver's age check.
    timestamp = int(time.time())
    headers = {
        "content-type": "application/json",
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        # Signs these exact bytes, which are sent unchanged.
        "webhook-signature": signing.signature_header(signing_keys, webhook_id, timestamp, body),
    }
    started = time.perf_counter()

    def elapsed_ms() -> int:
        return round((time.perf_counter() - started) * 1000)

    try:
        # Leaving the block closes the response without reading its body.
        with client.stream("POST", url, content=body, headers=headers) as response:
            status_code = response.status_code
    except httpx2.TimeoutException:
        # The receiver may already have processed the event; only its answer is missing.
        return AttemptResult(
            outcome=AttemptOutcome.RETRYABLE_FAILURE,
            error_category=ErrorCategory.TIMEOUT,
            duration_ms=elapsed_ms(),
        )
    except httpx2.RequestError:
        # A refused, reset or dropped connection, or anything else that stopped the request
        # from completing. Every such error is classified here, so no answer from the
        # receiver can crash the worker.
        return AttemptResult(
            outcome=AttemptOutcome.RETRYABLE_FAILURE,
            error_category=ErrorCategory.CONNECTION_ERROR,
            duration_ms=elapsed_ms(),
        )

    outcome = classify_status(status_code)
    return AttemptResult(
        outcome=outcome,
        http_status=status_code,
        error_category=None if outcome is AttemptOutcome.DELIVERED else ErrorCategory.HTTP_ERROR,
        duration_ms=elapsed_ms(),
    )
