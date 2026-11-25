"""Delivery worker: sends due deliveries to the receiver and records each result.

Run it with `uv run webhook-worker`. Ctrl+C (or SIGTERM) stops it after the current delivery.
"""

import logging
import os
import signal
import threading
from datetime import timedelta

import httpx2
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from webhook_service.config import Settings, get_settings
from webhook_service.db import get_engine
from webhook_service.domain import AttemptOutcome, AttemptResult, DeliveryStatus
from webhook_service.retry import backoff_delay
from webhook_service.schemas import WebhookMessage
from webhook_service.sender import send_webhook
from webhook_service.services import delivery_service

log = logging.getLogger("worker")


def process_one(engine: Engine, client: httpx2.Client, settings: Settings) -> bool:
    """Claim, send and record one due delivery. Returns False if nothing was due."""
    with Session(engine, expire_on_commit=False) as session:
        delivery = delivery_service.claim_next_due(
            session,
            claim_timeout=timedelta(seconds=settings.claim_timeout_seconds),
            max_attempts=settings.max_attempts,
        )
    if delivery is None:
        return False
    attempt, claim_token = delivery.attempt_count, delivery.claim_token
    assert claim_token is not None

    # The claim is committed, so no transaction is held open while the receiver answers.
    message = WebhookMessage(
        type=delivery.event_type, timestamp=delivery.created_at, data=delivery.payload
    )
    result = send_webhook(
        client, str(settings.receiver_url), str(delivery.id), message.model_dump(mode="json")
    )

    retry_delay = backoff_delay(
        attempt,
        base_seconds=settings.retry_base_delay_seconds,
        max_seconds=settings.retry_max_delay_seconds,
    )
    with Session(engine, expire_on_commit=False) as session:
        status = delivery_service.record_attempt(
            session,
            delivery.id,
            claim_token,
            result,
            retry_delay=retry_delay,
            max_attempts=settings.max_attempts,
        )

    if status is None:
        log.warning(
            "%s attempt %d: result discarded; the claim expired and another worker "
            "took the delivery over",
            delivery.id,
            attempt,
        )
    else:
        log.info("%s attempt %d: %s", delivery.id, attempt, describe(result, status, retry_delay))
    return True


def describe(result: AttemptResult, status: DeliveryStatus, retry_delay: timedelta) -> str:
    answer = f"HTTP {result.http_status}" if result.http_status else result.error_category
    if status is DeliveryStatus.DELIVERED:
        return f"delivered ({answer}, {result.duration_ms} ms)"
    if status is DeliveryStatus.PENDING:
        return f"failed ({answer}), retrying in {retry_delay.total_seconds():.0f}s"
    if result.outcome is AttemptOutcome.RETRYABLE_FAILURE:
        return f"failed ({answer}), no attempts left"
    return f"failed permanently ({answer})"


def run(engine: Engine, client: httpx2.Client, settings: Settings, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            worked = process_one(engine, client, settings)
        except OperationalError:
            log.warning("database unavailable, trying again in %ss", settings.poll_interval_seconds)
            worked = False
        except Exception:
            # A bug must not stop every delivery. A delivery claimed before the error is sent
            # again once its claim expires, and the attempt still counts towards the limit.
            log.exception("unexpected error, trying again in %ss", settings.poll_interval_seconds)
            worked = False
        if not worked:
            # Sleep until the next poll, but wake up at once when asked to stop.
            stop.wait(settings.poll_interval_seconds)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [worker] %(message)s", datefmt="%H:%M:%S"
    )
    # HTTPX2 logs every request at INFO; the worker already logs one line per attempt.
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    settings = get_settings()
    stop = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        log.info("stopping after the current delivery")
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    log.info("started (pid %d), sending to %s", os.getpid(), settings.receiver_url)
    with httpx2.Client(timeout=settings.delivery_timeout_seconds, follow_redirects=False) as client:
        run(get_engine(), client, settings, stop)
    log.info("stopped")


if __name__ == "__main__":
    main()
