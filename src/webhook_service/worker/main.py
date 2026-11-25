"""Delivery worker: sends due deliveries to the receiver and records each result.

Run it with `uv run webhook-worker`. Ctrl+C (or SIGTERM) stops it after the current delivery.
It waits for the database at startup, and does not start if the database's schema is older
than this code.
"""

import logging
import os
import signal
import sys
import threading
import time
from datetime import UTC, datetime, timedelta

import httpx2
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from webhook_service.config import Settings, get_settings
from webhook_service.db import describe_error, get_engine, storage_unavailable
from webhook_service.domain import AttemptOutcome, AttemptResult, DeliveryStatus, ErrorCategory
from webhook_service.logs import configure_logging
from webhook_service.metrics import start_metrics_server, stop_metrics_server
from webhook_service.retry import backoff_delay
from webhook_service.schema import schema_problem
from webhook_service.schemas import WebhookMessage
from webhook_service.sender import send_webhook
from webhook_service.services import delivery_service
from webhook_service.worker import metrics

log = logging.getLogger("worker")


def process_one(engine: Engine, client: httpx2.Client, settings: Settings) -> bool:
    """Claim, send and record one due delivery. Returns False if nothing was due."""
    with Session(engine, expire_on_commit=False) as session:
        delivery = delivery_service.claim_next_due(
            session,
            claim_timeout=timedelta(seconds=settings.claim_timeout_seconds),
            max_attempts=settings.max_attempts,
            on_expired_claim=report_expired_claim,
        )
        if delivery is None:
            return False
        producer_name = delivery.producer.name
    attempt, claim_token = delivery.attempt_count, delivery.claim_token
    assert claim_token is not None

    # The claim is committed, so no transaction is held open while the receiver answers.
    message = WebhookMessage(
        type=delivery.event_type,
        producer=producer_name,
        timestamp=delivery.created_at,
        data=delivery.payload,
    )
    result = send_webhook(
        client,
        str(settings.receiver_url),
        str(delivery.id),
        message.model_dump(mode="json"),
        signing_keys=settings.signing_keys,
    )

    retry_delay = backoff_delay(
        # Counted from the last replay, if any: a replayed delivery starts the schedule again.
        attempt - delivery.attempt_base,
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

    fields = {
        "delivery_id": str(delivery.id),
        "attempt": attempt,
        "producer": producer_name,
        "event_type": delivery.event_type,
    }
    if status is None:
        metrics.DISCARDED.inc()
        log.warning(
            "%s attempt %d: result discarded; the claim expired and another worker "
            "took the delivery over",
            delivery.id,
            attempt,
            extra={"event": "attempt_discarded", **fields},
        )
        return True

    metrics.ATTEMPTS.labels(result.outcome, result.error_category or "none").inc()
    metrics.ATTEMPT_DURATION.labels(result.outcome).observe(result.duration_ms / 1000)
    if status is not DeliveryStatus.PENDING:
        metrics.FINISHED.labels(status).inc()
    if status is DeliveryStatus.DELIVERED:
        waited = datetime.now(UTC) - delivery.created_at
        metrics.DELIVERY_LATENCY.observe(max(0.0, waited.total_seconds()))
    fields |= {
        "event": "attempt_finished",
        "outcome": result.outcome,
        "http_status": result.http_status,
        "error_category": result.error_category,
        "duration_ms": result.duration_ms,
        "status": status,
    }
    if status is DeliveryStatus.PENDING:
        fields["retry_in_seconds"] = round(retry_delay.total_seconds(), 1)
    log.info(
        "%s attempt %d: %s",
        delivery.id,
        attempt,
        describe(result, status, retry_delay),
        extra=fields,
    )
    return True


def report_expired_claim(expired: delivery_service.ExpiredClaim) -> None:
    """A worker stopped or froze before saving a result, and its claim expired."""
    metrics.ATTEMPTS.labels(AttemptOutcome.RETRYABLE_FAILURE, ErrorCategory.CLAIM_EXPIRED).inc()
    if expired.delivery_failed:
        metrics.FINISHED.labels(DeliveryStatus.FAILED).inc()
        what = "and no attempts are left: failed"
    else:
        what = "so the delivery is sent again"
    log.warning(
        "%s attempt %d: the claim expired without a result, %s",
        expired.delivery_id,
        expired.attempt,
        what,
        extra={
            "event": "claim_expired",
            "delivery_id": str(expired.delivery_id),
            "attempt": expired.attempt,
            "delivery_failed": expired.delivery_failed,
        },
    )


def describe(result: AttemptResult, status: DeliveryStatus, retry_delay: timedelta) -> str:
    answer = f"HTTP {result.http_status}" if result.http_status else result.error_category
    if status is DeliveryStatus.DELIVERED:
        return f"delivered ({answer}, {result.duration_ms} ms)"
    if status is DeliveryStatus.PENDING:
        return f"failed ({answer}), retrying in {retry_delay.total_seconds():.0f}s"
    if result.outcome is AttemptOutcome.RETRYABLE_FAILURE:
        return f"failed ({answer}), no attempts left"
    return f"failed permanently ({answer})"


class DatabaseErrors:
    """Logs the first database error, a reminder every minute while errors go on, and the end.

    During an outage the worker tries again on every poll. A line for each try would be a line
    a second; the webhooks_worker_errors_total metric counts them instead.
    """

    REMIND_AFTER_SECONDS = 60.0

    def __init__(self, settings: Settings) -> None:
        self.poll_interval = settings.poll_interval_seconds
        self.since: float | None = None
        self.reminded = 0.0

    def failed(self, error: BaseException) -> None:
        metrics.ERRORS.labels("database").inc()
        now = time.monotonic()
        reason = describe_error(error)
        fields = {"event": "database_error", "reason": reason}
        if self.since is None:
            self.since = self.reminded = now
            log.warning(
                "database error (%s), trying again every %ss",
                reason,
                self.poll_interval,
                extra=fields,
            )
        elif now - self.reminded >= self.REMIND_AFTER_SECONDS:
            self.reminded = now
            failing = round(now - self.since)
            log.warning(
                "database errors for %ds, the latest: %s",
                failing,
                reason,
                extra=fields | {"failing_for_seconds": failing},
            )

    def succeeded(self) -> None:
        if self.since is not None:
            failing = round(time.monotonic() - self.since)
            log.info(
                "database working again after %ds",
                failing,
                extra={"event": "database_recovered", "failing_for_seconds": failing},
            )
            self.since = None


def run(engine: Engine, client: httpx2.Client, settings: Settings, stop: threading.Event) -> None:
    database_errors = DatabaseErrors(settings)
    while not stop.is_set():
        try:
            worked = process_one(engine, client, settings)
            database_errors.succeeded()
        except Exception as error:
            if storage_unavailable(error):
                # Unreachable, or a time limit ran out. A delivery claimed before the error is
                # sent again once its claim expires.
                database_errors.failed(error)
            else:
                # A bug must not stop every delivery. A delivery claimed before the error is
                # sent again once its claim expires, and the attempt still counts.
                metrics.ERRORS.labels("unexpected").inc()
                log.exception(
                    "unexpected error, trying again in %ss",
                    settings.poll_interval_seconds,
                    extra={"event": "unexpected_error"},
                )
            worked = False
        if not worked:
            # Sleep until the next poll, but wake up at once when asked to stop.
            stop.wait(settings.poll_interval_seconds)


def wait_for_database(engine: Engine, settings: Settings, stop: threading.Event) -> bool:
    """Wait until the database answers, then exit unless its schema is recent enough.

    Returns False if asked to stop while waiting. A database that is down at startup is waited
    for, as it is later on. A schema that is behind is not: every claim would fail, so the worker
    could only log the same error on every poll until someone ran the migrations.
    """
    database_errors = DatabaseErrors(settings)
    while not stop.is_set():
        try:
            with engine.connect() as connection:
                problem = schema_problem(connection)
        except Exception as error:
            if not storage_unavailable(error):
                raise
            database_errors.failed(error)
            stop.wait(settings.poll_interval_seconds)
            continue
        database_errors.succeeded()
        if problem is not None:
            sys.exit(f"The worker cannot start: {problem}")
        return True
    return False


def main() -> None:
    settings = get_settings()
    configure_logging("worker", settings)
    if not settings.webhook_secrets:
        sys.exit(
            "WEBHOOK_SECRETS is not set, and the worker does not send unsigned webhooks. Create "
            "a secret with `uv run webhook-admin new-webhook-secret`, set WEBHOOK_SECRETS to it, "
            "and give the receiver the same secret."
        )
    stop = threading.Event()

    def request_stop(signum: int, frame: object) -> None:
        log.info("stopping after the current delivery", extra={"event": "worker_stopping"})
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    metrics_server = start_metrics_server(
        settings.metrics_host, settings.worker_metrics_port, setting="WORKER_METRICS_PORT"
    )
    log.info(
        "started (pid %d), sending to %s, signing with %d secret(s)",
        os.getpid(),
        settings.receiver_url,
        len(settings.webhook_secrets),
        extra={"event": "worker_started", "pid": os.getpid()},
    )
    engine = get_engine()
    try:
        if wait_for_database(engine, settings, stop):
            with httpx2.Client(
                timeout=settings.delivery_timeout_seconds, follow_redirects=False
            ) as client:
                run(engine, client, settings, stop)
    finally:
        stop_metrics_server(metrics_server)
    log.info("stopped", extra={"event": "worker_stopped"})


if __name__ == "__main__":
    main()
