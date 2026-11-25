"""The worker's metrics. Each worker counts what it did; Prometheus adds them up."""

from prometheus_client import Counter, Histogram

from webhook_service.domain import AttemptOutcome, DeliveryStatus, ErrorCategory

ATTEMPTS = Counter(
    "webhooks_attempts",
    "Attempts that ended, by outcome and error category (none for a 2xx).",
    ["outcome", "error_category"],
)
ATTEMPT_DURATION = Histogram(
    "webhooks_attempt_duration_seconds",
    "How long an attempt took, until the receiver's status code or the error.",
    ["outcome"],
)
FINISHED = Counter(
    "webhooks_deliveries_finished",
    "Deliveries that were delivered, or failed.",
    ["status"],
)
DELIVERY_LATENCY = Histogram(
    "webhooks_delivery_latency_seconds",
    "From acceptance to delivery, for delivered deliveries.",
    buckets=(0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400),
)
DISCARDED = Counter(
    "webhooks_results_discarded",
    "Attempt results not saved, because another worker had taken the delivery over.",
)
ERRORS = Counter(
    "webhooks_worker_errors",
    "Errors in the worker loop: database (unreachable, or a limit ran out), or unexpected (a bug).",
    ["kind"],
)

# Create every series now, at 0. Prometheus cannot see the step from a series that does not
# exist yet to its first value, so without this, rate() and increase() would miss the first
# failure after each worker start, and so would an alert on it.
for outcome, category in (
    (AttemptOutcome.DELIVERED, "none"),
    (AttemptOutcome.RETRYABLE_FAILURE, ErrorCategory.TIMEOUT),
    (AttemptOutcome.RETRYABLE_FAILURE, ErrorCategory.CONNECTION_ERROR),
    (AttemptOutcome.RETRYABLE_FAILURE, ErrorCategory.HTTP_ERROR),
    (AttemptOutcome.RETRYABLE_FAILURE, ErrorCategory.CLAIM_EXPIRED),
    (AttemptOutcome.PERMANENT_FAILURE, ErrorCategory.HTTP_ERROR),
):
    ATTEMPTS.labels(outcome, category)
for outcome in AttemptOutcome:
    ATTEMPT_DURATION.labels(outcome)
for status in (DeliveryStatus.DELIVERED, DeliveryStatus.FAILED):
    FINISHED.labels(status)
for kind in ("database", "unexpected"):
    ERRORS.labels(kind)
