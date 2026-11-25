"""The API's metrics: requests, submissions, and the state of the queue in the database."""

from collections.abc import Iterator

from prometheus_client import Counter, Histogram
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from webhook_service.db import storage_unavailable
from webhook_service.services import delivery_service

HTTP_REQUESTS = Counter(
    "webhooks_http_requests",
    "Requests the API answered, by route template (none: answered before routing).",
    ["method", "route", "status"],
)
HTTP_DURATION = Histogram(
    "webhooks_http_request_duration_seconds",
    "How long the API took to answer.",
    ["method", "route"],
)
SUBMISSIONS = Counter(
    "webhooks_submissions",
    "Deliveries submitted: stored, or a repeat with a known Idempotency-Key.",
    ["producer", "result"],
)


class QueueCollector(Collector):
    """Reads the state of the queue from the database at each scrape.

    The worker's own metrics vanish when every worker is down, which is exactly when the queue
    grows. These come from the API, so they still show it.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def describe(self) -> Iterator[Metric]:
        # Names only: without this, registering the collector would query the database.
        yield from self.families()

    def collect(self) -> Iterator[Metric]:
        up, due, oldest, by_status = self.families()
        try:
            with Session(self.engine) as session:
                snapshot = delivery_service.queue_snapshot(session)
        except Exception as error:
            if not storage_unavailable(error):
                raise
            up.add_metric([], 0)
            yield up
            return
        up.add_metric([], 1)
        due.add_metric([], snapshot.due)
        oldest.add_metric([], snapshot.oldest_due_seconds)
        for status, count in snapshot.by_status.items():
            by_status.add_metric([status], count)
        yield from (up, due, oldest, by_status)

    @staticmethod
    def families() -> tuple[GaugeMetricFamily, ...]:
        return (
            GaugeMetricFamily(
                "webhooks_database_up", "1 if the queue could be read from the database."
            ),
            GaugeMetricFamily("webhooks_queue_due", "Deliveries due to be sent now."),
            GaugeMetricFamily(
                "webhooks_queue_oldest_due_seconds",
                "How long the oldest due delivery has been waiting for a worker.",
            ),
            GaugeMetricFamily(
                "webhooks_deliveries",
                "Deliveries that are pending, processing or failed.",
                labels=["status"],
            ),
        )
