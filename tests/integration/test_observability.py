import json
import logging
import re
import uuid
from collections.abc import Iterator
from datetime import timedelta

from prometheus_client import REGISTRY, CollectorRegistry
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from webhook_service.api.metrics import QueueCollector
from webhook_service.config import get_settings
from webhook_service.db import get_session
from webhook_service.main import app
from webhook_service.services import delivery_service

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
NEW_REQUEST_ID = re.compile(r"[0-9a-f]{32}")


REQUESTS = "webhooks_http_requests_total"


def submit(client) -> str:
    return client.post("/deliveries", json=EVENT).json()["delivery_id"]


def test_a_submission_is_logged_with_its_fields_and_its_request_id(client, json_logs):
    response = client.post("/deliveries", json=EVENT, headers={"Idempotency-Key": "booking-42"})

    stored, finished = json_logs()
    delivery_id = response.json()["delivery_id"]
    request_id = response.headers["x-request-id"]
    assert NEW_REQUEST_ID.fullmatch(request_id)
    assert stored["message"] == f"stored delivery {delivery_id}, booking.confirmed from carrental"
    assert (
        stored.items()
        >= {
            "level": "info",
            "event": "delivery_stored",
            "delivery_id": delivery_id,
            "producer": "carrental",
            "event_type": "booking.confirmed",
            "idempotency_key": "booking-42",
            "request_id": request_id,
        }.items()
    )
    assert (
        finished.items()
        >= {
            "level": "info",
            "event": "request_finished",
            "method": "POST",
            "route": "/deliveries",
            "path": "/deliveries",
            "status": 202,
            "producer": "carrental",
            "request_id": request_id,
        }.items()
    )
    assert isinstance(finished["duration_ms"], int)


def test_a_usable_request_id_is_kept_and_any_other_is_replaced(client):
    kept = client.get("/health", headers={"X-Request-ID": "carrental:checkout-7.1"})
    too_long = client.get("/health", headers={"X-Request-ID": "x" * 129})
    with_a_space = client.get("/health", headers={"X-Request-ID": "two words"})

    assert kept.headers["x-request-id"] == "carrental:checkout-7.1"
    assert NEW_REQUEST_ID.fullmatch(too_long.headers["x-request-id"])
    assert NEW_REQUEST_ID.fullmatch(with_a_space.headers["x-request-id"])


def test_probes_are_logged_only_at_debug_level(client, json_logs):
    client.get("/health")
    assert json_logs() == []

    logging.getLogger().setLevel(logging.DEBUG)
    client.get("/health")

    [line] = [line for line in json_logs() if line.get("event") == "request_finished"]
    assert (line["level"], line["route"], line["status"]) == ("debug", "/health", 200)


def test_the_logs_hold_no_api_key_and_no_event_data(client, api_key, json_logs):
    guest = "Jane Doe, +358 40 123 4567"
    response = client.post(
        "/deliveries", json={"event_type": "booking.confirmed", "data": {"guest": guest}}
    )
    client.get(f"/deliveries/{response.json()['delivery_id']}")

    logged = json.dumps(json_logs())

    assert "booking.confirmed" in logged
    assert api_key not in logged
    assert guest not in logged


def test_requests_are_counted_by_route_template(client, count_changes):
    delivery_id = submit(client)
    route = "/deliveries/{delivery_id}"
    changes = count_changes(
        found=(REQUESTS, {"method": "GET", "route": route, "status": "200"}),
        unknown_id=(REQUESTS, {"method": "GET", "route": route, "status": "404"}),
        no_route=(REQUESTS, {"method": "GET", "route": "none", "status": "404"}),
        too_large=(REQUESTS, {"method": "POST", "route": "none", "status": "413"}),
        timed=(
            "webhooks_http_request_duration_seconds_count",
            {"method": "GET", "route": route},
        ),
    )

    client.get(f"/deliveries/{delivery_id}")
    client.get(f"/deliveries/{uuid.uuid4()}")
    client.get("/no-such-page")
    # Refused by its Content-Length header, before it reaches a route.
    client.post(
        "/deliveries",
        content=b"x" * (get_settings().max_event_bytes + 1),
        headers={"Content-Type": "application/json"},
    )

    assert changes() == {"found": 1, "unknown_id": 1, "no_route": 1, "too_large": 1, "timed": 2}
    # One series per route, not one per delivery.
    routes = {
        sample.labels["route"]
        for metric in REGISTRY.collect()
        if metric.name == "webhooks_http_requests"
        for sample in metric.samples
    }
    assert not any(delivery_id in route for route in routes)


def test_producers_cannot_read_the_metrics(client):
    # They are labelled by producer, so they are served on a port of their own, not the API's.
    assert client.get("/metrics").status_code == 404


def test_submissions_are_counted_as_stored_or_repeat(client, count_changes):
    submissions = "webhooks_submissions_total"
    changes = count_changes(
        stored=(submissions, {"producer": "carrental", "result": "stored"}),
        repeat=(submissions, {"producer": "carrental", "result": "repeat"}),
    )

    for _ in range(2):
        client.post("/deliveries", json=EVENT, headers={"Idempotency-Key": "booking-42"})

    assert changes() == {"stored": 1, "repeat": 1}


def test_an_unavailable_database_is_logged_with_the_request_it_failed(
    client, unreachable_engine, json_logs
):
    def no_database() -> Iterator[Session]:
        with Session(unreachable_engine) as session:
            yield session

    app.dependency_overrides[get_session] = no_database

    response = client.post("/deliveries", json=EVENT, headers={"X-Request-ID": "outage-1"})

    assert response.status_code == 503
    assert response.headers["x-request-id"] == "outage-1"
    error, finished = json_logs()
    assert (
        error.items()
        >= {
            "level": "warning",
            "event": "storage_unavailable",
            "request_id": "outage-1",
        }.items()
    )
    assert "refused" in error["reason"]
    assert (
        finished.items()
        >= {
            "level": "warning",
            "event": "request_finished",
            "status": 503,
            "request_id": "outage-1",
        }.items()
    )


def read_queue(engine: Engine) -> dict[tuple[str, str | None], float]:
    registry = CollectorRegistry()
    registry.register(QueueCollector(engine))
    return {
        (sample.name, sample.labels.get("status")): sample.value
        for metric in registry.collect()
        for sample in metric.samples
    }


def test_the_queue_metrics_are_read_from_the_database(client, db):
    submit(client)  # Claimed next, so processing, and its claim has not expired.
    with Session(db) as session:
        delivery_service.claim_next_due(
            session, claim_timeout=timedelta(seconds=60), max_attempts=3
        )
    submit(client)  # Due now.
    late, scheduled, failed = (submit(client) for _ in range(3))
    with db.begin() as connection:
        for delivery_id, change in (
            (late, "next_attempt_at = now() - interval '2 minutes'"),
            (scheduled, "next_attempt_at = now() + interval '1 minute'"),
            (failed, "status = 'failed'"),
        ):
            connection.execute(
                text(f"UPDATE deliveries SET {change} WHERE id = :id"), {"id": delivery_id}
            )

    queue = read_queue(db)

    assert queue[("webhooks_database_up", None)] == 1
    assert queue[("webhooks_queue_due", None)] == 2
    assert 120 <= queue[("webhooks_queue_oldest_due_seconds", None)] < 130
    assert queue[("webhooks_deliveries", "pending")] == 3
    assert queue[("webhooks_deliveries", "processing")] == 1
    assert queue[("webhooks_deliveries", "failed")] == 1


def test_the_queue_metrics_report_a_database_that_cannot_be_read(unreachable_engine):
    assert read_queue(unreachable_engine) == {("webhooks_database_up", None): 0}
