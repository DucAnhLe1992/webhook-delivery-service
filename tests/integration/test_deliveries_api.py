import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from webhook_service.config import get_settings
from webhook_service.db import get_session
from webhook_service.main import app
from webhook_service.services import delivery_service

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
KEY = {"Idempotency-Key": "booking-42-confirmed"}


def test_accepted_delivery_is_stored_as_pending(client):
    response = client.post("/deliveries", json=EVENT)

    assert response.status_code == 202
    accepted = response.json()
    assert accepted["status"] == "pending"
    assert response.headers["location"] == f"/deliveries/{accepted['delivery_id']}"

    # A separate request, so this only passes if the first one committed.
    stored = client.get(response.headers["location"])

    assert stored.status_code == 200
    delivery = stored.json()
    assert delivery["delivery_id"] == accepted["delivery_id"]
    assert delivery["event_type"] == "booking.confirmed"
    assert delivery["data"] == {"booking_id": 42}
    assert delivery["status"] == "pending"
    assert delivery["attempt_count"] == 0


def test_unknown_delivery_returns_404(client):
    response = client.get(f"/deliveries/{uuid.uuid4()}")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "payload",
    [
        {"event_type": "Booking Confirmed!", "data": {}},
        {"event_type": "booking.confirmed"},
        {**EVENT, "url": "http://169.254.169.254/latest/meta-data"},
    ],
    ids=["bad-event-type", "missing-data", "producer-chosen-url"],
)
def test_invalid_request_is_rejected_and_not_stored(client, engine, payload):
    response = client.post("/deliveries", json=payload)

    assert response.status_code == 422
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deliveries")) == 0


def test_repeating_a_request_with_the_same_idempotency_key_returns_the_original(client, db):
    first = client.post("/deliveries", json=EVENT, headers=KEY)
    # Same content with the keys in a different order.
    repeat = client.post(
        "/deliveries",
        json={"data": {"booking_id": 42}, "event_type": "booking.confirmed"},
        headers=KEY,
    )

    assert first.status_code == repeat.status_code == 202
    assert repeat.json()["delivery_id"] == first.json()["delivery_id"]
    assert "idempotent-replayed" not in first.headers
    assert repeat.headers["idempotent-replayed"] == "true"
    with db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deliveries")) == 1


def test_reusing_an_idempotency_key_for_different_content_is_rejected(client):
    client.post("/deliveries", json=EVENT, headers=KEY)

    response = client.post("/deliveries", json={**EVENT, "data": {"booking_id": 43}}, headers=KEY)

    assert response.status_code == 409


def submit_while_another_request_stores_the_key(
    client, db, wait_until_blocked, *, data, other_request_commits
):
    """Submit with KEY while another request has inserted KEY but not yet committed.

    Returns the other request's delivery ID and the response to this submission.
    """
    other_id = uuid.uuid4()
    with db.connect() as other_request, ThreadPoolExecutor(max_workers=1) as pool:
        other_request.execute(
            text(
                "INSERT INTO deliveries"
                " (id, producer_id, event_type, payload, idempotency_key, request_fingerprint)"
                " SELECT :id, id, :event_type, CAST(:payload AS jsonb), :key, :fingerprint"
                " FROM producers WHERE name = 'carrental'"
            ),
            {
                "id": other_id,
                "event_type": EVENT["event_type"],
                "payload": json.dumps(EVENT["data"]),
                "key": KEY["Idempotency-Key"],
                "fingerprint": delivery_service.request_fingerprint(
                    EVENT["event_type"], EVENT["data"]
                ),
            },
        )
        submission = pool.submit(
            client.post, "/deliveries", json={**EVENT, "data": data}, headers=KEY
        )
        # This submission's insert waits to see whether the other request commits.
        wait_until_blocked()
        if other_request_commits:
            other_request.commit()
        else:
            other_request.rollback()
        return other_id, submission.result(timeout=10)


def test_concurrent_requests_with_the_same_key_store_one_delivery(client, db, wait_until_blocked):
    other_id, response = submit_while_another_request_stores_the_key(
        client, db, wait_until_blocked, data=EVENT["data"], other_request_commits=True
    )

    assert response.status_code == 202
    assert response.json()["delivery_id"] == str(other_id)
    assert response.headers["idempotent-replayed"] == "true"


def test_concurrent_requests_with_the_same_key_and_different_content_conflict(
    client, db, wait_until_blocked
):
    _, response = submit_while_another_request_stores_the_key(
        client, db, wait_until_blocked, data={"booking_id": 43}, other_request_commits=True
    )

    assert response.status_code == 409


def test_a_key_released_by_a_rolled_back_request_is_stored_by_the_waiting_one(
    client, db, wait_until_blocked
):
    other_id, response = submit_while_another_request_stores_the_key(
        client, db, wait_until_blocked, data=EVENT["data"], other_request_commits=False
    )

    # The other request never committed, so there is nothing to replay: this one is stored.
    assert response.status_code == 202
    assert "idempotent-replayed" not in response.headers
    assert response.json()["delivery_id"] != str(other_id)
    with db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deliveries")) == 1


def test_a_request_that_waits_too_long_for_a_lock_gets_503(client, db, short_limits_engine):
    def short_limits_session():
        with Session(short_limits_engine, expire_on_commit=False) as session:
            yield session

    pool = ThreadPoolExecutor(max_workers=1)
    with db.connect() as stuck:
        # A request that inserted KEY and then froze before committing.
        stuck.execute(
            text(
                "INSERT INTO deliveries (id, producer_id, event_type, payload, idempotency_key)"
                " SELECT gen_random_uuid(), id, 'booking.confirmed', '{}', :key"
                " FROM producers WHERE name = 'carrental'"
            ),
            {"key": KEY["Idempotency-Key"]},
        )
        app.dependency_overrides[get_session] = short_limits_session
        started = time.monotonic()
        try:
            response = pool.submit(client.post, "/deliveries", json=EVENT, headers=KEY).result(
                timeout=10
            )
        finally:
            elapsed = time.monotonic() - started
            stuck.rollback()
            pool.shutdown(wait=False)

    # Without a lock timeout, the request would wait for as long as the stuck transaction lasts.
    assert response.status_code == 503
    assert elapsed < 5
    assert "Retry with the same Idempotency-Key" in response.json()["detail"]


class CommitThenConnectionLost(Session):
    """PostgreSQL commits, but the connection fails before the API hears back."""

    def commit(self) -> None:
        super().commit()
        raise OperationalError("COMMIT", {}, Exception("server closed the connection unexpectedly"))


def test_a_submission_whose_commit_was_not_confirmed_can_be_retried_with_its_key(client, db):
    def commit_then_connection_lost():
        with CommitThenConnectionLost(db, expire_on_commit=False) as session:
            yield session

    working_session = app.dependency_overrides[get_session]
    app.dependency_overrides[get_session] = commit_then_connection_lost
    try:
        unconfirmed = client.post("/deliveries", json=EVENT, headers=KEY)
    finally:
        app.dependency_overrides[get_session] = working_session
    retried = client.post("/deliveries", json=EVENT, headers=KEY)

    assert unconfirmed.status_code == 503
    assert "Retry with the same Idempotency-Key" in unconfirmed.json()["detail"]
    # The delivery was stored after all, and the retry finds it instead of storing it twice.
    assert retried.status_code == 202
    assert retried.headers["idempotent-replayed"] == "true"
    with db.connect() as connection:
        assert connection.scalars(text("SELECT id FROM deliveries")).all() == [
            uuid.UUID(retried.json()["delivery_id"])
        ]


def test_a_full_connection_pool_returns_503(db, api_key):
    # One pooled connection, which is in use, and a short wait for it to come back.
    busy = create_engine(db.url, pool_size=1, max_overflow=0, pool_timeout=0.1)

    def busy_session():
        with Session(busy) as session:
            yield session

    app.dependency_overrides[get_session] = busy_session
    try:
        with busy.connect():
            response = TestClient(app).post(
                "/deliveries", json=EVENT, headers=KEY | {"Authorization": f"Bearer {api_key}"}
            )
    finally:
        app.dependency_overrides.clear()
        busy.dispose()

    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"


def event_of_size(size: int) -> bytes:
    """A valid event whose JSON is exactly `size` bytes long."""
    padding = size - len(json.dumps({**EVENT, "data": {"padding": ""}}))
    return json.dumps({**EVENT, "data": {"padding": "x" * padding}}).encode()


def test_an_event_up_to_the_size_limit_is_accepted_and_a_larger_one_is_not(client, db):
    limit = get_settings().max_event_bytes
    json_body = {"content-type": "application/json"}

    at_limit = client.post("/deliveries", content=event_of_size(limit), headers=json_body)
    over_limit = client.post("/deliveries", content=event_of_size(limit + 1), headers=json_body)

    assert at_limit.status_code == 202
    assert over_limit.status_code == 413
    with db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deliveries")) == 1


def test_an_event_sent_in_chunks_is_held_to_the_size_limit(client, db):
    body = event_of_size(get_settings().max_event_bytes + 1)

    def chunks():
        for start in range(0, len(body), 4096):
            yield body[start : start + 4096]

    response = client.post(
        "/deliveries", content=chunks(), headers={"content-type": "application/json"}
    )

    # Without Content-Length, the size is only known by counting the body as it arrives.
    assert "content-length" not in response.request.headers
    assert response.status_code == 413
