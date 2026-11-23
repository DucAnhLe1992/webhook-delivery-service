import json
import threading
import uuid

import pytest
from sqlalchemy import text

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


def test_concurrent_requests_with_the_same_key_store_one_delivery(client, db, wait_until_blocked):
    first_id = uuid.uuid4()
    responses = []

    with db.connect() as first_request:
        # The first request has inserted its delivery but not committed yet.
        first_request.execute(
            text(
                "INSERT INTO deliveries"
                " (id, event_type, payload, idempotency_key, request_fingerprint)"
                " VALUES (:id, :event_type, CAST(:payload AS jsonb), :key, :fingerprint)"
            ),
            {
                "id": first_id,
                "event_type": EVENT["event_type"],
                "payload": json.dumps(EVENT["data"]),
                "key": KEY["Idempotency-Key"],
                "fingerprint": delivery_service.request_fingerprint(
                    EVENT["event_type"], EVENT["data"]
                ),
            },
        )
        second_request = threading.Thread(
            target=lambda: responses.append(client.post("/deliveries", json=EVENT, headers=KEY))
        )
        second_request.start()
        # The second request's insert waits to see whether the first one commits.
        wait_until_blocked()
        first_request.commit()
        second_request.join(timeout=10)

    [response] = responses
    assert response.status_code == 202
    assert response.json()["delivery_id"] == str(first_id)
    assert response.headers["idempotent-replayed"] == "true"
