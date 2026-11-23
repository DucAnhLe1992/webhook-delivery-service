import uuid

import pytest
from sqlalchemy import text

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}


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
