import json

import httpx2
import pytest

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}


def test_delivered_event_returns_200(api):
    sent = []

    def receiver(request):
        sent.append(request)
        return httpx2.Response(200)

    response = api(receiver).post("/deliveries", json=EVENT)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "delivered"
    [request] = sent
    assert request.headers["webhook-id"] == body["delivery_id"]
    message = json.loads(request.content)
    assert message["type"] == "booking.confirmed"
    assert message["data"] == {"booking_id": 42}
    assert message["timestamp"]


def test_receiver_error_returns_502(api):
    response = api(lambda request: httpx2.Response(500)).post("/deliveries", json=EVENT)

    assert response.status_code == 502
    body = response.json()
    assert body["status"] == "failed"
    assert body["attempt"]["outcome"] == "retryable_failure"
    assert body["attempt"]["http_status"] == 500


def test_receiver_timeout_returns_504(api):
    def receiver(request):
        raise httpx2.ReadTimeout("simulated", request=request)

    response = api(receiver).post("/deliveries", json=EVENT)

    assert response.status_code == 504
    assert response.json()["attempt"]["error_category"] == "timeout"


@pytest.mark.parametrize(
    "payload",
    [
        {"event_type": "Booking Confirmed!", "data": {}},
        {"event_type": "booking.confirmed"},
        {**EVENT, "url": "http://169.254.169.254/latest/meta-data"},
    ],
    ids=["bad-event-type", "missing-data", "producer-chosen-url"],
)
def test_invalid_request_is_rejected_without_sending(api, payload):
    def receiver(request):
        raise AssertionError("the receiver must not be called")

    response = api(receiver).post("/deliveries", json=payload)

    assert response.status_code == 422
