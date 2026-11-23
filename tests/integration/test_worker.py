import json
import threading
import time
import uuid
from datetime import datetime, timedelta

import httpx2
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from webhook_service.config import Settings
from webhook_service.services import delivery_service
from webhook_service.worker.main import process_one

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
SETTINGS = Settings(receiver_url="http://receiver.test/webhooks", retry_delay_seconds=60)


def fake_receiver(status_code: int) -> tuple[httpx2.Client, list[httpx2.Request]]:
    sent: list[httpx2.Request] = []

    def answer(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        return httpx2.Response(status_code)

    return httpx2.Client(transport=httpx2.MockTransport(answer)), sent


def submit(client) -> str:
    return client.post("/deliveries", json=EVENT).json()["delivery_id"]


def test_worker_sends_a_pending_delivery_and_marks_it_delivered(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(200)

    assert process_one(db, receiver, SETTINGS) is True

    [request] = sent
    assert str(request.url) == "http://receiver.test/webhooks"
    assert request.headers["webhook-id"] == delivery_id
    message = json.loads(request.content)
    assert message["type"] == "booking.confirmed"
    assert message["data"] == {"booking_id": 42}
    delivery = client.get(f"/deliveries/{delivery_id}").json()
    assert delivery["status"] == "delivered"
    assert delivery["attempt_count"] == 1
    assert delivery["last_http_status"] == 200
    assert process_one(db, receiver, SETTINGS) is False


def test_retryable_failure_is_retried_after_the_delay(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(503)

    assert process_one(db, receiver, SETTINGS) is True

    delivery = client.get(f"/deliveries/{delivery_id}").json()
    assert delivery["status"] == "pending"
    assert delivery["attempt_count"] == 1
    assert delivery["last_http_status"] == 503
    assert delivery["last_error_category"] == "http_error"
    retry_at = datetime.fromisoformat(delivery["next_attempt_at"])
    assert retry_at - datetime.fromisoformat(delivery["updated_at"]) == timedelta(seconds=60)
    # Not due yet, so the worker leaves it alone.
    assert process_one(db, receiver, SETTINGS) is False
    assert len(sent) == 1


def test_permanent_failure_is_not_retried(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(400)

    assert process_one(db, receiver, SETTINGS) is True

    delivery = client.get(f"/deliveries/{delivery_id}").json()
    assert delivery["status"] == "failed"
    assert delivery["last_http_status"] == 400
    assert process_one(db, receiver, SETTINGS) is False
    assert len(sent) == 1


def test_a_claimed_delivery_cannot_be_claimed_again(client, db):
    submit(client)

    with Session(db, expire_on_commit=False) as session:
        first = delivery_service.claim_next_due(session)
    with Session(db, expire_on_commit=False) as session:
        second = delivery_service.claim_next_due(session)

    assert first is not None
    assert first.status == "processing"
    assert second is None


def test_two_workers_never_claim_the_same_delivery(client, db):
    delivery_id = uuid.UUID(submit(client))
    claimed_by_b = {}

    def worker_b() -> None:
        with Session(db, expire_on_commit=False) as session:
            claimed_by_b["delivery"] = delivery_service.claim_next_due(session)

    with db.connect() as worker_a:
        # Worker A has claimed the delivery but not committed yet, so it holds the row lock.
        worker_a.execute(
            text("UPDATE deliveries SET status = 'processing' WHERE id = :id"), {"id": delivery_id}
        )
        thread = threading.Thread(target=worker_b)
        thread.start()
        # Worker B still sees the row as pending, picks it, and waits for A's lock.
        wait_until_a_query_waits_for_a_lock(db)
        worker_a.commit()
        thread.join(timeout=10)

    assert claimed_by_b["delivery"] is None


def wait_until_a_query_waits_for_a_lock(engine: Engine, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as observer:
        while time.monotonic() < deadline:
            waiting = observer.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND datname = current_database()"
                )
            )
            if waiting:
                return
            time.sleep(0.01)
    raise AssertionError("no query started waiting for a row lock")
