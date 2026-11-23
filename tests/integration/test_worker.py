import json
import threading
import uuid
from datetime import datetime, timedelta

import httpx2
from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from webhook_service.config import Settings
from webhook_service.sender import AttemptOutcome, AttemptResult, ErrorCategory
from webhook_service.services import delivery_service
from webhook_service.worker.main import process_one

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
SETTINGS = Settings(
    receiver_url="http://receiver.test/webhooks",
    max_attempts=3,
    retry_base_delay_seconds=60,
    claim_timeout_seconds=60,
)
CLAIM_OPTIONS = {"claim_timeout": timedelta(seconds=60), "max_attempts": 3}


def fake_receiver(status_code: int) -> tuple[httpx2.Client, list[httpx2.Request]]:
    sent: list[httpx2.Request] = []

    def answer(request: httpx2.Request) -> httpx2.Response:
        sent.append(request)
        return httpx2.Response(status_code)

    return httpx2.Client(transport=httpx2.MockTransport(answer)), sent


def submit(client) -> str:
    return client.post("/deliveries", json=EVENT).json()["delivery_id"]


def claim(db: Engine, **options):
    with Session(db, expire_on_commit=False) as session:
        return delivery_service.claim_next_due(session, **(CLAIM_OPTIONS | options))


def make_due(db: Engine, delivery_id: str) -> None:
    """Act as if time passed: the retry delay, or the current claim, has run out."""
    with db.begin() as connection:
        connection.execute(
            text("UPDATE deliveries SET next_attempt_at = now() WHERE id = :id"),
            {"id": uuid.UUID(delivery_id)},
        )


def history(client, delivery_id: str) -> list[tuple[int, str, str | None]]:
    attempts = client.get(f"/deliveries/{delivery_id}").json()["attempts"]
    return [(a["number"], a["outcome"], a["error_category"]) for a in attempts]


def scheduled_delay(client, delivery_id: str) -> timedelta:
    delivery = client.get(f"/deliveries/{delivery_id}").json()
    return datetime.fromisoformat(delivery["next_attempt_at"]) - datetime.fromisoformat(
        delivery["updated_at"]
    )


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
    assert history(client, delivery_id) == [(1, "delivered", None)]
    assert process_one(db, receiver, SETTINGS) is False


def test_retry_delay_doubles_after_each_failure(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(503)

    assert process_one(db, receiver, SETTINGS) is True
    first_delay = scheduled_delay(client, delivery_id)
    make_due(db, delivery_id)
    assert process_one(db, receiver, SETTINGS) is True
    second_delay = scheduled_delay(client, delivery_id)

    # About 60 s, then about 120 s, each give or take 20%.
    assert timedelta(seconds=48) <= first_delay <= timedelta(seconds=72)
    assert timedelta(seconds=96) <= second_delay <= timedelta(seconds=144)
    delivery = client.get(f"/deliveries/{delivery_id}").json()
    assert delivery["status"] == "pending"
    assert delivery["last_http_status"] == 503
    # Not due yet, so the worker leaves it alone.
    assert process_one(db, receiver, SETTINGS) is False
    assert len(sent) == 2


def test_retries_stop_after_max_attempts(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(503)

    for _ in range(3):
        assert process_one(db, receiver, SETTINGS) is True
        make_due(db, delivery_id)

    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "failed"
    assert history(client, delivery_id) == [
        (n, "retryable_failure", "http_error") for n in (1, 2, 3)
    ]
    assert process_one(db, receiver, SETTINGS) is False
    assert len(sent) == 3


def test_permanent_failure_is_not_retried(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(400)

    assert process_one(db, receiver, SETTINGS) is True

    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "failed"
    assert history(client, delivery_id) == [(1, "permanent_failure", "http_error")]
    assert process_one(db, receiver, SETTINGS) is False
    assert len(sent) == 1


def test_a_claimed_delivery_cannot_be_claimed_again(client, db):
    submit(client)

    first = claim(db)
    second = claim(db)

    assert first is not None
    assert first.status == "processing"
    assert second is None


def test_a_worker_skips_a_delivery_another_worker_is_claiming(client, db):
    first = submit(client)
    second = submit(client)

    with db.connect() as worker_a:
        # Worker A is in the middle of claiming the first delivery and holds its row lock.
        worker_a.execute(
            text("SELECT 1 FROM deliveries WHERE id = :id FOR UPDATE"), {"id": uuid.UUID(first)}
        )
        with Session(db, expire_on_commit=False) as session:
            # Fail instead of hanging if the claim ever waits for that lock.
            session.execute(text("SET LOCAL lock_timeout = '2s'"))
            claimed = delivery_service.claim_next_due(session, **CLAIM_OPTIONS)
        worker_a.rollback()

    assert str(claimed.id) == second


def test_an_expired_claim_is_taken_over_and_the_old_worker_cannot_overwrite_it(client, db):
    delivery_id = submit(client)
    stalled = claim(db)  # Worker A claims the delivery, then stops responding...
    make_due(db, delivery_id)  # ...until its claim expires.
    receiver, sent = fake_receiver(200)

    # Worker B takes the delivery over and delivers it.
    assert process_one(db, receiver, SETTINGS) is True

    # Worker A wakes up and tries to save its own, now outdated, result.
    late_result = AttemptResult(
        outcome=AttemptOutcome.PERMANENT_FAILURE,
        http_status=400,
        error_category=ErrorCategory.HTTP_ERROR,
        duration_ms=5,
    )
    with Session(db) as session:
        saved = delivery_service.record_attempt(
            session,
            stalled.id,
            stalled.claim_token,
            late_result,
            retry_delay=timedelta(seconds=60),
            max_attempts=3,
        )

    assert saved is None
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "delivered"
    assert history(client, delivery_id) == [
        (1, "retryable_failure", "claim_expired"),
        (2, "delivered", None),
    ]
    assert len(sent) == 1


def test_an_expired_claim_without_attempts_left_fails_the_delivery(client, db):
    delivery_id = submit(client)
    claim(db, max_attempts=1)
    make_due(db, delivery_id)

    assert claim(db, max_attempts=1) is None

    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "failed"
    assert history(client, delivery_id) == [(1, "retryable_failure", "claim_expired")]


def test_several_workers_send_each_delivery_exactly_once(client, db):
    delivery_ids = [submit(client) for _ in range(30)]
    sent_ids: list[str] = []
    lock = threading.Lock()

    def answer(request: httpx2.Request) -> httpx2.Response:
        with lock:
            sent_ids.append(request.headers["webhook-id"])
        return httpx2.Response(200)

    def worker() -> None:
        receiver = httpx2.Client(transport=httpx2.MockTransport(answer))
        while process_one(db, receiver, SETTINGS):
            pass

    workers = [threading.Thread(target=worker) for _ in range(4)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(timeout=30)

    assert sorted(sent_ids) == sorted(delivery_ids)
    with db.connect() as connection:
        delivered = connection.scalar(
            text("SELECT count(*) FROM deliveries WHERE status = 'delivered'")
        )
    assert delivered == 30
