import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from types import SimpleNamespace

import httpx2
import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from webhook_service import signing
from webhook_service.config import Settings
from webhook_service.domain import AttemptOutcome, AttemptResult, ErrorCategory
from webhook_service.services import delivery_service
from webhook_service.worker import main as worker
from webhook_service.worker.main import process_one, run

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
SETTINGS = Settings(
    receiver_url="http://receiver.test/webhooks",
    max_attempts=3,
    retry_base_delay_seconds=60,
    claim_timeout_seconds=60,
    webhook_secrets=[signing.new_secret()],
)
CLAIM_OPTIONS = {"claim_timeout": timedelta(seconds=60), "max_attempts": 3}
ATTEMPTS = "webhooks_attempts_total"
WORKER_METRICS = {
    "delivered": (ATTEMPTS, {"outcome": "delivered", "error_category": "none"}),
    "retried": (ATTEMPTS, {"outcome": "retryable_failure", "error_category": "http_error"}),
    "rejected": (ATTEMPTS, {"outcome": "permanent_failure", "error_category": "http_error"}),
    "claim_expired": (
        ATTEMPTS,
        {"outcome": "retryable_failure", "error_category": "claim_expired"},
    ),
    "finished_delivered": ("webhooks_deliveries_finished_total", {"status": "delivered"}),
    "finished_failed": ("webhooks_deliveries_finished_total", {"status": "failed"}),
    "latency_observed": ("webhooks_delivery_latency_seconds_count", {}),
    "discarded": ("webhooks_results_discarded_total", {}),
    "database_errors": ("webhooks_worker_errors_total", {"kind": "database"}),
    "unexpected_errors": ("webhooks_worker_errors_total", {"kind": "unexpected"}),
}


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
    signing.verify(SETTINGS.signing_keys, request.headers, request.content)
    message = json.loads(request.content)
    assert message["type"] == "booking.confirmed"
    assert message["producer"] == "carrental"
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


def test_concurrent_workers_never_claim_the_same_delivery(client, db):
    delivery_ids = [submit(client) for _ in range(30)]
    sent_ids: list[str] = []
    lock = threading.Lock()

    def answer(request: httpx2.Request) -> httpx2.Response:
        with lock:
            sent_ids.append(request.headers["webhook-id"])
        return httpx2.Response(200)

    def worker() -> None:
        with httpx2.Client(transport=httpx2.MockTransport(answer)) as receiver:
            while process_one(db, receiver, SETTINGS):
                pass

    pool = ThreadPoolExecutor(max_workers=4)
    try:
        workers = [pool.submit(worker) for _ in range(4)]
        for finished in as_completed(workers, timeout=30):
            finished.result()  # Re-raises an exception from the worker's thread.
    finally:
        pool.shutdown(wait=False)

    # Every attempt succeeded, so a delivery sent twice could only mean two workers claimed it.
    assert sorted(sent_ids) == sorted(delivery_ids)
    with db.connect() as connection:
        delivered = connection.scalar(
            text("SELECT count(*) FROM deliveries WHERE status = 'delivered'")
        )
    assert delivered == 30


class PausedBeforeCommit(Session):
    """A session that stops just before it commits, keeping its locks, until released."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.paused = threading.Event()
        self.release = threading.Event()

    def commit(self) -> None:
        self.flush()
        self.paused.set()
        assert self.release.wait(10)
        super().commit()


def test_a_late_result_waits_for_a_takeover_in_progress_and_is_then_discarded(
    client, db, wait_until_blocked
):
    delivery_id = submit(client)
    stalled = claim(db)  # Worker A claims the delivery, then stops responding...
    make_due(db, delivery_id)  # ...until its claim expires.
    takeover = PausedBeforeCommit(db, expire_on_commit=False)

    def worker_b_takes_over() -> None:
        with takeover:
            delivery_service.claim_next_due(takeover, **CLAIM_OPTIONS)

    def worker_a_saves_its_late_result() -> object:
        with Session(db) as session:
            return delivery_service.record_attempt(
                session,
                stalled.id,
                stalled.claim_token,
                AttemptResult(outcome=AttemptOutcome.DELIVERED, http_status=200, duration_ms=5),
                retry_delay=timedelta(seconds=60),
                max_attempts=3,
            )

    pool = ThreadPoolExecutor(max_workers=2)
    try:
        takeover_done = pool.submit(worker_b_takes_over)
        # Worker B holds the row lock and has replaced the claim token, but not committed.
        assert takeover.paused.wait(5)
        late_save = pool.submit(worker_a_saves_its_late_result)
        # Worker A's token check waits for B's lock instead of reading the old token.
        wait_until_blocked()
        takeover.release.set()
        takeover_done.result(timeout=10)
        saved = late_save.result(timeout=10)
    finally:
        takeover.release.set()
        pool.shutdown(wait=False)

    # Once B committed, A's check ran again on the new row, found B's token, and wrote nothing.
    assert saved is None
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "processing"
    assert history(client, delivery_id) == [
        (1, "retryable_failure", "claim_expired"),
        (2, None, None),
    ]


def test_a_result_lost_with_the_database_is_recovered_by_sending_again(client, db, monkeypatch):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(200)

    def database_gone(*args, **kwargs):
        raise OperationalError("UPDATE deliveries ...", {}, Exception("connection lost"))

    with monkeypatch.context() as patch:
        patch.setattr(delivery_service, "record_attempt", database_gone)
        with pytest.raises(OperationalError):
            process_one(db, receiver, SETTINGS)

    # The receiver has the event, but the database does not know. Once the claim expires,
    # another worker sends it again.
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "processing"
    make_due(db, delivery_id)
    assert process_one(db, receiver, SETTINGS) is True

    assert history(client, delivery_id) == [
        (1, "retryable_failure", "claim_expired"),
        (2, "delivered", None),
    ]
    # Both copies carry the same webhook-id, so the receiver can recognize the second.
    assert [request.headers["webhook-id"] for request in sent] == [delivery_id, delivery_id]


def test_an_unexpected_error_does_not_stop_the_worker(client, db, caplog, count_changes):
    broken = submit(client)
    healthy = submit(client)
    attempted: list[str] = []
    stop = threading.Event()
    changes = count_changes(**WORKER_METRICS)

    def answer(request: httpx2.Request) -> httpx2.Response:
        attempted.append(request.headers["webhook-id"])
        if len(attempted) == 2:
            stop.set()
        if request.headers["webhook-id"] == broken:
            raise RuntimeError("a bug while sending")
        return httpx2.Response(200)

    safety_net = threading.Timer(10, stop.set)
    safety_net.start()
    try:
        with httpx2.Client(transport=httpx2.MockTransport(answer)) as receiver:
            run(db, receiver, SETTINGS.model_copy(update={"poll_interval_seconds": 0.01}), stop)
    finally:
        safety_net.cancel()

    assert sorted(attempted) == sorted([broken, healthy])
    assert "unexpected error" in caplog.text
    # The broken attempt is counted once its claim expires and another worker records it.
    assert changes() == {
        "unexpected_errors": 1,
        "delivered": 1,
        "finished_delivered": 1,
        "latency_observed": 1,
    }
    assert client.get(f"/deliveries/{healthy}").json()["status"] == "delivered"
    # The broken delivery keeps its claim until it expires, then it is sent again.
    broken_delivery = client.get(f"/deliveries/{broken}").json()
    assert broken_delivery["status"] == "processing"
    assert broken_delivery["attempt_count"] == 1


def test_the_worker_does_not_start_without_webhook_secrets(monkeypatch):
    unsigned = SETTINGS.model_copy(update={"webhook_secrets": []})
    monkeypatch.setattr(worker, "get_settings", lambda: unsigned)
    monkeypatch.setattr(worker, "run", lambda *args: pytest.fail("the worker started"))

    with pytest.raises(SystemExit, match="WEBHOOK_SECRETS is not set"):
        worker.main()


def test_a_worker_that_meets_a_lock_while_saving_a_result_gives_up_and_carries_on(
    client, db, short_limits_engine, caplog
):
    delivery_id = submit(client)
    stuck = db.connect()
    stop = threading.Event()

    def answer(request: httpx2.Request) -> httpx2.Response:
        # While the worker sends, another transaction locks the delivery's row and keeps it.
        stuck.execute(
            text("SELECT 1 FROM deliveries WHERE id = :id FOR UPDATE"),
            {"id": uuid.UUID(delivery_id)},
        )
        threading.Timer(1, stop.set).start()
        return httpx2.Response(200)

    safety_net = threading.Timer(10, stop.set)
    safety_net.start()
    started = time.monotonic()
    try:
        with httpx2.Client(transport=httpx2.MockTransport(answer)) as receiver:
            run(
                short_limits_engine,
                receiver,
                SETTINGS.model_copy(update={"poll_interval_seconds": 0.05}),
                stop,
            )
    finally:
        safety_net.cancel()
        stuck.rollback()
        stuck.close()

    assert time.monotonic() - started < 5
    assert "database error (canceling statement due to lock timeout)" in caplog.text
    # The result was not saved, so the delivery keeps its claim and is sent again once the
    # claim expires.
    delivery = client.get(f"/deliveries/{delivery_id}").json()
    assert (delivery["status"], delivery["attempt_count"]) == ("processing", 1)


def test_each_attempt_is_counted_and_logged_with_its_fields(client, db, count_changes, json_logs):
    changes = count_changes(
        **WORKER_METRICS,
        timed=("webhooks_attempt_duration_seconds_count", {"outcome": "delivered"}),
    )
    delivery_ids = []
    for status_code in (200, 503, 400):
        delivery_ids.append(submit(client))
        process_one(db, fake_receiver(status_code)[0], SETTINGS)

    assert changes() == {
        "delivered": 1,
        "retried": 1,
        "rejected": 1,
        "finished_delivered": 1,
        "finished_failed": 1,
        "latency_observed": 1,
        "timed": 1,
    }
    lines = [line for line in json_logs() if line.get("event") == "attempt_finished"]
    assert [line["delivery_id"] for line in lines] == delivery_ids
    delivered, retried, rejected = lines
    assert (
        delivered.items()
        >= {
            "attempt": 1,
            "producer": "carrental",
            "event_type": "booking.confirmed",
            "outcome": "delivered",
            "http_status": 200,
            "error_category": None,
            "status": "delivered",
        }.items()
    )
    assert (
        retried.items()
        >= {
            "outcome": "retryable_failure",
            "http_status": 503,
            "error_category": "http_error",
            "status": "pending",
        }.items()
    )
    assert 48 <= retried["retry_in_seconds"] <= 72  # 60 seconds, give or take 20%
    assert (rejected["outcome"], rejected["status"]) == ("permanent_failure", "failed")
    assert "retry_in_seconds" not in rejected


def test_each_attempt_is_counted_once_even_when_its_result_is_discarded(
    client, db, count_changes, caplog
):
    delivery_id = submit(client)

    def answer_too_late(request: httpx2.Request) -> httpx2.Response:
        # While worker A waits for this answer, its claim expires and worker B takes over.
        make_due(db, delivery_id)
        assert process_one(db, fake_receiver(200)[0], SETTINGS) is True
        return httpx2.Response(200)

    changes = count_changes(**WORKER_METRICS)
    with httpx2.Client(transport=httpx2.MockTransport(answer_too_late)) as receiver:
        assert process_one(db, receiver, SETTINGS) is True

    # B counted attempt 1 as expired and attempt 2 as delivered. A's result was not saved.
    assert changes() == {
        "claim_expired": 1,
        "delivered": 1,
        "finished_delivered": 1,
        "latency_observed": 1,
        "discarded": 1,
    }
    assert caplog.messages == [
        f"{delivery_id} attempt 1: the claim expired without a result, so the delivery is "
        "sent again",
        f"{delivery_id} attempt 1: result discarded; the claim expired and another worker "
        "took the delivery over",
    ]


def test_a_worker_reports_each_expired_claim_it_finds(client, db, count_changes, caplog):
    taken_over = submit(client)
    claim(db)  # A worker claims it and stops.
    make_due(db, taken_over)
    changes = count_changes(**WORKER_METRICS)
    receiver, _ = fake_receiver(200)

    assert process_one(db, receiver, SETTINGS) is True

    out_of_attempts = submit(client)
    for _ in range(3):  # MAX_ATTEMPTS is 3, and every worker that claims it stops.
        claim(db)
        make_due(db, out_of_attempts)

    assert process_one(db, receiver, SETTINGS) is False
    assert client.get(f"/deliveries/{out_of_attempts}").json()["status"] == "failed"
    assert changes() == {
        "claim_expired": 2,
        "delivered": 1,
        "finished_delivered": 1,
        "finished_failed": 1,
        "latency_observed": 1,
    }
    assert caplog.messages == [
        f"{taken_over} attempt 1: the claim expired without a result, so the delivery is "
        "sent again",
        f"{out_of_attempts} attempt 3: the claim expired without a result, and no attempts "
        "are left: failed",
    ]


def test_database_errors_are_logged_when_they_start_every_minute_and_when_they_end(
    monkeypatch, caplog, count_changes
):
    caplog.set_level(logging.INFO, logger="worker")
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(worker, "time", SimpleNamespace(monotonic=lambda: clock.now))
    stop = threading.Event()

    def database_down_for_a_while(engine, client, settings) -> bool:
        clock.now += 25  # A poll every 25 seconds.
        if clock.now <= 1100:
            raise OperationalError("SELECT ...", {}, Exception("connection refused"))
        stop.set()
        return False

    monkeypatch.setattr(worker, "process_one", database_down_for_a_while)
    changes = count_changes(**WORKER_METRICS)

    run(None, None, SETTINGS.model_copy(update={"poll_interval_seconds": 0.001}), stop)

    # Four failed polls, but only the first and a reminder a minute later are logged.
    assert changes() == {"database_errors": 4}
    assert [(record.levelname, record.getMessage()) for record in caplog.records] == [
        ("WARNING", "database error (connection refused), trying again every 0.001s"),
        ("WARNING", "database errors for 75s, the latest: connection refused"),
        ("INFO", "database working again after 100s"),
    ]


def test_a_replayed_delivery_gets_max_attempts_new_attempts(client, db):
    delivery_id = submit(client)
    receiver, sent = fake_receiver(503)
    for _ in range(3):
        make_due(db, delivery_id)
        process_one(db, receiver, SETTINGS)
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "failed"

    with Session(db) as session:
        replayed = delivery_service.replay_failed(session, delivery_ids=[uuid.UUID(delivery_id)])

    assert replayed == [uuid.UUID(delivery_id)]
    claim(db)  # Attempt 4: the worker stops before saving a result.
    make_due(db, delivery_id)
    assert process_one(db, receiver, SETTINGS) is True  # Attempt 5: the receiver is still down.
    # The second attempt since the replay waits 2 x 60 s, as a second attempt does, give or take
    # 20%. Counted from the first attempt, it would wait 16 x 60 s.
    assert timedelta(seconds=96) <= scheduled_delay(client, delivery_id) <= timedelta(seconds=144)
    make_due(db, delivery_id)
    assert process_one(db, receiver, SETTINGS) is True  # Attempt 6, the last.
    make_due(db, delivery_id)
    assert process_one(db, receiver, SETTINGS) is False
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "failed"
    assert history(client, delivery_id) == [
        (1, "retryable_failure", "http_error"),
        (2, "retryable_failure", "http_error"),
        (3, "retryable_failure", "http_error"),
        (4, "retryable_failure", "claim_expired"),
        (5, "retryable_failure", "http_error"),
        (6, "retryable_failure", "http_error"),
    ]
    # The same event each time, so the receiver can recognize a copy it already processed.
    assert {request.headers["webhook-id"] for request in sent} == {delivery_id}
