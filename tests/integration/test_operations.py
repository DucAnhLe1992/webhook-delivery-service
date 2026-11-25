import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session

from webhook_service import admin
from webhook_service.config import Settings
from webhook_service.domain import AttemptOutcome, AttemptResult, ErrorCategory
from webhook_service.services import delivery_service, producer_service

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
DELIVERED = AttemptResult(outcome=AttemptOutcome.DELIVERED, http_status=200, duration_ms=5)
REJECTED = AttemptResult(
    outcome=AttemptOutcome.PERMANENT_FAILURE,
    http_status=400,
    error_category=ErrorCategory.HTTP_ERROR,
    duration_ms=5,
)
THIRTY_DAYS = timedelta(days=30)


def submit(client, **headers: str) -> str:
    return client.post("/deliveries", json=EVENT, headers=headers).json()["delivery_id"]


def finish(client, db: Engine, result: AttemptResult, **headers: str) -> str:
    """Submit an event and send it once, with this result."""
    delivery_id = submit(client, **headers)
    with Session(db, expire_on_commit=False) as session:
        delivery = delivery_service.claim_next_due(
            session, claim_timeout=timedelta(seconds=60), max_attempts=3
        )
    assert str(delivery.id) == delivery_id
    with Session(db) as session:
        delivery_service.record_attempt(
            session,
            delivery.id,
            delivery.claim_token,
            result,
            retry_delay=timedelta(seconds=60),
            max_attempts=3,
        )
    return delivery_id


def age(db: Engine, delivery_id: str, *, updated: timedelta, created: timedelta | None = None):
    """Act as if the delivery last changed (for a finished one: finished) `updated` ago."""
    with db.begin() as connection:
        connection.execute(
            text(
                "UPDATE deliveries SET updated_at = now() - :updated, "
                "created_at = now() - :created WHERE id = :id"
            ),
            {"id": delivery_id, "updated": updated, "created": created or updated},
        )


def statuses(db: Engine) -> dict[str, str]:
    with db.connect() as connection:
        rows = connection.execute(text("SELECT id, status FROM deliveries"))
        return {str(delivery_id): status for delivery_id, status in rows}


def with_attempts(db: Engine) -> set[str]:
    with db.connect() as connection:
        rows = connection.scalars(text("SELECT DISTINCT delivery_id FROM delivery_attempts"))
        return {str(delivery_id) for delivery_id in rows}


def test_replay_chooses_failed_deliveries_by_id_or_by_failure_time_and_producer(
    client, client_for, db
):
    friendster = client_for("friendster")
    failed_earlier = finish(client, db, REJECTED)
    failed_now = finish(client, db, REJECTED)
    failed_for_friendster = finish(friendster, db, REJECTED)
    delivered = finish(client, db, DELIVERED)
    pending = submit(client)
    age(db, failed_earlier, updated=timedelta(hours=2))

    with Session(db) as session:
        carrental = producer_service.producer_id(session, "carrental")
        by_time_and_producer = delivery_service.replay_failed(
            session, failed_since=datetime.now(UTC) - timedelta(hours=1), producer_id=carrental
        )
    with Session(db) as session:
        by_id = delivery_service.replay_failed(
            session,
            delivery_ids=[uuid.UUID(d) for d in (failed_earlier, delivered, pending)]
            + [uuid.uuid4()],
        )

    assert by_time_and_producer == [uuid.UUID(failed_now)]
    # Only failed deliveries are replayed: the others are left as they are.
    assert by_id == [uuid.UUID(failed_earlier)]
    assert statuses(db) == {
        failed_earlier: "pending",
        failed_now: "pending",
        failed_for_friendster: "failed",
        delivered: "delivered",
        pending: "pending",
    }


def test_the_replay_command_names_what_it_replayed_and_what_it_did_not(client, db, capsys):
    failed = finish(client, db, REJECTED)
    delivered = finish(client, db, DELIVERED)

    assert admin.main(["replay", failed, delivered], engine=db) == 1

    out, err = capsys.readouterr()
    assert out == (
        "Replayed 1 delivery. Workers send each again, with up to MAX_ATTEMPTS new attempts.\n"
        f"  {failed}\n"
    )
    assert err == (
        f"webhook-admin: not replayed, because they are not failed or do not exist: {delivered}\n"
    )


def test_the_replay_command_reads_a_time_without_a_zone_as_local_time(
    client, client_for, db, capsys, monkeypatch
):
    failed_earlier = finish(client, db, REJECTED)
    failed_now = finish(client, db, REJECTED)
    finish(client_for("friendster"), db, REJECTED)
    age(db, failed_earlier, updated=timedelta(hours=2))
    # Far from UTC, so reading the time as UTC would choose different deliveries.
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    try:
        an_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat(timespec="seconds")
        code = admin.main(
            ["replay", "--failed-since", an_hour_ago, "--producer", "carrental"], engine=db
        )
    finally:
        monkeypatch.undo()
        time.tzset()

    assert code == 0
    assert capsys.readouterr().out.splitlines()[1:] == [f"  {failed_now}"]


def test_the_replay_command_says_so_when_nothing_matched(client, db, capsys):
    delivered = finish(client, db, DELIVERED)

    assert admin.main(["replay", delivered], engine=db) == 1

    out, err = capsys.readouterr()
    assert out == "Replayed nothing: no failed delivery matched.\n"
    assert err.endswith(f"not failed or do not exist: {delivered}\n")


def test_the_replay_command_refuses_an_unknown_producer(db, capsys):
    since = "2026-11-25T20:00"
    assert admin.main(["replay", "--failed-since", since, "--producer", "nobody"], engine=db) == 1
    assert capsys.readouterr().err == "webhook-admin: There is no producer named nobody.\n"


def test_purge_deletes_deliveries_that_finished_before_the_retention_period(client, db):
    delivered_long_ago = finish(client, db, DELIVERED)
    failed_long_ago = finish(client, db, REJECTED)
    delivered_recently = finish(client, db, DELIVERED)
    replayed_recently = finish(client, db, REJECTED)  # Accepted long ago, finished recently.
    processing = submit(client)
    with Session(db) as session:
        delivery_service.claim_next_due(
            session, claim_timeout=timedelta(seconds=60), max_attempts=3
        )
    pending = submit(client)
    for delivery_id in (delivered_long_ago, failed_long_ago, processing, pending):
        age(db, delivery_id, updated=timedelta(days=31))
    age(db, delivered_recently, updated=timedelta(days=29))
    age(db, replayed_recently, updated=timedelta(days=10), created=timedelta(days=40))

    with Session(db) as session:
        deleted = delivery_service.purge_finished(session, older_than=THIRTY_DAYS)

    assert deleted == 2
    kept = {delivered_recently, replayed_recently, processing, pending}
    assert set(statuses(db)) == kept
    # Their attempts went with them.
    assert with_attempts(db) == kept - {pending}


def test_purge_deletes_in_batches_until_none_are_left(client, db):
    for _ in range(5):
        age(db, finish(client, db, DELIVERED), updated=timedelta(days=31))

    with Session(db) as session:
        deleted = delivery_service.purge_finished(session, older_than=THIRTY_DAYS, batch_size=2)

    assert deleted == 5
    assert statuses(db) == {}


def test_purge_skips_a_delivery_another_transaction_has_locked(client, db, short_limits_engine):
    locked, unlocked = (finish(client, db, REJECTED) for _ in range(2))
    for delivery_id in (locked, unlocked):
        age(db, delivery_id, updated=timedelta(days=31))

    with db.connect() as replay_in_progress:
        replay_in_progress.execute(
            text("SELECT 1 FROM deliveries WHERE id = :id FOR UPDATE"), {"id": locked}
        )
        # With a lock timeout of 0.2 s, waiting for the lock would fail the purge.
        with Session(short_limits_engine) as session:
            deleted = delivery_service.purge_finished(session, older_than=THIRTY_DAYS)
        replay_in_progress.rollback()

    assert deleted == 1
    assert set(statuses(db)) == {locked}


def test_after_a_purge_a_repeated_request_is_stored_as_a_new_delivery(client, db):
    first = finish(client, db, DELIVERED, **{"Idempotency-Key": "booking-42"})
    age(db, first, updated=timedelta(days=31))
    with Session(db) as session:
        delivery_service.purge_finished(session, older_than=THIRTY_DAYS)

    repeated = client.post("/deliveries", json=EVENT, headers={"Idempotency-Key": "booking-42"})

    # The key was deleted with the delivery, so the repeat gets a new ID, and so a new
    # webhook-id: the receiver cannot tell it is a copy.
    assert repeated.status_code == 202
    assert "idempotent-replayed" not in repeated.headers
    assert repeated.json()["delivery_id"] != first


def test_the_purge_command_can_count_before_it_deletes(client, db, capsys, monkeypatch):
    monkeypatch.setattr(admin, "get_settings", lambda: Settings(_env_file=None, retention_days=30))
    old = finish(client, db, DELIVERED)
    age(db, old, updated=timedelta(days=31))

    assert admin.main(["purge", "--dry-run"], engine=db) == 0
    assert capsys.readouterr().out == (
        "Would delete 1 delivery that finished more than 30 days ago.\n"
    )
    assert set(statuses(db)) == {old}

    assert admin.main(["purge"], engine=db) == 0
    assert capsys.readouterr().out == (
        "Deleted 1 delivery that finished more than 30 days ago, with their attempts.\n"
    )
    assert statuses(db) == {}
