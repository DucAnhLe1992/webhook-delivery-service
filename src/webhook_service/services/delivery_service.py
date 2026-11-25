import hashlib
import json
import uuid
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, selectinload

from webhook_service.domain import AttemptOutcome, AttemptResult, DeliveryStatus, ErrorCategory
from webhook_service.models import Delivery, DeliveryAttempt


class IdempotencyKeyReused(Exception):
    """The Idempotency-Key was already used for a request with different content."""


@dataclass(frozen=True)
class ExpiredClaim:
    """An attempt whose worker stopped before saving a result, found by another worker."""

    delivery_id: uuid.UUID
    attempt: int
    # No attempts were left, so the delivery failed instead of being sent again.
    delivery_failed: bool


@dataclass(frozen=True)
class QueueSnapshot:
    """How much work is waiting, for metrics."""

    due: int
    oldest_due_seconds: float
    by_status: dict[DeliveryStatus, int]


def request_fingerprint(event_type: str, data: dict[str, Any]) -> str:
    """A hash of the request's content. The order of keys in `data` does not matter."""
    canonical = json.dumps(
        {"event_type": event_type, "data": data}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_delivery(
    session: Session,
    *,
    producer_id: uuid.UUID,
    event_type: str,
    data: dict[str, Any],
    idempotency_key: str | None = None,
) -> tuple[Delivery, bool]:
    """Store a new delivery and return it with created=True.

    If the producer used the idempotency key before, return the original delivery with
    created=False instead, or raise IdempotencyKeyReused if the content differs.
    Either way the result is committed before the API answers: "accepted" means "stored".
    """
    if idempotency_key is None:
        delivery = Delivery(producer_id=producer_id, event_type=event_type, payload=data)
        session.add(delivery)
        session.commit()
        return delivery, True

    fingerprint = request_fingerprint(event_type, data)
    # If a request with the same key is being stored at this moment, PostgreSQL waits for
    # it to finish and then skips this insert, instead of failing on the unique key.
    inserted = session.scalars(
        insert(Delivery)
        .values(
            producer_id=producer_id,
            event_type=event_type,
            payload=data,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
        )
        .on_conflict_do_nothing(index_elements=[Delivery.producer_id, Delivery.idempotency_key])
        .returning(Delivery)
    ).one_or_none()
    session.commit()
    if inserted is not None:
        return inserted, True

    original = session.scalars(
        select(Delivery).where(
            Delivery.producer_id == producer_id, Delivery.idempotency_key == idempotency_key
        )
    ).one()
    if original.request_fingerprint != fingerprint:
        raise IdempotencyKeyReused(idempotency_key)
    return original, False


def get_delivery(
    session: Session, delivery_id: uuid.UUID, *, producer_id: uuid.UUID
) -> Delivery | None:
    """The producer's delivery with this ID, or None, also if another producer owns it."""
    return session.scalars(
        select(Delivery)
        .where(Delivery.id == delivery_id, Delivery.producer_id == producer_id)
        .options(selectinload(Delivery.attempts))
    ).one_or_none()


def claim_next_due(
    session: Session,
    *,
    claim_timeout: timedelta,
    max_attempts: int,
    on_expired_claim: Callable[[ExpiredClaim], None] | None = None,
) -> Delivery | None:
    """Claim the next due delivery for one attempt, or return None if nothing is due.

    A delivery is due when it is pending and its next_attempt_at has passed, or when it is
    processing but its claim expired: the worker that held it stopped or hung before
    saving a result. on_expired_claim hears about each expired claim, once it is committed.

    FOR UPDATE SKIP LOCKED lets several workers claim at the same time: a row that another
    worker is claiming right now is skipped instead of waited for.
    """
    while True:
        delivery = session.scalars(
            select(Delivery)
            .where(
                Delivery.status.in_([DeliveryStatus.PENDING, DeliveryStatus.PROCESSING]),
                Delivery.next_attempt_at <= func.now(),
            )
            .order_by(Delivery.next_attempt_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        ).one_or_none()
        if delivery is None:
            return None

        expired = None
        if delivery.status == DeliveryStatus.PROCESSING:
            close_expired_attempt(session, delivery.claim_token)
            if delivery.attempt_count - delivery.attempt_base >= max_attempts:
                # Attempts are counted when claimed, not when their result is saved, so a
                # delivery that crashes every worker that sends it still runs out of attempts.
                delivery.status = DeliveryStatus.FAILED
                delivery.claim_token = None
                session.commit()
                if on_expired_claim:
                    on_expired_claim(ExpiredClaim(delivery.id, delivery.attempt_count, True))
                continue
            expired = ExpiredClaim(delivery.id, delivery.attempt_count, False)

        attempt = DeliveryAttempt(
            id=uuid.uuid4(), delivery_id=delivery.id, number=delivery.attempt_count + 1
        )
        session.add(attempt)
        delivery.status = DeliveryStatus.PROCESSING
        delivery.claim_token = attempt.id
        delivery.attempt_count = attempt.number
        delivery.next_attempt_at = func.now() + claim_timeout
        session.commit()
        if expired and on_expired_claim:
            on_expired_claim(expired)
        return delivery


def close_expired_attempt(session: Session, attempt_id: uuid.UUID | None) -> None:
    session.execute(
        update(DeliveryAttempt)
        .where(DeliveryAttempt.id == attempt_id)
        .values(
            finished_at=func.now(),
            outcome=AttemptOutcome.RETRYABLE_FAILURE,
            error_category=ErrorCategory.CLAIM_EXPIRED,
        )
    )


def record_attempt(
    session: Session,
    delivery_id: uuid.UUID,
    claim_token: uuid.UUID,
    result: AttemptResult,
    *,
    retry_delay: timedelta,
    max_attempts: int,
) -> DeliveryStatus | None:
    """Save the result of the attempt that holds the claim and return the new status.

    Returns None and changes nothing if this attempt lost the claim: it expired and
    another worker took over the delivery. Only the current claim holder may write.
    """
    delivery = session.scalars(
        select(Delivery)
        .where(Delivery.id == delivery_id, Delivery.claim_token == claim_token)
        .with_for_update()
    ).one_or_none()
    if delivery is None:
        return None

    session.execute(
        update(DeliveryAttempt)
        .where(DeliveryAttempt.id == claim_token)
        .values(
            finished_at=func.now(),
            outcome=result.outcome,
            http_status=result.http_status,
            error_category=result.error_category,
            duration_ms=result.duration_ms,
        )
    )
    if result.outcome is AttemptOutcome.DELIVERED:
        delivery.status = DeliveryStatus.DELIVERED
    elif (
        result.outcome is AttemptOutcome.RETRYABLE_FAILURE
        and delivery.attempt_count - delivery.attempt_base < max_attempts
    ):
        delivery.status = DeliveryStatus.PENDING
        delivery.next_attempt_at = func.now() + retry_delay
    else:
        delivery.status = DeliveryStatus.FAILED
    delivery.claim_token = None
    delivery.last_http_status = result.http_status
    delivery.last_error_category = result.error_category
    session.commit()
    return DeliveryStatus(delivery.status)


def replay_failed(
    session: Session,
    *,
    delivery_ids: Collection[uuid.UUID] = (),
    failed_since: datetime | None = None,
    producer_id: uuid.UUID | None = None,
) -> list[uuid.UUID]:
    """Queue failed deliveries again, and return the IDs of those that were.

    Choose them by ID, or by when they failed. Each gets MAX_ATTEMPTS attempts again, so a
    receiver that is still unsteady after an outage has the whole retry schedule to recover.
    It keeps its webhook-id and its history; the new attempts are numbered on from the old.
    Deliveries that are not failed are left alone.
    """
    if not delivery_ids and failed_since is None:
        raise ValueError("choose deliveries by ID or by when they failed")
    statement = update(Delivery).where(Delivery.status == DeliveryStatus.FAILED)
    if delivery_ids:
        statement = statement.where(Delivery.id.in_(delivery_ids))
    if failed_since is not None:
        statement = statement.where(Delivery.updated_at >= failed_since)
    if producer_id is not None:
        statement = statement.where(Delivery.producer_id == producer_id)
    replayed = session.scalars(
        statement.values(
            status=DeliveryStatus.PENDING,
            next_attempt_at=func.now(),
            attempt_base=Delivery.attempt_count,
        )
        .returning(Delivery.id)
        .execution_options(synchronize_session=False)
    ).all()
    session.commit()
    return list(replayed)


def finished_before(older_than: timedelta):
    """Deliveries that were delivered or failed, and have not changed for `older_than`."""
    return select(Delivery.id).where(
        Delivery.status.in_([DeliveryStatus.DELIVERED, DeliveryStatus.FAILED]),
        Delivery.updated_at < func.now() - older_than,
    )


def count_finished_before(session: Session, older_than: timedelta) -> int:
    return session.scalar(select(func.count()).select_from(finished_before(older_than).subquery()))


def purge_finished(session: Session, *, older_than: timedelta, batch_size: int = 1000) -> int:
    """Delete deliveries that finished more than `older_than` ago, with their attempts.

    Pending and processing deliveries are never deleted. The deletes run in batches, each in a
    short transaction of its own, so a large purge never holds many locks for long. A delivery
    that is being replayed at that moment is skipped.
    """
    deleted = 0
    while True:
        batch = finished_before(older_than).limit(batch_size).with_for_update(skip_locked=True)
        count = session.execute(
            delete(Delivery)
            .where(Delivery.id.in_(batch.scalar_subquery()))
            .execution_options(synchronize_session=False)
        )
        session.commit()
        deleted += count.rowcount
        if count.rowcount < batch_size:
            return deleted


def queue_snapshot(session: Session) -> QueueSnapshot:
    """What is waiting: deliveries due now, how long the oldest has waited, and status counts.

    Both are served by partial indexes: the first reads only unfinished deliveries, through
    ix_deliveries_due, and the count of failed ones uses ix_deliveries_finished.
    """
    due = Delivery.next_attempt_at <= func.now()
    unfinished = session.execute(
        select(
            func.count().filter(due),
            func.coalesce(
                func.extract("epoch", func.now() - func.min(Delivery.next_attempt_at).filter(due)),
                0,
            ),
            func.count().filter(Delivery.status == DeliveryStatus.PENDING),
            func.count().filter(Delivery.status == DeliveryStatus.PROCESSING),
        ).where(Delivery.status.in_([DeliveryStatus.PENDING, DeliveryStatus.PROCESSING]))
    ).one()
    failed = session.scalar(select(func.count()).where(Delivery.status == DeliveryStatus.FAILED))
    session.commit()
    return QueueSnapshot(
        due=unfinished[0],
        oldest_due_seconds=float(unfinished[1]),
        by_status={
            DeliveryStatus.PENDING: unfinished[2],
            DeliveryStatus.PROCESSING: unfinished[3],
            DeliveryStatus.FAILED: failed,
        },
    )
