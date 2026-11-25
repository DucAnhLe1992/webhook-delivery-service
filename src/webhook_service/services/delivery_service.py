import hashlib
import json
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, selectinload

from webhook_service.domain import AttemptOutcome, AttemptResult, DeliveryStatus, ErrorCategory
from webhook_service.models import Delivery, DeliveryAttempt


class IdempotencyKeyReused(Exception):
    """The Idempotency-Key was already used for a request with different content."""


def request_fingerprint(event_type: str, data: dict[str, Any]) -> str:
    """A hash of the request's content. The order of keys in `data` does not matter."""
    canonical = json.dumps(
        {"event_type": event_type, "data": data}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_delivery(
    session: Session,
    *,
    event_type: str,
    data: dict[str, Any],
    idempotency_key: str | None = None,
) -> tuple[Delivery, bool]:
    """Store a new delivery and return it with created=True.

    If the idempotency key was used before, return the original delivery with
    created=False instead, or raise IdempotencyKeyReused if the content differs.
    Either way the result is committed before the API answers: "accepted" means "stored".
    """
    if idempotency_key is None:
        delivery = Delivery(event_type=event_type, payload=data)
        session.add(delivery)
        session.commit()
        return delivery, True

    fingerprint = request_fingerprint(event_type, data)
    # If a request with the same key is being stored at this moment, PostgreSQL waits for
    # it to finish and then skips this insert, instead of failing on the unique key.
    inserted = session.scalars(
        insert(Delivery)
        .values(
            event_type=event_type,
            payload=data,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
        )
        .on_conflict_do_nothing(index_elements=[Delivery.idempotency_key])
        .returning(Delivery)
    ).one_or_none()
    session.commit()
    if inserted is not None:
        return inserted, True

    original = session.scalars(
        select(Delivery).where(Delivery.idempotency_key == idempotency_key)
    ).one()
    if original.request_fingerprint != fingerprint:
        raise IdempotencyKeyReused(idempotency_key)
    return original, False


def get_delivery(session: Session, delivery_id: uuid.UUID) -> Delivery | None:
    return session.get(Delivery, delivery_id, options=[selectinload(Delivery.attempts)])


def claim_next_due(
    session: Session, *, claim_timeout: timedelta, max_attempts: int
) -> Delivery | None:
    """Claim the next due delivery for one attempt, or return None if nothing is due.

    A delivery is due when it is pending and its next_attempt_at has passed, or when it is
    processing but its claim expired: the worker that held it stopped or hung before
    saving a result.

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

        if delivery.status == DeliveryStatus.PROCESSING:
            close_expired_attempt(session, delivery.claim_token)
            if delivery.attempt_count >= max_attempts:
                # Attempts are counted when claimed, not when their result is saved, so a
                # delivery that crashes every worker that sends it still runs out of attempts.
                delivery.status = DeliveryStatus.FAILED
                delivery.claim_token = None
                session.commit()
                continue

        attempt = DeliveryAttempt(
            id=uuid.uuid4(), delivery_id=delivery.id, number=delivery.attempt_count + 1
        )
        session.add(attempt)
        delivery.status = DeliveryStatus.PROCESSING
        delivery.claim_token = attempt.id
        delivery.attempt_count = attempt.number
        delivery.next_attempt_at = func.now() + claim_timeout
        session.commit()
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
        result.outcome is AttemptOutcome.RETRYABLE_FAILURE and delivery.attempt_count < max_attempts
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
