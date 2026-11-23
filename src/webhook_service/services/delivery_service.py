import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from webhook_service.models import Delivery, DeliveryStatus
from webhook_service.sender import AttemptOutcome, AttemptResult


def create_delivery(session: Session, *, event_type: str, data: dict[str, Any]) -> Delivery:
    delivery = Delivery(event_type=event_type, payload=data)
    session.add(delivery)
    # Commit before the API answers 202: "accepted" must mean "stored".
    session.commit()
    return delivery


def get_delivery(session: Session, delivery_id: uuid.UUID) -> Delivery | None:
    return session.get(Delivery, delivery_id)


def claim_next_due(session: Session) -> Delivery | None:
    """Mark the oldest due pending delivery as processing and return it, or None.

    One atomic statement:

        UPDATE deliveries SET status = 'processing'
        WHERE id = (SELECT id FROM deliveries
                    WHERE status = 'pending' AND next_attempt_at <= now()
                    ORDER BY next_attempt_at LIMIT 1)
          AND status = 'pending'
        RETURNING *
    """
    oldest_due = (
        select(Delivery.id)
        .where(Delivery.status == DeliveryStatus.PENDING, Delivery.next_attempt_at <= func.now())
        .order_by(Delivery.next_attempt_at)
        .limit(1)
        .scalar_subquery()
    )
    delivery = session.scalars(
        update(Delivery)
        # The second status check runs again if another worker updated the row first,
        # so two workers can never claim the same delivery.
        .where(Delivery.id == oldest_due, Delivery.status == DeliveryStatus.PENDING)
        .values(status=DeliveryStatus.PROCESSING)
        .returning(Delivery)
    ).one_or_none()
    session.commit()
    return delivery


def record_attempt(
    session: Session, delivery_id: uuid.UUID, result: AttemptResult, *, retry_delay: timedelta
) -> None:
    """Store the outcome of one attempt on a delivery that was claimed for processing."""
    if result.outcome is AttemptOutcome.DELIVERED:
        changes: dict[str, Any] = {"status": DeliveryStatus.DELIVERED}
    elif result.outcome is AttemptOutcome.RETRYABLE_FAILURE:
        changes = {"status": DeliveryStatus.PENDING, "next_attempt_at": func.now() + retry_delay}
    else:
        changes = {"status": DeliveryStatus.FAILED}

    session.execute(
        update(Delivery)
        .where(Delivery.id == delivery_id, Delivery.status == DeliveryStatus.PROCESSING)
        .values(
            attempt_count=Delivery.attempt_count + 1,
            last_http_status=result.http_status,
            last_error_category=result.error_category,
            **changes,
        )
    )
    session.commit()
