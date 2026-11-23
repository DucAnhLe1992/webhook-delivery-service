import uuid
from typing import Any

from sqlalchemy.orm import Session

from webhook_service.models import Delivery


def create_delivery(session: Session, *, event_type: str, data: dict[str, Any]) -> Delivery:
    delivery = Delivery(event_type=event_type, payload=data)
    session.add(delivery)
    # Commit before the API answers 202: "accepted" must mean "stored".
    session.commit()
    return delivery


def get_delivery(session: Session, delivery_id: uuid.UUID) -> Delivery | None:
    return session.get(Delivery, delivery_id)
