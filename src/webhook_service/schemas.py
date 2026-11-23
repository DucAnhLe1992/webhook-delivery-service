import uuid
from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field

from webhook_service.models import Delivery, DeliveryStatus


class DeliveryRequest(BaseModel):
    """What a producer submits to POST /deliveries."""

    model_config = ConfigDict(extra="forbid")

    event_type: str = Field(
        min_length=1,
        max_length=100,
        pattern=r"^[a-z0-9_]+(\.[a-z0-9_]+)*$",
        examples=["booking.confirmed"],
    )
    data: dict[str, Any] = Field(examples=[{"booking_id": 42}])


class DeliveryAccepted(BaseModel):
    delivery_id: uuid.UUID
    status: DeliveryStatus


class DeliveryDetail(BaseModel):
    delivery_id: uuid.UUID
    event_type: str
    data: dict[str, Any]
    status: DeliveryStatus
    attempt_count: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, delivery: Delivery) -> Self:
        return cls(
            delivery_id=delivery.id,
            event_type=delivery.event_type,
            data=delivery.payload,
            status=DeliveryStatus(delivery.status),
            attempt_count=delivery.attempt_count,
            created_at=delivery.created_at,
            updated_at=delivery.updated_at,
        )


class WebhookMessage(BaseModel):
    """The JSON body the receiver gets, in the shape the Standard Webhooks spec recommends."""

    type: str
    timestamp: datetime
    data: dict[str, Any]
