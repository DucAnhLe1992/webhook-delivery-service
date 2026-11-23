from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from webhook_service.sender import AttemptResult


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


class WebhookMessage(BaseModel):
    """The JSON body the receiver gets, in the shape the Standard Webhooks spec recommends."""

    type: str
    timestamp: datetime
    data: dict[str, Any]


class DeliveryStatus(StrEnum):
    DELIVERED = "delivered"
    FAILED = "failed"


class DeliveryResponse(BaseModel):
    delivery_id: str
    status: DeliveryStatus
    attempt: AttemptResult
