import uuid
from datetime import UTC, datetime
from typing import Annotated

import httpx2
from fastapi import APIRouter, Depends, Request, Response, status

from webhook_service.config import Settings, get_settings
from webhook_service.schemas import (
    DeliveryRequest,
    DeliveryResponse,
    DeliveryStatus,
    WebhookMessage,
)
from webhook_service.sender import AttemptOutcome, ErrorCategory, send_webhook

router = APIRouter(prefix="/deliveries", tags=["deliveries"])


def get_http_client(request: Request) -> httpx2.Client:
    return request.app.state.http_client


# Milestone 1: the producer's request waits while the webhook is sent. If the receiver
# is down, the producer gets an error and nothing remembers the event. Milestone 2
# stores the delivery first and answers 202 Accepted instead.
#
# This is a plain `def`, not `async def`: FastAPI runs it in a thread pool, so the
# blocking HTTPX2 call does not stall other requests.
@router.post(
    "",
    responses={
        status.HTTP_502_BAD_GATEWAY: {
            "model": DeliveryResponse,
            "description": "The receiver failed or could not be reached",
        },
        status.HTTP_504_GATEWAY_TIMEOUT: {
            "model": DeliveryResponse,
            "description": "The receiver did not answer in time",
        },
    },
)
def create_delivery(
    body: DeliveryRequest,
    response: Response,
    client: Annotated[httpx2.Client, Depends(get_http_client)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> DeliveryResponse:
    delivery_id = str(uuid.uuid4())
    message = WebhookMessage(type=body.event_type, timestamp=datetime.now(UTC), data=body.data)
    attempt = send_webhook(
        client, str(settings.receiver_url), delivery_id, message.model_dump(mode="json")
    )

    if attempt.outcome is AttemptOutcome.DELIVERED:
        return DeliveryResponse(
            delivery_id=delivery_id, status=DeliveryStatus.DELIVERED, attempt=attempt
        )

    response.status_code = (
        status.HTTP_504_GATEWAY_TIMEOUT
        if attempt.error_category is ErrorCategory.TIMEOUT
        else status.HTTP_502_BAD_GATEWAY
    )
    return DeliveryResponse(delivery_id=delivery_id, status=DeliveryStatus.FAILED, attempt=attempt)
