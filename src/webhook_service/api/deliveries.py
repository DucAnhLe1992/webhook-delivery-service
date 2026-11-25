import uuid
from typing import Annotated

from fastapi import APIRouter, Header, HTTPException, Response, status

from webhook_service.api.dependencies import ProducerDep, SessionDep
from webhook_service.domain import DeliveryStatus
from webhook_service.schemas import DeliveryAccepted, DeliveryDetail, DeliveryRequest
from webhook_service.services import delivery_service

router = APIRouter(
    prefix="/deliveries",
    tags=["deliveries"],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Missing or invalid API key"}},
)


# These are plain `def` functions, not `async def`: FastAPI runs them in a thread pool,
# so the blocking database calls do not stall other requests.
@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "The Idempotency-Key was already used for a different request"
        },
        status.HTTP_413_CONTENT_TOO_LARGE: {
            "description": "The request body is larger than MAX_EVENT_BYTES"
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "description": "Storage is unavailable or too busy, so it is not known whether the "
            "delivery was stored. Retry with the same Idempotency-Key."
        },
    },
)
def submit_delivery(
    body: DeliveryRequest,
    response: Response,
    producer: ProducerDep,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
) -> DeliveryAccepted:
    """Store an event for delivery. 202 means it is saved in the database, not yet sent.

    With an Idempotency-Key header, repeating the request returns the original delivery
    (marked with Idempotent-Replayed: true) instead of storing it twice. Each producer has
    its own idempotency keys.
    """
    try:
        delivery, created = delivery_service.create_delivery(
            session,
            producer_id=producer.id,
            event_type=body.event_type,
            data=body.data,
            idempotency_key=idempotency_key,
        )
    except delivery_service.IdempotencyKeyReused:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail="This Idempotency-Key was already used for a different request",
        ) from None
    response.headers["Location"] = f"/deliveries/{delivery.id}"
    if not created:
        response.headers["Idempotent-Replayed"] = "true"
    return DeliveryAccepted(delivery_id=delivery.id, status=DeliveryStatus(delivery.status))


@router.get(
    "/{delivery_id}",
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "This producer has no delivery with this ID"},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Storage is unavailable or too busy"},
    },
)
def read_delivery(
    delivery_id: uuid.UUID, producer: ProducerDep, session: SessionDep
) -> DeliveryDetail:
    delivery = delivery_service.get_delivery(session, delivery_id, producer_id=producer.id)
    if delivery is None:
        # Also when the delivery belongs to another producer: 403 would confirm that it exists.
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Delivery not found")
    return DeliveryDetail.from_model(delivery)
