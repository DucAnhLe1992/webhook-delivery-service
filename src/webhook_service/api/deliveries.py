import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.domain import DeliveryStatus
from webhook_service.schemas import DeliveryAccepted, DeliveryDetail, DeliveryRequest
from webhook_service.services import delivery_service

router = APIRouter(prefix="/deliveries", tags=["deliveries"])

SessionDep = Annotated[Session, Depends(get_session)]


# These are plain `def` functions, not `async def`: FastAPI runs them in a thread pool,
# so the blocking database calls do not stall other requests.
@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_409_CONFLICT: {
            "description": "The Idempotency-Key was already used for a different request"
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "description": "Storage is unavailable, so it is not known whether the delivery "
            "was stored. Retry with the same Idempotency-Key."
        },
    },
)
def submit_delivery(
    body: DeliveryRequest,
    response: Response,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
) -> DeliveryAccepted:
    """Store an event for delivery. 202 means it is saved in the database, not yet sent.

    With an Idempotency-Key header, repeating the request returns the original delivery
    (marked with Idempotent-Replayed: true) instead of storing it twice.
    """
    try:
        delivery, created = delivery_service.create_delivery(
            session, event_type=body.event_type, data=body.data, idempotency_key=idempotency_key
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
        status.HTTP_404_NOT_FOUND: {"description": "No delivery with this ID"},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Storage is unavailable"},
    },
)
def read_delivery(delivery_id: uuid.UUID, session: SessionDep) -> DeliveryDetail:
    delivery = delivery_service.get_delivery(session, delivery_id)
    if delivery is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Delivery not found")
    return DeliveryDetail.from_model(delivery)
