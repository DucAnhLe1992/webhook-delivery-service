import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.models import DeliveryStatus
from webhook_service.schemas import DeliveryAccepted, DeliveryDetail, DeliveryRequest
from webhook_service.services import delivery_service

router = APIRouter(prefix="/deliveries", tags=["deliveries"])

SessionDep = Annotated[Session, Depends(get_session)]


# These are plain `def` functions, not `async def`: FastAPI runs them in a thread pool,
# so the blocking database calls do not stall other requests.
@router.post("", status_code=status.HTTP_202_ACCEPTED)
def submit_delivery(
    body: DeliveryRequest, response: Response, session: SessionDep
) -> DeliveryAccepted:
    """Store an event for delivery. 202 means it is saved in the database, not yet sent."""
    delivery = delivery_service.create_delivery(session, event_type=body.event_type, data=body.data)
    response.headers["Location"] = f"/deliveries/{delivery.id}"
    return DeliveryAccepted(delivery_id=delivery.id, status=DeliveryStatus(delivery.status))


@router.get(
    "/{delivery_id}",
    responses={status.HTTP_404_NOT_FOUND: {"description": "No delivery with this ID"}},
)
def read_delivery(delivery_id: uuid.UUID, session: SessionDep) -> DeliveryDetail:
    delivery = delivery_service.get_delivery(session, delivery_id)
    if delivery is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Delivery not found")
    return DeliveryDetail.from_model(delivery)
