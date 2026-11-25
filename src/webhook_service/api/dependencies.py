"""What the routes depend on: a database session, and the producer making the request."""

from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.models import Producer
from webhook_service.services import producer_service

# The session closes when the route function returns, before the response is sent. By
# default it would stay open while the response travels to the client, so a slow client would
# keep a pooled connection, and an open transaction with its locks, for as long as it liked.
SessionDep = Annotated[Session, Depends(get_session, scope="function")]

bearer = HTTPBearer(
    auto_error=False,
    description="The producer's API key, created with `webhook-admin create-producer`.",
)


def unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": "Bearer"}
    )


def authenticate(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    session: SessionDep,
) -> Producer:
    """The producer whose API key the request carries. Any other request gets 401."""
    if credentials is None:
        # Answered before the database is used, so requests without a key cost almost nothing.
        raise unauthorized("Missing API key. Send it in the header Authorization: Bearer <key>")
    producer = producer_service.find_producer(session, credentials.credentials)
    if producer is None:
        # The same answer for unknown and revoked keys: a caller cannot tell which keys existed.
        raise unauthorized("Invalid API key")
    return producer


ProducerDep = Annotated[Producer, Depends(authenticate)]
