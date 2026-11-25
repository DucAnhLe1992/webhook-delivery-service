"""Liveness and readiness, for whatever starts and routes traffic to the API."""

from typing import Literal

from fastapi import APIRouter, Response, status
from pydantic import BaseModel

from webhook_service.api.dependencies import SessionDep
from webhook_service.db import storage_unavailable
from webhook_service.schema import schema_problem

router = APIRouter(tags=["health"])


class Readiness(BaseModel):
    status: Literal["ready", "not ready"]
    detail: str | None = None


@router.get("/health")
def health() -> dict[str, str]:
    """Liveness: the process is up and answering.

    It does not use the database. A database outage is not cured by restarting the API, so it
    must not make the API look dead.
    """
    return {"status": "ok"}


@router.get(
    "/ready",
    response_model_exclude_none=True,
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": Readiness,
            "description": "The database cannot be reached, or its schema is behind the code",
        }
    },
)
def ready(session: SessionDep, response: Response) -> Readiness:
    """Readiness: whether this instance can serve requests now.

    It checks that a pooled connection can be had and that the database's schema is the one
    this code needs. Send traffic only to ready instances, but do not restart one that is not.
    """
    try:
        problem = schema_problem(session.connection())
    except Exception as error:
        if not storage_unavailable(error):
            raise
        problem = "the database is unavailable or too busy"
    if problem is not None:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return Readiness(status="not ready", detail=problem)
    return Readiness(status="ready")
