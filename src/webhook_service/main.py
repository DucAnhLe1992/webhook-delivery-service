import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from prometheus_client import REGISTRY
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from webhook_service.api import deliveries, health
from webhook_service.api.body_limit import BodySizeLimit
from webhook_service.api.metrics import QueueCollector
from webhook_service.api.observe import ObserveRequests
from webhook_service.config import get_settings
from webhook_service.db import describe_error, get_engine
from webhook_service.logs import configure_logging
from webhook_service.metrics import start_metrics_server, stop_metrics_server

log = logging.getLogger("webhook_service.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Every request reads the settings. Reading them once at startup makes an invalid value
    # stop the API with a clear error, instead of failing each request with 500.
    settings = get_settings()
    configure_logging("api", settings)
    queue = QueueCollector(get_engine())
    REGISTRY.register(queue)
    metrics_server = start_metrics_server(
        settings.metrics_host, settings.api_metrics_port, setting="API_METRICS_PORT"
    )
    try:
        yield
    finally:
        stop_metrics_server(metrics_server)
        REGISTRY.unregister(queue)


app = FastAPI(
    title="Webhook Delivery Service",
    version=version("webhook-delivery-service"),
    lifespan=lifespan,
)
app.add_middleware(BodySizeLimit)
# Added last, so it runs first: it also sees requests that the body limit refuses.
app.add_middleware(ObserveRequests)
app.include_router(deliveries.router)
app.include_router(health.router)


@app.exception_handler(OperationalError)
@app.exception_handler(PoolTimeoutError)
def storage_unavailable(request: Request, exc: Exception) -> JSONResponse:
    # The database could not be reached, or a limit ran out: a statement, a lock wait, or the
    # wait for a pooled connection. Never answer 202 then. A failed submission is not proof that
    # nothing was stored, though: the connection can fail after PostgreSQL committed but before
    # the API heard back. Retrying with the same Idempotency-Key is safe either way, because it
    # finds the stored delivery.
    log.warning(
        "storage unavailable or too busy (%s)",
        describe_error(exc),
        extra={"event": "storage_unavailable", "reason": describe_error(exc)},
    )
    if request.method == "POST":
        detail = (
            "Storage is unavailable or too busy, so it is not known whether the delivery was "
            "stored. Retry with the same Idempotency-Key."
        )
    else:
        detail = "Storage is unavailable or too busy. Retry later."
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": detail},
        headers={"Retry-After": "5"},
    )
