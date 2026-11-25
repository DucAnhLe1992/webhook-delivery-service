from importlib.metadata import version

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from webhook_service.api import deliveries

app = FastAPI(title="Webhook Delivery Service", version=version("webhook-delivery-service"))
app.include_router(deliveries.router)


@app.exception_handler(OperationalError)
@app.exception_handler(PoolTimeoutError)
def storage_unavailable(request: Request, exc: Exception) -> JSONResponse:
    # The database could not be reached, or no pooled connection became free in time. Never
    # answer 202 then. A failed submission is not proof that nothing was stored, though: the
    # connection can fail after PostgreSQL committed but before the API heard back. Retrying
    # with the same Idempotency-Key is safe either way, because it finds the stored delivery.
    if request.method == "POST":
        detail = (
            "Storage is unavailable, so it is not known whether the delivery was stored. "
            "Retry with the same Idempotency-Key."
        )
    else:
        detail = "Storage is unavailable. Retry later."
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": detail},
        headers={"Retry-After": "5"},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
