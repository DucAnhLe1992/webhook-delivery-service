from importlib.metadata import version

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError

from webhook_service.api import deliveries

app = FastAPI(title="Webhook Delivery Service", version=version("webhook-delivery-service"))
app.include_router(deliveries.router)


@app.exception_handler(OperationalError)
def storage_unavailable(request: Request, exc: OperationalError) -> JSONResponse:
    # The database could not be reached, so the delivery was not accepted. Never answer
    # 202 in this case; tell the producer to try again instead.
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Storage is unavailable; the delivery was not accepted. Retry later."},
        headers={"Retry-After": "5"},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
