from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx2
from fastapi import FastAPI

from webhook_service.api import deliveries
from webhook_service.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    # One shared client reuses connections. Redirects are not followed: the destination is fixed.
    with httpx2.Client(timeout=settings.delivery_timeout_seconds, follow_redirects=False) as client:
        app.state.http_client = client
        yield


app = FastAPI(title="Webhook Delivery Service", version="0.1.0", lifespan=lifespan)
app.include_router(deliveries.router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
