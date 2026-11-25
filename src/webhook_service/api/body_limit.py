from fastapi import HTTPException, status
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from webhook_service.config import get_settings


class BodySizeLimit:
    """Refuse request bodies larger than MAX_EVENT_BYTES with 413, without reading them whole.

    FastAPI reads the whole body into memory before any dependency or route runs, so the limit
    is applied here, around the server, while the body arrives.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        limit = get_settings().max_event_bytes
        too_large = f"The request body is larger than the limit of {limit} bytes"

        declared = dict(scope["headers"]).get(b"content-length", b"")
        if declared.isdigit() and int(declared) > limit:
            # Refused before a single byte of the body is read.
            response = JSONResponse({"detail": too_large}, status.HTTP_413_CONTENT_TOO_LARGE)
            await response(scope, receive, send)
            return

        received = 0

        async def receive_within_limit() -> Message:
            # A body without Content-Length (sent in chunks) is counted as it arrives.
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    # FastAPI passes an HTTPException raised while it reads the body on to
                    # its exception handling, which answers 413.
                    raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, detail=too_large)
            return message

        await self.app(scope, receive_within_limit, send)
