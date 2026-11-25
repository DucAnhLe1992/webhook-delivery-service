import logging
import re
import time
import uuid

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from webhook_service.api.metrics import HTTP_DURATION, HTTP_REQUESTS
from webhook_service.logs import request_id

log = logging.getLogger("webhook_service.api")

# A caller's request ID is used only if it looks like one. Anything else, such as a line break
# that could forge a log line, gets a new ID instead.
USABLE_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
PROBES = {"/health", "/ready"}


class ObserveRequests:
    """Gives each request an ID, logs one line when it is answered, and records its metrics.

    The ID comes from the X-Request-ID header if the caller sent a usable one, and goes back in
    the answer's X-Request-ID header. Every line logged while handling the request carries it.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        given = Headers(scope=scope).get("x-request-id", "")
        current = given if USABLE_REQUEST_ID.fullmatch(given) else uuid.uuid4().hex
        token = request_id.set(current)
        started = time.perf_counter()
        status = 500

        async def send_with_id(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = [
                    *message.get("headers", []),
                    (b"x-request-id", current.encode()),
                ]
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        finally:
            self.record(scope, status, time.perf_counter() - started)
            request_id.reset(token)

    @staticmethod
    def record(scope: Scope, status: int, seconds: float) -> None:
        # The route template, such as /deliveries/{delivery_id}, keeps the number of metric
        # series small. "none" means the request was answered before it reached a route.
        route = getattr(scope.get("route"), "path", "none")
        producer = scope.get("state", {}).get("producer")
        method = scope["method"]
        HTTP_REQUESTS.labels(method, route, str(status)).inc()
        HTTP_DURATION.labels(method, route).observe(seconds)
        if status >= 500:
            level = logging.WARNING
        elif route in PROBES and status < 400:
            level = logging.DEBUG  # Probes come every few seconds.
        else:
            level = logging.INFO
        milliseconds = round(seconds * 1000)
        log.log(
            level,
            "%s %s %d (%d ms%s)",
            method,
            scope["path"],
            status,
            milliseconds,
            f", {producer}" if producer else "",
            extra={
                "event": "request_finished",
                "method": method,
                "route": route,
                "path": scope["path"],
                "status": status,
                "duration_ms": milliseconds,
                "producer": producer,
            },
        )
