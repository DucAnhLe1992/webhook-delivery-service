"""Logging for the API and the worker: lines for people, or JSON objects for a log collector.

The service's own log calls pass fields with `extra=`, such as delivery_id, attempt and
outcome. The text format shows only the message. The JSON format adds every field, so logs can
be searched by delivery, producer or outcome. Request and event bodies, API keys and webhook
secrets are never logged.
"""

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime

from webhook_service.config import Settings

# The ID of the request being handled. Every line logged while handling it carries it.
request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)

# Attributes every log record has. Any other attribute is a field passed with extra=.
STANDARD_ATTRIBUTES = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "asctime",
}


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        current = request_id.get()
        if current is not None and not hasattr(record, "request_id"):
            record.request_id = current
        return True


class TextFormatter(logging.Formatter):
    def __init__(self, process: str) -> None:
        super().__init__(f"%(asctime)s [{process}] %(message)s", datefmt="%H:%M:%S")


class JsonFormatter(logging.Formatter):
    def __init__(self, process: str) -> None:
        super().__init__()
        self.process = process

    def format(self, record: logging.LogRecord) -> str:
        created = datetime.fromtimestamp(record.created, UTC)
        entry = {
            "time": created.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "process": self.process,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for name, value in vars(record).items():
            if name not in STANDARD_ATTRIBUTES and not name.startswith("_"):
                entry[name] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(process: str, settings: Settings) -> None:
    """Send every log line, the service's and Uvicorn's, to stderr in the chosen format."""
    handler = logging.StreamHandler(sys.stderr)
    formatter = JsonFormatter if settings.log_format == "json" else TextFormatter
    handler.setFormatter(formatter(process))
    handler.addFilter(RequestIdFilter())
    handler.webhook_service = True  # type: ignore[attr-defined]
    root = logging.getLogger()
    # Replace a handler from an earlier call, and keep any others, such as a test runner's.
    others = [h for h in root.handlers if not getattr(h, "webhook_service", False)]
    root.handlers[:] = [*others, handler]
    root.setLevel(settings.log_level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers.clear()
        logging.getLogger(name).propagate = True
    # The API logs each request itself, with more fields than Uvicorn's access log.
    logging.getLogger("uvicorn.access").disabled = True
    # HTTPX2 logs every request at INFO; the worker already logs one line per attempt.
    logging.getLogger("httpx2").setLevel(logging.WARNING)
