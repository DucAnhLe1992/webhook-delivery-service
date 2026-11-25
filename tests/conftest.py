import io
import json
import logging
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from prometheus_client import REGISTRY

from webhook_service.config import Settings
from webhook_service.logs import configure_logging


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """Undo configure_logging, which the worker's main() calls, so no test logs into the next."""
    root = logging.getLogger()
    level = root.level
    yield
    root.handlers[:] = [h for h in root.handlers if not getattr(h, "webhook_service", False)]
    root.setLevel(level)


@pytest.fixture
def json_logs() -> Callable[[], list[dict[str, Any]]]:
    """Log in JSON, as the services do with LOG_FORMAT=json. Returns a reader of the new lines."""
    configure_logging("api", Settings(_env_file=None, log_format="json"))
    [handler] = [h for h in logging.getLogger().handlers if getattr(h, "webhook_service", False)]
    # Not capsys: pytest gives each test phase a new stream and closes the one before.
    stream = io.StringIO()
    handler.setStream(stream)  # type: ignore[attr-defined]

    def read() -> list[dict[str, Any]]:
        lines = stream.getvalue().splitlines()
        stream.seek(0)
        stream.truncate()
        return [json.loads(line) for line in lines]

    return read


@pytest.fixture
def count_changes() -> Callable[..., Callable[[], dict[str, float]]]:
    """Watch metric samples, given as name=(sample, labels). Returns a function that gives the
    ones that changed since, and by how much. Metrics are kept for the whole test run, so tests
    compare before and after."""

    def watch(**samples: tuple[str, dict[str, str]]) -> Callable[[], dict[str, float]]:
        def read() -> dict[str, float]:
            return {
                key: REGISTRY.get_sample_value(name, labels) or 0.0
                for key, (name, labels) in samples.items()
            }

        before = read()
        return lambda: {key: now - before[key] for key, now in read().items() if now != before[key]}

    return watch
