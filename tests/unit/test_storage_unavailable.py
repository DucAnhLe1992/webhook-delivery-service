import threading
import uuid
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from webhook_service.config import Settings
from webhook_service.db import get_session
from webhook_service.main import app
from webhook_service.worker import main as worker

# Nothing listens on port 1, so every connection attempt is refused.
UNREACHABLE = "postgresql+psycopg://user:password@127.0.0.1:1/db"


@pytest.fixture
def storage_down() -> Iterator[TestClient]:
    unreachable = create_engine(UNREACHABLE)

    def unreachable_session() -> Iterator[Session]:
        with Session(unreachable) as session:
            yield session

    app.dependency_overrides[get_session] = unreachable_session
    # With a key, so that checking it is what needs the database.
    yield TestClient(app, headers={"Authorization": "Bearer wds_any"})
    app.dependency_overrides.clear()


def test_a_submission_while_storage_is_unavailable_returns_503_not_202(storage_down):
    response = storage_down.post(
        "/deliveries", json={"event_type": "booking.confirmed", "data": {}}
    )

    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"
    # Not "not stored": the API cannot always know, so it says how to retry safely.
    assert "Retry with the same Idempotency-Key" in response.json()["detail"]


def test_a_read_while_storage_is_unavailable_returns_503(storage_down):
    response = storage_down.get(f"/deliveries/{uuid.uuid4()}")

    assert response.status_code == 503
    assert response.json() == {"detail": "Storage is unavailable or too busy. Retry later."}


def test_the_api_stays_live_but_is_not_ready_while_storage_is_unavailable(storage_down):
    live = storage_down.get("/health")
    ready = storage_down.get("/ready")

    # Restarting the API would not bring the database back, so liveness ignores it.
    assert (live.status_code, live.json()) == (200, {"status": "ok"})
    assert ready.status_code == 503
    assert ready.json() == {
        "status": "not ready",
        "detail": "the database is unavailable or too busy",
    }


def test_the_worker_waits_for_a_database_that_is_down_at_startup(caplog):
    settings = Settings(_env_file=None, poll_interval_seconds=0.05)
    stop = threading.Event()
    threading.Timer(0.5, stop.set).start()

    started = worker.wait_for_database(create_engine(UNREACHABLE), settings, stop)

    # It kept trying until asked to stop, instead of exiting.
    assert started is False
    assert "database error (connection failed" in caplog.text
