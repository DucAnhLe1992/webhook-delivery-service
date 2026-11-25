import uuid
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.main import app


@pytest.fixture
def storage_down() -> Iterator[TestClient]:
    # Nothing listens on port 1, so every connection attempt is refused.
    unreachable = create_engine(
        "postgresql+psycopg://user:password@127.0.0.1:1/db", connect_args={"connect_timeout": 1}
    )

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
    assert response.json() == {"detail": "Storage is unavailable. Retry later."}
