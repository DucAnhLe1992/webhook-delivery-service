from collections.abc import Iterator

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.main import app


def test_unreachable_database_returns_503_not_202():
    # Nothing listens on port 1, so every connection attempt is refused.
    unreachable = create_engine(
        "postgresql+psycopg://user:password@127.0.0.1:1/db", connect_args={"connect_timeout": 1}
    )

    def unreachable_session() -> Iterator[Session]:
        with Session(unreachable) as session:
            yield session

    app.dependency_overrides[get_session] = unreachable_session
    try:
        response = TestClient(app).post(
            "/deliveries", json={"event_type": "booking.confirmed", "data": {}}
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"
