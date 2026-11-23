import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import URL, Engine, create_engine, make_url, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from webhook_service.db import get_session
from webhook_service.main import app

ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = make_url(
    os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_test",
    )
)


def create_database_if_missing(url: URL) -> None:
    server = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with server.connect() as connection:
            exists = connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
            )
            if not exists:
                connection.execute(text(f'CREATE DATABASE "{url.database}"'))
    finally:
        server.dispose()


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    """The test database, migrated to the latest schema with the real Alembic migrations."""
    try:
        create_database_if_missing(TEST_DATABASE_URL)
    except OperationalError:
        pytest.fail(
            f"PostgreSQL is not reachable at {TEST_DATABASE_URL.render_as_string()}. "
            "Start it with `docker compose up -d`.",
            pytrace=False,
        )
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = TEST_DATABASE_URL
    command.upgrade(config, "head")

    engine = create_engine(TEST_DATABASE_URL)
    yield engine
    engine.dispose()


@pytest.fixture
def db(engine: Engine) -> Engine:
    """The test database, emptied before each test."""
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE deliveries"))
    return engine


@pytest.fixture
def client(db: Engine) -> Iterator[TestClient]:
    """An API client that uses the emptied test database."""

    def test_session() -> Iterator[Session]:
        with Session(db, expire_on_commit=False) as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    yield TestClient(app)
    app.dependency_overrides.clear()
