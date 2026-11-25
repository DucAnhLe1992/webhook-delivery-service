import os
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import URL, Engine, create_engine, make_url, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from webhook_service.config import get_settings
from webhook_service.db import get_session
from webhook_service.main import app
from webhook_service.services import producer_service

ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = make_url(
    os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_test",
    )
)


def refuse_unless_disposable(url: URL) -> None:
    """The tests delete all data in their database, so refuse any database not meant for that."""
    if not (url.database or "").endswith("_test"):
        pytest.fail(
            f"TEST_DATABASE_URL names the database {url.database!r}. The tests delete all of "
            "its data, so its name must end in _test.",
            pytrace=False,
        )
    if url.database == make_url(str(get_settings().database_url)).database:
        pytest.fail(
            f"TEST_DATABASE_URL and DATABASE_URL both name {url.database!r}. The tests delete "
            "all data in their database, so it must not be the one the service uses.",
            pytrace=False,
        )


def create_database_if_missing(url: URL) -> None:
    server = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with server.connect() as connection:
            exists = connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database}
            )
            if not exists:
                name = connection.dialect.identifier_preparer.quote_identifier(url.database)
                connection.execute(text(f"CREATE DATABASE {name}"))
    finally:
        server.dispose()


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    """The test database, migrated to the latest schema with the real Alembic migrations."""
    refuse_unless_disposable(TEST_DATABASE_URL)
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
        connection.execute(text("TRUNCATE deliveries, delivery_attempts, api_keys, producers"))
    return engine


def authenticated_as(api_key: str) -> TestClient:
    return TestClient(app, headers={"Authorization": f"Bearer {api_key}"})


@pytest.fixture
def api_key(db: Engine) -> str:
    """The API key of the producer carrental."""
    with Session(db) as session:
        _, key = producer_service.create_producer(session, "carrental")
    return key


@pytest.fixture
def client(db: Engine, api_key: str) -> Iterator[TestClient]:
    """An API client for the producer carrental, using the emptied test database."""

    def test_session() -> Iterator[Session]:
        with Session(db, expire_on_commit=False) as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    yield authenticated_as(api_key)
    app.dependency_overrides.clear()


@pytest.fixture
def client_for(client: TestClient, db: Engine) -> Callable[[str], TestClient]:
    """Return a function that creates a producer and an API client for it."""

    def create(name: str) -> TestClient:
        with Session(db) as session:
            _, key = producer_service.create_producer(session, name)
        return authenticated_as(key)

    return create


@pytest.fixture
def wait_until_blocked(db: Engine) -> Callable[[], None]:
    """Return a function that waits until a query is blocked by another transaction's lock."""

    def wait(timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        with db.connect().execution_options(isolation_level="AUTOCOMMIT") as observer:
            while time.monotonic() < deadline:
                waiting = observer.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' AND datname = current_database()"
                    )
                )
                if waiting:
                    return
                time.sleep(0.01)
        raise AssertionError("no query started waiting for a lock")

    return wait
