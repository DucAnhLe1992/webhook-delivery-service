import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session

from webhook_service import schema, signing
from webhook_service.config import Settings
from webhook_service.db import storage_unavailable
from webhook_service.main import app
from webhook_service.worker import main as worker

ROOT = Path(__file__).resolve().parents[2]
EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}
# The test database is migrated to the newest revision, which is the one the code requires.
CURRENT = schema.REQUIRED_REVISION
OLDER, NEWER = (f"{int(CURRENT) + step:04d}" for step in (-1, 1))


LOCK_PRODUCER = text("SELECT 1 FROM producers WHERE id = :id FOR UPDATE")


def a_producer(db: Engine):
    with db.begin() as connection:
        return connection.scalar(
            text("INSERT INTO producers (id, name) VALUES (gen_random_uuid(), 'p') RETURNING id")
        )


def test_a_statement_that_runs_too_long_is_cancelled(short_limits_engine):
    started = time.monotonic()

    with short_limits_engine.connect() as connection, pytest.raises(OperationalError) as error:
        connection.execute(text("SELECT pg_sleep(30)"))

    assert time.monotonic() - started < 5
    assert "statement timeout" in str(error.value)
    assert storage_unavailable(error.value)


def test_a_wait_for_a_lock_is_cancelled(db, short_limits_engine):
    producer_id = a_producer(db)
    with db.connect() as stuck:
        # A transaction that holds a lock and does not end.
        stuck.execute(LOCK_PRODUCER, {"id": producer_id})
        started = time.monotonic()
        with short_limits_engine.connect() as connection, pytest.raises(OperationalError) as error:
            connection.execute(LOCK_PRODUCER, {"id": producer_id})

    assert time.monotonic() - started < 5
    assert "lock timeout" in str(error.value)
    assert storage_unavailable(error.value)


def test_a_wait_for_a_pooled_connection_is_cut_short(short_limits_engine):
    held = []
    try:
        with pytest.raises(PoolTimeoutError) as error:
            for _ in range(100):  # Far more connections than the pool hands out.
                started = time.monotonic()
                held.append(short_limits_engine.connect())
        waited = time.monotonic() - started
    finally:
        for connection in held:
            connection.close()

    assert waited < 5
    assert storage_unavailable(error.value)


def test_a_transaction_left_idle_is_ended_and_its_locks_released(db, short_limits_engine):
    producer_id = a_producer(db)

    with Session(short_limits_engine) as frozen:
        frozen.execute(LOCK_PRODUCER, {"id": producer_id})
        time.sleep(1)  # A process that freezes in the middle of its transaction.

        # The lock is free again, so PostgreSQL ended the frozen transaction. Otherwise this
        # waits for the lock and fails with a lock timeout.
        with db.connect() as other:
            other.execute(LOCK_PRODUCER, {"id": producer_id})
        with pytest.raises(InternalError) as error:
            frozen.execute(text("SELECT 1"))

    assert "idle-in-transaction timeout" in str(error.value)
    assert storage_unavailable(error.value)


def test_a_bug_is_not_mistaken_for_an_unavailable_database(db):
    with db.connect() as connection, pytest.raises(IntegrityError) as error:
        # A producer without a name breaks a constraint: a bug, which must not look like an outage.
        connection.execute(text("INSERT INTO producers (id) VALUES (gen_random_uuid())"))

    assert not storage_unavailable(error.value)


class PoolWatcher:
    """Wraps the app and counts the pooled connections in use when a response starts."""

    def __init__(self, app, engine: Engine):
        self.app, self.engine, self.in_use = app, engine, []

    async def __call__(self, scope, receive, send):
        async def watching_send(message):
            if message["type"] == "http.response.start":
                self.in_use.append(self.engine.pool.checkedout())
            await send(message)

        await self.app(scope, receive, watching_send)


def test_a_request_holds_no_connection_while_its_response_is_sent(client, db, api_key):
    delivery_id = client.post("/deliveries", json=EVENT).json()["delivery_id"]
    watcher = PoolWatcher(app, db)

    response = TestClient(watcher, headers={"Authorization": f"Bearer {api_key}"}).get(
        f"/deliveries/{delivery_id}"
    )

    assert response.status_code == 200
    # A slow client cannot keep a connection, or a transaction and its locks, while it reads.
    assert watcher.in_use == [0]


@pytest.mark.parametrize(
    ("required", "ready"),
    [(OLDER, True), (CURRENT, True), (NEWER, False)],
    ids=["code-older-than-database", "same", "code-newer-than-database"],
)
def test_the_api_is_ready_unless_its_code_is_newer_than_the_database(
    client, monkeypatch, required, ready
):
    monkeypatch.setattr(schema, "REQUIRED_REVISION", required)

    # Readiness needs no API key: whatever routes traffic calls it.
    response = TestClient(app).get("/ready")

    if ready:
        assert (response.status_code, response.json()) == (200, {"status": "ready"})
    else:
        assert response.status_code == 503
        assert response.json() == {
            "status": "not ready",
            "detail": f"the database schema is at revision {CURRENT}, and this version of the "
            f"service needs {NEWER} or later. Run `uv run alembic upgrade head`.",
        }


def test_a_database_without_tables_is_reported(engine):
    server = create_engine(engine.url.set(database="postgres"))
    try:
        with server.connect() as connection:
            problem = schema.schema_problem(connection)
    finally:
        server.dispose()

    assert problem == "the database has no tables yet. Run `uv run alembic upgrade head`."


def test_the_worker_does_not_start_when_its_code_is_newer_than_the_database(db, monkeypatch):
    settings = Settings(
        _env_file=None, webhook_secrets=[signing.new_secret()], worker_metrics_port=0
    )
    monkeypatch.setattr(schema, "REQUIRED_REVISION", NEWER)
    monkeypatch.setattr(worker, "get_settings", lambda: settings)
    monkeypatch.setattr(worker, "get_engine", lambda: db)
    monkeypatch.setattr(worker.signal, "signal", lambda *args: None)
    monkeypatch.setattr(worker, "run", lambda *args: pytest.fail("the worker started"))

    with pytest.raises(SystemExit, match=f"needs {NEWER} or later"):
        worker.main()


def test_a_migration_waiting_for_a_lock_gives_up_instead_of_holding_up_every_query(engine):
    url = engine.url.set(database=f"{engine.url.database}_migrations_test")
    server = create_engine(engine.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with server.connect() as connection:
        connection.execute(text(f'DROP DATABASE IF EXISTS "{url.database}" WITH (FORCE)'))
        connection.execute(text(f'CREATE DATABASE "{url.database}"'))
    config = Config()
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.attributes["database_url"] = url
    config.attributes["lock_timeout_seconds"] = 0.3
    command.upgrade(config, "0003")
    database = create_engine(url)
    pool = ThreadPoolExecutor(max_workers=1)

    def revision() -> str:
        with database.connect() as connection:
            return connection.scalar(text("SELECT version_num FROM alembic_version"))

    try:
        with database.connect() as reader:
            # A long transaction that has read deliveries, such as a report.
            reader.execute(text("SELECT count(*) FROM deliveries"))
            started = time.monotonic()
            # Migration 0004 alters deliveries, which needs the reader to finish first. While
            # it waits, every other query on deliveries would wait behind it.
            with pytest.raises(OperationalError, match="lock timeout"):
                pool.submit(command.upgrade, config, "head").result(timeout=10)
            assert time.monotonic() - started < 5
        assert revision() == "0003"

        command.upgrade(config, "head")  # Run again once the reader is done.

        assert revision() == CURRENT
    finally:
        pool.shutdown(wait=False)
        database.dispose()
        with server.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{url.database}" WITH (FORCE)'))
        server.dispose()
