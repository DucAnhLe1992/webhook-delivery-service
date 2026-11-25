from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy import URL, Engine, create_engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session

from webhook_service.config import Settings, get_settings

# If the database host or the network to it dies without closing the connection, a process
# waiting for an answer gets no error at all. The operating system's keepalive probes would
# start only after two hours. With these, a dead connection fails after about 25 seconds:
# probes start after 10 seconds of silence, and the connection is dropped once nothing has
# come back for 25 seconds, also when data that was sent is never acknowledged.
TCP_SETTINGS = {
    "keepalives": 1,
    "keepalives_idle": 10,
    "keepalives_interval": 5,
    "keepalives_count": 3,
    "tcp_user_timeout": 25_000,
}


def milliseconds(seconds: float) -> int:
    # PostgreSQL reads 0 as "no limit", so a small value is never rounded down to 0.
    return max(1, round(seconds * 1000))


def create_db_engine(url: str | URL, settings: Settings) -> Engine:
    """An engine whose every wait on the database is bounded, as the settings say."""
    idle = settings.database_idle_in_transaction_timeout_seconds
    server_limits = {
        "statement_timeout": settings.database_statement_timeout_seconds,
        "lock_timeout": settings.database_lock_timeout_seconds,
        "idle_in_transaction_session_timeout": idle,
    }
    return create_engine(
        url,
        # Test pooled connections before use, so the API recovers after a database restart.
        pool_pre_ping=True,
        pool_timeout=settings.database_pool_timeout_seconds,
        connect_args={
            "connect_timeout": settings.database_connect_timeout_seconds,
            # PostgreSQL applies these to every session the engine opens.
            "options": " ".join(
                f"-c {name}={milliseconds(seconds)}" for name, seconds in server_limits.items()
            ),
            **TCP_SETTINGS,
        },
    )


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    return create_db_engine(str(settings.database_url), settings)


def get_session() -> Iterator[Session]:
    # expire_on_commit=False keeps loaded values readable after commit without another query.
    with Session(get_engine(), expire_on_commit=False) as session:
        yield session


def storage_unavailable(error: BaseException) -> bool:
    """Whether a database error means the database could not serve the request in time.

    That is the case when it could not be reached, a limit ran out (a statement, a lock wait,
    or a wait for a pooled connection), or it ended the session. Other errors are bugs.
    """
    if isinstance(error, OperationalError | PoolTimeoutError):
        return True
    # For example, PostgreSQL ended a session that sat idle in a transaction for too long.
    return isinstance(error, DBAPIError) and error.connection_invalidated


def describe_error(error: BaseException) -> str:
    """The first line of a database error, such as "canceling statement due to lock timeout"."""
    cause = error.orig if isinstance(error, DBAPIError) else error
    lines = str(cause).strip().splitlines()
    return lines[0] if lines else type(cause).__name__
