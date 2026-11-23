from collections.abc import Iterator
from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from webhook_service.config import get_settings


@lru_cache
def get_engine() -> Engine:
    return create_engine(
        str(get_settings().database_url),
        # Test pooled connections before use, so the API recovers after a database restart.
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


def get_session() -> Iterator[Session]:
    # expire_on_commit=False keeps loaded values readable after commit without another query.
    with Session(get_engine(), expire_on_commit=False) as session:
        yield session
