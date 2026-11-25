from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from webhook_service.config import get_settings
from webhook_service.db import milliseconds
from webhook_service.models import Base

config = context.config

if config.config_file_name is not None:
    # Keep loggers created before migrations run (for example in tests) enabled.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

# Tests pass their own values; otherwise use the app's settings.
database_url = config.attributes.get("database_url") or str(get_settings().database_url)
lock_timeout_seconds = (
    config.attributes.get("lock_timeout_seconds") or get_settings().database_lock_timeout_seconds
)


def run_migrations_offline() -> None:
    """Print the SQL instead of running it: `alembic upgrade head --sql`."""
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # While a migration waits for a lock, every later query on that table queues behind it,
    # even plain reads. So a migration gives up after DATABASE_LOCK_TIMEOUT_SECONDS, and is run
    # again later, rather than stall the service behind a long transaction. Its statements have
    # no time limit: changing data can take as long as it needs.
    engine = create_engine(
        database_url,
        poolclass=pool.NullPool,
        connect_args={"options": f"-c lock_timeout={milliseconds(lock_timeout_seconds)}"},
    )
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
