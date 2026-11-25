"""Which database schema this version of the service needs, and whether the database has it."""

from sqlalchemy import Connection, text

# The newest migration this code needs. A test checks that it is the newest file in
# alembic/versions, so a migration cannot be added without updating it.
REQUIRED_REVISION = "0004"


def schema_problem(connection: Connection) -> str | None:
    """What keeps this code from working with the database's schema, or None if nothing does.

    A later revision is accepted. Migrations run before new code replaces old code, so for a
    while the old code runs against the new schema, which migrations must keep working for it.
    Revisions are numbered in order, so comparing the numbers tells which one is later.
    """
    if connection.scalar(text("SELECT to_regclass('alembic_version')")) is None:
        return "the database has no tables yet. Run `uv run alembic upgrade head`."
    revision = connection.scalar(text("SELECT version_num FROM alembic_version"))
    if revision is None or not revision.isdigit() or int(revision) < int(REQUIRED_REVISION):
        return (
            f"the database schema is at revision {revision}, and this version of the service "
            f"needs {REQUIRED_REVISION} or later. Run `uv run alembic upgrade head`."
        )
    return None
