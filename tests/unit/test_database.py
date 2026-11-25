from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import ValidationError

from webhook_service import schema
from webhook_service.config import Settings

ROOT = Path(__file__).resolve().parents[2]


def test_a_lock_timeout_must_be_shorter_than_the_statement_timeout():
    with pytest.raises(ValidationError, match="DATABASE_LOCK_TIMEOUT_SECONDS"):
        Settings(
            _env_file=None,
            database_statement_timeout_seconds=2,
            database_lock_timeout_seconds=2,
        )


def test_migrations_are_numbered_in_order_and_the_code_requires_the_newest():
    config = Config()
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)

    newest_first = [revision.revision for revision in script.walk_revisions()]

    # The schema check compares revision numbers, which only works if they count up.
    assert newest_first[::-1] == [f"{number:04d}" for number in range(1, len(newest_first) + 1)]
    assert newest_first[0] == schema.REQUIRED_REVISION
