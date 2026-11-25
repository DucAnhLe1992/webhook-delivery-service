from datetime import timedelta

import pytest
from pydantic import ValidationError

from webhook_service.config import Settings
from webhook_service.retry import backoff_delay


def without_jitter() -> float:
    return 0.5


@pytest.mark.parametrize(("attempt", "seconds"), [(1, 10), (2, 20), (3, 40), (4, 80), (10, 3600)])
def test_delay_doubles_after_each_attempt_up_to_the_maximum(attempt, seconds):
    delay = backoff_delay(
        attempt, base_seconds=10, max_seconds=3600, random_fraction=without_jitter
    )

    assert delay == timedelta(seconds=seconds)


def test_jitter_varies_the_delay_by_up_to_20_percent():
    shortest = backoff_delay(1, base_seconds=10, max_seconds=3600, random_fraction=lambda: 0.0)
    longest = backoff_delay(1, base_seconds=10, max_seconds=3600, random_fraction=lambda: 1.0)

    assert (shortest, longest) == (timedelta(seconds=8), timedelta(seconds=12))


def test_jitter_never_takes_the_delay_beyond_the_maximum():
    shortest = backoff_delay(10, base_seconds=10, max_seconds=3600, random_fraction=lambda: 0.0)
    longest = backoff_delay(10, base_seconds=10, max_seconds=3600, random_fraction=lambda: 1.0)

    assert (shortest, longest) == (timedelta(seconds=2880), timedelta(seconds=3600))


def test_claim_timeout_must_outlast_a_slow_attempt():
    with pytest.raises(ValidationError, match="CLAIM_TIMEOUT_SECONDS"):
        Settings(delivery_timeout_seconds=5, claim_timeout_seconds=10)
