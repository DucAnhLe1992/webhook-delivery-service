import random
from collections.abc import Callable
from datetime import timedelta


def backoff_delay(
    attempt: int,
    *,
    base_seconds: float,
    max_seconds: float,
    random_fraction: Callable[[], float] = random.random,
) -> timedelta:
    """How long to wait after the given attempt failed before trying again.

    The delay doubles with each attempt (base, 2 x base, 4 x base...) up to max_seconds,
    then varies by up to 20% either way. The randomness spreads out deliveries that failed
    together, so a receiver coming back from an outage is not hit by all of them at once.
    """
    nominal = min(max_seconds, base_seconds * 2 ** (attempt - 1))
    return timedelta(seconds=nominal * (0.8 + 0.4 * random_fraction()))
