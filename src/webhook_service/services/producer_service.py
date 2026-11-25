"""Producers and their API keys.

A key is shown once, when it is created. The database keeps only its SHA-256 hash, so a copy
of the database holds no usable keys, and a lost key cannot be recovered: create a new one.
"""

import hashlib
import re
import secrets
from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from webhook_service.models import ApiKey, Producer

KEY_PREFIX = "wds_"
# Receivers read the name in every webhook to tell producers apart, so it is kept plain.
NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,99}")


class ProducerError(Exception):
    """A request an operator can correct, such as a name that is already taken."""


def hash_api_key(key: str) -> str:
    # A fast hash is enough: unlike a password, a key is random, so trying guesses against a
    # stolen hash is hopeless however many guesses per second an attacker can make.
    return hashlib.sha256(key.encode()).hexdigest()


def add_api_key(producer: Producer) -> str:
    """Give the producer a new key and return it. Only its hash is kept."""
    # 32 random bytes. The prefix makes a leaked key easy to recognize, by people and scanners.
    key = KEY_PREFIX + secrets.token_urlsafe(32)
    producer.api_keys.append(ApiKey(key_hash=hash_api_key(key), prefix=key[:12]))
    return key


def create_producer(session: Session, name: str) -> tuple[Producer, str]:
    """Create a producer with its first API key, and return both."""
    if not NAME_PATTERN.fullmatch(name):
        raise ProducerError(
            f"{name!r} is not a valid producer name. Use up to 100 lower-case letters, "
            "digits, - and _, starting with a letter or digit, such as carrental."
        )
    if session.scalar(select(Producer.id).where(Producer.name == name)) is not None:
        raise ProducerError(f"A producer named {name} already exists.")
    producer = Producer(name=name)
    key = add_api_key(producer)
    session.add(producer)
    session.commit()
    return producer, key


def create_api_key(session: Session, producer_name: str) -> str:
    """Add a key to an existing producer, for example to replace an old one."""
    producer = session.scalar(select(Producer).where(Producer.name == producer_name))
    if producer is None:
        raise ProducerError(f"There is no producer named {producer_name}.")
    key = add_api_key(producer)
    session.commit()
    return key


def revoke_api_key(session: Session, prefix: str) -> None:
    """Revoke the active key that starts with `prefix`. It is refused from the next request."""
    keys = session.scalars(
        select(ApiKey).where(ApiKey.prefix == prefix, ApiKey.revoked_at.is_(None))
    ).all()
    if not keys:
        raise ProducerError(f"No active API key starts with {prefix}.")
    if len(keys) > 1:
        # 48 random bits make this practically impossible, but revoking the wrong key is worse.
        raise ProducerError(f"Several active API keys start with {prefix}.")
    keys[0].revoked_at = func.now()
    session.commit()


def find_producer(session: Session, key: str) -> Producer | None:
    """The producer an active key belongs to, or None for an unknown or revoked key."""
    return session.scalar(
        select(Producer)
        .join(ApiKey)
        .where(ApiKey.key_hash == hash_api_key(key), ApiKey.revoked_at.is_(None))
    )


def list_producers(session: Session) -> Sequence[Producer]:
    return session.scalars(
        select(Producer).options(selectinload(Producer.api_keys)).order_by(Producer.name)
    ).all()
