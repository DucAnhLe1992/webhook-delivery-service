"""Webhook signatures, as the Standard Webhooks specification defines them (v1, HMAC-SHA256).

The worker signs every attempt with secrets it shares with the receiver. A signature covers
the webhook-id, the webhook-timestamp and the exact body, so a receiver that verifies it knows
the webhook comes from a holder of the secret, was not changed on the way, and is recent.
"""

import base64
import binascii
import hashlib
import hmac
import secrets
import time
from collections.abc import Mapping, Sequence

SECRET_PREFIX = "whsec_"
# How far a webhook's timestamp may be from the receiver's clock before it is refused as a
# possible replay. The specification leaves the value to receivers; 5 minutes is the usual one.
DEFAULT_TOLERANCE_SECONDS = 300


class InvalidSignature(Exception):
    """The webhook is not signed with a known secret, or its timestamp is not recent."""


def new_secret() -> str:
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(32)).decode()


def decode_secret(secret: str) -> bytes:
    """The key in a whsec_ secret. Errors describe the problem without repeating the secret."""
    if not secret.startswith(SECRET_PREFIX):
        raise ValueError(f"it does not start with {SECRET_PREFIX}")
    try:
        key = base64.b64decode(secret.removeprefix(SECRET_PREFIX), validate=True)
    except binascii.Error:
        raise ValueError(f"the part after {SECRET_PREFIX} is not valid base64") from None
    if not 24 <= len(key) <= 64:
        raise ValueError(f"it holds {len(key)} bytes, and it must hold 24 to 64")
    return key


def sign(key: bytes, webhook_id: str, timestamp: int, body: bytes) -> str:
    signed_content = f"{webhook_id}.{timestamp}.".encode() + body
    return "v1," + base64.b64encode(hmac.digest(key, signed_content, hashlib.sha256)).decode()


def signature_header(keys: Sequence[bytes], webhook_id: str, timestamp: int, body: bytes) -> str:
    """One signature per key, separated by spaces.

    While secrets are being rotated, signing with the old and the new key lets every receiver
    verify the webhook, whichever of the two it has.
    """
    return " ".join(sign(key, webhook_id, timestamp, body) for key in keys)


def verify(
    keys: Sequence[bytes],
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance_seconds: float = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> None:
    """Check a webhook as a receiver should, and raise InvalidSignature if it fails.

    `headers` must look names up in lower case, as the headers of most web frameworks do.
    `body` must be the raw bytes as received: decoding and re-encoding the JSON can change
    them, and then no signature matches.
    """
    try:
        webhook_id = headers["webhook-id"]
        timestamp = int(headers["webhook-timestamp"])
        signatures = headers["webhook-signature"].split()
    except (KeyError, ValueError):
        raise InvalidSignature("missing or malformed webhook headers") from None
    now = time.time() if now is None else now
    if abs(now - timestamp) > tolerance_seconds:
        raise InvalidSignature("the timestamp is too old or too far in the future")
    expected = [sign(key, webhook_id, timestamp, body).encode() for key in keys]
    # compare_digest takes as long whether the first or the last character differs, so the
    # response time does not tell an attacker how much of a forged signature is right.
    if not any(
        hmac.compare_digest(candidate, signature.encode())
        for candidate in expected
        for signature in signatures
    ):
        raise InvalidSignature("no signature matches")
