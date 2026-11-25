import base64
import time

import pytest
from pydantic import ValidationError

from webhook_service import signing
from webhook_service.config import Settings

KEY = signing.decode_secret(signing.new_secret())
BODY = b'{"type":"booking.confirmed","producer":"carrental","data":{"booking_id":42}}'
NOW = int(time.time())


def signed_headers(keys=(KEY,), webhook_id="msg_1", timestamp=NOW, body=BODY) -> dict[str, str]:
    return {
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": signing.signature_header(keys, webhook_id, timestamp, body),
    }


def test_matches_the_reference_signature_of_the_standard_webhooks_specification():
    # The example from the specification's own test suite, which every implementation of
    # Standard Webhooks reproduces. Matching it means any of their libraries can verify ours.
    key = signing.decode_secret("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw")

    signature = signing.sign(
        key, "msg_p5jXN8AQM9LWM0D4loKWxJek", 1614265330, b'{"test": 2432232314}'
    )

    assert signature == "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="


def test_a_webhook_signed_with_a_known_secret_verifies():
    signing.verify([KEY], signed_headers(), BODY, now=NOW)


@pytest.mark.parametrize(
    ("headers", "body"),
    [
        (signed_headers(), BODY.replace(b"42", b"43")),
        (signed_headers() | {"webhook-id": "msg_2"}, BODY),
        (signed_headers(keys=[signing.decode_secret(signing.new_secret())]), BODY),
        (signed_headers(timestamp=NOW - 301), BODY),
        (signed_headers(timestamp=NOW - 600) | {"webhook-timestamp": str(NOW)}, BODY),
        (signed_headers(timestamp=NOW + 301), BODY),
        ({"webhook-id": "msg_1", "webhook-timestamp": str(NOW)}, BODY),
    ],
    ids=[
        "body-changed",
        "id-changed",
        "other-secret",
        "replayed-after-5-minutes",
        "replayed-with-a-new-timestamp",
        "from-the-future",
        "unsigned",
    ],
)
def test_a_changed_forged_or_replayed_webhook_is_refused(headers, body):
    with pytest.raises(signing.InvalidSignature):
        signing.verify([KEY], headers, body, now=NOW)


def test_while_secrets_are_rotated_a_webhook_verifies_with_the_old_or_the_new_one():
    old, new = KEY, signing.decode_secret(signing.new_secret())

    headers = signed_headers(keys=[old, new])

    assert len(headers["webhook-signature"].split()) == 2
    signing.verify([old], headers, BODY, now=NOW)
    signing.verify([new], headers, BODY, now=NOW)


def test_webhook_secrets_are_read_from_one_variable_separated_by_spaces(monkeypatch):
    old, new = signing.new_secret(), signing.new_secret()
    monkeypatch.setenv("WEBHOOK_SECRETS", f"{old}  {new}")

    settings = Settings(_env_file=None)

    assert settings.signing_keys == [signing.decode_secret(old), signing.decode_secret(new)]


@pytest.mark.parametrize(
    "invalid",
    [
        "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
        "whsec_not*base64",
        "whsec_" + base64.b64encode(bytes(16)).decode(),
        "whsec_" + base64.b64encode(bytes(65)).decode(),
    ],
    ids=["no-prefix", "not-base64", "too-short", "too-long"],
)
def test_an_invalid_secret_is_refused_without_being_repeated(monkeypatch, invalid):
    valid = signing.new_secret()
    monkeypatch.setenv("WEBHOOK_SECRETS", f"{valid} {invalid}")

    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None)

    message = str(error.value)
    assert "secret 2 in WEBHOOK_SECRETS" in message
    # Startup errors end up in logs, so not even part of a secret may appear in them.
    assert valid[6:16] not in message
    assert invalid[-10:] not in message
