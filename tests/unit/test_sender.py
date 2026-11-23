import json

import httpx2
import pytest

from webhook_service.sender import (
    AttemptOutcome,
    ErrorCategory,
    classify_status,
    send_webhook,
)


def send_to(receiver):
    with httpx2.Client(transport=httpx2.MockTransport(receiver)) as client:
        return send_webhook(client, "http://receiver.test/webhooks", "wh_123", {"type": "a.b"})


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (200, AttemptOutcome.DELIVERED),
        (204, AttemptOutcome.DELIVERED),
        (500, AttemptOutcome.RETRYABLE_FAILURE),
        (503, AttemptOutcome.RETRYABLE_FAILURE),
        (408, AttemptOutcome.RETRYABLE_FAILURE),
        (429, AttemptOutcome.RETRYABLE_FAILURE),
        (400, AttemptOutcome.PERMANENT_FAILURE),
        (404, AttemptOutcome.PERMANENT_FAILURE),
        (301, AttemptOutcome.PERMANENT_FAILURE),
    ],
)
def test_classify_status(status_code, expected):
    assert classify_status(status_code) is expected


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (httpx2.ReadTimeout, ErrorCategory.TIMEOUT),
        (httpx2.ConnectTimeout, ErrorCategory.TIMEOUT),
        (httpx2.ConnectError, ErrorCategory.CONNECTION_ERROR),
        (httpx2.RemoteProtocolError, ErrorCategory.CONNECTION_ERROR),
    ],
)
def test_network_failures_are_retryable(error, category):
    def receiver(request):
        raise error("simulated", request=request)

    result = send_to(receiver)

    assert result.outcome is AttemptOutcome.RETRYABLE_FAILURE
    assert result.error_category is category
    assert result.http_status is None


def test_sends_json_body_with_standard_webhooks_headers():
    sent = []

    def receiver(request):
        sent.append(request)
        return httpx2.Response(204)

    result = send_to(receiver)

    assert result.outcome is AttemptOutcome.DELIVERED
    assert result.http_status == 204
    [request] = sent
    assert request.headers["webhook-id"] == "wh_123"
    assert request.headers["webhook-timestamp"].isdigit()
    assert request.headers["content-type"] == "application/json"
    assert json.loads(request.content) == {"type": "a.b"}
