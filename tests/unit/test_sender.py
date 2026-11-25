import http.server
import json
import socket
import threading
import time

import httpx2
import pytest

from webhook_service import signing
from webhook_service.domain import AttemptOutcome, ErrorCategory
from webhook_service.sender import classify_status, send_webhook

MB = 1024 * 1024
KEY = signing.decode_secret(signing.new_secret())


def send_to(receiver):
    with httpx2.Client(transport=httpx2.MockTransport(receiver)) as client:
        return send_webhook(
            client, "http://receiver.test/webhooks", "wh_123", {"type": "a.b"}, signing_keys=[KEY]
        )


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


def test_sends_a_signed_json_body_with_standard_webhooks_headers():
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
    # The signature covers the bytes that were actually sent.
    signing.verify([KEY], request.headers, request.content)


# The tests below use a real HTTP server on a local socket, because the behaviour they check
# (timeouts, dropped connections, bodies that are slow, huge or corrupt) happens in the
# network layer that MockTransport replaces.


class ReceiverHandler(http.server.BaseHTTPRequestHandler):
    """Answers according to the request path."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers["content-length"]))
        try:
            if self.path == "/trickle":
                # The status line at once, then the body one byte every 100 ms: 10 s in all.
                self.start_answer(200, length=100)
                for _ in range(100):
                    self.wfile.write(b"x")
                    time.sleep(0.1)
            elif self.path == "/huge":
                self.start_answer(200, length=64 * MB)
                chunk = b"x" * 65536
                for _ in range(64 * MB // len(chunk)):
                    self.wfile.write(chunk)
                    self.server.body_bytes_sent += len(chunk)
            elif self.path == "/corrupt-gzip":
                self.start_answer(200, length=16, headers={"content-encoding": "gzip"})
                self.wfile.write(b"this is not gzip")
            elif self.path == "/drop":
                # The request was read, maybe processed, but no answer is sent.
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            elif self.path == "/silent":
                time.sleep(1)
                self.close_connection = True
        except OSError:
            # The client closed the connection without reading the whole answer.
            self.close_connection = True
        finally:
            self.server.finished.set()

    def start_answer(self, status, *, length, headers=None):
        self.send_response(status)
        self.send_header("content-length", str(length))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()


class ReceiverServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ReceiverHandler)
        self.body_bytes_sent = 0
        self.finished = threading.Event()

    def handle_error(self, request, client_address):
        pass


@pytest.fixture
def real_receiver():
    server = ReceiverServer()
    threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


def send_over_http(server, path, *, timeout=0.5):
    url = f"http://127.0.0.1:{server.server_address[1]}{path}"
    with httpx2.Client(timeout=timeout) as client:
        return send_webhook(client, url, "wh_123", {"type": "a.b"}, signing_keys=[KEY])


def test_a_slowly_sent_body_does_not_hold_up_the_attempt(real_receiver):
    started = time.monotonic()

    result = send_over_http(real_receiver, "/trickle")

    assert result.outcome is AttemptOutcome.DELIVERED
    # Reading the body would take 10 s, although no single byte takes longer than the timeout.
    assert time.monotonic() - started < 2


def test_a_large_body_is_not_downloaded(real_receiver):
    result = send_over_http(real_receiver, "/huge")

    assert result.outcome is AttemptOutcome.DELIVERED
    assert real_receiver.finished.wait(5)
    # Only what fitted in the socket buffers left the receiver before the connection closed.
    assert real_receiver.body_bytes_sent < 16 * MB


def test_a_body_that_cannot_be_decoded_does_not_break_the_attempt(real_receiver):
    result = send_over_http(real_receiver, "/corrupt-gzip")

    assert result.outcome is AttemptOutcome.DELIVERED
    assert result.http_status == 200


def test_a_connection_dropped_without_an_answer_is_retried(real_receiver):
    result = send_over_http(real_receiver, "/drop")

    assert result.outcome is AttemptOutcome.RETRYABLE_FAILURE
    assert result.error_category is ErrorCategory.CONNECTION_ERROR
    assert result.http_status is None


def test_a_receiver_that_does_not_answer_in_time_times_out(real_receiver):
    result = send_over_http(real_receiver, "/silent", timeout=0.2)

    assert result.outcome is AttemptOutcome.RETRYABLE_FAILURE
    assert result.error_category is ErrorCategory.TIMEOUT
