import logging
import socket
from importlib.metadata import version

import httpx2

from webhook_service.metrics import start_metrics_server, stop_metrics_server


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_the_metrics_server_serves_the_metrics():
    port = free_port()
    server = start_metrics_server("127.0.0.1", port, setting="API_METRICS_PORT")
    assert server is not None
    try:
        page = httpx2.get(f"http://127.0.0.1:{port}/metrics").text
    finally:
        stop_metrics_server(server)

    assert f'webhooks_build_info{{version="{version("webhook-delivery-service")}"}} 1.0' in page


def test_port_0_turns_the_metrics_off():
    assert start_metrics_server("127.0.0.1", 0, setting="API_METRICS_PORT") is None


def test_a_taken_port_turns_the_metrics_off_with_a_warning(caplog):
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]

        server = start_metrics_server("127.0.0.1", port, setting="WORKER_METRICS_PORT")

    assert server is None
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert f"cannot listen on 127.0.0.1:{port}" in record.getMessage()
    assert "Set WORKER_METRICS_PORT to a free port, or to 0." in record.getMessage()
