"""Prometheus metrics, served by the API and the worker on ports of their own."""

import logging
from importlib.metadata import version
from wsgiref.simple_server import WSGIServer

from prometheus_client import Info, start_http_server

log = logging.getLogger(__name__)

BUILD = Info("webhooks_build", "The version of the webhook delivery service.")
BUILD.info({"version": version("webhook-delivery-service")})


def start_metrics_server(host: str, port: int, *, setting: str) -> WSGIServer | None:
    """Serve /metrics on host:port, or nothing if port is 0.

    If the port is taken, the process carries on without metrics, because sending webhooks
    matters more. The warning names the setting to change, and Prometheus shows the target as
    down, which is what an alert should watch.
    """
    if port == 0:
        return None
    try:
        server, _ = start_http_server(port, addr=host)
    except OSError as error:
        log.warning(
            "metrics are off: cannot listen on %s:%d (%s). Set %s to a free port, or to 0.",
            host,
            port,
            error.strerror,
            setting,
            extra={"event": "metrics_unavailable", "port": port},
        )
        return None
    bound = server.server_address[1]
    log.info(
        "metrics on http://%s:%d/metrics",
        host,
        bound,
        extra={"event": "metrics_started", "port": bound},
    )
    return server


def stop_metrics_server(server: WSGIServer | None) -> None:
    if server is not None:
        server.shutdown()
        server.server_close()
