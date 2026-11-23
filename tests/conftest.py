from collections.abc import Callable, Iterator

import httpx2
import pytest
from fastapi.testclient import TestClient

from webhook_service.api.deliveries import get_http_client
from webhook_service.main import app

FakeReceiver = Callable[[httpx2.Request], httpx2.Response]


@pytest.fixture
def api() -> Iterator[Callable[[FakeReceiver], TestClient]]:
    """Build a test client whose outgoing webhooks go to a fake receiver, not the network."""

    def build(receiver: FakeReceiver) -> TestClient:
        fake_client = httpx2.Client(transport=httpx2.MockTransport(receiver))
        app.dependency_overrides[get_http_client] = lambda: fake_client
        return TestClient(app)

    yield build
    app.dependency_overrides.clear()
