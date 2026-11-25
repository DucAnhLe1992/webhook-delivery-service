"""The API server: `uv run webhook-api`.

It runs Uvicorn with the service's own logging from the very first line, which matters for
LOG_FORMAT=json: every line is then a JSON object. `uvicorn webhook_service.main:app` works too,
but Uvicorn writes its first lines in its own format.
"""

import argparse
from collections.abc import Sequence

import uvicorn

from webhook_service.config import get_settings
from webhook_service.logs import configure_logging


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="webhook-api", description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="address to listen on")
    parser.add_argument("--port", type=int, default=8000, help="port to listen on")
    parser.add_argument("--reload", action="store_true", help="restart when the code changes")
    args = parser.parse_args(argv)

    configure_logging("api", get_settings())
    uvicorn.run(
        "webhook_service.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        # Leave logging as configured above, and skip Uvicorn's access log: the API logs each
        # request itself, with its route, producer and request ID.
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
