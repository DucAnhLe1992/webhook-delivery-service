"""Test receiver for local development. It is not part of the service.

uv run python dev/receiver.py                 answer 200
uv run python dev/receiver.py --mode slow     process the event, then answer too late
curl -X PUT localhost:9000/mode/error         switch mode without restarting
curl localhost:9000/received                  what arrived, and how many times
"""

import argparse
import asyncio
import json
import logging
from enum import StrEnum
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response

log = logging.getLogger("receiver")


class Mode(StrEnum):
    OK = "ok"  # process the event, answer 200
    SLOW = "slow"  # process the event, then wait longer than the sender's timeout
    ERROR = "error"  # answer 500 without processing
    REJECT = "reject"  # answer 400 without processing


app = FastAPI(title="Test receiver")
app.state.mode = Mode.OK
app.state.slow_seconds = 10.0
app.state.received = {}


@app.post("/webhooks")
async def receive_webhook(request: Request) -> Response:
    mode = app.state.mode
    webhook_id = request.headers.get("webhook-id", "<missing>")

    if mode is Mode.ERROR:
        log.info("%s -> 500 (mode=error, not processed)", webhook_id)
        return Response(status_code=500)
    if mode is Mode.REJECT:
        log.info("%s -> 400 (mode=reject, not processed)", webhook_id)
        return Response(status_code=400)

    payload = json.loads(await request.body())
    entry = app.state.received.setdefault(webhook_id, {"count": 0, "payload": payload})
    entry["count"] += 1
    seen = "DUPLICATE" if entry["count"] > 1 else "first time"
    log.info("%s processed (%s) type=%s", webhook_id, seen, payload.get("type"))

    if mode is Mode.SLOW:
        log.info(
            "%s waiting %.0fs before answering (mode=slow)", webhook_id, app.state.slow_seconds
        )
        await asyncio.sleep(app.state.slow_seconds)
    return Response(status_code=200)


@app.put("/mode/{mode}")
def set_mode(mode: Mode) -> dict[str, str]:
    app.state.mode = mode
    log.info("mode set to %s", mode)
    return {"mode": mode}


@app.get("/received")
def list_received() -> dict[str, Any]:
    return app.state.received


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mode", type=Mode, choices=list(Mode), default=Mode.OK)
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--slow-seconds", type=float, default=10.0)
    args = parser.parse_args()

    app.state.mode = args.mode
    app.state.slow_seconds = args.slow_seconds
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [receiver] %(message)s", datefmt="%H:%M:%S"
    )
    uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
