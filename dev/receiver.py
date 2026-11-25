"""Test receiver for local development. It is not part of the service.

uv run python dev/receiver.py                 answer 200
uv run python dev/receiver.py --mode slow     process the event, then answer too late
curl -X PUT localhost:9000/mode/error         switch mode without restarting
curl localhost:9000/received                  what arrived, and how many times

It checks every webhook's signature as a real receiver should, with the secrets in
WEBHOOK_SECRETS (from the environment or .env) or the ones given with --secret, and answers
401 to a webhook that fails the check.
"""

import argparse
import asyncio
import json
import logging
from enum import StrEnum
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response

from webhook_service import signing
from webhook_service.config import get_settings

log = logging.getLogger("receiver")
SIGNED_HEADERS = ("webhook-id", "webhook-timestamp", "webhook-signature")


class Mode(StrEnum):
    OK = "ok"  # process the event, answer 200
    SLOW = "slow"  # process the event, then wait longer than the sender's timeout
    ERROR = "error"  # answer 500 without processing
    REJECT = "reject"  # answer 400 without processing


app = FastAPI(title="Test receiver")
app.state.mode = Mode.OK
app.state.slow_seconds = 10.0
app.state.signing_keys = []
app.state.received = {}


@app.post("/webhooks")
async def receive_webhook(request: Request) -> Response:
    mode = app.state.mode
    webhook_id = request.headers.get("webhook-id", "<missing>")
    # The raw bytes as they arrived. The signature covers exactly these, so it must be
    # checked before the JSON is parsed.
    body = await request.body()
    if app.state.signing_keys:
        try:
            signing.verify(app.state.signing_keys, request.headers, body)
        except signing.InvalidSignature as error:
            log.info("%s -> 401 (signature check failed: %s)", webhook_id, error)
            return Response(status_code=401)

    if mode is Mode.ERROR:
        log.info("%s -> 500 (mode=error, not processed)", webhook_id)
        return Response(status_code=500)
    if mode is Mode.REJECT:
        log.info("%s -> 400 (mode=reject, not processed)", webhook_id)
        return Response(status_code=400)

    payload = json.loads(body)
    entry = app.state.received.setdefault(webhook_id, {"count": 0, "payload": payload})
    entry["count"] += 1
    # The last copy exactly as it arrived, so it can be sent again to try a replay.
    entry["request"] = {
        "headers": {name: request.headers.get(name) for name in SIGNED_HEADERS},
        "body": body.decode(),
    }
    seen = "DUPLICATE" if entry["count"] > 1 else "first time"
    log.info(
        "%s processed (%s) type=%s producer=%s",
        webhook_id,
        seen,
        payload.get("type"),
        payload.get("producer"),
    )

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
    parser.add_argument(
        "--secret",
        action="append",
        help="a whsec_ secret to check signatures with; repeat for several "
        "(default: the secrets in WEBHOOK_SECRETS)",
    )
    args = parser.parse_args()

    app.state.mode = args.mode
    app.state.slow_seconds = args.slow_seconds
    if args.secret:
        app.state.signing_keys = [signing.decode_secret(secret) for secret in args.secret]
    else:
        app.state.signing_keys = get_settings().signing_keys
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [receiver] %(message)s", datefmt="%H:%M:%S"
    )
    if app.state.signing_keys:
        log.info("checking signatures with %d secret(s)", len(app.state.signing_keys))
    else:
        log.warning("no secret given: accepting webhooks without checking their signatures")
    uvicorn.run(app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
