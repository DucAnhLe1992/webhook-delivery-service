# Webhook Delivery Service

A small Python service that accepts events from producer apps (such as
[CarRental](https://github.com/DucAnhLe1992/CarRental)) and delivers them as webhooks to a
receiver.

The focus is explicit guarantees, not the number of endpoints: what survives a crash, what can
be duplicated, and what happens when the receiver is slow, down, or answers with an error.

Stack: Python 3.12+, FastAPI, Pydantic, [HTTPX2](https://github.com/pydantic/httpx2) (the
maintained continuation of HTTPX), pytest, Ruff and uv.

> **Status: milestone 1 of 5.** The API sends each webhook while the producer waits. Nothing is
> stored yet, so there are **no delivery guarantees**: if the receiver is down, the event is lost
> unless the producer tries again.

## How it works today

```text
producer ── POST /deliveries ──▶ API ── POST /webhooks ──▶ receiver
         ◀── 200 / 502 / 504 ───      ◀── answer or timeout ──
```

1. The API validates `{event_type, data}` and rejects unknown fields.
2. It gives the delivery an ID and sends the webhook to the one configured receiver URL.
3. It answers with what happened:

| Receiver result | API answer | `attempt.outcome` |
|---|---|---|
| 2xx | 200 | `delivered` |
| 5xx, 408 or 429 | 502 | `retryable_failure` |
| Connection refused or dropped | 502 | `retryable_failure` |
| No answer within the timeout | 504 | `retryable_failure` |
| Other 4xx, or a 3xx redirect | 502 | `permanent_failure` |

Nothing is retried yet. The outcome records whether a retry *would* help; the worker in
milestone 3 acts on it.

### Webhook format

Headers and body follow the [Standard Webhooks](https://www.standardwebhooks.com) spec:

```http
POST /webhooks
content-type: application/json
webhook-id: 5d0e8c8e-6f0b-4c43-9a51-0c1f3e2b7a10
webhook-timestamp: 1795467600

{"type":"booking.confirmed","timestamp":"2026-11-23T21:00:00Z","data":{"booking_id":42}}
```

- `webhook-id` is the delivery ID. Receivers should use it to ignore duplicates.
- `webhook-timestamp` is when this attempt was sent. The body's `timestamp` is when the event
  happened.
- Signing (`webhook-signature`) comes in milestone 5.

## Run it

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Terminal 1, the test receiver:

```bash
uv run python dev/receiver.py
```

Terminal 2, the API. Open http://127.0.0.1:8000/docs for an interactive form.

```bash
uv run uvicorn webhook_service.main:app --reload
```

Terminal 3, send an event:

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H 'content-type: application/json' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42}}'
```

## Experiments

Milestone 1 is done when you can explain each result.

1. **Receiver stopped.** Stop terminal 1 with Ctrl+C and send again. You get `502` with
   `connection_error`. Where is the event now?
2. **Receiver slow.** Run `curl -X PUT localhost:9000/mode/slow` and send again. You get `504`
   after 5 seconds, yet terminal 1 shows the receiver processed the event. If the producer sends
   it again, can the receiver tell it is a duplicate? Compare the IDs in
   `curl localhost:9000/received`.
3. **Receiver failing.** Try `/mode/error` (500) and `/mode/reject` (400). Why is one worth
   retrying and the other not?

Switch back with `curl -X PUT localhost:9000/mode/ok`.

## Configuration

Set environment variables, or copy `.env.example` to `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | The only destination. Producers cannot choose a URL. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | How long one attempt waits for the receiver. |

## Development

```bash
uv run pytest          # tests; the receiver is faked with httpx2.MockTransport
uv run ruff check .    # lint
uv run ruff format .   # format
```

## Layout

```text
src/webhook_service/
  main.py              app setup, shared HTTP client, /health
  config.py            settings from the environment
  schemas.py           request, response and webhook message models
  sender.py            sends one attempt and classifies the outcome
  api/deliveries.py    POST /deliveries
dev/receiver.py        test receiver with ok / slow / error / reject modes
tests/                 pytest suite
```

## Roadmap

- [x] **1. HTTP path.** The endpoint sends directly to the receiver. No database.
- [ ] **2. Store before acknowledging.** PostgreSQL, SQLAlchemy and Alembic. `POST /deliveries`
  commits a `pending` row and answers 202. `GET /deliveries/{id}` shows its status. Records
  survive an API restart. Nothing is sent yet.
- [ ] **3. Worker.** A separate process polls for due deliveries, sends them with `sender.py` and
  records the results. The API can stop while the worker keeps going.
- [ ] **4. Reliability.** Attempt history, bounded retries with backoff, `Idempotency-Key` on
  submission, expiring claims with claim tokens, `FOR UPDATE SKIP LOCKED` for concurrent
  workers, and concurrency tests.
- [ ] **5. Operability.** Producer authentication, signed webhooks, structured logs, metrics,
  graceful shutdown and a runbook.

Not planned for now: destination URLs chosen by producers, multiple tenants, ordering guarantees
and a dashboard.
