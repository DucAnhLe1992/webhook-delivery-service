# Webhook Delivery Service

A small Python service that accepts events from producer apps (such as
[CarRental](https://github.com/DucAnhLe1992/CarRental)) and delivers them as webhooks to a
receiver.

The focus is explicit guarantees, not the number of endpoints: what survives a crash, what can
be duplicated, and what happens when the receiver is slow, down, or answers with an error.

Stack: Python 3.12+, FastAPI, Pydantic, PostgreSQL, SQLAlchemy, Alembic,
[HTTPX2](https://github.com/pydantic/httpx2) (the maintained continuation of HTTPX), pytest, Ruff
and uv.

> **Status: milestone 3 of 5.** A separate worker sends stored deliveries and records the
> results. Retries use a fixed delay with no limit yet, and a worker that crashes mid-send leaves
> its delivery stuck; milestone 4 fixes both.

## How it works today

```text
producer ── POST /deliveries ──▶ API ── INSERT + COMMIT ──▶ PostgreSQL
         ◀── 202 Accepted ──────                                ▲
                                                 claim, record  │
                                                                ▼
                                   receiver ◀── POST /webhooks ── worker
```

The API stores each event as a `pending` delivery and answers `202`. The worker, a separate
process, repeats four steps:

1. **Claim.** One `UPDATE` marks the oldest due `pending` delivery `processing` and returns it.
2. **Send.** It posts the webhook. No database transaction is open while it waits.
3. **Record.** It adds 1 to `attempt_count`, stores the HTTP status or error, and sets the state:
   `delivered`; back to `pending` with `next_attempt_at` moved `RETRY_DELAY_SECONDS` ahead; or
   `failed`.
4. **Wait.** If nothing was due, it sleeps `POLL_INTERVAL_SECONDS` and looks again.

### Guarantees

- `202` means the delivery is committed to PostgreSQL, so it survives an API restart. If the
  database is unreachable, the API answers `503` with `Retry-After`, never `202`.
- Each accepted delivery is attempted until it is delivered or fails permanently. Retryable
  failures are tried again every `RETRY_DELAY_SECONDS`.
- **At least once, not exactly once.** If the receiver processes an event but its answer is lost,
  the worker sends it again. Every attempt carries the same `webhook-id`, so receivers can drop
  duplicates.
- The API and the worker are independent. Either can stop; deliveries wait in the database.
- Two workers never claim the same delivery (a test covers this race).
- Ctrl+C or `SIGTERM` stops the worker after it finishes the current delivery.
- **Not yet (milestone 4):**
  - A worker that is killed mid-send (crash, `kill -9`, power loss) leaves its delivery in
    `processing` forever.
  - Retries never stop and never slow down.
  - Only the last attempt's result is kept.
  - A producer that sends the same event twice gets two deliveries.

### Delivery states

```text
pending ──▶ processing ──▶ delivered
   ▲            │
   └── retry ───┤
                └────────▶ failed
```

The database only allows these four states.

### Webhook format and retry rules

Webhooks follow the [Standard Webhooks](https://www.standardwebhooks.com) spec:

```http
POST /webhooks
content-type: application/json
webhook-id: 5d0e8c8e-6f0b-4c43-9a51-0c1f3e2b7a10
webhook-timestamp: 1795467600

{"type":"booking.confirmed","timestamp":"2026-11-23T21:00:00Z","data":{"booking_id":42}}
```

- `webhook-id` is the delivery ID, the same on every attempt. Receivers should use it to ignore
  duplicates.
- `webhook-timestamp` is when this attempt was sent. The body's `timestamp` is when the event
  was accepted.

| Receiver result | Outcome | New state |
|---|---|---|
| 2xx | `delivered` | `delivered` |
| 5xx, 408 or 429; connection refused or dropped; no answer in time | `retryable_failure` | `pending` |
| Other 4xx, or a 3xx redirect (never followed) | `permanent_failure` | `failed` |

## Run it

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
docker compose up -d          # PostgreSQL on localhost:5433
uv run alembic upgrade head   # create or update the tables
```

Then start each process in its own terminal:

```bash
uv run python dev/receiver.py                      # test receiver on :9000
uv run uvicorn webhook_service.main:app --reload   # API on :8000
uv run webhook-worker                              # worker
```

Submit an event, then check it. Open http://127.0.0.1:8000/docs for an interactive form.

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H 'content-type: application/json' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42}}'

curl http://127.0.0.1:8000/deliveries/<delivery_id>
```

## Experiments

Milestone 3 is done when you can explain each result.

1. **Stop the API.** Run `curl -X PUT localhost:9000/mode/error`, submit two events, then stop
   the API with Ctrl+C. The worker keeps retrying. Run `curl -X PUT localhost:9000/mode/ok`: both
   are delivered while the API is down. Why can the two processes run independently?
2. **Slow receiver.** Run `curl -X PUT localhost:9000/mode/slow` and submit. The worker times out
   after 5 seconds and tries again 10 seconds later, and the receiver logs the same ID as
   `DUPLICATE`. In milestone 1 a resend got a new ID. Why does a stable ID matter here, and why
   must the receiver still check it?
3. **Kill the worker mid-send.** Keep `slow` mode, submit, and while the worker waits, run
   `kill -9 <pid>` with the PID from the worker's first log line. Run
   `curl localhost:8000/deliveries/<delivery_id>`: it stays `processing`, even after you restart the
   worker. Now try the same with Ctrl+C instead. Why do the two endings differ, and what would
   recover the stuck delivery?

## Configuration

Set environment variables, or copy `.env.example` to `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks` | Used by the API, the worker and migrations. |
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | The only destination. Producers cannot choose a URL. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | How long one attempt waits for the receiver. |
| `RETRY_DELAY_SECONDS` | `10` | How long the worker waits before retrying a retryable failure. |
| `POLL_INTERVAL_SECONDS` | `1` | How often an idle worker looks for due deliveries. |

## Development

```bash
docker compose up -d
uv run pytest              # all tests; integration tests need PostgreSQL
uv run pytest tests/unit   # only the tests that need no database
uv run ruff check .        # lint
uv run ruff format .       # format
```

Integration tests create a `webhooks_test` database on the same server and run the real
migrations on it. Set `TEST_DATABASE_URL` to use another one.

After changing `models.py`, generate and review a migration:

```bash
uv run alembic revision --autogenerate -m "describe the change"
uv run alembic upgrade head
uv run alembic check   # fails if models.py and the migrations disagree
```

## Layout

```text
src/webhook_service/
  main.py                       API app, 503 when storage is down, /health
  config.py                     settings from the environment
  db.py                         database engine and per-request session
  models.py                     deliveries table and delivery states
  schemas.py                    request, response and webhook message models
  services/delivery_service.py  create, read, claim and record deliveries; owns the transactions
  api/deliveries.py             POST and GET /deliveries
  worker/main.py                the worker loop: claim, send, record
  sender.py                     sends one attempt and classifies the outcome
alembic/versions/               database migrations
dev/receiver.py                 test receiver with ok / slow / error / reject modes
tests/unit/                     tests that need no database
tests/integration/              tests against real PostgreSQL
```

## Roadmap

- [x] **1. HTTP path.** The endpoint sent each webhook directly. Lessons: an event held only in
  memory is lost when the receiver is down, and a timeout means "unknown", not "failed".
- [x] **2. Store before acknowledging.** `POST /deliveries` commits a `pending` row before
  answering `202`. Lesson: `202` is a promise that the event is saved, so the API must answer
  `503` rather than `202` when it cannot save it.
- [x] **3. Worker.** A separate process claims due deliveries, sends them and records the
  results. Lesson: accepting and delivering are separate jobs, so either side can stop without
  losing work.
- [ ] **4. Reliability.** Expiring claims with claim tokens (recovers stuck deliveries), attempt
  limits with growing delays, attempt history, `Idempotency-Key` on submission, and
  `FOR UPDATE SKIP LOCKED` so several workers can share the queue.
- [ ] **5. Operability.** Producer authentication, signed webhooks, structured logs, metrics,
  container deployment and a runbook.

Not planned for now: destination URLs chosen by producers, multiple tenants, ordering guarantees
and a dashboard.
