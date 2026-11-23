# Webhook Delivery Service

A small Python service that accepts events from producer apps (such as
[CarRental](https://github.com/DucAnhLe1992/CarRental)) and delivers them as webhooks to a
receiver.

The focus is explicit guarantees, not the number of endpoints: what survives a crash, what can
be duplicated, and what happens when the receiver is slow, down, or answers with an error.

Stack: Python 3.12+, FastAPI, Pydantic, PostgreSQL, SQLAlchemy, Alembic,
[HTTPX2](https://github.com/pydantic/httpx2) (the maintained continuation of HTTPX), pytest, Ruff
and uv.

> **Status: milestone 4 of 5.** Deliveries survive worker crashes, retries slow down and stop,
> every attempt is recorded, producers can repeat requests safely, and several workers can share
> the work. Milestone 5 covers operations: authentication, signatures, metrics and deployment.

## How it works today

```text
producer ── POST /deliveries ──▶ API ── INSERT + COMMIT ──▶ PostgreSQL
         ◀── 202 Accepted ──────                                ▲
                                                 claim, record  │
                                                                ▼
                                  receiver ◀── POST /webhooks ── worker(s)
```

The API stores each event as a `pending` delivery and answers `202`. Each worker repeats:

1. **Claim.** `SELECT … FOR UPDATE SKIP LOCKED` picks the oldest due delivery: one that is
   `pending` and due, or one whose claim expired. The worker marks it `processing`, opens attempt
   number N, and stores that attempt's ID as the delivery's **claim token**. The claim expires
   after `CLAIM_TIMEOUT_SECONDS` unless a result is saved first.
2. **Send.** It posts the webhook. No database transaction is open while it waits.
3. **Record.** It saves the result only if the delivery still holds its claim token. The
   delivery becomes `delivered`; `pending` again after a growing delay; or `failed` (a permanent
   error, or no attempts left).
4. **Wait.** If nothing was due, it sleeps `POLL_INTERVAL_SECONDS` and looks again.

### Guarantees

- `202` means the delivery is committed to PostgreSQL. If the database is unreachable, the API
  answers `503` with `Retry-After`, never `202`.
- **Repeating a request is safe with an `Idempotency-Key` header.** The same key with the same
  content returns the original delivery, marked `Idempotent-Replayed: true`. The same key with
  different content gets `409`. Without a key, a repeated request is stored twice.
- **At least once, not exactly once.** Every attempt for a delivery carries the same
  `webhook-id`. Receivers should skip IDs they have already processed and still answer `2xx`.
- **Bounded retries.** A delivery gets at most `MAX_ATTEMPTS` attempts. The wait doubles from
  `RETRY_BASE_DELAY_SECONDS` up to `RETRY_MAX_DELAY_SECONDS`, give or take 20%. With the
  defaults, the last attempt comes about 21 minutes after the first, so receivers should keep
  IDs at least that long.
- **Crash recovery.** If a worker crashes or hangs mid-attempt, its claim expires after
  `CLAIM_TIMEOUT_SECONDS` and another worker sends the delivery again. The lost attempt is
  recorded as `claim_expired`. If the old worker wakes up later, its result is discarded.
- Attempts are counted when claimed, so a delivery that crashes every worker still stops after
  `MAX_ATTEMPTS`.
- **Any number of workers** can run at once; each claims different deliveries (tests cover these
  races).
- Every attempt is stored and shown by `GET /deliveries/{id}`.
- **No ordering guarantee.** A retried event can arrive after a newer one.
- **Not yet (milestone 5):**
  - Producers are not authenticated, so all producers share one space of idempotency keys.
  - Keys never expire.
  - Webhooks are not signed.
  - No metrics or structured logs.

### Delivery states

```text
pending ──▶ processing ──▶ delivered
   ▲            │
   └── retry ───┤
                └────────▶ failed
```

A delivery is `processing` exactly while one of its attempts holds the claim; the database
enforces this. While `processing`, `next_attempt_at` is when the claim expires.

`GET /deliveries/{id}` lists every attempt, for example after a worker crashed:

```json
"attempts": [
  {"number": 1, "outcome": "retryable_failure", "error_category": "claim_expired", "http_status": null},
  {"number": 2, "outcome": "delivered", "error_category": null, "http_status": 200}
]
```

### Webhook format and retry rules

Webhooks follow the [Standard Webhooks](https://www.standardwebhooks.com) spec:

```http
POST /webhooks
content-type: application/json
webhook-id: 5d0e8c8e-6f0b-4c43-9a51-0c1f3e2b7a10
webhook-timestamp: 1795467600

{"type":"booking.confirmed","timestamp":"2026-11-23T21:00:00Z","data":{"booking_id":42}}
```

- `webhook-id` is the delivery ID, the same on every attempt.
- `webhook-timestamp` is when this attempt was sent. The body's `timestamp` is when the event
  was accepted.

| Receiver result | Attempt outcome | Delivery becomes |
|---|---|---|
| 2xx | `delivered` | `delivered` |
| 5xx, 408 or 429; connection refused or dropped; no answer in time | `retryable_failure` | `pending`, or `failed` if no attempts are left |
| Other 4xx, or a 3xx redirect (never followed) | `permanent_failure` | `failed` |

## Run it

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
docker compose up -d          # PostgreSQL on localhost:5433
uv run alembic upgrade head   # create or update the tables
```

Then start each process in its own terminal. Start more workers to share the work.

```bash
uv run python dev/receiver.py                      # test receiver on :9000
uv run uvicorn webhook_service.main:app --reload   # API on :8000
uv run webhook-worker                              # worker
```

Submit an event, then check it. Open http://127.0.0.1:8000/docs for an interactive form.

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: booking-42-confirmed' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42}}'

curl http://127.0.0.1:8000/deliveries/<delivery_id>
```

## Experiments

Milestone 4 is done when you can explain each result.

1. **Crash a worker, again.** Start the worker with
   `CLAIM_TIMEOUT_SECONDS=15 uv run webhook-worker` and set the receiver to `slow`. Submit, and
   `kill -9` the worker while it waits. Set the receiver to `ok` and start the worker again with
   the same command. About 15 seconds after the first claim, the delivery is sent again and
   delivered. `GET` shows attempt 1 as `claim_expired` and attempt 2 as `delivered`, and the
   receiver logged the ID twice. Why can't the service avoid that duplicate? If the first worker
   had only been frozen, what stops it from overwriting the result when it wakes up?
2. **Watch it give up.** Set the receiver to `error` and run `MAX_ATTEMPTS=4 uv run webhook-worker`.
   Submit one event. The worker retries after about 10, 20 and 40 seconds, then marks it
   `failed`. Why should the wait grow, why add randomness, and why stop at all?
3. **Send the same event twice.** Run the `curl` above twice: both answers carry the same
   `delivery_id`, and the second has `Idempotent-Replayed: true`. Change `booking_id` and send
   it with the same key: `409`. The API already answers `202`, so why does the producer need a
   key at all? (What if the `202` gets lost?)

## Configuration

Set environment variables, or copy `.env.example` to `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks` | Used by the API, the worker and migrations. |
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | The only destination. Producers cannot choose a URL. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | How long one attempt waits for the receiver. |
| `MAX_ATTEMPTS` | `8` | Attempts per delivery before it is marked `failed`. |
| `RETRY_BASE_DELAY_SECONDS` | `10` | Wait after the first failed attempt; doubles after each one. |
| `RETRY_MAX_DELAY_SECONDS` | `3600` | Longest wait between attempts. |
| `CLAIM_TIMEOUT_SECONDS` | `60` | How long a claim lasts. Must be at least 3 x `DELIVERY_TIMEOUT_SECONDS`. |
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
migrations on it. Set `TEST_DATABASE_URL` to use another one. The race tests hold real row locks
in one connection and wait until PostgreSQL reports the other one blocked, so they do not depend
on timing.

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
  models.py                     deliveries and delivery_attempts tables
  schemas.py                    request, response and webhook message models
  services/delivery_service.py  create (idempotent), read, claim and record; owns the transactions
  api/deliveries.py             POST and GET /deliveries
  worker/main.py                the worker loop: claim, send, record
  sender.py                     sends one attempt and classifies the outcome
  retry.py                      growing, capped, randomized retry delays
alembic/versions/               database migrations
dev/receiver.py                 test receiver with ok / slow / error / reject modes
tests/unit/                     tests that need no database
tests/integration/              tests against real PostgreSQL, including race tests
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
- [x] **4. Reliability.** Expiring claims with claim tokens, attempt history, retry limits with
  growing delays, `Idempotency-Key`, and `FOR UPDATE SKIP LOCKED` for several workers. Lesson:
  any process can die at any moment, so every step must be safe to repeat, and every write must
  check that its writer is still the owner.
- [ ] **5. Operability.** Producer authentication, signed webhooks, structured logs, metrics,
  container deployment and a runbook.

Not planned for now: destination URLs chosen by producers, multiple tenants, ordering guarantees
and a dashboard.
