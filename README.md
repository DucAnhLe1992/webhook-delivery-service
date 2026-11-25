# Webhook Delivery Service

[![CI](https://github.com/DucAnhLe1992/webhook-delivery-service/actions/workflows/ci.yml/badge.svg)](https://github.com/DucAnhLe1992/webhook-delivery-service/actions/workflows/ci.yml)

A Python service that accepts events from producer apps, such as
[CarRental](https://github.com/DucAnhLe1992/CarRental), and reliably delivers them as webhooks to
a receiver.

The focus is explicit, tested guarantees rather than the number of endpoints: what survives a
crash, what can be repeated, and what happens when the receiver is slow, down or answers with an
error.

**Stack:** Python 3.12+, FastAPI, Pydantic, PostgreSQL 18, SQLAlchemy 2, Alembic,
[HTTPX2](https://github.com/pydantic/httpx2), pytest, Ruff, uv, Docker Compose and GitHub Actions.

## Highlights

- **`202` means stored.** The API commits the event before it answers. If the database is
  unreachable, it answers `503` with `Retry-After`, never `202`. A `503` is not proof that
  nothing was stored, because the connection can fail just after the commit, so producers retry
  with the same `Idempotency-Key`.
- **At-least-once delivery within a retry window.** Failed attempts are retried, and every
  attempt carries the same `webhook-id`, in the [Standard Webhooks](https://www.standardwebhooks.com)
  format, so receivers can drop duplicates.
- **Crash recovery with fencing.** A worker's claim on a delivery expires if the worker dies, and
  another worker takes over. A claim token stops the old worker, if it was only frozen, from
  overwriting the result once another worker has taken over.
- **Safe producer retries.** An `Idempotency-Key` header turns a repeated request into a replay of
  the original. The same key with different content gets `409`.
- **Bounded, polite retries.** Exponential backoff with ±20% jitter, a hard cap and an attempt
  limit, with every attempt recorded.
- **Receiver answers are untrusted input.** The worker decides by the status code alone and never
  reads the response body, so no answer can exhaust its memory, hold it up or crash it.
- **Any number of workers.** `FOR UPDATE SKIP LOCKED` lets workers share the queue without
  waiting for each other. Race tests use real PostgreSQL locks and do not depend on timing.

## How it works

```mermaid
flowchart LR
    P[Producer] -->|"POST /deliveries"| API[API]
    API -->|"INSERT and COMMIT, then 202"| DB[(PostgreSQL)]
    W[Workers] -->|"claim, then record"| DB
    W -->|"POST with webhook-id"| R[Receiver]
```

1. The **API** validates the event, stores it as a `pending` delivery, commits, and answers `202`.
2. A **worker** claims the oldest due delivery in a short transaction. The claim opens an attempt
   and expires after `CLAIM_TIMEOUT_SECONDS`.
3. The worker sends the webhook with no transaction open, and reads only the status code.
4. It saves the result only if it still holds the claim. The delivery becomes `delivered`, goes
   back to `pending` with a growing delay, or becomes `failed`.

The API and the workers never call each other; PostgreSQL is their only connection. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the data model, state machine, locking and design
decisions.

## Quick start

You need Python 3.12+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
docker compose up -d --wait   # PostgreSQL on localhost:5433
uv run alembic upgrade head   # create or update the tables
```

Start each process in its own terminal:

```bash
uv run python dev/receiver.py                      # a test receiver on :9000
uv run uvicorn webhook_service.main:app --reload   # the API on :8000
uv run webhook-worker                              # a worker; start more to share the work
```

Submit an event:

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: booking-42-confirmed' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42, "car_id": 7}}'
```

```http
HTTP/1.1 202 Accepted
location: /deliveries/73731487-6839-4966-b9b2-9b2a1a83054c

{"delivery_id":"73731487-6839-4966-b9b2-9b2a1a83054c","status":"pending"}
```

Then follow it with `curl http://127.0.0.1:8000/deliveries/<delivery_id>`, which shows its status
and every attempt. Interactive API docs are at http://127.0.0.1:8000/docs.

### See the guarantees in action

`uv run python dev/demo.py` starts its own receiver, API and worker against a separate
`webhooks_demo` database and plays four scenes in about 20 seconds: a repeated request, a receiver
that is down, an answer that comes too late, and a worker killed in the middle of a delivery.
The last scene:

```text
Scene 4: a worker dies in the middle of a delivery
    21:57:35  receiver  3f6daa35 processed (first time) type=booking.confirmed
  Killed the worker while it was waiting for the receiver's answer.
  The delivery is processing, and attempt 1 has no result: nobody is left to save it.
  Started a new worker.
    21:57:41  receiver  3f6daa35 processed (DUPLICATE) type=booking.confirmed
    21:57:41  worker-2  3f6daa35 attempt 2: delivered (HTTP 200, 2 ms)
  GET /deliveries/3f6daa35: delivered
    attempt 1  + 0.0 s  retryable_failure  claim_expired
    attempt 2  + 6.0 s  delivered          HTTP 200
```

## API

| Request | Answers |
|---|---|
| `POST /deliveries` | `202` stored (or replayed, with `Idempotent-Replayed: true`), `409` key reused for different content, `422` invalid request, `503` storage unavailable, so whether it was stored is unknown: retry with the same key |
| `GET /deliveries/{id}` | `200` with the status and every attempt, `404` unknown ID, `422` malformed ID, `503` storage unavailable |
| `GET /health` | `200` while the API process is up |

The receiver gets one `POST` per attempt:

```http
POST /webhooks
content-type: application/json
webhook-id: 73731487-6839-4966-b9b2-9b2a1a83054c
webhook-timestamp: 1795476541

{"type":"booking.confirmed","timestamp":"2026-11-23T23:28:50.230425Z","data":{"car_id":7,"booking_id":42}}
```

| Receiver answers | The delivery |
|---|---|
| 2xx | becomes `delivered` |
| 5xx, 408 or 429, a timeout, or a refused or dropped connection | is retried later, or becomes `failed` when no attempts are left |
| Another 4xx, or a 3xx (redirects are never followed) | becomes `failed` at once |

Only the status code counts. The worker closes the connection without reading the response body.

The [user manual](docs/USER_MANUAL.md) covers every response, how producers should react, and
how receivers should answer and drop duplicates.

## Guarantees

1. **Durable acceptance:** `202` is sent only after the commit is confirmed. A `503` or a lost
   answer means the outcome is unknown, and retrying with the same `Idempotency-Key` is safe
   either way.
2. **At least once, within the retry window:** every accepted delivery is attempted until it is
   delivered, rejected or out of attempts. Duplicates are possible, and recognizable by
   `webhook-id`.
3. **Idempotent submission:** the same `Idempotency-Key` with the same content stores one
   delivery, even when the requests arrive at the same moment.
4. **Crash recovery:** a delivery held by a dead worker is retried after `CLAIM_TIMEOUT_SECONDS`.
   Once another worker has taken it over, the old worker cannot change anything in the database.
   It cannot recall a request it already sent, though, so the receiver may get both.
5. **Bounded retries:** at most `MAX_ATTEMPTS` attempts. With the defaults, the last one comes
   about 21 minutes after the first.
6. **No ordering guarantee:** a retried event can arrive after a newer one.

How each failure is handled, from a crashed API to a frozen worker, is listed in
[ARCHITECTURE.md](ARCHITECTURE.md#guarantees-and-failure-modes).

## Configuration

Set environment variables, or copy `.env.example` to `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks` | Used by the API, the worker and migrations. |
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | The only destination. Producers cannot choose a URL. Deliveries waiting for a retry go to the current value, so every worker needs the same one. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | How long each network step of an attempt may take: connecting, sending, and waiting for the status code. It does not limit the attempt as a whole. |
| `MAX_ATTEMPTS` | `8` | Attempts per delivery before it is marked `failed`. |
| `RETRY_BASE_DELAY_SECONDS` | `10` | Wait after the first failed attempt. It doubles after each one. |
| `RETRY_MAX_DELAY_SECONDS` | `3600` | Longest wait between attempts, jitter included. |
| `CLAIM_TIMEOUT_SECONDS` | `60` | How long a claim lasts. Must be at least 3 × `DELIVERY_TIMEOUT_SECONDS`. |
| `POLL_INTERVAL_SECONDS` | `1` | How often an idle worker looks for due deliveries. |

## Development

```bash
docker compose up -d --wait
uv run pytest              # all tests; integration tests need PostgreSQL
uv run pytest tests/unit   # only the tests that need no database
uv run ruff check .        # lint
uv run ruff format .       # format
uv run python dev/demo.py  # the four scenes, with real processes
```

Integration tests create a `webhooks_test` database on the same server and run the real
migrations on it. Set `TEST_DATABASE_URL` to use another one. The tests delete all data in it, so
they refuse a database whose name does not end in `_test`, or the one `DATABASE_URL` names.
The sender's tests run a real HTTP server on a local port, to check timeouts, dropped connections
and slow, huge or corrupt response bodies.

After changing `models.py`, generate and review a migration:

```bash
uv run alembic revision --autogenerate -m "describe the change"
uv run alembic upgrade head
uv run alembic check   # fails if models.py and the migrations disagree
```

CI runs Ruff, the format check, the migrations, `alembic check` and all tests against a PostgreSQL
service.

## Project layout

```text
src/webhook_service/
  main.py                       API app, 503 when storage is down, /health
  config.py                     settings from the environment
  db.py                         database engine and per-request session
  domain.py                     delivery states and attempt outcomes, shared by every layer
  models.py                     deliveries and delivery_attempts tables
  schemas.py                    request, response and webhook message models
  services/delivery_service.py  create (idempotent), read, claim and record; owns the transactions
  api/deliveries.py             POST and GET /deliveries
  worker/main.py                the worker loop: claim, send, record
  sender.py                     sends one attempt and classifies it by status code
  retry.py                      growing, capped, randomized retry delays
alembic/versions/               database migrations
dev/receiver.py                 test receiver with ok / slow / error / reject modes
dev/demo.py                     scripted demonstration with real processes
tests/unit/                     tests that need no database, including real-HTTP sender tests
tests/integration/              tests against real PostgreSQL, including race tests
docs/                           user manual and build history
```

## Documentation

| Document | What it covers |
|---|---|
| [User manual](docs/USER_MANUAL.md) | Running the service, submitting and tracking events, receiving webhooks, how a delivery flows step by step, configuration, operating and troubleshooting. |
| [Architecture](ARCHITECTURE.md) | Components, data model, state machine, sequence diagrams, locking, retry policy, failure modes and design decisions. |
| [How it was built](docs/MILESTONES.md) | The milestones, the experiments that motivated each one, and what they taught. |

## Status

Milestones 1 to 4.1 are done, and each is tagged (`milestone-1` to `milestone-4.1`).

- [x] **1. Send it now.** Direct delivery during the request, to learn what can go wrong.
- [x] **2. Store before acknowledging.** `202` only after commit, `503` otherwise.
- [x] **3. A separate worker.** Claims, sends, records and retries, independently of the API.
- [x] **4. Reliability.** Expiring claims with fencing tokens, attempt history, backoff and
  limits, `Idempotency-Key`, and `SKIP LOCKED` for several workers.
- [x] **4.1. Tighter guarantees.** After an external review: honest `503` answers, a sender that
  reads only the status code, a worker no answer can crash, a hard retry cap, a guarded test
  database, more race tests, and the demo.
- [ ] **5. Operability.** Producer authentication, signed webhooks, database timeouts, a readiness
  check, body-size limits, structured logs and metrics, a replay endpoint, retention, container
  deployment and a runbook.

Known limitations: one receiver, no authentication or signatures yet, idempotency keys and old
rows are kept forever, no hard deadline for a whole attempt, and no ordering guarantee.
Destinations chosen by producers, multiple tenants and a dashboard are not planned.
