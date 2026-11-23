# Webhook Delivery Service

A small Python service that accepts events from producer apps (such as
[CarRental](https://github.com/DucAnhLe1992/CarRental)) and delivers them as webhooks to a
receiver.

The focus is explicit guarantees, not the number of endpoints: what survives a crash, what can
be duplicated, and what happens when the receiver is slow, down, or answers with an error.

Stack: Python 3.12+, FastAPI, Pydantic, PostgreSQL, SQLAlchemy, Alembic,
[HTTPX2](https://github.com/pydantic/httpx2) (the maintained continuation of HTTPX), pytest, Ruff
and uv.

> **Status: milestone 2 of 5.** Events are saved in PostgreSQL before the API answers. Nothing
> sends them yet: deliveries stay `pending` until the worker arrives in milestone 3.

## How it works today

```text
producer ── POST /deliveries ──────▶ API ── INSERT + COMMIT ──▶ PostgreSQL
         ◀── 202 Accepted ─────────
producer ── GET /deliveries/{id} ──▶ API ── SELECT ───────────▶ PostgreSQL
```

1. The API validates `{event_type, data}` and rejects unknown fields.
2. It inserts a `pending` row into `deliveries` and commits.
3. Only then does it answer `202 Accepted`, with the delivery ID and a `Location` header.

### Guarantees

- `202` means the delivery is committed to PostgreSQL, so it survives an API restart.
- If the database is unreachable, the API answers `503` with `Retry-After`, never `202`.
- **Not yet:** sending (milestone 3). Duplicate protection: if the `202` is lost on its way back
  and the producer sends the event again, it is stored twice (milestone 4 adds
  `Idempotency-Key`).

### Delivery states

```text
pending ──▶ processing ──▶ delivered
   ▲            │
   └── retry ───┤
                └────────▶ failed
```

The database only allows these four states. Only `pending` is used so far; the worker will move
deliveries through the rest.

### Webhook format and retry rules (from milestone 3)

Webhooks follow the [Standard Webhooks](https://www.standardwebhooks.com) spec:

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

`sender.py` classifies each attempt:

| Receiver result | Outcome |
|---|---|
| 2xx | `delivered` |
| 5xx, 408 or 429; connection refused or dropped; no answer in time | `retryable_failure` |
| Other 4xx, or a 3xx redirect (never followed) | `permanent_failure` |

## Run it

Requires Python 3.12+, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync
docker compose up -d          # PostgreSQL on localhost:5433
uv run alembic upgrade head   # create or update the tables
uv run uvicorn webhook_service.main:app --reload
```

Open http://127.0.0.1:8000/docs for an interactive form, or use curl:

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H 'content-type: application/json' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42}}'

curl http://127.0.0.1:8000/deliveries/<delivery_id>
```

## Experiments

Milestone 2 is done when you can explain each result.

1. **Restart the API.** Submit an event, stop the API with Ctrl+C, start it again and `GET` the
   delivery. It is still there. What does `202` promise that milestone 1's `200` could not?
2. **Stop the database.** Run `docker compose stop db` and submit: `503`. Why must the API never
   answer `202` here, and what should the producer do? Run `docker compose start db` and submit
   again. The API recovers without a restart.
3. **Look inside.** Run
   `docker compose exec db psql -U webhooks -c "SELECT id, status, attempt_count FROM deliveries"`.
   Every row stays `pending`. Who is going to send them?

## Configuration

Set environment variables, or copy `.env.example` to `.env`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks` | Used by the API and by migrations. |
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | The only destination (from milestone 3). Producers cannot choose a URL. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | How long one attempt waits for the receiver (from milestone 3). |

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
  main.py                       app setup, 503 when storage is down, /health
  config.py                     settings from the environment
  db.py                         database engine and per-request session
  models.py                     deliveries table and delivery states
  schemas.py                    request, response and webhook message models
  services/delivery_service.py  create and read deliveries; owns the commit
  api/deliveries.py             POST and GET /deliveries
  sender.py                     sends one attempt and classifies the outcome (from milestone 3)
alembic/versions/               database migrations
dev/receiver.py                 test receiver with ok / slow / error / reject modes
tests/unit/                     tests that need no database
tests/integration/              tests against real PostgreSQL
```

## Roadmap

- [x] **1. HTTP path.** The endpoint sent each webhook directly. Lessons: an event held only in
  memory is lost when the receiver is down, and a timeout means "unknown", not "failed".
- [x] **2. Store before acknowledging.** PostgreSQL, SQLAlchemy and Alembic. `POST /deliveries`
  commits a `pending` row before answering `202`; `GET /deliveries/{id}` reads it back.
- [ ] **3. Worker.** A separate process polls for due deliveries, sends them with `sender.py` and
  records the results. The API can stop while the worker keeps going.
- [ ] **4. Reliability.** Attempt history, bounded retries with backoff, `Idempotency-Key` on
  submission, expiring claims with claim tokens, `FOR UPDATE SKIP LOCKED` for concurrent
  workers, and concurrency tests.
- [ ] **5. Operability.** Producer authentication, signed webhooks, structured logs, metrics,
  graceful shutdown and a runbook.

Not planned for now: destination URLs chosen by producers, multiple tenants, ordering guarantees
and a dashboard.
