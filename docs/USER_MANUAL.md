# User manual

This manual explains how to run the webhook delivery service, how to send it events, how to
receive its webhooks, how a delivery moves through the system, and how to operate it. For the
internal design (data model, locking, design decisions), see [ARCHITECTURE.md](../ARCHITECTURE.md).

Who should read what:

| You are… | Read |
|---|---|
| Running the service | [Concepts](#1-concepts), [Running the service](#2-running-the-service), [Configuration](#7-configuration-reference) |
| Building a producer, an app that sends events | [Submitting events](#3-submitting-events), [Tracking deliveries](#4-tracking-deliveries) |
| Building a receiver, an app that gets webhooks | [Receiving webhooks](#5-receiving-webhooks) |
| Curious how it all fits together | [How a delivery flows](#6-how-a-delivery-flows) |
| Managing producers, API keys and secrets | [Producers and API keys](#producers-and-api-keys), [Rotating the webhook secret](#rotating-the-webhook-secret) |
| Keeping it healthy | [Operating the service](#8-operating-the-service) |

## Contents

1. [Concepts](#1-concepts)
2. [Running the service](#2-running-the-service)
3. [Submitting events](#3-submitting-events)
4. [Tracking deliveries](#4-tracking-deliveries)
5. [Receiving webhooks](#5-receiving-webhooks)
6. [How a delivery flows](#6-how-a-delivery-flows)
7. [Configuration reference](#7-configuration-reference)
8. [Operating the service](#8-operating-the-service)
9. [Limitations](#9-limitations)
10. [Glossary](#10-glossary)

## 1. Concepts

The service is a reliable mail room for events. An application hands it an event such as "booking
42 was confirmed". The service stores the event, delivers it to a receiver as an HTTP request (a
webhook), keeps trying when the receiver is down, and records every attempt.

| Term | Meaning |
|---|---|
| **Producer** | An application that submits events, for example CarRental. An operator registers it with `webhook-admin`. It sees only its own deliveries. |
| **API key** | A producer's credential, sent with every request as `Authorization: Bearer <key>`. A producer can have several. The service stores only a hash of each. |
| **Receiver** | The HTTP endpoint that gets the webhooks. There is one, set by `RECEIVER_URL`. |
| **Webhook secret** | A secret the workers and the receiver share. Workers sign every webhook with it, and the receiver checks the signature. |
| **Delivery** | One accepted event and its delivery state. Its ID is sent to the receiver as `webhook-id`. |
| **Attempt** | One try at sending a delivery. It is recorded when a worker claims the delivery, so an attempt cut short by a crash may never have reached the receiver. A delivery can have several. |
| **API** | The process that accepts and reports deliveries (`uvicorn webhook_service.main:app`). |
| **Worker** | A process that sends deliveries (`webhook-worker`). You can run several. |
| **Claim** | A worker's temporary ownership of a delivery while it sends it. Claims expire. |

A delivery is always in one of four states:

| State | Meaning |
|---|---|
| `pending` | Stored and waiting to be sent, either for the first time or for a retry. |
| `processing` | A worker has claimed it and is sending it now. |
| `delivered` | The receiver answered with a 2xx status. Final. |
| `failed` | The receiver rejected it permanently, or every allowed attempt failed. Final. |

What the service promises, in plain words:

- **Only registered producers can submit or read deliveries, and each sees only its own.**
- **When the API answers `202`, the event is saved.** It survives restarts of the API, the workers
  and the database server.
- **When the API answers `503`, or no answer arrives, the event may or may not be saved.** The
  connection can fail just after the database saved it. Retrying with the same `Idempotency-Key`
  is safe either way.
- **Every saved event is sent until it is delivered or clearly fails.** Failures are retried with
  growing waits, up to `MAX_ATTEMPTS` attempts.
- **An event may arrive more than once.** When the service cannot tell whether the receiver got it
  (a timeout, or a worker crash), it sends it again with the same `webhook-id`. Receivers must skip
  IDs they have already processed.
- **Every webhook is signed.** A receiver that checks the signature can refuse webhooks that were
  forged, changed on the way, or replayed later.
- **Sending the same request twice is safe** if the producer includes an `Idempotency-Key` header.
- **Events can arrive out of order.**

## 2. Running the service

### Requirements

- Python 3.12 or newer, and [uv](https://docs.astral.sh/uv/)
- Docker, for PostgreSQL

### First-time setup

```bash
uv sync                       # install dependencies into .venv
docker compose up -d --wait   # start PostgreSQL 18 on localhost:5433 and wait until it is ready
uv run alembic upgrade head   # create the tables

# A secret to sign webhooks with. The worker signs with it, and the test receiver checks with it.
echo "WEBHOOK_SECRETS=$(uv run webhook-admin new-webhook-secret)" >> .env

# A producer. Its API key is printed only once, so keep it.
API_KEY=$(uv run webhook-admin create-producer carrental)
```

PostgreSQL listens only on `127.0.0.1`, with user, password and database all set to `webhooks`.
Its data lives in the `pgdata` volume declared in `compose.yaml`.

Every process started in this folder reads `.env`, and Git ignores it. `create-producer` prints
only the key on standard output, so `$(...)` captures it. If you lose it, create another with
`uv run webhook-admin create-key carrental`. [Producers and API keys](#producers-and-api-keys)
covers the rest.

### Starting the processes

Use one terminal per process:

```bash
uv run python dev/receiver.py                      # a test receiver on :9000
uv run uvicorn webhook_service.main:app --reload   # the API on :8000
uv run webhook-worker                              # a worker
```

The API shows one access-log line per request:

```text
INFO:     127.0.0.1:54630 - "POST /deliveries HTTP/1.1" 202 Accepted
```

The worker shows one line per attempt, starting with the delivery ID and the attempt number. Here
the receiver answered `500` to the first attempt and `200` to the retry 11 seconds later:

```text
01:28:38 [worker] started (pid 186013), sending to http://127.0.0.1:9000/webhooks, signing with 1 secret(s)
01:28:50 [worker] 73731487-6839-4966-b9b2-9b2a1a83054c attempt 1: failed (HTTP 500), retrying in 10s
01:29:01 [worker] 73731487-6839-4966-b9b2-9b2a1a83054c attempt 2: delivered (HTTP 200, 2 ms)
```

Times are in the machine's local time. Other worker messages:

| Message | Meaning |
|---|---|
| `failed (HTTP 500), no attempts left` | The last allowed attempt failed; the delivery is now `failed`. |
| `failed permanently (HTTP 400)` | The receiver rejected it; it will not be retried. |
| `failed (timeout), retrying in 9s` | No answer in time; the receiver may still have processed it. |
| `failed (connection_error), retrying in 10s` | The receiver could not be reached. |
| `result discarded; the claim expired and another worker took the delivery over` | This worker was so slow that its claim expired and another worker took over, so its result was not saved. |
| `database unavailable, trying again in 1.0s` | PostgreSQL is down; the worker keeps polling. |
| `unexpected error, trying again in 1.0s`, with a traceback | A bug. The worker carries on with other deliveries; the one it held is sent again when its claim expires. |
| `stopping after the current delivery`, then `stopped` | A clean stop after Ctrl+C or `SIGTERM`. |
| `WEBHOOK_SECRETS is not set, and the worker does not send unsigned webhooks…`, and the worker exits | No webhook secret is configured. See [First-time setup](#first-time-setup). |

### Watching it work: the demo

`uv run python dev/demo.py` shows the guarantees with real processes. It needs PostgreSQL running,
creates its own `webhooks_demo` database with two producers, carrental and friendster, and makes a
webhook secret for the run. It then starts a test receiver, the API and a worker on free ports
with short timeouts, and plays five scenes in about 20 seconds:

| Scene | What happens | What it shows |
|---|---|---|
| 1 | The same request is sent twice with one `Idempotency-Key`, then with different content. | `202`, a replay with `Idempotent-Replayed: true`, then `409`. One delivery. |
| 2 | The receiver is stopped. | `connection_error` attempts with growing waits, then delivery once the receiver is back. |
| 3 | The receiver answers after 3 seconds; the worker waits 2. | A `timeout`, a retry, and the receiver processing the same `webhook-id` twice. |
| 4 | The worker is killed with `SIGKILL` while it waits for the receiver. | The delivery stuck in `processing` until its claim expires, then `claim_expired` and delivery by a new worker. |
| 5 | A request without a key; the same `Idempotency-Key` from both producers; friendster reading carrental's delivery; a 100 KB event. Then an attacker resends a captured webhook unchanged, with its body changed, with a new timestamp, and signed with a guessed secret. | `401`; two separate deliveries; `404`; `413`. The unchanged copy passes the signature check and is caught as a duplicate; the other three get `401`. |

It prints each step, the worker's and the receiver's log lines, and each delivery's attempt
history. The demo database is emptied at the start of every run and left in place afterwards, so
you can look at it with `psql`.

### Running more workers

Start more `uv run webhook-worker` processes, on this machine or on others that can reach the same
`DATABASE_URL`. Each worker claims different deliveries, and none waits for another. Give all
workers the same settings.

### Stopping

| What | How | Effect |
|---|---|---|
| API | Ctrl+C | Stops accepting. Stored deliveries are unaffected. |
| Worker | Ctrl+C or `kill <pid>` (`SIGTERM`) | Finishes the current delivery (at most a few seconds), then exits. |
| Worker, forcibly | `kill -9 <pid>` | Stops at once. Its current delivery is retried by another worker after `CLAIM_TIMEOUT_SECONDS`. |
| Database | `docker compose stop` | Data is kept. The API answers `503` and workers wait until it is back. |
| Database container | `docker compose down` | The container is removed; the `pgdata` volume, and your data, are kept. |
| All data | `docker compose down -v` | **Deletes the volume and every delivery.** |

### Upgrading

Stop the workers, update the code, then run `uv sync` and `uv run alembic upgrade head` before
starting the processes again. Migrations are versioned in `alembic/versions`.

Milestone 5.1 (version 0.5.1) added API keys and signatures, so two more steps come before the
restart: set `WEBHOOK_SECRETS` for the workers and the receiver, and create a producer and an API
key for each app that submits events. Until a producer sends its key, its requests get `401`.
Deliveries stored before the upgrade belong to the producer `legacy`, and are still delivered.

## 3. Submitting events

### The request

```http
POST /deliveries
Authorization: Bearer wds_t408ROrX...
Content-Type: application/json
Idempotency-Key: booking-42-confirmed

{"event_type": "booking.confirmed", "data": {"booking_id": 42, "car_id": 7}}
```

| Part | Rules |
|---|---|
| `Authorization` header | Required: `Bearer` and the producer's API key. See [Producers and API keys](#producers-and-api-keys). |
| `event_type` | Required. 1 to 100 characters. Lowercase letters, digits and underscores, in parts joined by dots: `booking.confirmed`, `user_profile.updated`. |
| `data` | Required. A JSON object. It is delivered as is, but stored as JSONB, so key order and whitespace are not kept. |
| Other fields | Rejected. In particular, a producer cannot choose the destination URL. |
| `Idempotency-Key` header | Optional, but recommended. 1 to 255 characters. See [Idempotency keys](#idempotency-keys). |
| Size | The whole body may be at most `MAX_EVENT_BYTES` bytes, 65,536 by default. |

Keep events small: send IDs and the facts a receiver needs, not whole records. The Standard
Webhooks specification recommends payloads "usually smaller than 20kb", because every receiver
pays for every byte, whether it needs the data or not.

With curl, using the key from [First-time setup](#first-time-setup):

```bash
curl -i -X POST http://127.0.0.1:8000/deliveries \
  -H "Authorization: Bearer $API_KEY" \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: booking-42-confirmed' \
  -d '{"event_type": "booking.confirmed", "data": {"booking_id": 42, "car_id": 7}}'
```

### Responses

**`202 Accepted`**: the delivery is stored. It has not been sent yet.

```http
HTTP/1.1 202 Accepted
content-type: application/json
location: /deliveries/73731487-6839-4966-b9b2-9b2a1a83054c

{"delivery_id":"73731487-6839-4966-b9b2-9b2a1a83054c","status":"pending"}
```

**`202 Accepted` with `Idempotent-Replayed: true`**: this key was used before with the same
content. You get the original delivery back, and nothing new is stored. `status` shows the
delivery's current state, which may already be `delivered`.

```http
HTTP/1.1 202 Accepted
content-type: application/json
location: /deliveries/73731487-6839-4966-b9b2-9b2a1a83054c
idempotent-replayed: true

{"delivery_id":"73731487-6839-4966-b9b2-9b2a1a83054c","status":"pending"}
```

**`401 Unauthorized`**: the request has no API key, or the key is unknown or revoked. Nothing was
stored.

```http
HTTP/1.1 401 Unauthorized
www-authenticate: Bearer

{"detail": "Invalid API key"}
```

Without an `Authorization` header, `detail` says how to send the key. An unknown key and a revoked
one get the same answer, so the answer does not reveal which keys exist.

**`409 Conflict`**: this producer used the key before with *different* content.

```json
{"detail": "This Idempotency-Key was already used for a different request"}
```

**`413 Content Too Large`**: the body is larger than `MAX_EVENT_BYTES`. Nothing was stored.

```json
{"detail": "The request body is larger than the limit of 65536 bytes"}
```

**`422 Unprocessable Content`**: the request is invalid, and nothing was stored. `detail` says
what is wrong and where:

```json
{
  "detail": [
    {
      "type": "string_pattern_mismatch",
      "loc": ["body", "event_type"],
      "msg": "String should match pattern '^[a-z0-9_]+(\\.[a-z0-9_]+)*$'",
      "input": "Booking Confirmed",
      "ctx": {"pattern": "^[a-z0-9_]+(\\.[a-z0-9_]+)*$"}
    }
  ]
}
```

Other examples are `extra_forbidden` with `loc: ["body", "url"]` for an unknown field, and
`string_too_long` with `loc: ["header", "idempotency-key"]` for a key longer than 255 characters.

**`503 Service Unavailable`**: the database could not be reached, or was too busy to hand out a
connection in time.

```http
HTTP/1.1 503 Service Unavailable
retry-after: 5

{"detail": "Storage is unavailable, so it is not known whether the delivery was stored. Retry with the same Idempotency-Key."}
```

Usually nothing was stored. But if the connection failed while the API was committing, the
database may have saved the delivery without the API hearing back, so the API cannot promise
either way. A retry with the same key stores the delivery, or finds the stored one and returns it
as a replay.

### What a producer should do

| The producer sees | Meaning | Do this |
|---|---|---|
| `202` | Stored, now or by an earlier identical request. | Done. Mark the event as handed over. |
| `503` | Not confirmed: usually not stored, but possibly stored just before the connection failed. | Wait at least `Retry-After` seconds, then retry with the same key. |
| A timeout, a dropped connection or another 5xx | Unknown: it may or may not be stored. | Retry with the same key, waiting longer each time. |
| `401` | The API key is missing, wrong or revoked. Nothing was stored. | Do not keep retrying: alert someone to fix the key. Events wait in the outbox meanwhile. |
| `409` | The key was reused for different content. | Do not retry. This is a bug in how the producer builds keys. |
| `413` | The event is larger than `MAX_EVENT_BYTES`. Nothing was stored. | Do not retry. Send less data. |
| `422` | The request is invalid. | Do not retry. Fix the request. |

### Idempotency keys

A response can be lost after the service stored the event. The producer then cannot tell "stored"
from "never arrived", and must send again. The key makes that resend harmless.

| Situation | Result |
|---|---|
| No key | A new delivery every time. A resend creates a duplicate the receiver cannot detect, because it gets a new `webhook-id`. That includes a resend after a `503` or a lost answer, if the first request was in fact stored. |
| A new key | A new delivery, stored with the key. |
| A known key, same content | `202` with the original delivery and `Idempotent-Replayed: true`. |
| A known key, different content | `409 Conflict`. |
| The same new key sent twice at the same moment | One delivery. The second request waits for the first, then gets the replay, or `409` if the content differs. If the first request fails before it commits, the second stores its own delivery. |

"Same content" means the same `event_type` and `data`. The order of keys inside `data` does not
matter.

To choose keys:

- Derive the key from what makes the event unique, and **create it once**: store it with the event
  and reuse it on every retry. A new random key per attempt protects nothing.
- Good keys: the ID of the producer's outbox row, or `booking-42-confirmed-<booking version>`.
- Bad keys: the booking ID alone, because it would be reused for `booking.cancelled` and rejected
  with `409`.
- Keys belong to the producer that sends them: two producers can use the same key without
  affecting each other. Keys never expire.

### Recommended integration: the transactional outbox

A producer such as CarRental should not call the service directly in its business code:

- **Calling after its own commit:** a crash between the commit and the call loses the event.
- **Calling inside its transaction:** locks are held during a network call.

Instead, write the event to an outbox table in the same transaction as the business change, and
let a small relay forward it:

```mermaid
sequenceDiagram
    participant B as Booking code
    participant O as Outbox table
    participant L as Relay
    participant S as Delivery service
    B->>O: INSERT the event in the same transaction as the booking change
    Note over B,O: the booking and its event commit together, or not at all
    loop every few seconds
        L->>O: SELECT unsent events FOR UPDATE SKIP LOCKED
        L->>S: POST /deliveries with Idempotency-Key set to the outbox row ID
        S-->>L: 202
        L->>O: mark the event as sent
    end
```

The relay sends the producer's API key with every request. On `503`, timeouts and network errors,
it leaves the row unsent and tries again later with the same key. On `401`, `409`, `413` or
`422`, it should alert someone: the key is wrong or revoked, or the producer has a bug. The row
stays unsent in the meantime, so nothing is lost while it is fixed.

## 4. Tracking deliveries

### `GET /deliveries/{delivery_id}`

```bash
curl -H "Authorization: Bearer $API_KEY" \
  http://127.0.0.1:8000/deliveries/73731487-6839-4966-b9b2-9b2a1a83054c
```

This is the delivery from the worker log above. Its first attempt got a `500`, and the retry
11 seconds later was delivered:

```json
{
  "delivery_id": "73731487-6839-4966-b9b2-9b2a1a83054c",
  "event_type": "booking.confirmed",
  "data": {"car_id": 7, "booking_id": 42},
  "status": "delivered",
  "attempt_count": 2,
  "last_http_status": 200,
  "last_error_category": null,
  "next_attempt_at": "2026-11-23T23:30:01.487041Z",
  "created_at": "2026-11-23T23:28:50.230425Z",
  "updated_at": "2026-11-23T23:29:01.500920Z",
  "attempts": [
    {
      "number": 1,
      "started_at": "2026-11-23T23:28:50.379859Z",
      "finished_at": "2026-11-23T23:28:50.396936Z",
      "outcome": "retryable_failure",
      "http_status": 500,
      "error_category": "http_error",
      "duration_ms": 2
    },
    {
      "number": 2,
      "started_at": "2026-11-23T23:29:01.487041Z",
      "finished_at": "2026-11-23T23:29:01.500920Z",
      "outcome": "delivered",
      "http_status": 200,
      "error_category": null,
      "duration_ms": 2
    }
  ]
}
```

`data` lists `car_id` first, although the producer sent `booking_id` first, because JSONB does not
keep key order. `next_attempt_at` is 60 seconds after attempt 2 started: it is the expiry of the
last claim, left over and no longer meaningful.

| Field | Meaning |
|---|---|
| `delivery_id` | Also sent to the receiver as `webhook-id`. |
| `event_type`, `data` | What the producer submitted. |
| `status` | `pending`, `processing`, `delivered` or `failed`. |
| `attempt_count` | Attempts started so far, including one in progress or one cut short by a crash. |
| `last_http_status`, `last_error_category` | Result of the latest attempt whose worker saved a result. An attempt closed as `claim_expired` does not change them. |
| `next_attempt_at` | While `pending`: when the next attempt is due. While `processing`: when the current claim expires. Ignore it for `delivered` and `failed`. |
| `created_at` | When it was accepted. Also the body's `timestamp` in the webhook. |
| `updated_at` | When it last changed. |
| `attempts` | Every attempt, oldest first. |

Each attempt has:

| Field | Meaning |
|---|---|
| `number` | 1, 2, 3… |
| `started_at`, `finished_at` | `finished_at` is `null` while the attempt is in progress. |
| `outcome` | `delivered`, `retryable_failure`, `permanent_failure`, or `null` while in progress. |
| `http_status` | The receiver's status, or `null` if it never answered. |
| `error_category` | `timeout`, `connection_error`, `http_error`, `claim_expired`, or `null`. |
| `duration_ms` | Time until the receiver's status code arrived, or until the request failed. |

A producer can read only its own deliveries. An unknown ID returns `404` with
`{"detail": "Delivery not found"}`, and so does the ID of another producer's delivery: the answer
does not even confirm that the ID exists. A malformed ID returns `422`, a missing or invalid API
key `401`, and if the database is unavailable, the answer is `503` with
`{"detail": "Storage is unavailable. Retry later."}`.

### Reading the history

**A worker crashed mid-attempt.** Attempt 1 has no HTTP status and ends 15 seconds after it
started, which is exactly the claim timeout used. Another worker then delivered it. The receiver
may have received it twice.

```json
"status": "delivered",
"attempts": [
  {"number": 1, "started_at": "…23:06:36.918Z", "finished_at": "…23:06:52.836Z",
   "outcome": "retryable_failure", "http_status": null, "error_category": "claim_expired"},
  {"number": 2, "started_at": "…23:06:52.836Z", "finished_at": "…23:06:52.854Z",
   "outcome": "delivered", "http_status": 200, "error_category": null}
]
```

**It gave up.** Four attempts all got a `500`, about 12, 17 and 48 seconds apart (with
`MAX_ATTEMPTS=4`), and then the delivery was marked `failed`.

```json
"status": "failed", "attempt_count": 4, "last_http_status": 500,
"attempts": [
  {"number": 1, "started_at": "…23:07:37.692Z", "outcome": "retryable_failure", "http_status": 500},
  {"number": 2, "started_at": "…23:07:49.807Z", "outcome": "retryable_failure", "http_status": 500},
  {"number": 3, "started_at": "…23:08:06.982Z", "outcome": "retryable_failure", "http_status": 500},
  {"number": 4, "started_at": "…23:08:55.369Z", "outcome": "retryable_failure", "http_status": 500}
]
```

**It was rejected.** A single attempt with `permanent_failure` and a 4xx status means the receiver
refused the event. It is not retried.

### Other endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | Returns `{"status": "ok"}` while the API process is up. It does not check the database. |
| `GET /docs` | Interactive API documentation. |
| `GET /openapi.json` | The OpenAPI schema. |

## 5. Receiving webhooks

### What a receiver gets

The service sends one `POST` per attempt to `RECEIVER_URL`. Headers and body follow the
[Standard Webhooks](https://www.standardwebhooks.com) spec. This is the second attempt of the
delivery shown in [Tracking deliveries](#4-tracking-deliveries):

```http
POST /webhooks
content-type: application/json
webhook-id: 73731487-6839-4966-b9b2-9b2a1a83054c
webhook-timestamp: 1795476541
webhook-signature: v1,lPzexW1LHchpWqMHICUwhzcH5Jo2FsZLjKP5xfY7g8E=

{"type":"booking.confirmed","producer":"carrental","timestamp":"2026-11-23T23:28:50.230425Z","data":{"car_id":7,"booking_id":42}}
```

| Part | Meaning |
|---|---|
| `webhook-id` | The delivery ID. **The same on every attempt.** Use it to detect duplicates. |
| `webhook-timestamp` | When this attempt was sent, in Unix seconds. Here 23:29:01 UTC, the second attempt. |
| `webhook-signature` | `v1,` and a signature, once per secret in `WEBHOOK_SECRETS`, separated by spaces. It covers the timestamp, so every attempt has a new one. See [Checking signatures](#checking-signatures). |
| `type` | The producer's `event_type`. |
| `producer` | The producer that submitted the event, such as `carrental`. Producers choose event types freely, so this is how a receiver knows who sent a `booking.confirmed`. |
| `timestamp` | When the service accepted the event, in ISO 8601 UTC. It is the same on every attempt. |
| `data` | The producer's `data`. |

### How to answer

| The receiver answers | The service |
|---|---|
| 2xx within `DELIVERY_TIMEOUT_SECONDS` (5 s by default) | Marks it `delivered`. |
| 5xx, 408 or 429 | Retries later. |
| Nothing in time, a refused connection or a dropped connection | Retries later. |
| Any other 4xx | Marks it `failed`, with no retry. |
| 3xx | Marks it `failed`. Redirects are never followed. |

Only the status code counts. The service closes the connection as soon as your status line
arrives, without reading the body, so the body can be empty; anything in it is ignored.

So a receiver should:

1. **Check the signature first,** and answer `401` if it fails. See
   [Checking signatures](#checking-signatures).
2. **Answer quickly.** Save the event, answer `200`, and do slow work afterwards. A slow answer
   becomes a timeout, and a timeout becomes a resend.
3. **Skip duplicates, but still answer `200`.** A duplicate is a success from the sender's point
   of view. Answering `409` would mark it `failed`, and `500` would keep it retrying.
4. **Use 4xx only for events that can never succeed,** such as a payload it cannot parse. Use 5xx
   or `429` for temporary problems.
5. **Not rely on order.** A retried `booking.confirmed` can arrive after a later
   `booking.cancelled`. Use the event's data and `timestamp`, not arrival order.

### Skipping duplicates

Record each `webhook-id` in the same database transaction as the work it triggers:

```sql
CREATE TABLE processed_webhooks (
    webhook_id   text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- Inside the transaction that does the work:
INSERT INTO processed_webhooks (webhook_id) VALUES ($1)
ON CONFLICT (webhook_id) DO NOTHING;
-- 1 row inserted: new event. Do the work, commit, answer 200.
-- 0 rows inserted: duplicate. Skip the work, commit, answer 200.
```

Because the ID and the work commit together, a crash can never leave one without the other. Two
copies can even arrive at the same moment, when a frozen worker's request and its replacement
overlap. The same pattern handles that: the second insert waits for the first transaction, then
inserts nothing. Keep IDs at least as long as the retry window, about 21 minutes with the default
settings, and longer if operators may [replay](#replaying-a-failed-delivery) old deliveries.
Keeping them for a few days is a safe choice. CarRental already does the same for Stripe events,
with its `stripe_events` table.

The same table stops replays. A captured webhook resent within the five minutes its timestamp
allows passes the signature check, but its `webhook-id` has been processed already.

### Checking signatures

Every webhook is signed as the [Standard Webhooks](https://www.standardwebhooks.com) specification
describes, so a receiver can tell a real webhook from a forged, changed or replayed one. The
signature is an HMAC-SHA256 of three parts joined by dots:

```text
{webhook-id}.{webhook-timestamp}.{the body, byte for byte}
```

The key is the part of the secret after `whsec_`, decoded from base64. The header holds `v1,` and
the signature in base64, once for each secret the workers have, separated by spaces.

To check a webhook:

1. **Keep the raw body.** Check the bytes that arrived, before parsing the JSON. Parsing and
   encoding it again can change spacing or key order, and then no signature matches.
2. **Check the timestamp.** Refuse a `webhook-timestamp` more than five minutes away from your
   clock, so a captured webhook cannot be replayed later.
3. **Compute the signature** with each secret you have, and compare it with each signature in the
   header, in constant time. Accept the webhook if any pair matches.
4. **Then parse the JSON, and skip `webhook-id`s you have processed** (see
   [Skipping duplicates](#skipping-duplicates)).

Each signed part closes a gap:

| Signed part | An attacker who changes it, to… | Is stopped because |
|---|---|---|
| The body | …change the event, say its `booking_id` | the signature no longer matches. |
| `webhook-timestamp` | …replay an old webhook with a fresh timestamp | the signature no longer matches, and without a fresh timestamp the age check refuses it. |
| `webhook-id` | …replay a webhook under a new ID, past the duplicate check | the signature no longer matches. |

A webhook resent unchanged within five minutes passes the signature and age checks, which is why
the duplicate check is part of verification, not an extra.

Answer `401` when the check fails. The service treats it like any other 4xx: the delivery fails at
once, without retries, because sending the same webhook again cannot fix secrets that do not
match. Once they are fixed, [replay](#replaying-a-failed-delivery) the failed deliveries.

**In TypeScript,** with Node's built-in `crypto`:

```ts
import { createHmac, timingSafeEqual } from "node:crypto";

const TOLERANCE_SECONDS = 5 * 60;

// Throws unless the webhook is signed with one of the secrets and is recent.
// rawBody must be the body exactly as it arrived, before any JSON parsing.
export function verifyWebhook(
  secrets: string[],
  headers: Record<string, string | string[] | undefined>,
  rawBody: Buffer,
  nowSeconds = Date.now() / 1000,
): void {
  const id = headers["webhook-id"];
  const timestamp = headers["webhook-timestamp"];
  const signatures = headers["webhook-signature"];
  if (typeof id !== "string" || typeof timestamp !== "string" || typeof signatures !== "string") {
    throw new Error("missing webhook headers");
  }
  if (!/^\d+$/.test(timestamp) || Math.abs(nowSeconds - Number(timestamp)) > TOLERANCE_SECONDS) {
    throw new Error("timestamp too old or too new");
  }
  const signedContent = Buffer.concat([Buffer.from(`${id}.${timestamp}.`), rawBody]);
  const expected = secrets.map((secret) => {
    const key = Buffer.from(secret.replace(/^whsec_/, ""), "base64");
    const digest = createHmac("sha256", key).update(signedContent).digest("base64");
    return Buffer.from(`v1,${digest}`);
  });
  const matches = signatures.split(" ").some((signature) => {
    const given = Buffer.from(signature);
    return expected.some((e) => e.length === given.length && timingSafeEqual(e, given));
  });
  if (!matches) {
    throw new Error("no signature matches");
  }
}
```

In an Express app such as CarRental, mount the route with `express.raw()`, so the body arrives as
a `Buffer`, as CarRental already does for Stripe:

```ts
webhookRoutes.post("/delivery-service", express.raw({ type: "application/json" }), (req, res) => {
  try {
    verifyWebhook(process.env.WEBHOOK_SECRETS!.split(" "), req.headers, req.body);
  } catch {
    res.sendStatus(401);
    return;
  }
  const event = JSON.parse(req.body.toString("utf8"));
  // Record req.headers["webhook-id"] and do the work in one transaction, then:
  res.sendStatus(200);
});
```

**In Python,** with the specification's own library (`pip install standardwebhooks`), here in a
FastAPI app:

```python
import os

from fastapi import FastAPI, Request, Response
from standardwebhooks.webhooks import Webhook, WebhookVerificationError

# One Webhook per secret: during a rotation, the receiver may know two.
verifiers = [Webhook(secret) for secret in os.environ["WEBHOOK_SECRETS"].split()]
app = FastAPI()


@app.post("/webhooks")
async def receive_webhook(request: Request) -> Response:
    body = await request.body()  # the raw bytes, before any JSON parsing
    for verifier in verifiers:
        try:
            event = verifier.verify(body, dict(request.headers))
            break
        except WebhookVerificationError:
            continue
    else:
        return Response(status_code=401)
    # Record request.headers["webhook-id"] and do the work in one transaction, then:
    return Response(status_code=200)
```

Libraries for other languages are listed on [standardwebhooks.com](https://www.standardwebhooks.com).
Both examples above, and the official Python and JavaScript libraries, were checked against
signatures made by this service: they accept real webhooks, including the two-signature webhooks
of a rotation, and refuse a changed body, another secret, an old timestamp and a refreshed one.
The service's own implementation is `src/webhook_service/signing.py`.

During a [secret rotation](#rotating-the-webhook-secret), each webhook carries two signatures, one
per secret. A receiver that has either secret accepts it, so it can switch at any point while both
are sent.

A signature proves who sent a webhook and that it was not changed, but it does not hide its
content. In production, the receiver should use HTTPS.

### The test receiver

`dev/receiver.py` is a receiver for experiments. It keeps everything in memory.

```bash
uv run python dev/receiver.py [--mode ok|slow|error|reject] [--port 9000] [--slow-seconds 10]
                              [--secret whsec_...]
```

It checks every webhook's signature with the secrets in `WEBHOOK_SECRETS`, from the environment or
`.env`, or with the ones given with `--secret`, which can be repeated. A webhook that fails the
check gets `401` and is not processed. With no secret at all, it logs a warning and accepts
everything.

| Mode | Behaviour |
|---|---|
| `ok` | Processes the event and answers `200`. |
| `slow` | Processes the event, waits `--slow-seconds`, then answers `200`. The worker times out first, so the event is processed and then sent again. |
| `error` | Answers `500` without processing. |
| `reject` | Answers `400` without processing. |

| Endpoint | Purpose |
|---|---|
| `POST /webhooks` | Checks the signature, then receives the webhook and logs `processed (first time)` or `processed (DUPLICATE)`. |
| `PUT /mode/{mode}` | Switches the mode without a restart: `curl -X PUT localhost:9000/mode/slow`. |
| `GET /received` | Every processed `webhook-id`, with how many times it was processed, its payload, and the latest copy exactly as it arrived (the signed headers and the raw body), for replay experiments. |

Its log for the delivery above, where the mode was switched from `error` to `ok` between the
attempts:

```text
01:28:50 [receiver] 73731487-6839-4966-b9b2-9b2a1a83054c -> 500 (mode=error, not processed)
01:28:55 [receiver] mode set to ok
01:29:01 [receiver] 73731487-6839-4966-b9b2-9b2a1a83054c processed (first time) type=booking.confirmed producer=carrental
```

A webhook that fails the signature check is logged as
`<webhook-id> -> 401 (signature check failed: no signature matches)`, or with the reason
`the timestamp is too old or too far in the future`.

## 6. How a delivery flows

This section follows one event from submission to its final state.

### 6.1 Submission

1. While the body arrives, the API counts its bytes. A body larger than `MAX_EVENT_BYTES` gets
   `413` before it is read into memory.
2. It looks up the hash of the API key. A missing, unknown or revoked key gets `401`.
3. It validates the body. An invalid body gets `422`, and nothing is stored. A body that is not
   JSON at all gets its `422` while it is read, before the key is checked.
4. Without an `Idempotency-Key`, it inserts a `pending` delivery for the producer and commits.
   With one, it inserts with `ON CONFLICT DO NOTHING` on the producer and the key:
   - a key the producer has not used is stored;
   - a key the producer used with the same content returns the original delivery;
   - a key the producer used with different content gets `409`.
5. Only after the commit does it answer `202`, with the delivery ID and a `Location` header. If
   the database cannot be reached, it answers `503`, and the producer retries with the same key.

The new delivery is `pending`, with `attempt_count` 0 and `next_attempt_at` set to now, so it is
due immediately.

### 6.2 One attempt

Each worker repeats this loop:

1. **Claim.** In one short transaction, the worker locks the oldest due delivery with
   `FOR UPDATE SKIP LOCKED`. A delivery is due if it is `pending` with `next_attempt_at` in the
   past, or `processing` with an expired claim. For an expired claim, it first closes the lost
   attempt as `claim_expired` (see [6.4](#64-when-a-worker-crashes-or-freezes)). The worker then:
   - marks the delivery `processing` and adds 1 to `attempt_count`;
   - opens an attempt row and stores its ID as the delivery's claim token;
   - moves `next_attempt_at` to now plus `CLAIM_TIMEOUT_SECONDS`, the moment the claim expires.
2. **Send.** It signs the webhook with every secret in `WEBHOOK_SECRETS`, posts it with no
   transaction open, and reads only the status code. Nothing in the database is locked while the
   receiver thinks.
3. **Record.** In a new short transaction, it locks the delivery again, but only if the claim
   token is still its own. An expired claim still counts as its own until another worker takes
   the delivery over. If the token changed, another worker took over, and this worker discards
   its result. Otherwise it finishes the attempt row, clears the claim token, and sets:
   - `delivered` for a 2xx;
   - `pending`, with `next_attempt_at` moved ahead by the retry delay, for a retryable failure
     with attempts left;
   - `failed` for a permanent failure, or when no attempts are left.
4. **Wait.** If nothing was due, it sleeps `POLL_INTERVAL_SECONDS`.

### 6.3 Retries

After a retryable failure, the wait doubles from `RETRY_BASE_DELAY_SECONDS` and varies by up to
20% either way, but never exceeds `RETRY_MAX_DELAY_SECONDS`. With the defaults:

| Attempt | Starts about this long after the first | Wait if it fails |
|---|---|---|
| 1 | 0 s | 10 s |
| 2 | 10 s | 20 s |
| 3 | 30 s | 40 s |
| 4 | 70 s | 80 s |
| 5 | 2.5 min | 160 s |
| 6 | 5 min | 320 s |
| 7 | 10.5 min | 640 s |
| 8 | 21 min | none; the delivery becomes `failed` |

Each attempt can also start up to `POLL_INTERVAL_SECONDS` late, because an idle worker looks for
due deliveries once per interval. That is why the example in
[Tracking deliveries](#4-tracking-deliveries) retried after 11 seconds, not 10.

### 6.4 When a worker crashes or freezes

1. Worker A claims a delivery and sends it, then dies or freezes before saving a result. The
   delivery stays `processing`, with attempt 1 open.
2. After `CLAIM_TIMEOUT_SECONDS`, the delivery's `next_attempt_at` has passed, so it is due again.
3. Worker B claims it. It closes attempt 1 as `retryable_failure` with `claim_expired`, opens
   attempt 2 with a new claim token, and sends it again with the same `webhook-id`.
4. If worker A wakes up and tries to save its result, its claim token no longer matches, so nothing
   changes. Had it woken up before worker B took over, its result would have been saved: expiry
   only makes the delivery available, and the takeover is what revokes A's claim.

The receiver may get the event twice, because worker A might have reached it. The claim token
protects the database, not the receiver: it cannot recall a request A already sent. That is why
receivers skip duplicates. If attempts have run out when the claim expires, the delivery becomes
`failed` instead of being claimed again.

A clean stop (Ctrl+C or `SIGTERM`) avoids all of this: the worker finishes the current delivery
before exiting.

### 6.5 Several workers

Every worker runs the same claim query. `SKIP LOCKED` makes each one pass over rows that another
worker is claiming at that moment, so they share the queue without waiting for each other and
never claim the same delivery. In a live run, two workers split 20 events 9 and 11. The test
`test_concurrent_workers_never_claim_the_same_delivery` runs four workers on 30 deliveries and
checks that none was claimed twice.

### 6.6 One event from start to finish

This is the delivery from [Tracking deliveries](#4-tracking-deliveries): the receiver answers
`500` once, then `200`. Each row shows the state after that step.

| Step | Time (UTC) | What happens | `deliveries` row | `delivery_attempts` |
|---|---|---|---|---|
| 1 | 23:28:50.230 | The producer submits with a key; `202`. | `pending`, attempt_count 0, due now | none |
| 2 | 23:28:50.380 | A worker claims it. | `processing`, attempt_count 1, claim token A1, claim expires in 60 s | A1 open |
| 3 | | The receiver answers `500`. | unchanged | unchanged |
| 4 | 23:28:50.397 | The worker records the result. | `pending`, no claim token, due in about 10 s, last status 500 | A1 `retryable_failure`, 500, `http_error` |
| 5 | 23:29:01.487 | A worker claims it again. | `processing`, attempt_count 2, claim token A2, claim expires in 60 s | A2 open |
| 6 | 23:29:01.501 | The receiver answers `200`, and the worker records it. | `delivered`, no claim token, last status 200 | A2 `delivered`, 200 |

The receiver got two requests with the same `webhook-id`, and processed only the second, because
the first answered `500` without processing. The whole delivery took 11.3 seconds, almost all of
it the retry wait.

## 7. Configuration reference

Settings are read from environment variables, or from a `.env` file in the working directory.
Environment variables win. Copy `.env.example` to start.

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks` | API, worker, migrations, `webhook-admin` | PostgreSQL connection. |
| `MAX_EVENT_BYTES` | `65536` | API | The largest request body the API reads, in bytes. A larger one gets `413`. |
| `WEBHOOK_SECRETS` | none | Worker, test receiver | Secrets to sign webhooks with, separated by spaces. Each is `whsec_` and 24 to 64 random bytes in base64; `uv run webhook-admin new-webhook-secret` makes one. The worker does not start without one. Two only during a [rotation](#rotating-the-webhook-secret). |
| `RECEIVER_URL` | `http://127.0.0.1:9000/webhooks` | Worker | The only destination. Deliveries waiting for a retry go to the value a worker has when it sends them, so changing it moves them to the new receiver. Give every worker the same value. |
| `DELIVERY_TIMEOUT_SECONDS` | `5` | Worker | How long each network step of an attempt may take: connecting, sending, and waiting for the status code. It does not limit the attempt as a whole. |
| `MAX_ATTEMPTS` | `8` | Worker | Attempts per delivery before it is marked `failed`. |
| `RETRY_BASE_DELAY_SECONDS` | `10` | Worker | Wait after the first failed attempt; it doubles after each one. |
| `RETRY_MAX_DELAY_SECONDS` | `3600` | Worker | Longest wait between attempts, jitter included. |
| `CLAIM_TIMEOUT_SECONDS` | `60` | Worker | How long a claim lasts. Must be at least 3 × `DELIVERY_TIMEOUT_SECONDS`, or the worker refuses to start. |
| `POLL_INTERVAL_SECONDS` | `1` | Worker | How often an idle worker looks for due deliveries. |
| `TEST_DATABASE_URL` | `postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_test` | Tests | Database for integration tests. It is created and migrated automatically. The tests delete all its data, so its name must end in `_test`, and it must not be the database `DATABASE_URL` names. |

An invalid setting stops the API and the worker at startup. The message names the setting and the
problem but never repeats the value, because some values are secrets: `WEBHOOK_SECRETS`, and the
password in `DATABASE_URL`.

Some guidance:

- **Total retry window:** with base *b*, *n* attempts and no cap reached, the last attempt starts
  about *b* × (2^(n−1) − 1) seconds after the first. That is 1,270 s, about 21 minutes, with the
  defaults. Receivers must remember `webhook-id`s at least that long.
- **Claim timeout:** too short, and slow but healthy workers lose their claims, causing extra
  duplicates. Too long, and deliveries from crashed workers wait longer to be recovered. The
  3 × rule covers a receiver that is slow at every step; a receiver that sends its status line a
  few bytes at a time can still outlast any claim, which causes a duplicate.
- **Delivery timeout:** raise it only if the receiver is legitimately slow, and raise
  `CLAIM_TIMEOUT_SECONDS` with it.

## 8. Operating the service

### Producers and API keys

`webhook-admin` manages producers and their keys. It works on the database directly, as the
migrations do, so it needs `DATABASE_URL` but not a running API.

```bash
uv run webhook-admin create-producer friendster   # a new producer and its first API key
uv run webhook-admin create-key carrental          # another key for an existing producer
uv run webhook-admin list                          # producers and their keys
uv run webhook-admin revoke-key wds_t408ROrX       # refuse a key from the next request on
```

`list` shows the start of each key, never a whole key:

```text
carrental
  wds_t408ROrX...  created 2026-11-25 20:41 UTC  active
friendster
  wds_q9Vj9Jw3...  created 2026-11-25 20:41 UTC  active
```

- **A key is shown once,** when it is created. The database keeps only its SHA-256 hash and its
  first 12 characters, so nobody can read a key back, not even from a copy of the database. A lost
  key is replaced, not recovered.
- **To replace a key without downtime,** create a new one, give it to the producer, and revoke the
  old one once no instance of the producer uses it any more. Both keys work in between.
- **If a key leaks,** revoke it at once, then create a new one. Deliveries submitted with it stay
  stored and are delivered.
- **Names** are lower-case letters, digits, `-` and `_`, such as `carrental`. Receivers see the
  name in every webhook, so choose it once.
- **Keys start with `wds_`,** so a leaked key is easy to spot in code or logs, by people and by
  secret scanners.
- **Deliveries stored before milestone 5.1** belong to a producer named `legacy`, which the
  migration created. It has no keys. To read those deliveries through the API, run
  `uv run webhook-admin create-key legacy`.

### Rotating the webhook secret

Replace the secret now and then, and at once if it may have leaked: anyone who has it can sign
webhooks that the receiver will trust. Workers sign every webhook with every secret in
`WEBHOOK_SECRETS`, so for a while they sign with both the old and the new one:

1. Create a new secret: `uv run webhook-admin new-webhook-secret`.
2. Set `WEBHOOK_SECRETS="<old secret> <new secret>"` for the workers, and restart them one at a
   time. Each finishes its current delivery before it stops. Webhooks now carry two signatures.
3. Give the receiver the new secret. Whichever of the two secrets it has, one signature matches.
4. Set `WEBHOOK_SECRETS="<new secret>"` for the workers, and restart them again.

No webhook fails at any step. If the receiver switches to the new secret before every worker signs
with it, webhooks get `401` and fail at once. Give the workers both secrets, then
[replay](#replaying-a-failed-delivery) the failed deliveries. The test receiver reads
`WEBHOOK_SECRETS` too, so locally both sides change together.

### Checking health with SQL

Connect with `docker compose exec db psql -U webhooks`.

```sql
-- How many deliveries are in each state?
SELECT status, count(*) FROM deliveries GROUP BY status ORDER BY status;

-- The same, per producer.
SELECT p.name, d.status, count(*)
FROM deliveries d JOIN producers p ON p.id = d.producer_id
GROUP BY 1, 2 ORDER BY 1, 2;

-- How far behind are the workers? The age of the oldest delivery that is due but not sent.
SELECT now() - min(next_attempt_at) AS oldest_due_waiting
FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now();

-- What is being sent right now, and when do those claims expire?
SELECT id, attempt_count, next_attempt_at AS claim_expires_at
FROM deliveries WHERE status = 'processing' ORDER BY next_attempt_at;

-- What failed in the last day, and why?
SELECT id, event_type, attempt_count, last_http_status, last_error_category, updated_at
FROM deliveries WHERE status = 'failed' AND updated_at > now() - interval '1 day'
ORDER BY updated_at DESC;

-- Which deliveries may have reached the receiver more than once?
SELECT DISTINCT delivery_id FROM delivery_attempts
WHERE error_category IN ('timeout', 'claim_expired');

-- The full history of one delivery.
SELECT number, started_at, finished_at, outcome, http_status, error_category, duration_ms
FROM delivery_attempts WHERE delivery_id = '<delivery id>' ORDER BY number;

-- Attempt outcomes in the last hour, to see the retry rate.
SELECT coalesce(outcome, 'in progress') AS outcome, count(*)
FROM delivery_attempts WHERE started_at > now() - interval '1 hour'
GROUP BY 1 ORDER BY 1;
```

The most useful single number is `oldest_due_waiting`. If it keeps growing, workers are missing,
stuck or too few.

The duplicate query lists deliveries where the service could not tell whether an attempt reached
the receiver. A connection dropped after the request was sent is recorded as `connection_error`,
so in rare cases it is another source of duplicates.

### Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `POST` returns `503` | PostgreSQL is unreachable. | Run `docker compose ps` and start the database. Producers retry by themselves, with the same key. |
| `POST` returns `503` after about 30 seconds while the database is up | Every pooled database connection is busy. | The API or the database is overloaded. Producers retry; add capacity if it persists. |
| Every request gets `401` | The API key is missing, mistyped or revoked, or was created in another database. | Run `uv run webhook-admin list` with the API's `DATABASE_URL`, and create a new key if needed. |
| `POST` returns `413` | The event is larger than `MAX_EVENT_BYTES`. | Send less data, such as IDs, or raise the limit. |
| Deliveries stay `pending`, and `oldest_due_waiting` grows | No worker is running, or workers cannot reach the database. | Start workers and read their logs. |
| Many `connection_error` attempts | The receiver is down, or `RECEIVER_URL` is wrong. | Fix the receiver. Deliveries retry by themselves until `MAX_ATTEMPTS`. |
| Many `timeout` attempts | The receiver is slow. | Make it answer before doing slow work. Raise the timeouts only if that is not possible. |
| `permanent_failure` with a 4xx | The receiver rejects the event. | Fix the receiver or the producer, then replay. |
| `permanent_failure` with `401` | The receiver refused the signature: it has none of the secrets the workers sign with. | Give both sides the same secret, as in [Rotating the webhook secret](#rotating-the-webhook-secret), then replay. |
| A delivery is `processing` for a long time | A worker is sending it, or crashed while doing so. | Wait `CLAIM_TIMEOUT_SECONDS`; another worker takes over by itself. |
| `claim_expired` in a history | A worker crashed or froze mid-attempt. | Check that worker's health. The receiver may have a duplicate. |
| A worker logs `unexpected error` with a traceback | A bug, or a row edited by hand. | Read the traceback. The worker carries on, and the delivery it held is sent again when its claim expires. |
| `409` on submit | A producer reused a key for different content. | Fix how the producer builds keys. |
| `422` on submit | The request is invalid. | Read `detail` and fix the producer. |
| The worker or the API exits at start with a validation error; the API also logs `Application startup failed` | A setting is invalid, for example `CLAIM_TIMEOUT_SECONDS` is too small or a secret is malformed. The message names the setting, not its value. | Fix the setting. |
| The worker exits at start: `WEBHOOK_SECRETS is not set` | No webhook secret is configured. | See [First-time setup](#first-time-setup). |

### Replaying a failed delivery

There is no replay endpoint yet. After fixing the cause, put a failed delivery back in the queue
with SQL:

```sql
UPDATE deliveries SET status = 'pending', next_attempt_at = now(), updated_at = now()
WHERE id = '<delivery id>' AND status = 'failed';
```

The next free worker sends it with the same `webhook-id`, and the history keeps the earlier
attempts. `attempt_count` is not reset, so if it is already at `MAX_ATTEMPTS`, the delivery gets
exactly one more attempt.

Do not edit other columns by hand:

- **Lowering `attempt_count`** makes the next claim reuse an attempt number. The database rejects
  that, so every claim of that delivery fails. Workers log an `unexpected error` each time and
  carry on, but the delivery stays first in line, so a single worker sends nothing else until the
  row is fixed.
- **Setting `processing`** without a claim token is rejected by the database.

### Data retention

Nothing is deleted. Over time, `delivered` rows and their attempts accumulate. They are left out of
the worker's index, so they do not slow down delivery. A cleanup job that deletes old finished
deliveries (their attempts are deleted with them) is future work.

## 9. Limitations

- One receiver, with one set of webhook secrets. Producers cannot choose destinations, by design.
- No rate limit per producer: a producer with a valid key can submit as fast as the database
  accepts.
- API keys do not expire, and the service does not record when a key was last used.
- Idempotency keys never expire.
- No metrics or structured logs yet; use the SQL queries above.
- No replay endpoint and no cleanup of old rows.
- No deadline for a whole attempt: a receiver that sends its status line a few bytes at a time can
  outlast the claim, which causes a duplicate.
- No ordering guarantee.
- HTTPS is left to the deployment: a reverse proxy in front of the API, and an `https://`
  `RECEIVER_URL`.

Several of these are planned for the rest of milestone 5; see
[ARCHITECTURE.md](../ARCHITECTURE.md#limitations-and-future-work).

## 10. Glossary

| Term | Meaning |
|---|---|
| API key | A producer's credential, sent as a bearer token. The service stores only its SHA-256 hash. |
| At least once | Every event is delivered, possibly more than once, as long as the receiver accepts it within the retry window. The opposite trade-off, at most once, risks losing events. |
| Backoff | Waiting longer after each failure. |
| Bearer token | A credential sent as `Authorization: Bearer <token>`. Whoever holds it is let in, so it must stay secret. |
| Claim | A worker's temporary, expiring ownership of a delivery. |
| Claim token | The ID of the attempt holding the claim. Only the holder may save a result. In general this is called a fencing token. It guards the database, not the receiver. |
| Claim timeout | How long a claim lasts. After that, another worker may take the delivery over; until one does, the original worker's result is still accepted. Message queues call this a visibility timeout. |
| Constant-time comparison | Comparing two values in a time that does not depend on where they differ, so response times do not tell an attacker how close a guess is. |
| Fingerprint | A hash of a request's content, used to detect a key reused with different content. |
| HMAC | A hash computed with a secret key. Only a holder of the key can compute it, so a matching HMAC proves who sent the content and that it did not change. |
| Idempotency key | A producer-chosen name for a request, so that repeating it has no extra effect. |
| Jitter | Randomness added to retry waits so that many retries do not happen at the same moment. |
| Outbox | A table where a producer saves events in the same transaction as the change that caused them, for a relay to send later. |
| Poison delivery | A delivery that fails or crashes every attempt. Attempt limits stop it. |
| Replay attack | Resending a captured, genuine request. A signature alone does not stop it; the age check and the `webhook-id` check do. |
| Secret rotation | Replacing a secret without downtime, by signing with, or accepting, both the old and the new one for a while. |
| `SKIP LOCKED` | PostgreSQL's option to pass over rows another transaction has locked, instead of waiting. |
| `webhook-id` | The delivery ID, sent with every attempt so that receivers can detect duplicates. |
| Webhook secret | The `whsec_` secret that workers sign webhooks with and the receiver checks them with. |
