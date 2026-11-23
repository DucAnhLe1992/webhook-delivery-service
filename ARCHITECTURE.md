# Architecture

This document explains how the webhook delivery service is built and why. To run and use it, see
the [user manual](docs/USER_MANUAL.md). To see how the design grew one guarantee at a time, see
[how it was built](docs/MILESTONES.md).

## Contents

- [Goals and non-goals](#goals-and-non-goals)
- [System overview](#system-overview)
- [Components](#components)
- [Code structure](#code-structure)
- [Data model](#data-model)
- [Delivery state machine](#delivery-state-machine)
- [Flows](#flows)
- [Concurrency control](#concurrency-control)
- [Retry policy](#retry-policy)
- [Guarantees and failure modes](#guarantees-and-failure-modes)
- [Design decisions](#design-decisions)
- [Testing strategy](#testing-strategy)
- [Limitations and future work](#limitations-and-future-work)

## Goals and non-goals

**Goals**

- Accept events from producer applications and deliver them to a receiver over HTTP.
- State every guarantee explicitly and test it: what is stored, what can repeat, and what happens
  when a process dies at any moment.
- Keep the moving parts few: one codebase, one PostgreSQL database, two kinds of process.

**Non-goals for now**

- Destinations chosen by producers (a server-side request forgery risk), multiple tenants,
  ordering guarantees and a dashboard.
- Message brokers such as Kafka, RabbitMQ or Redis, and Kubernetes. PostgreSQL is enough at this
  scale, and one system is easier to reason about.

## System overview

```mermaid
flowchart LR
    P[Producer app] -->|"POST /deliveries"| API[API process<br/>FastAPI]
    P -->|"GET /deliveries/:id"| API
    API -->|"INSERT and COMMIT"| DB[(PostgreSQL)]
    W[Worker processes<br/>one or more] -->|"claim and record"| DB
    W -->|"POST /webhooks"| R[Receiver]
```

The API and the workers never call each other. The database is their only connection: the API
writes deliveries, and the workers claim, send and update them. That is why each side can stop,
restart or scale on its own. Both depend on PostgreSQL, which is the one shared point of failure.

## Components

| Component | Owns | Does not own |
|---|---|---|
| API: `main.py`, `api/deliveries.py` | Request validation, idempotent submission, reading deliveries, mapping errors to `409`, `422` and `503` | Sending webhooks |
| Service layer: `services/delivery_service.py` | Every transaction: create, read, claim and record | HTTP on either side |
| Worker: `worker/main.py` | The loop (claim, send, record, wait), logging, clean stop on `SIGINT`/`SIGTERM`, surviving database outages | Deciding what an attempt means: `sender.py` classifies it, `retry.py` sets the delay, and the service layer applies the attempt limit |
| Sender: `sender.py` | One HTTP attempt and its classification | Database state |
| Retry policy: `retry.py` | The delay before the next attempt | Whether to retry at all, which depends on the outcome and the attempt limit |
| PostgreSQL | Durable state, the queue, uniqueness, row locks, invariants enforced by constraints | Talking to receivers |
| Receiver (external) | Processing events and skipping duplicates by `webhook-id` | Retrying, which the service does |

## Code structure

```mermaid
flowchart TD
    API[api/deliveries.py] --> SVC[services/delivery_service.py]
    WRK[worker/main.py] --> SVC
    WRK --> SND[sender.py]
    WRK --> RTY[retry.py]
    SVC --> MOD[models.py]
    MOD --> DB[(PostgreSQL)]
    SND --> RCV[Receiver]
```

Three rules keep the layers honest:

1. **The service layer owns the transactions.** HTTP handlers and the worker loop contain no SQL
   and never commit; each service function commits its own work before it returns.
2. **No network call happens inside a database transaction.** The worker commits its claim before
   sending and opens a new transaction to record the result, so a slow receiver can never hold
   database locks.
3. **Code is synchronous.** SQLAlchemy and HTTPX2 are used in their blocking forms, and the API
   endpoints are plain `def` functions, which FastAPI runs in a thread pool. Throughput comes from
   running more workers, not from concurrency inside one.

## Data model

```mermaid
erDiagram
    deliveries ||--o{ delivery_attempts : has
    deliveries {
        uuid id PK
        varchar event_type
        jsonb payload
        varchar status
        int attempt_count
        timestamptz next_attempt_at
        uuid claim_token
        varchar idempotency_key UK
        varchar request_fingerprint
        int last_http_status
        varchar last_error_category
        timestamptz created_at
        timestamptz updated_at
    }
    delivery_attempts {
        uuid id PK
        uuid delivery_id FK
        int number
        timestamptz started_at
        timestamptz finished_at
        varchar outcome
        int http_status
        varchar error_category
        int duration_ms
    }
```

### `deliveries`

One row per accepted event. The row is both the delivery's record and its queue entry, so the two
can never disagree.

| Column | Meaning |
|---|---|
| `id` | Delivery ID. Sent as `webhook-id` on every attempt. |
| `event_type` | For example `booking.confirmed`. |
| `payload` | The producer's `data` object, stored as JSONB. Key order and whitespace are not kept. |
| `status` | `pending`, `processing`, `delivered` or `failed`. |
| `attempt_count` | Attempts claimed so far. |
| `next_attempt_at` | While `pending`: when the delivery is due. While `processing`: when the current claim expires. Not meaningful once `delivered` or `failed`. |
| `claim_token` | ID of the attempt that holds the claim. Set only while `processing`. |
| `idempotency_key` | The producer's `Idempotency-Key`, if it sent one. Unique. |
| `request_fingerprint` | SHA-256 of the request's canonical JSON, to detect a key reused with different content. |
| `last_http_status`, `last_error_category` | Result of the latest attempt whose worker saved it. An attempt closed as `claim_expired` by another worker does not change them. |
| `created_at` | When the delivery was accepted. Sent as the body's `timestamp`. |
| `updated_at` | When the row last changed. |

### `delivery_attempts`

One row per attempt, created when a worker claims the delivery and finished when the result is
saved.

| Column | Meaning |
|---|---|
| `id` | Attempt ID. While the attempt runs, this is also the delivery's `claim_token`. |
| `delivery_id` | The delivery. Deleting a delivery deletes its attempts. |
| `number` | 1, 2, 3 and so on, unique per delivery. |
| `started_at` | When the delivery was claimed for this attempt. |
| `finished_at` | When the result was saved, or when another worker found the claim expired. `NULL` while running. |
| `outcome` | `delivered`, `retryable_failure` or `permanent_failure`. `NULL` while running. |
| `http_status` | The receiver's status code, if it answered. |
| `error_category` | `timeout`, `connection_error`, `http_error` or `claim_expired`. |
| `duration_ms` | Time spent on the HTTP request. |

### Constraints and indexes

The database enforces the invariants, so neither a bug nor a manual edit can break them.

| Name | Rule | Why |
|---|---|---|
| `ck_deliveries_status_valid` | `status` is one of the four states | The state machine cannot be corrupted. |
| `ck_deliveries_claimed_while_processing` | `(status = 'processing') = (claim_token IS NOT NULL)` | Every in-flight delivery has exactly one owner, and no other row has one. |
| `ck_deliveries_attempt_count_not_negative` | `attempt_count >= 0` | Sanity. |
| `uq_deliveries_idempotency_key` | `idempotency_key` is unique; `NULL`s are allowed | One delivery per key. The basis of `ON CONFLICT`. |
| `ck_delivery_attempts_outcome_valid` | `outcome` is `NULL` or one of three values | Valid history. |
| `ck_delivery_attempts_finished_with_outcome` | `(finished_at IS NULL) = (outcome IS NULL)` | An attempt is either running, or finished with an outcome. |
| `uq_delivery_attempts_delivery_id` | `(delivery_id, number)` is unique | No two attempts share a number. Also indexes lookups by delivery. |
| `ix_deliveries_due` | Index on `next_attempt_at` `WHERE status IN ('pending', 'processing')` | Serves the claim query. Finished deliveries, which become most rows over time, are left out. |

Migrations live in `alembic/versions`:

| Migration | Change |
|---|---|
| `0001` | `deliveries` table with status and attempt-count checks. |
| `0002` | Last-attempt columns and an index for due `pending` rows. |
| `0003` | `delivery_attempts`, claim tokens, idempotency columns and the claim constraint. It also moves rows left in `processing` by the old, non-expiring claims back to `pending` before adding the constraint. |

CI runs `alembic upgrade head` and `alembic check`, so a model change without a migration fails the
build.

## Delivery state machine

```mermaid
stateDiagram-v2
    [*] --> pending: POST /deliveries
    pending --> processing: claimed by a worker
    processing --> delivered: receiver answered 2xx
    processing --> pending: retryable failure, attempts left
    processing --> failed: permanent failure
    processing --> failed: retryable failure, no attempts left
    processing --> processing: claim expired, another worker takes over
    processing --> failed: claim expired, no attempts left
    delivered --> [*]
    failed --> [*]
```

| Trigger | From | To | Guard | Writes |
|---|---|---|---|---|
| `POST /deliveries` | none | `pending` | Valid request; the key is unused or was used with the same content | Insert the delivery. |
| Claim | `pending` | `processing` | `next_attempt_at <= now()` and the row is not locked | `attempt_count + 1`, new attempt row, `claim_token` = attempt ID, `next_attempt_at = now() + CLAIM_TIMEOUT_SECONDS`. |
| Claim (takeover) | `processing` | `processing` | The claim expired and attempts are left | Close the old attempt as `claim_expired`, then as above. |
| Claim (exhausted) | `processing` | `failed` | The claim expired and `attempt_count >= MAX_ATTEMPTS` | Close the old attempt as `claim_expired`; clear `claim_token`. |
| Record a 2xx | `processing` | `delivered` | `claim_token` matches | Finish the attempt; clear `claim_token`. |
| Record a retryable failure | `processing` | `pending` | `claim_token` matches and `attempt_count < MAX_ATTEMPTS` | Finish the attempt; `next_attempt_at = now() + backoff`. |
| Record the last retryable failure | `processing` | `failed` | `claim_token` matches and `attempt_count >= MAX_ATTEMPTS` | Finish the attempt. |
| Record a permanent failure | `processing` | `failed` | `claim_token` matches | Finish the attempt. |
| Late result from a stale worker | any | unchanged | `claim_token` does not match | Nothing. |

## Flows

### Submitting an event

```mermaid
sequenceDiagram
    participant P as Producer
    participant A as API
    participant D as PostgreSQL
    P->>A: POST /deliveries with Idempotency-Key
    A->>A: validate the body and compute the fingerprint
    A->>D: INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING
    A->>D: COMMIT
    alt a row came back, so the key is new
        A-->>P: 202 with delivery_id and status pending
    else no row came back, so the key was used before
        A->>D: SELECT the original delivery by key
        alt same fingerprint
            A-->>P: 202 with the original delivery and Idempotent-Replayed true
        else different fingerprint
            A-->>P: 409 Conflict
        end
    end
    Note over A,D: If PostgreSQL is unreachable the API answers 503 with Retry-After, never 202
```

Without an `Idempotency-Key`, the API does a plain `INSERT` and `COMMIT`. In every case the commit
happens before the response is sent, so `202` always means "stored".

### Delivering

```mermaid
sequenceDiagram
    participant W as Worker
    participant D as PostgreSQL
    participant R as Receiver
    W->>D: SELECT the oldest due delivery FOR UPDATE SKIP LOCKED
    W->>D: mark it processing and open attempt N as the claim token
    W->>D: COMMIT
    Note over W,D: no transaction is open from here until the result is saved
    W->>R: POST /webhooks with webhook-id and webhook-timestamp
    R-->>W: status code, or a timeout or connection error
    W->>D: SELECT the delivery WHERE claim_token matches FOR UPDATE
    alt the claim is still held
        W->>D: finish attempt N and set delivered, pending or failed
        W->>D: COMMIT
    else another worker took over
        W->>D: ROLLBACK and change nothing
    end
```

### Crash takeover and fencing

```mermaid
sequenceDiagram
    participant A as Worker A
    participant B as Worker B
    participant D as PostgreSQL
    participant R as Receiver
    A->>D: claim the delivery as attempt 1
    A->>R: POST /webhooks
    Note over A: crashes or freezes before saving a result
    Note over D: CLAIM_TIMEOUT_SECONDS pass and the delivery is due again
    B->>D: claim it, close attempt 1 as claim_expired, open attempt 2
    B->>R: POST /webhooks with the same webhook-id
    R-->>B: 200, the receiver skips or processes the duplicate
    B->>D: save delivered, the claim token matches
    A->>D: try to save a late result for attempt 1
    D-->>A: no row has that claim token, nothing changes
```

## Concurrency control

The statements below are what SQLAlchemy sends, shortened for reading.

**Claim**, one short transaction:

```sql
SELECT * FROM deliveries
WHERE status IN ('pending', 'processing') AND next_attempt_at <= now()
ORDER BY next_attempt_at
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- Only if the row was 'processing': its claim expired, so close the lost attempt.
UPDATE delivery_attempts
SET finished_at = now(), outcome = 'retryable_failure', error_category = 'claim_expired'
WHERE id = :old_claim_token;

UPDATE deliveries
SET status = 'processing', attempt_count = :n, claim_token = :attempt_id,
    next_attempt_at = now() + :claim_timeout, updated_at = now()
WHERE id = :id;

INSERT INTO delivery_attempts (id, delivery_id, number) VALUES (:attempt_id, :id, :n);
COMMIT;
```

**Record**, one short transaction, fenced by the claim token:

```sql
SELECT * FROM deliveries WHERE id = :id AND claim_token = :attempt_id FOR UPDATE;
-- No row: this attempt lost its claim. Roll back and change nothing.

UPDATE delivery_attempts
SET finished_at = now(), outcome = :outcome, http_status = :status,
    error_category = :category, duration_ms = :ms
WHERE id = :attempt_id;

UPDATE deliveries
SET status = :new_status, claim_token = NULL,
    next_attempt_at = now() + :retry_delay,  -- only when retrying
    last_http_status = :status, last_error_category = :category, updated_at = now()
WHERE id = :id;
COMMIT;
```

**Idempotent submission**:

```sql
INSERT INTO deliveries (id, event_type, payload, idempotency_key, request_fingerprint)
VALUES (...)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING *;
-- No row returned: select the existing delivery by key and compare fingerprints.
```

| Operation | Transaction | Locks | Why it is safe |
|---|---|---|---|
| Claim | Short, no network call | One row, `FOR UPDATE SKIP LOCKED` | Workers never wait for, or take, a row another worker is claiming. An expired claim is simply a due row. |
| Send | None | None | A slow or hanging receiver cannot hold database locks. |
| Record | Short | The delivery row, `FOR UPDATE` | The token check and the write are atomic. A stale worker's token no longer matches. |
| Submit with a key | Short | The unique-index entry | A concurrent insert of the same key waits for the first, then does nothing. |

Three details make the claim work:

- **`next_attempt_at` doubles as the claim expiry.** A claim moves it to
  `now() + CLAIM_TIMEOUT_SECONDS`. If no result is saved by then, the row is due again and the
  ordinary claim query picks it up. Message queues call this a visibility timeout. One index and
  one query serve both new work and abandoned work.
- **The claim token is the attempt's ID.** No extra state is needed, and the history records
  exactly which claim did what.
- **Attempts are counted when claimed,** not when their result is saved. A delivery that crashes
  every worker that sends it still reaches `MAX_ATTEMPTS` and fails.

## Retry policy

Each attempt ends in one of three outcomes:

| Receiver result | Outcome |
|---|---|
| 2xx | `delivered` |
| 5xx, 408 or 429; connection refused or dropped; no answer within `DELIVERY_TIMEOUT_SECONDS` | `retryable_failure` |
| Other 4xx, or a 3xx redirect (redirects are never followed) | `permanent_failure` |

After a retryable failure, the wait before the next attempt is:

```text
wait = min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS × 2^(attempt − 1)) × random(0.8, 1.2)
```

The wait doubles so that brief blips are retried quickly while long outages are not hammered. The
random factor spreads out deliveries that failed together, so a receiver coming back from an outage
is not hit by all of them at the same moment. The limit turns a hopeless delivery into a visible
`failed` one, and tells receivers how long they must remember `webhook-id`s.

With the defaults (10-second base, 1-hour cap, 8 attempts), give or take 20%:

| Attempt | Starts after the first | Wait if it fails |
|---|---|---|
| 1 | 0 s | 10 s |
| 2 | about 10 s | 20 s |
| 3 | about 30 s | 40 s |
| 4 | about 70 s | 80 s |
| 5 | about 2.5 min | 160 s |
| 6 | about 5 min | 320 s |
| 7 | about 10.5 min | 640 s |
| 8 | about 21 min | none; the delivery becomes `failed` |

## Guarantees and failure modes

**Guarantees**

1. **Durable acceptance.** `202` is sent only after the delivery is committed. If the database is
   unreachable, the answer is `503` with `Retry-After`, never `202`.
2. **At-least-once delivery.** Every accepted delivery is attempted until it is delivered, fails
   permanently or runs out of attempts. Every attempt carries the same `webhook-id`, so receivers
   can drop duplicates.
3. **Idempotent submission.** Repeating a request with the same `Idempotency-Key` returns the
   original delivery, even when the repeats arrive at the same moment.
4. **Crash recovery.** A delivery claimed by a worker that dies is retried after
   `CLAIM_TIMEOUT_SECONDS`. A worker that wakes up after losing its claim cannot change anything.
5. **Bounded retries.** At most `MAX_ATTEMPTS` attempts, with growing, randomized delays.
6. **No ordering guarantee.** A retried event can arrive after a newer one.

| What fails | What happens | Result |
|---|---|---|
| API process crashes | Accepted deliveries are already committed, and workers keep sending | A producer whose request was cut off retries; its `Idempotency-Key` prevents a second copy. |
| PostgreSQL unreachable | The API answers `503` with `Retry-After`; workers log "database unavailable" and poll again | Nothing is accepted or sent until it returns. No restart is needed, because pooled connections are checked before use. |
| Receiver down, 5xx, 408, 429 or timeout | Retried with growing delays, up to `MAX_ATTEMPTS` | Then `failed`, which is visible. |
| Receiver answers another 4xx or a 3xx | `failed` immediately | Visible as `permanent_failure`. |
| Receiver processed the event but its answer was lost | Retried with the same `webhook-id` | A duplicate, which the receiver must skip. |
| Worker crashes before sending | The claim expires and another worker sends it | Delayed by up to `CLAIM_TIMEOUT_SECONDS`. |
| Worker crashes after the receiver processed it | The claim expires and it is sent again | A duplicate, recorded as `claim_expired`. |
| Worker freezes past its claim, then wakes up | Its late result is discarded | No overwrite. |
| Worker stopped with Ctrl+C or `SIGTERM` | It finishes the current delivery first | No claim is left behind. |
| Two workers reach the same delivery at once | `SKIP LOCKED`: one claims it, the other moves on | The race itself never sends twice. |
| The producer's `202` is lost | The producer retries with the same key | It gets the original delivery back, marked `Idempotent-Replayed`. |
| The same key is used with different content | `409 Conflict` | A producer bug is surfaced instead of hidden. |
| A delivery crashes every worker that sends it | Attempts are counted when claimed | It fails after `MAX_ATTEMPTS`. |

## Design decisions

| Decision | Alternative | Why |
|---|---|---|
| A PostgreSQL table as the queue | Redis, RabbitMQ or Kafka | One system to run and back up. The delivery record and its queue entry are the same row, so they cannot disagree. `SKIP LOCKED` makes it a safe work queue at this scale. |
| A separate worker process | FastAPI `BackgroundTasks` | Background tasks live inside the API process: they vanish when it restarts and are never retried. |
| Store first, answer `202` | Send during the request | Accepting events no longer depends on the receiver, and `202` means "stored". |
| `SELECT … FOR UPDATE SKIP LOCKED` to claim | Advisory locks, or a status check alone | Concurrent workers skip each other's rows instead of waiting. A status check alone let two workers claim the same row. |
| `next_attempt_at` as the claim expiry | A separate `claimed_until` column | One index and one query serve both due and abandoned rows. |
| The attempt ID as the claim token | A version counter or a separate token | The history row identifies the claim holder. No extra state. |
| Count attempts at claim time | Count when the result is saved | A delivery that crashes workers still runs out of attempts. |
| A write fenced by `claim_token` | A status check | After a takeover the status is `processing` again; only a token tells the claims apart. |
| `INSERT … ON CONFLICT DO NOTHING` | Check, then insert; or catch the unique-key error | Atomic under concurrency, with no exception handling. |
| A fingerprint of canonical JSON | Comparing raw bodies | Key order and whitespace do not cause false conflicts. |
| Exponential backoff with ±20% jitter, a cap and a limit | A fixed delay forever | Spares struggling receivers, spreads out synchronized retries, and ends in a visible failure. |
| Standard Webhooks headers and body | A custom format | Receivers can use existing libraries, and signing (milestone 5) follows the same spec. |
| One configured destination | Destinations chosen by producers | Avoids server-side request forgery. Arbitrary destinations need URL validation, redirect rules and per-destination credentials. |
| Synchronous code | `async` throughout | Simpler to reason about and test. Throughput comes from running more workers. |
| HTTPX2 | HTTPX | The maintained continuation of HTTPX, by Pydantic, with the same API. |
| Alembic migrations checked in CI | `create_all` | A reviewable schema history, data migrations, and drift detection. |

## Testing strategy

- **Unit tests** need no database: attempt classification, the webhook headers and body, retry
  delays, configuration checks, and the `503` path with an unreachable database.
- **Integration tests** run against real PostgreSQL, migrated with the real Alembic migrations,
  and emptied before each test.
- **Race tests are deterministic.** One connection holds a real row lock, and the test waits until
  `pg_stat_activity` reports the other blocked, or sets a `lock_timeout` so a wrong wait fails
  instead of hanging. They cover:
  - two workers claiming the same delivery;
  - `SKIP LOCKED` moving on to the next row;
  - two requests with the same idempotency key at the same moment.
- **A concurrency test** runs four worker threads against thirty deliveries and checks that each
  was sent exactly once.
- **Mutation checks** were done by hand during development: each safeguard was removed in turn
  (the commit before `202`, the `503` handler, the due-time filter, `SKIP LOCKED`, the claim
  token check, claim expiry, `ON CONFLICT`, the fingerprint check, both attempt limits), and each
  removal made a specific test fail.
- **CI** (GitHub Actions) runs Ruff, the format check, `alembic upgrade head`, `alembic check`
  and the whole test suite against a PostgreSQL service container.

## Limitations and future work

Planned for milestone 5:

- Producer authentication, with idempotency keys scoped per producer.
- Signed webhooks (`webhook-signature`, as in Standard Webhooks).
- Structured logs and metrics: the age of the oldest due delivery, the retry rate, and failures.
- An operator endpoint to replay failed deliveries.
- Container images, deployment instructions and a runbook.

Known gaps:

- Idempotency keys never expire, and old rows are never cleaned up.
- The size of `data` is not limited.
- `DELIVERY_TIMEOUT_SECONDS` applies to each phase of a request (connecting, sending, reading),
  not to the request as a whole.
- `/health` reports that the API process is up; it does not check the database.
