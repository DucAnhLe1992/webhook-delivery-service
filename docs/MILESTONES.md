# How it was built

The service was built in four milestones. Each one added a single guarantee, then ended with
three experiments that broke the system on purpose. Each experiment's result explains why the
next milestone was needed. This document records the steps, the questions and the answers.

For how the finished service works, see the [user manual](USER_MANUAL.md) and
[ARCHITECTURE.md](../ARCHITECTURE.md).

| Milestone | Git tag | What it added | Guarantee gained | What its experiments exposed |
|---|---|---|---|---|
| [1. Send it now](#milestone-1-send-it-now) | `milestone-1` | An endpoint that sends each webhook during the request | None yet: an event arrives only if the receiver is up at that moment | Events are lost, and a timeout means "unknown" |
| [2. Store before acknowledging](#milestone-2-store-before-acknowledging) | `milestone-2` | PostgreSQL, `202` after commit, `503` when storage is down | An accepted event is never lost | Nothing sends the stored events |
| [3. A separate worker](#milestone-3-a-separate-worker) | `milestone-3` | A worker process that claims, sends, records and retries | Stored events are sent and retried, even while the API is down | A crashed worker strands its delivery, retries never stop, and duplicates are possible |
| [4. Reliability](#milestone-4-reliability) | `milestone-4` | Expiring claims, claim tokens, attempt history, backoff and limits, `Idempotency-Key`, `SKIP LOCKED` | At-least-once delivery through crashes, bounded retries, safe producer retries, many workers | Nothing new; the remaining work is operations (milestone 5) |

## Milestone 1: send it now

**Goal.** Get the HTTP path working end to end: accept an event, send it as a webhook in the
[Standard Webhooks](https://www.standardwebhooks.com) format, and classify the result.

**What was built.**

- `POST /deliveries` validated the event, gave it a new random ID, and sent it to the receiver
  while the producer waited. It answered `200` if the receiver accepted it, `502` if the receiver
  failed or could not be reached, and `504` on a timeout.
- `sender.py`: one attempt, classified as delivered, retryable failure or permanent failure. It is
  still used today.
- `dev/receiver.py`, a test receiver with `ok`, `slow`, `error` and `reject` modes.
- Tests with a mock HTTP transport, Ruff and CI. HTTPX2, Pydantic's maintained continuation of
  HTTPX, replaced HTTPX because Starlette's test client prefers it.

**Experiments.**

1. **Receiver stopped.** Stop the receiver and send an event: `502` with `connection_error`.
   *Where is the event now?*

   Nowhere. It existed only in the API's memory during the request, and the API kept no copy.
   Only the producer can save it now, by storing and resending it itself, so the service adds
   nothing yet.

2. **Receiver slow.** Switch the receiver to `slow` and send: `504` after 5 seconds, yet the
   receiver processed the event. *If the producer sends it again, can the receiver tell it is a
   duplicate?*

   No. A timeout means "unknown", not "failed": the request arrived and was processed, and only
   the answer was missing. The resend gets a new random ID, so the receiver sees two different
   events and might, for example, confirm a booking twice. A retry is safe only if every copy
   carries the same ID.

3. **Receiver failing.** Compare `/mode/error` (500) with `/mode/reject` (400). *Why is one
   worth retrying and the other not?*

   A `500` means the receiver has a problem right now, such as a crash, a deploy or a database
   blip, and the same request may succeed later. A `400` means the request itself is wrong.
   Sending it again gets the same answer, so retrying only wastes work and hides the bug.
   Timeouts, connection errors, `408` and `429` are temporary too.

**Lesson.** Sending during the request ties accepting an event to the receiver being up. An event
needs a home that outlives the request, and an ID that outlives an attempt.

## Milestone 2: store before acknowledging

**Goal.** Never lose an accepted event.

**What was built.**

- PostgreSQL 18 in Docker Compose, SQLAlchemy models, and the Alembic migration `0001` for the
  `deliveries` table.
- `POST /deliveries` stores a `pending` row, commits, and only then answers `202 Accepted` with a
  `Location` header. It no longer sends anything.
- `GET /deliveries/{id}`.
- `503` with `Retry-After: 5` when the database is unreachable. Pooled connections are tested
  before use, so the API recovers from a database restart without being restarted.
- Integration tests against a real, migrated `webhooks_test` database, and CI with a PostgreSQL
  service, `alembic upgrade head` and `alembic check`.

**Experiments.**

1. **Restart the API.** Submit an event, restart the API, and `GET` the delivery: it is still
   there. *What does `202` promise that milestone 1's `200` could not?*

   `202` means "stored: the service is now responsible for this event". It survives restarts
   because it is committed. Milestone 1's `200` meant "delivered just now", and every other answer
   meant "this is still your problem". The promise moved from the receiver's state, which the
   service does not control, to the service's own storage, which it does.

2. **Stop the database.** Submit: `503`. *Why must the API never answer `202` here, and what
   should the producer do?*

   `202` would be a lie: the producer would drop its copy, and the event would exist nowhere.
   With `503`, the producer keeps the event and tries again after `Retry-After`. When the database
   returns, the API recovers by itself.

3. **Look inside.** Every row stays `pending`. *Who is going to send them?*

   Nobody yet. The API's job ends at the commit. Sending needs its own process, which is
   milestone 3.

**Lesson.** An acknowledgement is a promise. Make it only once the event is durable, and say "not
accepted" plainly when it is not.

## Milestone 3: a separate worker

**Goal.** Send stored deliveries, and retry failures, independently of the API.

**What was built.**

- `uv run webhook-worker`: a loop that claims the oldest due `pending` delivery, sends it with no
  transaction open, records the result, and sleeps `POLL_INTERVAL_SECONDS` when nothing is due.
- The claim was one atomic statement,
  `UPDATE … WHERE id = (the oldest due row) AND status = 'pending' RETURNING *`. Because
  PostgreSQL checks the status again after waiting for a row lock, two workers could not claim
  the same row.
- A fixed retry delay (`RETRY_DELAY_SECONDS`, 10 seconds), with no limit on attempts.
- Migration `0002`: columns for the last result, and a partial index for due `pending` rows.
- A clean stop on Ctrl+C and `SIGTERM`, and polling through database outages.
- A race test that holds a real row lock and waits until `pg_stat_activity` shows the second claim
  blocked, so it does not depend on timing.

**Experiments.**

1. **Stop the API.** With the receiver answering `500`, submit two events and stop the API. Then
   switch the receiver to `ok`: both are delivered while the API is down. *Why can the two
   processes run independently?*

   They never talk to each other. The API writes rows, the worker reads and updates them, and the
   database is the contract between them. Each can be stopped, restarted, deployed or scaled on
   its own. Their only shared dependency is PostgreSQL.

2. **Slow receiver.** The worker times out after 5 seconds and tries again 10 seconds later, and
   the receiver logs the same ID as `DUPLICATE`. *Why does a stable ID matter here, and why must
   the receiver still check it?*

   The delivery ID is created once, when the event is stored, and sent as `webhook-id` with every
   attempt. In milestone 1, a resend got a new ID. A stable ID makes duplicates recognizable, but
   the service cannot prevent them: after a timeout it cannot know whether the receiver processed
   the event, and not resending would risk losing it. So delivery is *at least once*, and
   dropping duplicates is the receiver's job.

3. **Kill the worker mid-send.** With `kill -9`, the delivery stays `processing`, even after the
   worker restarts. With Ctrl+C, it is finished and recorded. *Why do the two endings differ, and
   what would recover the stuck delivery?*

   Ctrl+C runs the worker's signal handler, which lets it finish the current delivery and save
   the result. `kill -9` cannot be caught: the process dies between claim and record, and the row
   says `processing` forever, because workers only claim `pending` rows. A restarted worker does
   not know what the dead one held. The fix is a claim that expires, called a lease, plus a token
   so that the old worker, if it was only frozen, cannot overwrite the new worker's result. That
   is milestone 4.

**Lesson.** Separating accepting from delivering separates their failures. But any claim a process
holds must survive that process dying.

## Milestone 4: reliability

**Goal.** Stay correct when any process dies at any moment, when producers repeat requests, and
when several workers run at once.

**What was built.**

- The `delivery_attempts` table, with one row per attempt, opened when the delivery is claimed.
- Claims that expire. `next_attempt_at` doubles as the claim's expiry, `CLAIM_TIMEOUT_SECONDS`
  after the claim, and the claim token is the attempt's ID. Saving a result is fenced by
  `WHERE claim_token = :mine`.
- `FOR UPDATE SKIP LOCKED` replaced the repeated status check, so workers skip rows that others
  are claiming instead of waiting for them.
- Attempts counted at claim time, `MAX_ATTEMPTS`, and exponential backoff with ±20% jitter,
  capped at `RETRY_MAX_DELAY_SECONDS`.
- `Idempotency-Key`, stored with `INSERT … ON CONFLICT DO NOTHING`, plus a SHA-256 fingerprint of
  the request. A replay is marked `Idempotent-Replayed: true`, and a reused key with different
  content gets `409`.
- Migration `0003`. It includes a data step that puts deliveries stranded in `processing` by
  milestone 3's never-expiring claims back in the queue.
- 39 tests, including deterministic race tests. Seven safeguards were removed one at a time to
  confirm that a test fails each time.

**Experiments.**

1. **Crash a worker, again.** Start a worker with `CLAIM_TIMEOUT_SECONDS=15`, set the receiver to
   `slow`, submit, and `kill -9` the worker while it waits. About 15 seconds after the first claim,
   another worker sends the delivery again. Attempt 1 is `claim_expired`, attempt 2 is
   `delivered`, and the receiver logged the ID twice. *Why can't the service avoid that
   duplicate? If the first worker had only been frozen, what stops it from overwriting the result
   when it wakes up?*

   The receiver processed the event, but the worker died before it could record that. To the
   database, a worker that died before sending, one that died after sending, and one that is only
   slow all look the same: a claim with no result. The only safe choice is to send again, so the
   duplicate is the price of never losing an event. A frozen worker is stopped by its claim token.
   The takeover replaced the token, so the late write, `WHERE claim_token = :old`, matches no row
   and changes nothing. This is a *fencing token*.

   In the live run, attempt 1 started at 23:06:36.918 UTC and was closed as `claim_expired`
   15.9 seconds later, and attempt 2 was delivered in 2 ms.

2. **Watch it give up.** With the receiver answering `500` and `MAX_ATTEMPTS=4`, the worker
   retried after 12.1, 17.2 and 48.4 seconds, then marked the delivery `failed`. These are the
   nominal waits of 10, 20 and 40 seconds, with jitter and up to a second for the next poll. *Why
   should the wait grow, why add randomness, and why stop at all?*

   Growing waits retry short blips quickly, but give a struggling receiver room to recover instead
   of hammering it. Randomness spreads out deliveries that failed together, so they do not all
   return in the same second when the receiver comes back. Stopping turns a hopeless delivery into
   a visible `failed` one, frees the workers for other deliveries, and gives receivers a bounded
   window, about 21 minutes with the defaults, for remembering IDs. Because attempts are counted
   at claim time, even a delivery that crashes every worker stops.

3. **Send the same event twice.** The same key returns the same `delivery_id`, marked
   `Idempotent-Replayed: true`. Different content with that key gets `409`. *The API already
   answers `202`, so why does the producer need a key at all? What if the `202` gets lost?*

   If the connection drops after the commit but before the `202` arrives, the producer cannot tell
   "stored" from "never arrived", and has to send again. Without a key, the resend becomes a
   second delivery with a new `webhook-id`, which the receiver cannot recognize as a duplicate.
   With a key, the resend finds the original. The fingerprint turns a key reused for different
   content into a loud `409`, instead of silently returning the wrong delivery.

**Lesson.** Any process can die between any two steps. So every step must be safe to repeat, and
every write must prove that its writer still owns the work.

## Three hops, three identifiers

An event crosses three hops, and each one can repeat. Each hop has its own identifier that makes
a repeat harmless:

| Hop | Repeats when | Identifier | Who drops the repeat |
|---|---|---|---|
| Producer to service | The producer resends after a lost answer or a `503` | `Idempotency-Key` | The service, with a unique key and `ON CONFLICT DO NOTHING` |
| Worker to database | A frozen worker wakes up and saves a late result | Claim token | The database, because the write is fenced by `WHERE claim_token = :mine` |
| Service to receiver | The service resends after a timeout or a worker crash | `webhook-id` | The receiver, for example with a `processed_webhooks` table |

Every hop delivers at least once, and every repeat is recognized and dropped. Together, as long as
the receiver drops duplicates, each event takes effect once, with no distributed transaction.

## Milestone 5: operability (planned)

- Producer authentication, with idempotency keys scoped per producer.
- Signed webhooks (`webhook-signature`, as in Standard Webhooks).
- Structured logs and metrics: the age of the oldest due delivery, the retry rate, and failures.
- An endpoint to replay failed deliveries.
- Container images, deployment instructions and a runbook.

After that, CarRental can publish its booking events through a transactional outbox, as described
in the [user manual](USER_MANUAL.md#recommended-integration-the-transactional-outbox).

## Revisiting a milestone

Each milestone is tagged. To run an older one without touching your working copy or your data,
check it out in a separate folder and give it its own databases:

```bash
# In the main checkout, with PostgreSQL running:
docker compose exec db createdb -U webhooks webhooks_m3
git worktree add ../webhook-delivery-service-m3 milestone-3

cd ../webhook-delivery-service-m3
export DATABASE_URL=postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_m3
export TEST_DATABASE_URL=postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_m3_test
uv sync
uv run alembic upgrade head
uv run pytest   # creates and migrates webhooks_m3_test
```

Then follow that milestone's README. Do not run `docker compose up` in the second folder: it would
try to start another PostgreSQL on the same port. Milestone 1 needs no database. To clean up:

```bash
cd ../webhook-delivery-service
git worktree remove ../webhook-delivery-service-m3
docker compose exec db dropdb -U webhooks webhooks_m3
docker compose exec db dropdb -U webhooks webhooks_m3_test
```
