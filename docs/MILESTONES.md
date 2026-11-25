# How it was built

The service was built in four milestones, tightened in a pass called 4.1 after an external
review, then opened to several producers safely in 5.1 and given a limit on every wait in 5.2,
the first two parts of milestone 5. Each milestone added a single guarantee, then ended with three experiments that broke the system on
purpose. Each experiment's result explains why the next milestone was needed. This
document records the steps, the questions and the answers.

For how the finished service works, see the [user manual](USER_MANUAL.md) and
[ARCHITECTURE.md](../ARCHITECTURE.md).

| Milestone | Git tag | What it added | Guarantee gained | What its experiments exposed |
|---|---|---|---|---|
| [1. Send it now](#milestone-1-send-it-now) | `milestone-1` | An endpoint that sends each webhook during the request | None yet: an event arrives only if the receiver is up at that moment | Events are lost, and a timeout means "unknown" |
| [2. Store before acknowledging](#milestone-2-store-before-acknowledging) | `milestone-2` | PostgreSQL, `202` after commit, `503` when storage is down | An accepted event is never lost | Nothing sends the stored events |
| [3. A separate worker](#milestone-3-a-separate-worker) | `milestone-3` | A worker process that claims, sends, records and retries | Stored events are sent and retried, even while the API is down | A crashed worker strands its delivery, retries never stop, and duplicates are possible |
| [4. Reliability](#milestone-4-reliability) | `milestone-4` | Expiring claims, claim tokens, attempt history, backoff and limits, `Idempotency-Key`, `SKIP LOCKED` | At-least-once delivery through crashes, bounded retries, safe producer retries, many workers | Nothing new. An external review then found guarantees stated more strongly than the code kept them |
| [4.1. Tighter guarantees](#milestone-41-tighter-guarantees) | `milestone-4.1` | A sender that reads only the status code, a worker loop no error can stop, honest `503` answers, a hard retry cap, a guarded test database, 14 more tests, the demo | Every guarantee worded exactly as the code keeps it, and no receiver answer can stop a worker | The remaining work is operations (milestone 5) |
| [5.1. Trust](#milestone-51-trust) | `milestone-5.1` | Producers with hashed API keys, deliveries and idempotency keys scoped per producer, a body-size limit, signed webhooks with secret rotation, settings that never print secrets, 27 more tests | Only registered producers get in, each sees only its own deliveries, and a receiver can tell a genuine webhook from a forged, changed or replayed one | A signature covers only what it signs, so the ID, the timestamp and the producer's name must be inside it |
| [5.2. Bounded waits](#milestone-52-bounded-waits) | `milestone-5.2` | Limits on every database wait, migrations that give up on a lock, sessions that close before the response, TCP keepalives, `/ready` next to `/health`, a worker that waits for the database and checks its schema, 18 more tests | A stuck lock, a slow statement or a dead network gives a quick error that is retried, never a hang, and an outage makes the API unready rather than dead | A database that freezes while its network keeps answering still holds up queries already sent to it |

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

## Milestone 4.1: tighter guarantees

**Goal.** Make every guarantee exactly as strong as the code, no stronger, and test the edges where
an outcome becomes uncertain. No new features.

**Why.** After milestone 4, an external review (by Perplexity) read the code against the
documentation. Each finding below was checked against the code, and the behavioral ones were
reproduced with a local test server. The two marked "found while checking" were not in the review.

| Finding | Evidence |
|---|---|
| A `503` said "the delivery was not accepted", but the connection can fail after PostgreSQL has committed. | Now a test: a session that commits, then raises a connection error. |
| `DELIVERY_TIMEOUT_SECONDS` limits each network step, not the attempt, so "3 × the timeout" does not bound an attempt. | A body sent at 1 byte per second kept an attempt with a 2-second timeout busy for 7 seconds. |
| The worker read every response body in full. | A 256 MB body added 513 MB to the worker's memory. |
| A body that does not decode stopped the worker (found while checking). | A `200` with a corrupt gzip body raised `DecodingError` out of the worker loop. |
| A full connection pool answered `500`, not `503` (found while checking). | SQLAlchemy's pool `TimeoutError` is not an `OperationalError`. |
| The retry "maximum" could be exceeded by 20%. | A capped wait with maximum jitter came to 4,320 seconds against a 3,600-second maximum. |
| The tests would empty any database `TEST_DATABASE_URL` named. | The fixture checked nothing before `TRUNCATE`. |
| Some sentences in the docs claimed more than the code does. | Claim expiry versus takeover, what an attempt row proves, and which invariants the database enforces. |

**What changed.**

- The sender streams the response, decides by the status code, and closes the connection without
  reading the body. Every request error, not only transport errors, becomes a retryable result.
- The worker loop logs an unexpected error with its traceback and carries on. The delivery it held
  is sent again once its claim expires, and the attempt still counts.
- A `503` for a submission now says the outcome is unknown and to retry with the same
  `Idempotency-Key`. Reads get their own message, and a full connection pool gives `503` too.
- Waits are capped again after the jitter, so `RETRY_MAX_DELAY_SECONDS` is a hard limit.
- The test fixtures refuse a database whose name does not end in `_test`, or the one
  `DATABASE_URL` names, and quote the name when they create a database.
- Delivery states and attempt outcomes moved to `domain.py`, so the database models no longer
  import the HTTP sender.
- 14 more tests, 53 in all:
  - the sender against a real HTTP server (a slow body, a huge body, a corrupt body, a dropped
    connection, no answer);
  - a commit whose answer is lost, and a full connection pool;
  - a late result racing a takeover that holds the row lock;
  - the same key with different content at the same moment, and after the first request rolls
    back;
  - a database failure while a result is saved, and an unexpected error in the worker loop.

  The multi-worker test now runs its threads as futures, so an error or a hang in any thread
  fails it.
- `dev/demo.py` plays four scenes with real processes, including a worker killed with `SIGKILL`.
- The docs state each guarantee exactly.

**Decided against.**

- **Saving the receiver URL on each delivery.** A planned move of the receiver would strand
  waiting deliveries at the old address. Waiting deliveries follow the current `RECEIVER_URL`, and
  every worker needs the same value. With several receivers, a delivery would point to a
  destination by ID, whose URL could change.
- **Renaming "at least once".** The term stays, qualified: within the retry window.
- **Changing the timestamps from `now()`.** It is the transaction's start time, which differs from
  the wall clock only by the length of these transactions: milliseconds.
- **Killing a real worker in CI.** It depends on timing, so it lives in the demo.
- **A constraint for every invariant.** Some, such as ordering of timestamps, would turn a clock
  adjustment into a failure. The docs now list exactly which invariants the database enforces.
- **A deadline for a whole attempt, for now.** It needs `async` cancellation or a custom transport.
  Streaming removed the slow-body case. A receiver that sends its status line a few bytes at a time
  can still outlast a claim, which costs a duplicate, never a corrupted state.

**Experiments.**

1. **Run the demo.** Run `uv run python dev/demo.py`. Scenes 3 and 4 both end with the receiver
   processing the same event twice. *The two duplicates have different causes. What are they, and
   why can the service prevent neither?*

   In scene 3, the receiver processed the event but answered after the worker had stopped
   waiting. In scene 4, the worker died after sending and before saving a result. In both cases,
   the service is left without an answer. "Processed but unconfirmed" and "never arrived" look
   identical from its side, and only the receiver knows which happened. Sending again is the only
   choice that never loses an event. So the service makes the duplicate recognizable, with the
   same `webhook-id`, and the receiver drops it.

2. **Break the record step.** Remove `.with_for_update()` from the `SELECT` in `record_attempt`
   and run `uv run pytest -k "late_result or taken_over"`. The new test fails, while the older
   takeover test still passes. *The token is checked in the `WHERE` clause anyway. Why does the
   check need a row lock?*

   Without the lock, the late worker's `SELECT` reads the last committed version of the row. The
   takeover has not committed yet, so that version still has the late worker's token, and the
   check passes. The late worker's updates then wait behind the takeover's locks and apply after
   it commits. They overwrite attempt 1's `claim_expired` and mark the delivery `delivered`, while
   worker B is still sending attempt 2. With `FOR UPDATE`, the `SELECT` waits for the takeover, and
   PostgreSQL evaluates the `WHERE` clause again on the new version of the row. The new token does
   not match, so nothing is returned. The check and the write become one step that a takeover
   cannot slip between. The older test lets the takeover finish first, so the old token is gone
   either way; only real contention shows the difference.

3. **Lose a commit.** Read
   `test_a_submission_whose_commit_was_not_confirmed_can_be_retried_with_its_key`. PostgreSQL
   commits, then the connection fails, and the API answers `503`. *Why can't the API answer
   correctly, and why does an `Idempotency-Key` make it harmless?*

   The answer to `COMMIT` travels over the same connection that failed. "The commit never arrived"
   and "the commit happened, but its answer was lost" look the same to the API: the same
   uncertainty as a receiver's timeout, one boundary further in. Finding out would mean asking a
   database that is not answering. So the API says the outcome is unknown and tells the producer
   how to settle it. A retry with the same key either stores the delivery or runs into the stored
   one and replays it, so exactly one delivery exists either way. Without a key, the retry could
   store a second one.

**Lesson.** A guarantee is only as strong as its weakest sentence. Every boundary where an answer
can be lost, whether at the receiver, the database or a worker, has the same shape: "not done" and
"done but unconfirmed" look alike. The answer is always a stable identifier that makes the retry
safe, and a test that puts the system exactly on that boundary.

## Milestone 5.1: trust

**Goal.** Know who is calling, keep producers apart, and let the receiver prove that each webhook
came from the service, unchanged and recently. Milestone 5 was split into four parts, and this one
came first: CarRental and Friendster, the first two producers, should not share an open endpoint.

**Why.** Until now, anyone who could reach the API could submit events and read any delivery by
its ID. Every caller shared one set of idempotency keys. A request body of any size was read into
memory. And a receiver could not tell a webhook from the service from a request anyone could
send. That was acceptable on one machine, not with two apps and a receiver on a network.

**What was built.**

- `producers` and `api_keys` tables, in migration `0004`. Every delivery now has a producer, and
  deliveries stored earlier get one named `legacy`. Idempotency keys are unique per producer.
- API keys: `wds_` and 32 random bytes, shown once, stored as a SHA-256 hash with the first 12
  characters for display. A FastAPI dependency turns `Authorization: Bearer <key>` into a
  producer, or answers `401`. A producer can have several keys, and a revoked key is refused from
  the next request.
- Every read and every idempotency check is scoped to the producer. Another producer's delivery
  is `404`.
- `webhook-admin`, a command to create producers and keys, list them, revoke keys, and make
  webhook secrets.
- `MAX_EVENT_BYTES`, 64 KiB by default, enforced while the body arrives: by `Content-Length`
  before anything is read, and by counting the bytes of a body sent in chunks.
- `signing.py`, with Standard Webhooks `v1` signatures. The sender signs every attempt with every
  secret in `WEBHOOK_SECRETS`, and the worker does not start without one. The webhook body names
  its producer.
- The test receiver checks signatures, and keeps the latest copy of each webhook exactly as it
  arrived, for replay experiments. The demo gained a fifth scene, with two producers and an
  attacker.
- 27 more tests, 80 in all. Sixteen hand mutations, one per safeguard, each made a test fail.

**Found while building.**

| Finding | What changed |
|---|---|
| Pydantic repeats a rejected setting in its error message, so a malformed `WEBHOOK_SECRETS`, or a `DATABASE_URL` with its password, would be printed at startup, into the logs. | Settings errors name the setting and the problem, never the value. Pydantic shortens long values in the middle, so the test looks for fragments of the secrets, not whole ones. |
| The body limit reads the settings on every request, so an invalid setting let the API start, then answer `500` to every request, even `/health`. | The API reads its settings at startup and refuses to start. |
| With one receiver for several producers, the receiver could not tell who sent an event, because producers choose event types freely. | The producer's name is in the signed body. A header would not be covered by the signature. |
| With idempotency keys shared by all producers, a second producer using the same key and content would be told its event was stored, and get the other producer's delivery back. | Keys are unique per producer. |
| The demo read friendster's delivery with carrental's key, and got `404`. | The demo reads each delivery as its owner. The scoping worked on its first real test. |

**Decided against.**

- **JWTs or OAuth 2 client credentials.** They suit many clients and a separate identity
  provider. For a few server-side producers, an opaque key that can be revoked at once is simpler;
  a JWT stays valid until it expires, unless a deny list is added.
- **A slow hash for API keys.** See experiment 3.
- **An administration endpoint.** It would need an administrator credential of its own.
  `webhook-admin` relies on database access, which operators have anyway.
- **Webhook secrets in the database.** The worker needs the secret itself to sign, so it could
  not be stored as a hash, and every database copy would contain it.
- **A webhook secret per producer.** The signature proves that the service sent the webhook. Which
  producer submitted the event is the `producer` field, inside the signed body.
- **Public-key signatures (`v1a`).** Worth it for third-party receivers, which should not hold a
  secret that can sign. Not for one receiver run by the same team.
- **Retrying when the receiver answers `401`.** It is a 4xx like the others: sending the same
  webhook again cannot fix secrets that differ. The rotation procedure avoids it, and failed
  deliveries can be replayed once the secrets are fixed.
- **Recording when each key was last used, for now.** It would make every authenticated request
  a write, or need batching. It is listed as a known gap, with rate limits and key expiry.

**Experiments.**

1. **Forge and replay a webhook.** Run `uv run python dev/demo.py` and read scene 5. Or, with the
   test receiver and a worker running, submit an event, read its captured copy with
   `curl localhost:9000/received`, and post it back to `localhost:9000/webhooks` with `curl`:
   unchanged, with another `booking_id` in the body, and with a new `webhook-timestamp`. *A
   signature proves who sent a webhook and that its body is unchanged. Why does it also cover the
   timestamp and the `webhook-id`?*

   A receiver relies on three things: what happened (the body), when the webhook was sent (the
   timestamp), and which event it is (the ID). Whoever captures a webhook can change any part the
   signature does not cover. If the timestamp were not signed, a webhook captured a month ago
   could be sent with today's timestamp and pass the age check. If the ID were not signed, the
   copy could get a new ID and pass the duplicate check. With both signed, the only replay left is
   an exact copy within five minutes, and an exact copy carries an ID the receiver has already
   processed. That is what scene 5 shows: the unchanged copy gets `200` but is logged as a
   duplicate, and the changed ones get `401`. Two things a signature does not do: it does not hide
   the content, which is HTTPS's job, and it does not prove the event is true. It proves that the
   service sent it, and the `producer` field says which authenticated producer submitted it.

2. **Two producers, one key.** Run `uv run webhook-admin create-producer friendster`, submit an
   event with an `Idempotency-Key` that carrental already used, then ask for one of carrental's
   deliveries with friendster's key. *Why are idempotency keys scoped to a producer, and why is
   the answer `404` rather than `403`?*

   An idempotency key is a name a producer gives its own request. Two apps that both choose
   `order-1` are a coincidence, not a repeat. With one shared set of keys, the second app would be
   told its event was stored while getting the other app's delivery back, so its own event would
   silently vanish. With different content it would get `409`, and a legitimate event would be
   refused. A producer could also find out which keys others had used. Scoping makes the producer
   part of what identifies a request. As for `404`: a `403` says "this exists, but not for you",
   which confirms the ID. A `404` is the same answer as for an ID that never existed, so it tells
   the caller nothing. Delivery IDs are random UUIDs, so guessing one is hopeless anyway; the
   uniform answer means that even a leaked ID tells another producer nothing. GitHub answers
   `404` for private repositories for the same reason.

3. **Look inside `api_keys`, then replace a key.** Run
   `docker compose exec db psql -U webhooks -c "SELECT prefix, key_hash, revoked_at FROM api_keys"`,
   then `uv run webhook-admin create-key carrental`, switch to the new key, and revoke the old one
   with `uv run webhook-admin revoke-key <prefix>`. *Why can a lost key not be recovered, and why
   is SHA-256 enough here when passwords need a slow hash such as bcrypt or Argon2?*

   Only the hash is stored, and a hash cannot be reversed. Nobody can read a key back: not the
   operator, and not an attacker with a copy of the database, which is the point, because a copy
   of the database then grants no access. A lost key is replaced, and several keys per producer
   make that painless. Passwords need slow hashes because people choose them, from a space small
   enough to search: with a leaked hash, an attacker tries billions of likely passwords per second,
   and a slow hash cuts that to thousands. A key is 32 random bytes, 2^256 possibilities. At a
   trillion guesses per second, finding one would take about 10^57 years, so making each guess
   100,000 times slower changes nothing. It would cost every request tens of milliseconds of CPU,
   though, which makes the API easier to overload. And slow hashes are salted, so the API could no
   longer find a key by looking up its hash in an index. The randomness of the key does the job a
   slow hash does for a password.

**Lesson.** Trust does not travel on its own. Each boundary needs its own proof: an API key from
producer to service, a signature from service to receiver. And a proof covers only what it signs.
Whatever it leaves out, an attacker can change, which is why the signature covers the ID and the
timestamp, not only the body, and why the producer's name goes inside the body, not in a header.

## Milestone 5.2: bounded waits

**Goal.** Give every wait a limit, so that a slow, stuck or vanished database becomes quick errors
that are retried, instead of requests and workers that hang. And tell a process that is dead from
one that cannot serve right now.

**Why.** Each of these was measured before the change:

- A request whose `Idempotency-Key` was held by a transaction that had not ended waited exactly as
  long as that transaction: 10 seconds in the probe, and without any limit.
- A request waited up to 30 seconds, SQLAlchemy's default, for a pooled connection.
- While migration `0004` waited for one open reader, an ordinary `SELECT` on `deliveries` waited
  behind the migration for as long as the reader stayed open.
- A request kept its session, with its pooled connection and its transaction, until its response
  had been sent.
- A connection whose network was cut, with libpq's defaults, was still waiting for an answer 90
  seconds later. Keepalive probes would only have started after two hours.
- `/health` said nothing about the database, and since 4.1 a worker started against a database
  without the tables logged the same error every second.

**What was built.**

- Five settings: `DATABASE_CONNECT_TIMEOUT_SECONDS`, `DATABASE_POOL_TIMEOUT_SECONDS`,
  `DATABASE_STATEMENT_TIMEOUT_SECONDS`, `DATABASE_LOCK_TIMEOUT_SECONDS` and
  `DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS`. One engine factory, `db.create_db_engine`,
  applies them to every connection, as PostgreSQL startup options, and the API, the worker,
  `webhook-admin` and the tests all use it. The lock timeout must be shorter than the statement
  timeout.
- TCP keepalives and `TCP_USER_TIMEOUT` on every connection, so a dead one fails in about 25
  seconds.
- Migrations give up after `DATABASE_LOCK_TIMEOUT_SECONDS` waiting for a lock, and have no
  statement timeout.
- The request's session closes when the route function returns, before the response is sent.
- `GET /ready` next to `GET /health`. `schema.py` names the newest migration the code needs, and
  readiness requires that revision or a later one. A test checks that migrations are numbered in
  order and that the code requires the newest.
- At startup the worker waits for the database, then exits if the schema is behind. Its log names
  the reason for each database error, and a session that PostgreSQL ended counts as a database
  error, not a bug.
- The `503` answers say the storage is "unavailable or too busy".
- A sixth demo scene: a frozen transaction, an API that cannot reach its database, and a worker
  against a database without tables.
- 18 more tests, 98 in all. Sixteen more hand mutations, one per safeguard, each made a test fail.

**Found while building.**

| Finding | What changed |
|---|---|
| FastAPI 0.165 ends a dependency with `yield` after the response is sent. The session, its pooled connection and its open transaction stayed with the request while the response traveled to the client. | `SessionDep` uses `scope="function"`. A test checks that no pooled connection is in use when the response starts. |
| PostgreSQL ending an idle transaction surfaces as `InternalError`, not `OperationalError`, so the worker would have logged it as a bug, with a traceback. | `db.storage_unavailable` also counts a connection that the database ended. A test makes sure that a broken constraint still counts as a bug. |
| A migration waiting for a lock queues every later query on its table. Lock timeouts for requests alone would still let a migration stall the service. | Migrations get the lock timeout too. Measured on a copy at `0003`: the migration gave up after 2.0 seconds, the waiting `SELECT` finished at the same moment, and the database was still at `0003`. |
| The driver waits at least 2 seconds to connect, whatever the setting. | The connect timeout is whole seconds, and the documentation says so. |
| From the host, a network cut could not be seen: Docker Desktop's port forwarding answers TCP on the container's behalf. | The keepalive probe ran its client in a container on the database's own network: with the service's settings, the two queries failed 25 and 26 seconds after the cut. |
| A paused database container holds up queries already sent. New connections failed after 5.0 seconds, but a query on an open connection waited for the whole 20-second pause. | A known gap, documented: only the client could keep that deadline, and that needs asynchronous database access. |

**Decided against.**

- **Limits on the database role** (`ALTER ROLE webhooks SET statement_timeout = …`). They would
  also cut short an operator's maintenance statements, and would live outside the code and its
  tests.
- **`transaction_timeout`** (PostgreSQL 17). The service's transactions are two to four short
  statements, which the statement and idle limits already bound, and it ends the whole session
  rather than the statement.
- **A database check in `/health`.** See experiment 2.
- **Refusing to start the API against an old schema.** It must stay up to answer `/health` and
  `/ready`, and readiness already keeps traffic away.
- **Requiring the exact schema revision.** Old code keeps serving while a migration runs during a
  deploy; with an exact match, every deploy would make it unready.
- **Retrying a migration by itself after a lock timeout.** The migration should fail clearly. The
  deploy step that runs it can retry, in 5.4.
- **A deadline kept by the client on every query.** It would need asynchronous database access
  with cancellation, for the one case that monitoring the database covers.
- **A separate lock timeout for migrations.** One limit for every lock wait is easier to reason
  about, and requests queued behind a migration give up on their own after that long anyway.

**Experiments.**

1. **Leave a transaction open.** With the API running, and the `carrental` producer from the
   [quick start](../README.md#quick-start), open `psql` with
   `docker compose exec db psql -U webhooks` and leave this transaction open:

   ```sql
   BEGIN;
   INSERT INTO deliveries (id, producer_id, event_type, payload, idempotency_key)
   SELECT gen_random_uuid(), id, 'booking.confirmed', '{}', 'stuck-1'
   FROM producers WHERE name = 'carrental';
   ```

   Submit an event with `Idempotency-Key: stuck-1`: it gets `503` after about 2 seconds, while
   other keys still work. Run `ROLLBACK;` in `psql`, then submit again: `202`. *Why does the API
   give up after 2 seconds instead of waiting for the lock, and why must the lock timeout be
   shorter than the statement timeout?*

   The waiting request cannot know whether the other transaction will end in a millisecond or
   never; in `psql` it belongs to a person, who may have gone to lunch. While it waits, the
   request holds one of the API's threads and one of its 15 pooled connections, and so does every
   retry of it. Once the waiting requests hold every pooled connection, all other requests, whatever
   their key, wait for a connection too, and fail after the pool timeout, which used to be 30
   seconds. One stuck transaction has become an outage. With a limit, the request gives up, its
   `INSERT` is cancelled, so nothing was stored, and the `503` with `Retry-After` tells the producer
   to try again later with the same key. Giving up early keeps the damage to the one key: that is
   backpressure. The `psql` session itself has no limits, because the service's settings apply only
   to the service's own connections, so the service has to protect itself rather than count on
   everyone else behaving. As for the order: time spent waiting for a lock counts towards the
   statement timeout, so a lock timeout that is not shorter would never be reached. Two separate
   limits also make the error say which one ran out: `lock timeout` means something is held by
   someone, and `statement timeout` means the database is slow. They call for different fixes.

2. **Stop the database.** With the API and a worker running, run `docker compose stop db`, then
   `curl -i localhost:8000/health` and `curl -i localhost:8000/ready`, and read the worker's log.
   Run `docker compose start db` and check both again. *Why two endpoints? What would go wrong if
   `/health` checked the database too?*

   They answer different questions, for different decisions. Liveness asks whether the process is
   broken in a way that only a restart fixes, such as a deadlock. Readiness asks whether it should
   get requests right now. An orchestrator restarts a process that fails liveness, and stops sending
   traffic to one that fails readiness. A database outage is a readiness problem: every API
   instance is fine, and restarting them cannot bring the database back. If `/health` checked the
   database, an outage would fail every instance's liveness at once, and the orchestrator would
   restart them all, again and again while the outage lasts. Requests in flight would be dropped,
   each restart would take time, and when the database came back, every instance would reconnect
   at the same moment, against a database that had only just recovered. A partial outage would
   become a total one, and recovery would be slower. With two endpoints, the instances stay up,
   answer `503` quickly, report themselves not ready, and serve again the moment the database is
   back. In the probe, `/ready` went from `200` to `503` within milliseconds of the stop and back to
   `200` once the database was up, while `/health` stayed `200` throughout, and the worker logged a
   database error on each poll and then carried on by itself. A readiness check must also be
   bounded itself: one that hangs would be as useless as none, which is why it uses the pooled
   connections, with their limits.

3. **Run a migration behind a long transaction.** On a throwaway database:

   ```bash
   docker compose exec db createdb -U webhooks webhooks_queue
   export DATABASE_URL=postgresql+psycopg://webhooks:webhooks@localhost:5433/webhooks_queue
   uv run alembic upgrade 0003
   uv run webhook-worker   # exits: the schema is at 0003, and it needs 0004
   ```

   In a second terminal, open `docker compose exec db psql -U webhooks -d webhooks_queue` and leave
   a transaction open: `BEGIN; SELECT count(*) FROM deliveries;`. Back in the first, run
   `uv run alembic upgrade head`: it gives up after 2 seconds. To watch the queue form, give the
   migration a long lock timeout,
   `DATABASE_LOCK_TIMEOUT_SECONDS=60 DATABASE_STATEMENT_TIMEOUT_SECONDS=120 uv run alembic upgrade head`,
   and while it waits, run `SELECT count(*) FROM deliveries;` in a third `psql`: it waits too. End
   the open transaction with `COMMIT;`, and both finish. Remove the database with
   `docker compose exec db dropdb -U webhooks webhooks_queue`. *The migration would succeed if it
   simply waited. Why make it give up?*

   Because of what happens to everyone else while it waits. PostgreSQL grants the locks on a table
   in the order they were asked for. The migration's `ALTER TABLE` needs an exclusive lock, which
   conflicts with everything, reads included, so it waits for the open transaction to end. The
   third session's `SELECT` needs only a shared lock, which does not conflict with the open
   transaction, but it does conflict with the exclusive lock waiting ahead of it, and PostgreSQL
   does not let it jump the queue; otherwise a steady stream of reads could keep the migration
   waiting forever. So the `SELECT` waits behind the migration, which waits behind the open
   transaction. The migration would take milliseconds once it had its lock: the waiting is the
   outage. For as long as one forgotten transaction stays open, every query on `deliveries` stops,
   the API's and the workers' alike. The service's own lock timeout makes each of those queries give
   up after 2 seconds, but the service is still down while the migration waits. A lock timeout on
   the migration caps that at 2 seconds: it gives up, the queue drains, and because PostgreSQL runs
   schema changes in a transaction, nothing is left half done. Run it again, or first find and end
   the long transaction with the blocking-sessions query in the
   [user manual](USER_MANUAL.md#checking-health-with-sql). The worker that refused to start at
   `0003` shows the other side: code must not run ahead of its schema.

**Lesson.** A wait without a limit is an outage waiting for its trigger: one forgotten transaction,
one frozen process, one cut cable. Giving each wait a limit is half the work. The other half is
deciding what happens when it runs out: a `503` the producer can retry, a session the database
ends, a migration that is run again, an instance that says "not ready" rather than "dead". And a
limit only counts once it has been run into on purpose, by a test or a probe; the one wait that
could not be limited is written down.

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

Milestone 4.1 found one more place where an answer can be lost: the API's own commit. It needs no
new identifier. The producer's retry passes through the same first hop, so its `Idempotency-Key`
already makes the retry safe.

Milestone 5.1 made the identifiers trustworthy. An `Idempotency-Key` now belongs to its producer,
so another producer cannot collide with it. And `webhook-id` is signed, with the timestamp and the
body, so an attacker can neither invent a new ID to get past the receiver's duplicate check nor
reuse a captured webhook after five minutes.

## The rest of milestone 5 (planned)

Milestone 5 prepares the service to run for real. Trust came first, in 5.1, and bounded waits in
5.2. Two parts remain:

- **5.3, observability and operations.** Structured logs and metrics: the age of the oldest due
  delivery, the retry rate, and failures. An endpoint to replay failed deliveries. A retention
  job, which must keep deliveries longer than producers may retry, because deleting a delivery
  frees its idempotency key.
- **5.4, deployment.** A container image with API and worker entry points, migrations as a deploy
  step that is retried when it gives up on a lock, a shutdown grace period longer than an attempt,
  a limit on concurrent requests at the server, backups, a runbook, and static type checking in
  CI.

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
