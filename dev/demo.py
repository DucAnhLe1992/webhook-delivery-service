"""Watch the service keep its promises, with real processes.

    uv run python dev/demo.py

PostgreSQL must be running (docker compose up -d --wait). The demo uses its own database,
webhooks_demo, with two producers, carrental and friendster. It starts the test receiver,
the API and a worker on free ports, with a webhook secret made for the run. Short timeouts
keep it to about half a minute. It plays six scenes:

1. A request repeated with the same Idempotency-Key is stored once.
2. While the receiver is down, the worker retries with growing waits.
3. An answer that arrives too late makes the worker send the event again.
4. A worker killed in the middle of a delivery is replaced, and the delivery is sent again.
5. Only known producers may submit, each sees only its own deliveries, and the receiver
   refuses webhooks that were changed, forged or replayed.
6. A request stuck behind a frozen transaction gives up quickly, and PostgreSQL ends the frozen
   transaction. An API whose database is unreachable is alive but not ready, and a worker does
   not start against a database without tables.
"""

import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx2
from alembic import command
from alembic.config import Config
from sqlalchemy import URL, Connection, Engine, create_engine, make_url, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from webhook_service import signing
from webhook_service.config import Settings, get_settings
from webhook_service.db import create_db_engine, describe_error
from webhook_service.services import producer_service

ROOT = Path(__file__).resolve().parents[1]
DATABASE = "webhooks_demo"
SETTINGS = {
    "DELIVERY_TIMEOUT_SECONDS": "2",
    "CLAIM_TIMEOUT_SECONDS": "6",
    "RETRY_BASE_DELAY_SECONDS": "2",
    "MAX_ATTEMPTS": "5",
    "POLL_INTERVAL_SECONDS": "0.2",
    "DATABASE_LOCK_TIMEOUT_SECONDS": "1",
    "DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS": "3",
}
# Nothing listens on port 1, so connections to this database are refused at once.
UNREACHABLE_DATABASE = "postgresql+psycopg://webhooks:webhooks@127.0.0.1:1/webhooks"
SLOW_SECONDS = 3  # How long the receiver takes in slow mode: longer than the timeout.
UUID = re.compile(r"\b([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
LOG_LINE = re.compile(r"^(\d\d:\d\d:\d\d) \[(?:worker|receiver)\] (.*)$")


class DemoError(Exception):
    pass


class Process:
    """A service process whose log lines the demo prints as they appear."""

    def __init__(self, name: str, args: list[str], env: dict[str, str], log_dir: Path) -> None:
        self.name, self.args, self.env = name, args, env
        self.log_path = log_dir / f"{name}.log"
        self.log_path.touch()
        self.shown = 0
        self.popen: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        with self.log_path.open("ab") as log:
            self.popen = subprocess.Popen(
                self.args, cwd=ROOT, env=self.env, stdout=log, stderr=subprocess.STDOUT
            )

    def stop(self) -> None:
        """Ask the process to stop, as Ctrl+C would, and wait for it."""
        if self.popen and self.popen.poll() is None:
            self.popen.terminate()
            try:
                self.popen.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait()

    def kill(self) -> None:
        """Stop the process at once with SIGKILL. It gets no chance to clean up."""
        assert self.popen is not None
        self.popen.kill()
        self.popen.wait()

    def new_lines(self) -> list[tuple[str, str, str]]:
        data = self.log_path.read_bytes()[self.shown :]
        complete = data[: data.rfind(b"\n") + 1]
        self.shown += len(complete)
        lines = []
        for line in complete.decode(errors="replace").splitlines():
            if match := LOG_LINE.match(line):
                lines.append((match.group(1), self.name, match.group(2)))
        return lines


def short(value: str) -> str:
    """Shorten every UUID to its first 8 characters, for reading."""
    return UUID.sub(r"\1", value)


def say(message: str) -> None:
    print(textwrap.fill(message, width=96, initial_indent="  ", subsequent_indent="  "))


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def prepare_database() -> tuple[URL, dict[str, str]]:
    """Create and migrate the demo database if needed, empty it, and add two producers.

    Returns the database URL and each producer's API key.
    """
    url = make_url(str(get_settings().database_url)).set(database=DATABASE)
    server = create_engine(url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with server.connect() as connection:
            exists = connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": DATABASE}
            )
            if not exists:
                connection.execute(text(f"CREATE DATABASE {DATABASE}"))
    finally:
        server.dispose()
    config = Config()
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.attributes["database_url"] = url
    command.upgrade(config, "head")
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE deliveries, delivery_attempts, api_keys, producers"))
    keys = {}
    with Session(engine) as session:
        for name in ("carrental", "friendster"):
            _, keys[name] = producer_service.create_producer(session, name)
    engine.dispose()
    return url, keys


class Demo:
    def __init__(self, database_url: URL, api_keys: dict[str, str], log_dir: Path) -> None:
        self.database_url, self.log_dir = database_url, log_dir
        receiver_port, api_port = free_port(), free_port()
        # The worker signs with this secret, and the receiver checks signatures with it.
        self.secret = signing.new_secret()
        env = {
            **os.environ,
            **SETTINGS,
            "PYTHONUNBUFFERED": "1",
            "DATABASE_URL": database_url.render_as_string(hide_password=False),
            "RECEIVER_URL": f"http://127.0.0.1:{receiver_port}/webhooks",
            "WEBHOOK_SECRETS": self.secret,
        }
        self.env = env
        python = sys.executable
        self.receiver = Process(
            "receiver",
            [python, "dev/receiver.py", "--port", str(receiver_port)]
            + ["--slow-seconds", str(SLOW_SECONDS)],
            env,
            log_dir,
        )
        self.api = Process(
            "api",
            [python, "-m", "uvicorn", "webhook_service.main:app", "--port", str(api_port)],
            env,
            log_dir,
        )
        worker = [python, "-m", "webhook_service.worker.main"]
        self.worker = Process("worker", worker, env, log_dir)
        self.worker_2 = Process("worker-2", worker, env, log_dir)
        self.processes = [self.receiver, self.api, self.worker, self.worker_2]
        api_url = f"http://127.0.0.1:{api_port}"
        # Scenes 1 to 4 submit as carrental.
        self.api_client = self.producer_client(api_url, api_keys["carrental"])
        self.friendster_client = self.producer_client(api_url, api_keys["friendster"])
        self.anonymous_client = httpx2.Client(base_url=api_url, timeout=5)
        self.receiver_client = httpx2.Client(
            base_url=f"http://127.0.0.1:{receiver_port}", timeout=5
        )

    # Helpers

    @staticmethod
    def producer_client(api_url: str, api_key: str) -> httpx2.Client:
        return httpx2.Client(
            base_url=api_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=5
        )

    def show_logs(self) -> None:
        lines = [line for process in self.processes for line in process.new_lines()]
        for clock, name, message in sorted(lines, key=lambda line: line[0]):
            print(f"    {clock}  {name:<9} {short(message)}")

    def note(self, message: str) -> None:
        """Say something, after the log lines that came before it."""
        self.show_logs()
        say(message)

    def wait_until(self, what: str, check: Callable[[], Any], timeout: float = 30) -> Any:
        deadline = time.monotonic() + timeout
        while True:
            self.show_logs()
            try:
                result = check()
            except httpx2.HTTPError:
                result = None
            if result:
                return result
            if time.monotonic() > deadline:
                raise DemoError(f"Timed out waiting until {what}.")
            time.sleep(0.1)

    def submit(self, key: str, booking_id: int) -> httpx2.Response:
        return self.api_client.post(
            "/deliveries",
            json={"event_type": "booking.confirmed", "data": {"booking_id": booking_id}},
            headers={"Idempotency-Key": key},
        )

    def delivery(self, delivery_id: str, client: httpx2.Client | None = None) -> dict[str, Any]:
        """Read a delivery as its producer, carrental unless another client is given."""
        return (client or self.api_client).get(f"/deliveries/{delivery_id}").json()

    def wait_until_delivered(
        self, delivery_id: str, client: httpx2.Client | None = None, timeout: float = 30
    ) -> None:
        self.wait_until(
            "the delivery is delivered",
            lambda: self.delivery(delivery_id, client)["status"] == "delivered",
            timeout,
        )
        # The worker logs the result just after saving it.
        time.sleep(0.3)
        self.show_logs()

    def set_receiver_mode(self, mode: str) -> None:
        self.receiver_client.put(f"/mode/{mode}").raise_for_status()

    def times_received(self, delivery_id: str) -> int:
        received = self.receiver_client.get("/received").json()
        return received.get(delivery_id, {}).get("count", 0)

    def show_history(self, delivery_id: str) -> None:
        delivery = self.delivery(delivery_id)
        say(f"GET /deliveries/{short(delivery_id)}: {delivery['status']}")
        first = datetime.fromisoformat(delivery["attempts"][0]["started_at"])
        for attempt in delivery["attempts"]:
            offset = (datetime.fromisoformat(attempt["started_at"]) - first).total_seconds()
            status = attempt["http_status"]
            detail = f"HTTP {status}" if status else attempt["error_category"] or ""
            outcome = attempt["outcome"] or "in progress"
            print(f"    attempt {attempt['number']}  +{offset:4.1f} s  {outcome:<18} {detail}")

    # The demo

    def start(self) -> None:
        print("Starting the receiver, the API and a worker with these settings:")
        print("  " + ", ".join(f"{name}={value}" for name, value in SETTINGS.items()))
        print("  WEBHOOK_SECRETS=<a secret made for this run, which the receiver has too>")
        self.receiver.start()
        self.api.start()
        self.worker.start()
        self.wait_until("the receiver is up", lambda: self.receiver_client.get("/received"))
        self.wait_until("the API is up", lambda: self.api_client.get("/health"))
        self.wait_until("the worker is up", lambda: "started" in self.worker.log_path.read_text())

    def scene_1(self) -> None:
        print("\nScene 1: accepted means stored, and repeating a request is harmless")
        say(
            "The producer sends a booking event with an Idempotency-Key, then sends it again, "
            "as it would if the first answer had been lost."
        )
        first = self.submit("demo-booking-1", booking_id=1)
        delivery_id = first.json()["delivery_id"]
        self.note(f"{'POST /deliveries':<34}-> {first.status_code}, {short(delivery_id)}")
        again = self.submit("demo-booking-1", booking_id=1)
        replayed = again.headers.get("idempotent-replayed")
        self.note(
            f"{'POST again, same key and content':<34}-> {again.status_code}, "
            f"{short(again.json()['delivery_id'])}, Idempotent-Replayed: {replayed}"
        )
        conflict = self.submit("demo-booking-1", booking_id=2)
        self.note(f"{'POST same key, different content':<34}-> {conflict.status_code}")
        self.wait_until_delivered(delivery_id)
        self.show_history(delivery_id)

    def scene_2(self) -> None:
        print("\nScene 2: the receiver is down")
        say(
            "The worker cannot reach the receiver, so it tries again later, waiting about "
            "twice as long each time. Nothing is lost."
        )
        self.receiver.stop()
        self.note("Stopped the receiver.")
        delivery_id = self.submit("demo-booking-2", booking_id=2).json()["delivery_id"]
        self.wait_until(
            "two attempts have failed",
            lambda: (
                (d := self.delivery(delivery_id))["attempt_count"] >= 2 and d["status"] == "pending"
            ),
        )
        self.receiver.start()
        self.wait_until("the receiver is up", lambda: self.receiver_client.get("/received"))
        self.note("Started the receiver again.")
        self.wait_until_delivered(delivery_id)
        self.show_history(delivery_id)

    def scene_3(self) -> None:
        print("\nScene 3: an answer that comes too late")
        say(
            f"The receiver processes the event but answers after {SLOW_SECONDS} seconds, and "
            f"the worker stops waiting after {SETTINGS['DELIVERY_TIMEOUT_SECONDS']} seconds. It "
            "cannot tell this from a failure, so it sends the event again."
        )
        self.set_receiver_mode("slow")
        delivery_id = self.submit("demo-booking-3", booking_id=3).json()["delivery_id"]
        self.wait_until(
            "the first attempt has timed out",
            lambda: (
                [a["error_category"] for a in self.delivery(delivery_id)["attempts"]][:1]
                == ["timeout"]
            ),
        )
        self.set_receiver_mode("ok")
        self.wait_until_delivered(delivery_id)
        self.show_history(delivery_id)
        say(
            f"The receiver processed {short(delivery_id)} {self.times_received(delivery_id)} "
            "times, with the same webhook-id both times. A receiver that records the IDs it "
            "has processed skips the second copy."
        )

    def scene_4(self) -> None:
        print("\nScene 4: a worker dies in the middle of a delivery")
        say(
            "The receiver is slow again. While the worker waits for the answer, it is killed "
            "with SIGKILL, so it cannot save anything. A new worker takes the delivery over "
            f"once the dead worker's claim expires, after {SETTINGS['CLAIM_TIMEOUT_SECONDS']} "
            "seconds."
        )
        self.set_receiver_mode("slow")
        delivery_id = self.submit("demo-booking-4", booking_id=4).json()["delivery_id"]
        self.wait_until("the receiver has the event", lambda: self.times_received(delivery_id))
        self.worker.kill()
        self.note("Killed the worker while it was waiting for the receiver's answer.")
        stuck = self.delivery(delivery_id)
        self.note(
            f"The delivery is {stuck['status']}, and attempt 1 has no result: nobody is left "
            "to save it."
        )
        self.set_receiver_mode("ok")
        self.worker_2.start()
        self.note("Started a new worker.")
        self.wait_until_delivered(delivery_id)
        self.show_history(delivery_id)
        say(
            f"The receiver processed {short(delivery_id)} {self.times_received(delivery_id)} "
            "times. The service cannot know whether the dead worker's request arrived, so it "
            "had to send it again. Sending once and never again could lose the event."
        )

    def scene_5(self) -> None:
        print("\nScene 5: who may submit, who may read, and who sent a webhook")
        say(
            "The API knows two producers, carrental and friendster. Each has its own API key "
            "and its own idempotency keys. The worker signs every webhook with the secret it "
            "shares with the receiver."
        )
        event = {"event_type": "booking.confirmed", "data": {"booking_id": 5}}
        key = {"Idempotency-Key": "event-5"}
        anonymous = self.anonymous_client.post("/deliveries", json=event)
        self.note(f"{'POST without an API key':<44}-> {anonymous.status_code}")
        own = self.api_client.post("/deliveries", json=event, headers=key)
        own_id = own.json()["delivery_id"]
        self.note(
            f"{'carrental:  POST, Idempotency-Key event-5':<44}-> {own.status_code}, "
            f"{short(own_id)}"
        )
        other = self.friendster_client.post(
            "/deliveries", json={"event_type": "post.liked", "data": {"post_id": 5}}, headers=key
        )
        other_id = other.json()["delivery_id"]
        self.note(
            f"{'friendster: POST, Idempotency-Key event-5':<44}-> {other.status_code}, "
            f"{short(other_id)}, not a replay"
        )
        peek = self.friendster_client.get(f"/deliveries/{own_id}")
        label = f"friendster: GET carrental's {short(own_id)}"
        self.note(f"{label:<44}-> {peek.status_code}, as for an ID that does not exist")
        huge = {"event_type": "booking.confirmed", "data": {"notes": "x" * 100_000}}
        too_large = self.api_client.post("/deliveries", json=huge)
        self.note(f"{'carrental:  POST a 100 KB event':<44}-> {too_large.status_code}")
        self.wait_until_delivered(own_id)
        self.wait_until_delivered(other_id, self.friendster_client)
        say(
            "Both webhooks passed the receiver's signature check, and each says which "
            "producer it came from. Now an attacker who captured carrental's webhook on its "
            "way to the receiver tries to use it."
        )
        captured = self.receiver_client.get("/received").json()[own_id]["request"]
        headers, body = captured["headers"], captured["body"].encode()
        attacks = [
            ("sends it again, unchanged", headers, body),
            (
                "changes booking_id in the body",
                headers,
                body.replace(b'"booking_id":5', b'"booking_id":6'),
            ),
            (
                "gives it a new timestamp",
                headers | {"webhook-timestamp": str(int(time.time()) + 1)},
                body,
            ),
            (
                "signs it with a guessed secret",
                self.signed_with_a_guessed_secret(headers, body),
                body,
            ),
        ]
        for description, attack_headers, attack_body in attacks:
            response = self.receiver_client.post(
                "/webhooks",
                content=attack_body,
                headers=attack_headers | {"content-type": "application/json"},
            )
            time.sleep(0.2)
            self.note(f"{'Attacker ' + description:<44}-> {response.status_code}")
        say(
            "The copy sent unchanged passed the signature check, but it has the same webhook-id "
            "as the original, so the receiver's duplicate check catches it. Five minutes after "
            "it was signed, the receiver refuses it outright, and a new timestamp would break "
            "its signature."
        )

    def signed_with_a_guessed_secret(self, headers: dict[str, str], body: bytes) -> dict[str, str]:
        guess = signing.decode_secret(signing.new_secret())
        webhook_id, timestamp = headers["webhook-id"], int(headers["webhook-timestamp"])
        return headers | {
            "webhook-signature": signing.signature_header([guess], webhook_id, timestamp, body)
        }

    def scene_6(self) -> None:
        print("\nScene 6: every wait has a limit")
        lock_timeout = SETTINGS["DATABASE_LOCK_TIMEOUT_SECONDS"]
        idle_timeout = float(SETTINGS["DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS"])
        say(
            "A request froze in the middle of its transaction: it had stored Idempotency-Key "
            "event-6, but not committed. Another request with that key must wait to learn "
            f"whether it commits. It waits at most {lock_timeout} s, and PostgreSQL ends a "
            f"transaction that sits idle for {idle_timeout:.0f} s."
        )
        engine, frozen = self.freeze_a_request("event-6")
        frozen_at = time.monotonic()
        stuck = self.submit("event-6", booking_id=6)
        waited = time.monotonic() - frozen_at
        self.note(
            f"{'POST, Idempotency-Key event-6':<44}-> {stuck.status_code} after {waited:.1f} s"
        )
        ready = self.api_client.get("/ready")
        self.note(f"{'GET /ready':<44}-> {ready.status_code} {ready.json()['status']}")
        time.sleep(max(0.0, idle_timeout + 0.5 - (time.monotonic() - frozen_at)))
        try:
            frozen.execute(text("SELECT 1"))
            ended = "nothing: its transaction is still open"
        except DBAPIError as error:
            ended = describe_error(error)
        finally:
            frozen.close()
            engine.dispose()
        say(
            "The request gave up instead of waiting as long as the frozen one, and the API "
            "stayed ready: one stuck transaction is not an outage. PostgreSQL then ended the "
            "frozen transaction, which frees the key. When the frozen request wakes up, it finds:"
        )
        print(f"    {ended}")
        again = self.submit("event-6", booking_id=6)
        delivery_id = again.json()["delivery_id"]
        self.note(
            f"{'POST, Idempotency-Key event-6 again':<44}-> {again.status_code}, "
            f"{short(delivery_id)}, not a replay"
        )
        self.wait_until_delivered(delivery_id)

        say(
            "Liveness and readiness are different questions. A second API whose database "
            "cannot be reached is alive, so restarting it would not help. It is not ready, so "
            "traffic should go to other instances."
        )
        api_port = free_port()
        api_2 = Process(
            "api-2",
            [sys.executable, "-m", "uvicorn", "webhook_service.main:app", "--port", str(api_port)],
            self.env | {"DATABASE_URL": UNREACHABLE_DATABASE},
            self.log_dir,
        )
        api_2.start()
        with httpx2.Client(base_url=f"http://127.0.0.1:{api_port}", timeout=5) as client:
            try:
                self.wait_until("the second API is up", lambda: client.get("/health"))
                live, ready = client.get("/health"), client.get("/ready")
            finally:
                api_2.stop()
        self.note(f"{'Second API, GET /health':<30}-> {live.status_code} {live.json()['status']}")
        readiness = ready.json()
        self.note(
            f"{'Second API, GET /ready':<30}-> {ready.status_code} {readiness['status']}: "
            f"{readiness['detail']}"
        )

        say(
            "A worker pointed at a database without tables does not start, where it used to "
            "log the same error on every poll:"
        )
        database_without_tables = self.database_url.set(database="postgres")
        worker_3 = Process(
            "worker-3",
            [sys.executable, "-m", "webhook_service.worker.main"],
            self.env | {"DATABASE_URL": database_without_tables.render_as_string(False)},
            self.log_dir,
        )
        worker_3.start()
        assert worker_3.popen is not None
        exit_code = worker_3.popen.wait(timeout=30)
        last_line = worker_3.log_path.read_text().strip().splitlines()[-1]
        print(f"    worker-3 exits with code {exit_code}: {last_line}")

    def freeze_a_request(self, key: str) -> tuple[Engine, Connection]:
        """Store a delivery with the key and leave its transaction open, as a frozen request.

        It uses the API's own database settings, so its session has the same limits.
        """
        settings = Settings(
            _env_file=None, **{name.lower(): value for name, value in SETTINGS.items()}
        )
        engine = create_db_engine(self.database_url, settings)
        connection = engine.connect()
        connection.execute(
            text(
                "INSERT INTO deliveries (id, producer_id, event_type, payload, idempotency_key)"
                " SELECT gen_random_uuid(), id, 'booking.confirmed', '{}', :key"
                " FROM producers WHERE name = 'carrental'"
            ),
            {"key": key},
        )
        return engine, connection

    def stop(self) -> None:
        print("\nStopping the processes.")
        for process in reversed(self.processes):
            process.stop()
        self.show_logs()
        for client in (
            self.api_client,
            self.friendster_client,
            self.anonymous_client,
            self.receiver_client,
        ):
            client.close()


def main() -> None:
    log_dir = Path(tempfile.mkdtemp(prefix="webhook-demo-"))
    try:
        database_url, api_keys = prepare_database()
    except OperationalError:
        sys.exit("PostgreSQL is not reachable. Start it with: docker compose up -d --wait")
    demo = Demo(database_url, api_keys, log_dir)
    started = time.monotonic()
    failed = True
    try:
        demo.start()
        demo.scene_1()
        demo.scene_2()
        demo.scene_3()
        demo.scene_4()
        demo.scene_5()
        demo.scene_6()
        failed = False
    except DemoError as error:
        print(f"\n{error} The process logs are in {log_dir}.")
    finally:
        demo.stop()
        if not failed:
            shutil.rmtree(log_dir)
    if failed:
        sys.exit(1)
    print(f"\nDone in {time.monotonic() - started:.0f} s.")


if __name__ == "__main__":
    main()
