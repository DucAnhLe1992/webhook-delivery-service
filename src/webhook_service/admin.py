"""Manage producers, API keys and webhook secrets, replay failed deliveries, and purge old ones.

    uv run webhook-admin create-producer carrental    add a producer, print its first API key
    uv run webhook-admin create-key carrental         add another key, e.g. to replace one
    uv run webhook-admin revoke-key wds_AbCdEfGh      refuse a key from now on
    uv run webhook-admin list                         producers and their keys
    uv run webhook-admin new-webhook-secret           print a secret for WEBHOOK_SECRETS
    uv run webhook-admin replay <delivery id> ...     send failed deliveries again
    uv run webhook-admin replay --failed-since 2026-11-25T20:00
                                                      ...every delivery that failed since then
    uv run webhook-admin purge [--dry-run]            delete deliveries older than RETENTION_DAYS

It works on the database directly, as the migrations do, so it needs DATABASE_URL but not the
API. An endpoint for this would need an administrator login of its own.
"""

import argparse
import sys
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from webhook_service.config import get_settings
from webhook_service.db import get_engine
from webhook_service.services import delivery_service, producer_service
from webhook_service.signing import new_secret


def moment(value: str) -> datetime:
    """An ISO 8601 time, such as 2026-11-25T20:00. Without a time zone, it is local time."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a time such as 2026-11-25T20:00 or 2026-11-25T20:00+00:00"
        ) from None
    return parsed if parsed.tzinfo else parsed.astimezone()


def deliveries(count: int) -> str:
    return f"{count} {'delivery' if count == 1 else 'deliveries'}"


def main(argv: Sequence[str] | None = None, engine: Engine | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="webhook-admin",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    create_producer = commands.add_parser(
        "create-producer", help="add a producer and print its first API key"
    )
    create_producer.add_argument("name", help="lower-case letters, digits, - and _")
    create_key = commands.add_parser("create-key", help="add an API key to a producer")
    create_key.add_argument("producer", help="the producer's name")
    revoke_key = commands.add_parser("revoke-key", help="refuse an API key from now on")
    revoke_key.add_argument("prefix", help="the start of the key, as `list` shows it")
    commands.add_parser("list", help="list producers and their API keys")
    commands.add_parser("new-webhook-secret", help="print a new secret for WEBHOOK_SECRETS")
    replay = commands.add_parser(
        "replay",
        help="send failed deliveries again, each with MAX_ATTEMPTS new attempts",
    )
    replay.add_argument("delivery_ids", nargs="*", type=uuid.UUID, metavar="delivery_id")
    replay.add_argument(
        "--failed-since",
        type=moment,
        help="every delivery that failed since this time, such as 2026-11-25T20:00 (local time)",
    )
    replay.add_argument("--producer", help="only this producer's deliveries")
    purge = commands.add_parser(
        "purge", help="delete deliveries that finished more than RETENTION_DAYS ago"
    )
    purge.add_argument("--dry-run", action="store_true", help="only count them")
    args = parser.parse_args(argv)
    if args.command == "replay" and not args.delivery_ids and args.failed_since is None:
        replay.error("name the deliveries, or give --failed-since")

    if args.command == "new-webhook-secret":
        print(new_secret())
        return 0

    with Session(engine or get_engine(), expire_on_commit=False) as session:
        try:
            if args.command == "create-producer":
                _, key = producer_service.create_producer(session, args.name)
                print_new_key(key, f"Created the producer {args.name}.")
            elif args.command == "create-key":
                key = producer_service.create_api_key(session, args.producer)
                print_new_key(key, f"Created an API key for {args.producer}.")
            elif args.command == "revoke-key":
                producer_service.revoke_api_key(session, args.prefix)
                print(f"Revoked {args.prefix}. Requests with it now get 401.")
            elif args.command == "replay":
                return replay_failed(session, args)
            elif args.command == "purge":
                purge_finished(session, dry_run=args.dry_run)
            else:
                print_producers(session)
        except producer_service.ProducerError as error:
            print(f"webhook-admin: {error}", file=sys.stderr)
            return 1
    return 0


def replay_failed(session: Session, args: argparse.Namespace) -> int:
    producer_id = producer_service.producer_id(session, args.producer) if args.producer else None
    replayed = delivery_service.replay_failed(
        session,
        delivery_ids=args.delivery_ids,
        failed_since=args.failed_since,
        producer_id=producer_id,
    )
    if replayed:
        print(
            f"Replayed {deliveries(len(replayed))}. Workers send each again, with up to "
            "MAX_ATTEMPTS new attempts."
        )
    else:
        print("Replayed nothing: no failed delivery matched.")
    for delivery_id in replayed:
        print(f"  {delivery_id}")
    skipped = [str(delivery_id) for delivery_id in args.delivery_ids if delivery_id not in replayed]
    if skipped:
        print(
            "webhook-admin: not replayed, because they are not failed or do not exist: "
            + ", ".join(skipped),
            file=sys.stderr,
        )
        return 1
    return 0


def purge_finished(session: Session, *, dry_run: bool) -> None:
    days = get_settings().retention_days
    older_than = timedelta(days=days)
    if dry_run:
        count = delivery_service.count_finished_before(session, older_than)
        print(f"Would delete {deliveries(count)} that finished more than {days} days ago.")
    else:
        count = delivery_service.purge_finished(session, older_than=older_than)
        print(
            f"Deleted {deliveries(count)} that finished more than {days} days ago, with their "
            "attempts."
        )


def print_new_key(key: str, message: str) -> None:
    # Only the key goes to stdout, so a script can capture it: KEY=$(webhook-admin ...).
    print(f"{message} Its API key is shown only this once, so store it now:", file=sys.stderr)
    print(key)


def print_producers(session: Session) -> None:
    def when(moment: datetime) -> str:
        return f"{moment:%Y-%m-%d %H:%M %Z}"

    for producer in producer_service.list_producers(session):
        print(producer.name)
        if not producer.api_keys:
            print("  no API keys")
        for key in producer.api_keys:
            state = f"revoked {when(key.revoked_at)}" if key.revoked_at else "active"
            print(f"  {key.prefix}...  created {when(key.created_at)}  {state}")
