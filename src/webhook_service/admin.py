"""Manage producers, their API keys, and webhook secrets.

    uv run webhook-admin create-producer carrental    add a producer, print its first API key
    uv run webhook-admin create-key carrental         add another key, e.g. to replace one
    uv run webhook-admin revoke-key wds_AbCdEfGh      refuse a key from now on
    uv run webhook-admin list                         producers and their keys
    uv run webhook-admin new-webhook-secret           print a secret for WEBHOOK_SECRETS

It works on the database directly, as the migrations do, so it needs DATABASE_URL but not the
API. An endpoint for this would need an administrator login of its own.
"""

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from webhook_service.db import get_engine
from webhook_service.services import producer_service
from webhook_service.signing import new_secret


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
    args = parser.parse_args(argv)

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
            else:
                print_producers(session)
        except producer_service.ProducerError as error:
            print(f"webhook-admin: {error}", file=sys.stderr)
            return 1
    return 0


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
