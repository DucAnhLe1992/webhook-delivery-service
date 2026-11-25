import hashlib
import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from webhook_service import admin
from webhook_service.main import app
from webhook_service.services import producer_service

EVENT = {"event_type": "booking.confirmed", "data": {"booking_id": 42}}


def with_key(api_key: str) -> TestClient:
    return TestClient(app, headers={"Authorization": f"Bearer {api_key}"})


@pytest.mark.parametrize(
    "authorization",
    [None, "Basic Y2FycmVudGFsOnNlY3JldA==", "Bearer "],
    ids=["no-header", "basic-auth", "empty-bearer"],
)
def test_a_request_without_an_api_key_is_refused(client, db, authorization):
    anonymous = TestClient(app, headers={"Authorization": authorization} if authorization else {})

    submitted = anonymous.post("/deliveries", json=EVENT)
    read = anonymous.get(f"/deliveries/{uuid.uuid4()}")

    assert submitted.status_code == read.status_code == 401
    assert submitted.headers["www-authenticate"] == "Bearer"
    with db.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM deliveries")) == 0


def test_a_revoked_key_gets_the_same_answer_as_an_unknown_one(client, db, api_key):
    with Session(db) as session:
        producer_service.revoke_api_key(session, api_key[:12])

    revoked = client.post("/deliveries", json=EVENT)
    unknown = with_key(producer_service.KEY_PREFIX + "x" * 43).post("/deliveries", json=EVENT)

    assert revoked.status_code == unknown.status_code == 401
    assert revoked.json() == unknown.json() == {"detail": "Invalid API key"}


def test_a_producer_sees_only_its_own_deliveries(client, client_for):
    friendster = client_for("friendster")
    delivery_id = client.post("/deliveries", json=EVENT).json()["delivery_id"]

    by_owner = client.get(f"/deliveries/{delivery_id}")
    by_other = friendster.get(f"/deliveries/{delivery_id}")
    nonexistent = friendster.get(f"/deliveries/{uuid.uuid4()}")

    assert by_owner.status_code == 200
    # The same answer as for an ID that does not exist, so it does not even confirm the ID.
    assert by_other.status_code == nonexistent.status_code == 404
    assert by_other.json() == nonexistent.json()


def test_each_producer_has_its_own_idempotency_keys(client, client_for):
    producers = [client, client_for("friendster")]
    key = {"Idempotency-Key": "order-1"}

    first = [producer.post("/deliveries", json=EVENT, headers=key) for producer in producers]
    again = [producer.post("/deliveries", json=EVENT, headers=key) for producer in producers]

    # The same key from two producers stores two deliveries...
    assert [response.status_code for response in first] == [202, 202]
    assert not any("idempotent-replayed" in response.headers for response in first)
    assert first[0].json()["delivery_id"] != first[1].json()["delivery_id"]
    # ...and a repeat finds the producer's own.
    assert [response.json() for response in again] == [response.json() for response in first]
    assert all(response.headers["idempotent-replayed"] == "true" for response in again)


def test_only_a_hash_of_an_api_key_is_stored(db, api_key):
    with db.connect() as connection:
        stored = connection.execute(text("SELECT key_hash, prefix FROM api_keys")).all()

    # The prefix tells keys apart; the other 35 random characters are needed to use one.
    assert stored == [(hashlib.sha256(api_key.encode()).hexdigest(), api_key[:12])]


def test_a_key_is_replaced_by_adding_a_new_one_then_revoking_the_old_one(client, db, api_key):
    with Session(db) as session:
        new = with_key(producer_service.create_api_key(session, "carrental"))

    # Both work while the producer's instances switch to the new key...
    assert client.post("/deliveries", json=EVENT).status_code == 202
    assert new.post("/deliveries", json=EVENT).status_code == 202
    with Session(db) as session:
        producer_service.revoke_api_key(session, api_key[:12])

    # ...and once the old one is revoked, only the new one does.
    assert client.post("/deliveries", json=EVENT).status_code == 401
    assert new.post("/deliveries", json=EVENT).status_code == 202


def test_the_admin_command_creates_lists_and_revokes_keys(client, db, capsys):
    assert admin.main(["create-producer", "friendster"], engine=db) == 0
    key = capsys.readouterr().out.strip()
    friendster = with_key(key)
    assert friendster.post("/deliveries", json=EVENT).status_code == 202

    assert admin.main(["list"], engine=db) == 0
    listing = capsys.readouterr().out
    assert f"friendster\n  {key[:12]}...  created " in listing
    assert key not in listing

    assert admin.main(["revoke-key", key[:12]], engine=db) == 0
    assert friendster.post("/deliveries", json=EVENT).status_code == 401

    assert admin.main(["create-producer", "friendster"], engine=db) == 1
    assert "already exists" in capsys.readouterr().err
    assert admin.main(["create-producer", "Car Rental"], engine=db) == 1
    assert "not a valid producer name" in capsys.readouterr().err
