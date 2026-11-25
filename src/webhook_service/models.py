import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from webhook_service.domain import AttemptOutcome, DeliveryStatus


class Base(DeclarativeBase):
    # Predictable constraint names, so later migrations can refer to them.
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


def one_of(column: str, values: type[StrEnum]) -> str:
    return f"{column} IN (" + ", ".join(f"'{value}'" for value in values) + ")"


class Producer(Base):
    """An app that submits events, such as carrental. It sees only its own deliveries."""

    __tablename__ = "producers"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    api_keys: Mapped[list["ApiKey"]] = relationship(order_by="ApiKey.created_at")


class ApiKey(Base):
    """A key a producer authenticates with. Only a hash of the key is stored."""

    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    producer_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("producers.id", ondelete="CASCADE"), index=True
    )
    # SHA-256 of the key, in hex. The API looks keys up by it.
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    # The start of the key, which is not enough to use it, so people can tell keys apart.
    prefix: Mapped[str] = mapped_column(String(12))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # A revoked key is kept, so the history shows which keys existed and when they stopped.
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Delivery(Base):
    __tablename__ = "deliveries"
    __table_args__ = (
        # Each producer has its own idempotency keys. With one shared set, a producer could
        # collide with another producer's key, or tell from the answer that it was used.
        UniqueConstraint(
            "producer_id", "idempotency_key", name="uq_deliveries_producer_id_idempotency_key"
        ),
        CheckConstraint(one_of("status", DeliveryStatus), name="status_valid"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_not_negative"),
        # A delivery is processing exactly when one of its attempts holds the claim.
        CheckConstraint(
            "(status = 'processing') = (claim_token IS NOT NULL)", name="claimed_while_processing"
        ),
        # Serves the worker's "next due delivery" query; finished deliveries are left out.
        Index(
            "ix_deliveries_due",
            "next_attempt_at",
            postgresql_where=text("status IN ('pending', 'processing')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    producer_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("producers.id"))
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(20), server_default=DeliveryStatus.PENDING)
    attempt_count: Mapped[int] = mapped_column(server_default="0")
    # When a worker may next claim the delivery. While pending: when it is due. While
    # processing: when the current claim expires, so another worker can take over.
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    # The id of the attempt that holds the claim; set only while processing.
    claim_token: Mapped[uuid.UUID | None]
    # Set when the producer sends an Idempotency-Key header.
    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    request_fingerprint: Mapped[str | None] = mapped_column(String(64))
    # Result of the most recent attempt; every attempt is in delivery_attempts.
    last_http_status: Mapped[int | None]
    last_error_category: Mapped[str | None] = mapped_column(String(30))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    producer: Mapped[Producer] = relationship()
    attempts: Mapped[list["DeliveryAttempt"]] = relationship(order_by="DeliveryAttempt.number")


class DeliveryAttempt(Base):
    """One try at sending a delivery: created when claimed, finished when its result is saved."""

    __tablename__ = "delivery_attempts"
    __table_args__ = (
        UniqueConstraint("delivery_id", "number"),
        CheckConstraint(
            f"outcome IS NULL OR {one_of('outcome', AttemptOutcome)}", name="outcome_valid"
        ),
        CheckConstraint("(finished_at IS NULL) = (outcome IS NULL)", name="finished_with_outcome"),
    )

    # While the attempt is in progress, this is also the delivery's claim token.
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    delivery_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("deliveries.id", ondelete="CASCADE"))
    number: Mapped[int]
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(30))
    http_status: Mapped[int | None]
    error_category: Mapped[str | None] = mapped_column(String(30))
    duration_ms: Mapped[int | None]
