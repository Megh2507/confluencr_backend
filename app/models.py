import enum
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import DateTime, Index, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator


class Base(DeclarativeBase):
    pass


class UTCDateTime(TypeDecorator):
    """Timezone-aware UTC datetimes on every backend.

    Postgres stores `timestamptz` natively; SQLite has no timezone support, so values are
    stored as naive UTC and re-tagged as UTC when loaded.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        if dialect.name == "sqlite":
            value = value.replace(tzinfo=None)
        return value

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


class TransactionStatus(str, enum.Enum):
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class Transaction(Base):
    __tablename__ = "transactions"

    # The primary key on transaction_id is what makes webhook ingestion idempotent.
    transaction_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_account: Mapped[str] = mapped_column(String(128))
    destination_account: Mapped[str] = mapped_column(String(128))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))

    status: Mapped[str] = mapped_column(String(16), default=TransactionStatus.PROCESSING.value)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    # Set while a worker owns the transaction; only the owner may complete or fail it.
    lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # While leased: lease expiry. After a failed attempt: earliest time of the next retry.
    locked_until: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime)
    processed_at: Mapped[datetime | None] = mapped_column(UTCDateTime, nullable=True)

    __table_args__ = (Index("ix_transactions_status_locked_until", "status", "locked_until"),)
