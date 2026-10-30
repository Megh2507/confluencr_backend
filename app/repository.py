import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import case, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.models import Transaction, TransactionStatus
from app.schemas import TransactionWebhook

PROCESSING = TransactionStatus.PROCESSING.value
PROCESSED = TransactionStatus.PROCESSED.value
FAILED = TransactionStatus.FAILED.value


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _is_claimable(now: datetime):
    """Pending, not leased by a live worker, and not waiting out a retry backoff."""
    return (
        Transaction.status == PROCESSING,
        or_(Transaction.locked_until.is_(None), Transaction.locked_until <= now),
    )


class TransactionRepository:
    """All database access. Every state transition is a single conditional statement, so
    concurrent requests and workers (in one process or many) can never double-process."""

    def __init__(self, engine: AsyncEngine):
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._insert = pg_insert if engine.dialect.name == "postgresql" else sqlite_insert

    async def ping(self) -> None:
        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def insert_if_absent(self, payload: TransactionWebhook) -> bool:
        """Store a new transaction. Returns False if the transaction_id already exists."""
        now = utcnow()
        stmt = (
            self._insert(Transaction)
            .values(
                transaction_id=payload.transaction_id,
                source_account=payload.source_account,
                destination_account=payload.destination_account,
                amount=payload.amount,
                currency=payload.currency,
                status=PROCESSING,
                attempts=0,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=[Transaction.transaction_id])
            .returning(Transaction.transaction_id)
        )
        async with self._sessions() as session:
            inserted = (await session.execute(stmt)).scalar_one_or_none()
            await session.commit()
        return inserted is not None

    async def get(self, transaction_id: str) -> Transaction | None:
        async with self._sessions() as session:
            return await session.get(Transaction, transaction_id)

    async def find_claimable(self, limit: int) -> list[str]:
        stmt = (
            select(Transaction.transaction_id)
            .where(*_is_claimable(utcnow()))
            .order_by(Transaction.created_at)
            .limit(limit)
        )
        async with self._sessions() as session:
            return list((await session.execute(stmt)).scalars())

    async def claim(self, transaction_id: str, lease_seconds: float) -> str | None:
        """Atomically take ownership of a pending transaction.

        Returns a lease token, or None if the transaction is already processed, leased by
        another worker, or waiting for a retry.
        """
        now = utcnow()
        token = str(uuid.uuid4())
        stmt = (
            update(Transaction)
            .where(Transaction.transaction_id == transaction_id, *_is_claimable(now))
            .values(
                lease_token=token,
                locked_until=now + timedelta(seconds=lease_seconds),
                attempts=Transaction.attempts + 1,
                updated_at=now,
            )
            .returning(Transaction.transaction_id)
        )
        async with self._sessions() as session:
            claimed = (await session.execute(stmt)).scalar_one_or_none()
            await session.commit()
        return token if claimed is not None else None

    async def complete(self, transaction_id: str, token: str) -> bool:
        """Mark a leased transaction PROCESSED. Returns False if the lease was lost."""
        now = utcnow()
        stmt = (
            update(Transaction)
            .where(
                Transaction.transaction_id == transaction_id,
                Transaction.lease_token == token,
                Transaction.status == PROCESSING,
            )
            .values(
                status=PROCESSED,
                processed_at=now,
                updated_at=now,
                lease_token=None,
                locked_until=None,
                last_error=None,
            )
        )
        async with self._sessions() as session:
            result = await session.execute(stmt)
            await session.commit()
        return result.rowcount == 1

    async def record_failure(
        self, transaction_id: str, token: str, error: str, retry_at: datetime, max_attempts: int
    ) -> None:
        """Release a failed attempt: schedule a retry, or give up after max_attempts."""
        stmt = (
            update(Transaction)
            .where(Transaction.transaction_id == transaction_id, Transaction.lease_token == token)
            .values(
                status=case((Transaction.attempts >= max_attempts, FAILED), else_=PROCESSING),
                lease_token=None,
                locked_until=retry_at,
                last_error=error[:2000],
                updated_at=utcnow(),
            )
        )
        async with self._sessions() as session:
            await session.execute(stmt)
            await session.commit()

    async def release(self, leases: dict[str, str]) -> None:
        """Hand leases back (e.g. on shutdown) so another worker can resume immediately."""
        if not leases:
            return
        async with self._sessions() as session:
            for transaction_id, token in leases.items():
                await session.execute(
                    update(Transaction)
                    .where(Transaction.transaction_id == transaction_id, Transaction.lease_token == token)
                    # An interrupted attempt is not a failed one.
                    .values(
                        lease_token=None,
                        locked_until=None,
                        attempts=Transaction.attempts - 1,
                        updated_at=utcnow(),
                    )
                )
            await session.commit()
