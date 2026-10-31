import asyncio
import logging
from datetime import timedelta
from typing import Awaitable, Callable

from app.config import Settings
from app.models import Transaction
from app.repository import TransactionRepository, utcnow

logger = logging.getLogger(__name__)

TransactionHandler = Callable[[Transaction], Awaitable[None]]


def simulated_external_call(delay_seconds: float) -> TransactionHandler:
    """The 'real work' for a transaction: stands in for calls to banks/ledgers/fraud checks."""

    async def handle(transaction: Transaction) -> None:
        await asyncio.sleep(delay_seconds)

    return handle


class TransactionProcessor:
    """Processes stored transactions in the background.

    Work arrives two ways:
      * `submit()` - called right after a new webhook is stored, so processing starts at once.
      * the sweeper - periodically polls the database for anything still pending (startup
        recovery, retries after failures, transactions orphaned by a crashed instance).

    Both paths go through `TransactionRepository.claim()`, an atomic conditional UPDATE, so
    a transaction is only ever processed by one worker at a time, across any number of
    processes or instances sharing the database.
    """

    def __init__(
        self,
        repository: TransactionRepository,
        settings: Settings,
        handler: TransactionHandler | None = None,
    ):
        self._repo = repository
        self._settings = settings
        self._handler = handler or simulated_external_call(settings.processing_delay_seconds)
        self._lease_seconds = settings.processing_delay_seconds + settings.lease_grace_seconds
        self._slots = asyncio.Semaphore(settings.max_concurrent_jobs)
        self._scheduled: set[str] = set()
        self._tasks: set[asyncio.Task] = set()
        self._leases: dict[str, str] = {}
        self._sweeper: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        self._sweeper = asyncio.create_task(self._sweep_forever(), name="transaction-sweeper")

    async def stop(self) -> None:
        self._stopping = True
        tasks = [t for t in (self._sweeper, *self._tasks) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await asyncio.wait_for(self._repo.release(dict(self._leases)), timeout=5)
        except Exception:
            # Leases expire on their own; another instance (or our next start) will resume.
            logger.warning("Could not release %d lease(s) on shutdown", len(self._leases), exc_info=True)

    def submit(self, transaction_id: str) -> None:
        if self._stopping or transaction_id in self._scheduled:
            return
        self._scheduled.add(transaction_id)
        task = asyncio.create_task(self._run(transaction_id), name=f"process-{transaction_id}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _sweep_forever(self) -> None:
        while True:
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Sweep failed; will retry")
            await asyncio.sleep(self._settings.sweep_interval_seconds)

    async def sweep_once(self) -> None:
        capacity = self._settings.max_concurrent_jobs - len(self._scheduled)
        if capacity <= 0:
            return
        for transaction_id in await self._repo.find_claimable(limit=capacity):
            self.submit(transaction_id)

    async def _run(self, transaction_id: str) -> None:
        try:
            async with self._slots:
                await self._process(transaction_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Typically the database being unreachable. The lease (if any) expires and the
            # sweeper retries later, so nothing is lost.
            logger.exception("Error while processing transaction %s", transaction_id)
        finally:
            self._scheduled.discard(transaction_id)

    async def _process(self, transaction_id: str) -> None:
        transaction = await self._repo.claim(transaction_id, self._lease_seconds)
        if transaction is None:
            return  # already processed, or owned by another worker
        token = transaction.lease_token
        self._leases[transaction_id] = token
        try:
            logger.info("Processing transaction %s (attempt %d)", transaction_id, transaction.attempts)
            try:
                await self._handler(transaction)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._record_failure(transaction, token, exc)
                return
            if await self._repo.complete(transaction_id, token):
                logger.info("Transaction %s processed", transaction_id)
            else:
                logger.warning("Lost lease on transaction %s before completing it", transaction_id)
        finally:
            self._leases.pop(transaction_id, None)

    async def _record_failure(self, transaction: Transaction, token: str, exc: Exception) -> None:
        attempt = transaction.attempts
        backoff = self._settings.retry_backoff_seconds * 2 ** (attempt - 1)
        retry_at = utcnow() + timedelta(seconds=min(backoff, 600))
        if attempt >= self._settings.max_attempts:
            logger.error("Transaction %s failed permanently after %d attempts: %r",
                         transaction.transaction_id, attempt, exc)
        else:
            logger.warning("Transaction %s attempt %d failed (%r); retrying at %s",
                           transaction.transaction_id, attempt, exc, retry_at.isoformat())
        await self._repo.record_failure(
            transaction.transaction_id, token, repr(exc), retry_at, self._settings.max_attempts
        )
