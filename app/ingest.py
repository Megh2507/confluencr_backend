import asyncio
import logging

from app.repository import TransactionRepository
from app.schemas import TransactionWebhook
from app.worker import TransactionProcessor

logger = logging.getLogger(__name__)

# Backoff between attempts to persist an incoming webhook if the database hiccups.
_PERSIST_RETRY_DELAYS = (0.1, 0.5, 2.0)


class WebhookIngestor:
    """Persists incoming webhooks and hands new transactions to the processor.

    The write is durable *before* the 202 is sent in the normal case. If the database is
    slow, `accept()` stops waiting at the acknowledgement deadline so the 500ms response
    requirement still holds, while the write keeps running (and retrying) in the background.
    """

    def __init__(self, repository: TransactionRepository, processor: TransactionProcessor, ack_deadline_ms: int):
        self._repo = repository
        self._processor = processor
        self._deadline = ack_deadline_ms / 1000
        self._pending: set[asyncio.Task] = set()

    async def accept(self, payload: TransactionWebhook) -> bool | None:
        """Returns True if the transaction is new, False if it is a duplicate, or None if
        the write was still in progress at the deadline. Raises if it could not be stored."""
        task = asyncio.create_task(self._persist(payload))
        self._pending.add(task)
        task.add_done_callback(self._on_persist_done)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=self._deadline)
        except TimeoutError:
            logger.warning("Acknowledging %s before its write finished (deadline %.0fms)",
                           payload.transaction_id, self._deadline * 1000)
            return None

    async def drain(self, timeout: float = 10) -> None:
        """Give in-flight writes a chance to finish during shutdown."""
        if self._pending:
            await asyncio.wait(self._pending, timeout=timeout)

    async def _persist(self, payload: TransactionWebhook) -> bool:
        for attempt, delay in enumerate((*_PERSIST_RETRY_DELAYS, None), start=1):
            try:
                created = await self._repo.insert_if_absent(payload)
                break
            except Exception:
                if delay is None:
                    raise
                logger.warning("Storing %s failed (attempt %d); retrying in %.1fs",
                               payload.transaction_id, attempt, delay, exc_info=True)
                await asyncio.sleep(delay)
        if created:
            logger.info("Received transaction %s", payload.transaction_id)
            self._processor.submit(payload.transaction_id)
        else:
            logger.info("Ignoring duplicate webhook for %s", payload.transaction_id)
        return created

    def _on_persist_done(self, task: asyncio.Task) -> None:
        self._pending.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Failed to store webhook", exc_info=task.exception())
