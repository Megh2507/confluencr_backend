import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from app.config import Settings
from app.main import create_app


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        database_url=f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}",
        processing_delay_seconds=0.5,
        sweep_interval_seconds=0.1,
        lease_grace_seconds=1.0,
        retry_backoff_seconds=0.1,
        max_attempts=3,
        # Generous so tests can assert on the duplicate flag; the load test uses the real deadline.
        ack_deadline_ms=5000,
    )


class CountingHandler:
    """Simulated external call that records every invocation."""

    def __init__(self, delay: float, failures: int = 0):
        self.delay = delay
        self.failures = failures
        self.calls: list[str] = []

    async def __call__(self, transaction) -> None:
        self.calls.append(transaction.transaction_id)
        if len(self.calls) <= self.failures:
            raise RuntimeError("simulated upstream failure")
        await asyncio.sleep(self.delay)


@asynccontextmanager
async def running_client(settings: Settings, handler=None):
    app = create_app(settings, handler)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


def webhook(transaction_id: str = "txn_abc123def456", **overrides) -> dict:
    body = {
        "transaction_id": transaction_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    body.update(overrides)
    return body


async def wait_for_status(client, transaction_id: str, expected: str | tuple[str, ...], timeout: float = 10.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        response = await client.get(f"/v1/transactions/{transaction_id}")
        if response.status_code == 200 and response.json()[0]["status"] in ((expected,) if isinstance(expected, str) else expected):
            return response.json()[0]
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"{transaction_id} never reached {expected}: {response.text}")
        await asyncio.sleep(0.05)
