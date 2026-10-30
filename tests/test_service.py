import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from app.db import create_engine, init_models, normalize_database_url
from app.repository import TransactionRepository, utcnow
from app.schemas import TransactionWebhook
from tests.conftest import CountingHandler, running_client, wait_for_status, webhook


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


async def test_health_check(settings):
    async with running_client(settings) as client:
        response = await client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "HEALTHY"
    assert body["current_time"].endswith("Z")
    assert abs(parse(body["current_time"]) - datetime.now(timezone.utc)) < timedelta(seconds=5)


async def test_single_transaction_is_processed_after_delay(settings):
    handler = CountingHandler(delay=settings.processing_delay_seconds)
    async with running_client(settings, handler) as client:
        response = await client.post("/v1/webhooks/transactions", json=webhook())
        assert response.status_code == 202
        assert response.json() == {"status": "ACCEPTED", "transaction_id": "txn_abc123def456", "duplicate": False}

        pending = (await client.get("/v1/transactions/txn_abc123def456")).json()
        assert len(pending) == 1
        assert pending[0]["status"] == "PROCESSING"
        assert pending[0]["processed_at"] is None

        done = await wait_for_status(client, "txn_abc123def456", "PROCESSED")

    assert done == {
        "transaction_id": "txn_abc123def456",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500.0,
        "currency": "INR",
        "status": "PROCESSED",
        "created_at": done["created_at"],
        "processed_at": done["processed_at"],
    }
    elapsed = parse(done["processed_at"]) - parse(done["created_at"])
    assert elapsed >= timedelta(seconds=settings.processing_delay_seconds)
    assert handler.calls == ["txn_abc123def456"]


async def test_duplicate_webhooks_are_processed_once(settings):
    handler = CountingHandler(delay=settings.processing_delay_seconds)
    async with running_client(settings, handler) as client:
        first = await client.post("/v1/webhooks/transactions", json=webhook())
        sequential = [await client.post("/v1/webhooks/transactions", json=webhook()) for _ in range(5)]
        concurrent = await asyncio.gather(
            *(client.post("/v1/webhooks/transactions", json=webhook()) for _ in range(20))
        )
        await wait_for_status(client, "txn_abc123def456", "PROCESSED")
        # Duplicates arriving after processing are also ignored.
        late = await client.post("/v1/webhooks/transactions", json=webhook())
        await asyncio.sleep(settings.processing_delay_seconds + 0.5)
        rows = (await client.get("/v1/transactions/txn_abc123def456")).json()

    assert first.json()["duplicate"] is False
    for response in [*sequential, *concurrent, late]:
        assert response.status_code == 202
        assert response.json()["duplicate"] is True
    assert len(rows) == 1 and rows[0]["status"] == "PROCESSED"
    assert handler.calls == ["txn_abc123def456"]


async def test_concurrent_duplicate_first_delivery(settings):
    """The very first deliveries race each other: exactly one may win."""
    handler = CountingHandler(delay=settings.processing_delay_seconds)
    async with running_client(settings, handler) as client:
        responses = await asyncio.gather(
            *(client.post("/v1/webhooks/transactions", json=webhook("txn_race")) for _ in range(25))
        )
        await wait_for_status(client, "txn_race", "PROCESSED")

    assert all(r.status_code == 202 for r in responses)
    assert sum(r.json()["duplicate"] is False for r in responses) == 1
    assert handler.calls == ["txn_race"]


async def test_webhook_responds_fast_under_processing_load(settings):
    settings.processing_delay_seconds = 3.0
    settings.ack_deadline_ms = 300
    async with running_client(settings) as client:
        async def timed_post(i: int) -> float:
            started = time.perf_counter()
            response = await client.post("/v1/webhooks/transactions", json=webhook(f"txn_load_{i}"))
            assert response.status_code == 202
            return time.perf_counter() - started

        latencies = await asyncio.gather(*(timed_post(i) for i in range(100)))
        # Writes that missed the ack deadline finish in the background; none may be lost.
        stored = await asyncio.gather(
            *(wait_for_status(client, f"txn_load_{i}", ("PROCESSING", "PROCESSED"), timeout=30) for i in range(100))
        )

    assert max(latencies) < 0.5, f"slowest webhook took {max(latencies):.3f}s"
    assert len(stored) == 100


async def test_failed_processing_is_retried(settings):
    handler = CountingHandler(delay=0.1, failures=1)
    async with running_client(settings, handler) as client:
        await client.post("/v1/webhooks/transactions", json=webhook("txn_flaky"))
        done = await wait_for_status(client, "txn_flaky", "PROCESSED")
    assert done["processed_at"] is not None
    assert handler.calls == ["txn_flaky", "txn_flaky"]


async def test_gives_up_after_max_attempts(settings):
    handler = CountingHandler(delay=0.1, failures=100)
    async with running_client(settings, handler) as client:
        await client.post("/v1/webhooks/transactions", json=webhook("txn_broken"))
        done = await wait_for_status(client, "txn_broken", "FAILED")
    assert done["processed_at"] is None
    assert len(handler.calls) == settings.max_attempts


async def test_transactions_survive_a_crash(settings):
    """A transaction stored (or leased by a worker that died) is picked up on restart."""
    engine = create_engine(settings.database_url)
    await init_models(engine)
    repo = TransactionRepository(engine)
    await repo.insert_if_absent(TransactionWebhook(**webhook("txn_never_started")))
    await repo.insert_if_absent(TransactionWebhook(**webhook("txn_orphaned")))
    # Simulate a worker that claimed the transaction and then crashed: its lease expires.
    assert await repo.claim("txn_orphaned", lease_seconds=0.2) is not None
    await engine.dispose()

    handler = CountingHandler(delay=0.1)
    async with running_client(settings, handler) as client:
        await wait_for_status(client, "txn_never_started", "PROCESSED")
        await wait_for_status(client, "txn_orphaned", "PROCESSED")
    assert sorted(handler.calls) == ["txn_never_started", "txn_orphaned"]


async def test_shutdown_mid_processing_resumes_on_restart(settings):
    first = CountingHandler(delay=60)
    async with running_client(settings, first) as client:
        await client.post("/v1/webhooks/transactions", json=webhook("txn_interrupted"))
        await asyncio.sleep(0.3)
    assert first.calls == ["txn_interrupted"]

    second = CountingHandler(delay=0.1)
    async with running_client(settings, second) as client:
        await wait_for_status(client, "txn_interrupted", "PROCESSED")
    assert second.calls == ["txn_interrupted"]


async def test_two_instances_process_each_transaction_once(settings):
    handler = CountingHandler(delay=0.3)
    async with running_client(settings, handler) as a, running_client(settings, handler) as b:
        for i in range(20):
            client = a if i % 2 else b
            await client.post("/v1/webhooks/transactions", json=webhook(f"txn_multi_{i}"))
            await (b if i % 2 else a).post("/v1/webhooks/transactions", json=webhook(f"txn_multi_{i}"))
        for i in range(20):
            await wait_for_status(a, f"txn_multi_{i}", "PROCESSED")
    assert sorted(handler.calls) == sorted(f"txn_multi_{i}" for i in range(20))


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": -5},
        {"amount": "abc"},
        {"currency": "RUPEES"},
        {"transaction_id": ""},
        {"source_account": None},
    ],
)
async def test_invalid_payload_is_rejected(settings, overrides):
    async with running_client(settings) as client:
        response = await client.post("/v1/webhooks/transactions", json=webhook(**overrides))
    assert response.status_code == 422


async def test_unknown_transaction_returns_404(settings):
    async with running_client(settings) as client:
        response = await client.get("/v1/transactions/txn_missing")
    assert response.status_code == 404


async def test_decimal_amount_and_currency_normalisation(settings):
    async with running_client(settings) as client:
        await client.post("/v1/webhooks/transactions", json=webhook("txn_usd", amount=150.50, currency="usd"))
        row = (await client.get("/v1/transactions/txn_usd")).json()[0]
    assert row["amount"] == 150.5
    assert row["currency"] == "USD"


@pytest.mark.parametrize(
    "raw, url, connect_args",
    [
        (
            "postgresql://u:p@ep-x.neon.tech/db?sslmode=require&channel_binding=require",
            "postgresql+asyncpg://u:p@ep-x.neon.tech/db",
            {"ssl": "require"},
        ),
        ("postgres://u:p@localhost:5432/db", "postgresql+asyncpg://u:p@localhost:5432/db", {}),
        ("sqlite:///./local.db", "sqlite+aiosqlite:///./local.db", {"timeout": 30}),
    ],
)
def test_normalize_database_url(raw, url, connect_args):
    assert normalize_database_url(raw) == (url, connect_args)


def test_utcnow_is_aware():
    assert utcnow().tzinfo is not None
