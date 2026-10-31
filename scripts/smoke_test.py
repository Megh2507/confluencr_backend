"""End-to-end check of a running deployment against the assessment's success criteria.

    python scripts/smoke_test.py https://your-service.onrender.com
    python scripts/smoke_test.py http://localhost:8000 --load 200

Checks: health, single transaction (processed after ~30s), duplicate prevention,
webhook latency under load, and that no load-test transaction is lost.
"""
import argparse
import asyncio
import ssl
import statistics
import time
import uuid
from datetime import datetime

import httpx


def payload(transaction_id: str) -> dict:
    return {
        "transaction_id": transaction_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }


def parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


async def timed_post(client: httpx.AsyncClient, body: dict) -> tuple[httpx.Response, float]:
    started = time.perf_counter()
    response = await client.post("/v1/webhooks/transactions", json=body)
    return response, (time.perf_counter() - started) * 1000


async def wait_until_processed(client: httpx.AsyncClient, transaction_id: str, timeout: float) -> list[dict]:
    deadline = time.monotonic() + timeout
    while True:
        response = await client.get(f"/v1/transactions/{transaction_id}")
        if response.status_code == 200 and response.json()[0]["status"] == "PROCESSED":
            return response.json()
        if time.monotonic() > deadline:
            raise AssertionError(f"{transaction_id} not processed in {timeout}s: {response.text}")
        await asyncio.sleep(1)


async def main(base_url: str, load: int) -> None:
    run = uuid.uuid4().hex[:8]
    async with httpx.AsyncClient(base_url=base_url, timeout=30, verify=ssl.create_default_context()) as client:
        print("1. Health check")
        health = await client.get("/")
        print(f"   {health.status_code} {health.json()}")
        assert health.status_code == 200 and health.json()["status"] == "HEALTHY"

        print("2. Single transaction + 3. duplicate prevention")
        txn = f"txn_smoke_{run}"
        responses = [await timed_post(client, payload(txn)) for _ in range(5)]
        responses += await asyncio.gather(*(timed_post(client, payload(txn)) for _ in range(10)))
        for response, ms in responses:
            assert response.status_code == 202, response.text
        print(f"   15 webhooks for {txn}: all 202, "
              f"{sum(r.json()['duplicate'] is False for r, _ in responses)} new / "
              f"{sum(r.json()['duplicate'] is True for r, _ in responses)} duplicate, "
              f"slowest {max(ms for _, ms in responses):.0f}ms")
        status = (await client.get(f"/v1/transactions/{txn}")).json()
        print(f"   immediately: {status[0]['status']}, processed_at={status[0]['processed_at']}")

        print(f"4. Load: {load} concurrent webhooks while processing is in flight")
        load_ids = [f"txn_load_{run}_{i}" for i in range(load)]
        results = await asyncio.gather(*(timed_post(client, payload(i)) for i in load_ids))
        latencies = sorted(ms for _, ms in results)
        assert all(r.status_code == 202 for r, _ in results)
        print(f"   client-side latency ms: p50={statistics.median(latencies):.0f} "
              f"p95={latencies[int(len(latencies) * 0.95) - 1]:.0f} max={latencies[-1]:.0f} "
              f"(includes network round trip)")
        server_ms = sorted(float(r.headers.get("x-response-time-ms", "nan")) for r, _ in results)
        print(f"   server-side time ms:    p50={statistics.median(server_ms):.1f} max={server_ms[-1]:.1f}")

        print("   waiting for the single transaction to be processed (~30s)...")
        rows = await wait_until_processed(client, txn, timeout=120)
        took = (parse(rows[0]["processed_at"]) - parse(rows[0]["created_at"])).total_seconds()
        assert len(rows) == 1
        print(f"   {rows[0]}")
        print(f"   processed {took:.1f}s after receipt")

        print("   checking every load transaction gets processed...")
        await asyncio.gather(*(wait_until_processed(client, i, timeout=180) for i in load_ids))
        print(f"   all {load} processed, none lost")

    print("\nAll checks passed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url", nargs="?", default="http://localhost:8000")
    parser.add_argument("--load", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(main(args.base_url.rstrip("/"), args.load))
