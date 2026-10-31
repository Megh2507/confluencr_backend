# Transaction Webhook Service

A FastAPI service that receives transaction webhooks from payment processors (e.g. Razorpay),
acknowledges them immediately with `202 Accepted`, and processes each transaction **exactly once**
in the background (with a simulated 30-second external call), storing the result in Postgres.

**Live API:** `https://transaction-webhook-service-b1x1.onrender.com` · interactive docs at `https://transaction-webhook-service-b1x1.onrender.com/docs`

---

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Health check → `{"status": "HEALTHY", "current_time": "2024-01-15T10:30:00.000Z"}` (503 + `UNHEALTHY` if the database is unreachable) |
| `POST` | `/v1/webhooks/transactions` | Accept a transaction webhook → `202 Accepted` |
| `GET` | `/v1/transactions/{transaction_id}` | Transaction status (array with one object), `404` if unknown |

Webhook body:

```json
{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500,
  "currency": "INR"
}
```

Webhook response (`202`):

```json
{ "status": "ACCEPTED", "transaction_id": "txn_abc123def456", "duplicate": false }
```

`duplicate` is `true` when the `transaction_id` was already received. It is `null` in the rare case
where the database write was still running when the acknowledgement deadline passed (see below).
Invalid payloads get a `422`. Unknown extra fields are ignored. Every response carries an
`X-Response-Time-Ms` header so latency can be checked from outside.

Transaction status response:

```json
[{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500.0,
  "currency": "INR",
  "status": "PROCESSED",
  "created_at": "2024-01-15T10:30:00.000Z",
  "processed_at": "2024-01-15T10:30:30.012Z"
}]
```

`status` is `PROCESSING` until the work finishes, then `PROCESSED`. A transaction that keeps failing
is retried with exponential backoff and becomes `FAILED` after `MAX_ATTEMPTS` (default 5).

---

## Running locally

Requires Python 3.12+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate     macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt

uvicorn app.main:app --reload
```

With no configuration the service uses a local SQLite file (`transactions.db`). To use Postgres,
copy `.env.example` to `.env` and set `DATABASE_URL`. A connection string straight from Neon,
Supabase or Render works as-is; it is converted to the async driver automatically.

### Trying it by hand

```bash
curl http://localhost:8000/

curl -i -X POST http://localhost:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl http://localhost:8000/v1/transactions/txn_abc123def456   # PROCESSING, then PROCESSED after ~30s
```

Or open `http://localhost:8000/docs` for Swagger UI.

## Testing

**Automated tests:** `pytest` (21 tests, about 10s). They run against a temporary SQLite database
with a shortened processing delay and cover:

- the single-transaction lifecycle and its timing
- duplicates sent one after another, in parallel, and after processing has finished
- racing first deliveries
- webhook latency while 100 transactions are being processed
- retry after a failure, and giving up after `MAX_ATTEMPTS`
- recovery after a crash, including a lease left behind by a dead worker
- restart during processing
- two instances sharing one database
- input validation and 404s

**End-to-end smoke test** against any running instance, local or deployed. It uses the real
30-second delay:

```bash
python scripts/smoke_test.py http://localhost:8000
python scripts/smoke_test.py https://transaction-webhook-service-b1x1.onrender.com --load 100
```

It checks the four success criteria:

1. **Health:** the health check returns `HEALTHY`.
2. **Single transaction:** it shows `PROCESSING` immediately and is `PROCESSED` about 30s after receipt.
3. **Duplicate prevention:** 15 copies of one webhook give 1 new and 14 duplicates, with one row stored and one processing run.
4. **Performance and reliability:** it reports p50/p95/max webhook latency for a burst of concurrent
   webhooks, then confirms every one of them gets processed.

Sample output from a local run:

```
2. Single transaction + 3. duplicate prevention
   15 webhooks for txn_smoke_57043c68: all 202, 1 new / 14 duplicate
   immediately: PROCESSING, processed_at=None
   ...
   processed 30.0s after receipt
   all 200 processed, none lost
```

---

## Deployment

The repo has a [`render.yaml`](render.yaml) blueprint and a [`Dockerfile`](Dockerfile), so it runs
on Render, Railway, Fly.io, Cloud Run with always-on CPU, or any container host.

1. Create a free Postgres database on [Neon](https://neon.tech) and copy its connection string.
2. On [Render](https://render.com), choose **New → Blueprint** and select this repository.
3. Set `DATABASE_URL` to the Neon connection string. The table is created on startup.

---

## Technical choices

### Architecture

```
 POST /v1/webhooks/transactions
        │
        ▼
 INSERT … ON CONFLICT (transaction_id) DO NOTHING   ── durable before the 202 is sent
        │ new row?
        ▼
 processor.submit(id) ──► claim lease (atomic UPDATE) ──► 30s external call ──► mark PROCESSED
                               ▲                                                   (only if the
 sweeper, every 5s ────────────┘  pending / retryable / orphaned rows              lease is still ours)
```

**FastAPI + asyncio.** The workload is almost entirely waiting: a database round trip on ingest
and a 30-second simulated I/O call during processing. One async process can keep hundreds of
transactions in flight with negligible CPU. The webhook handler never waits on processing, so its
latency doesn't depend on how much processing is in progress.

**Postgres as both the store and the job queue.** I used no separate broker such as
Redis/Celery or SQS. The `transactions` table holds the processing state (`status`,
`lease_token`, `locked_until`, `attempts`), so "is it stored?" and "is it queued?" are the same
fact, committed in one statement. You can't end up with a stored-but-never-queued transaction,
or a queued one that was never stored. That's one fewer moving part to deploy and fail.
Neon was chosen as the managed database because its free tier has no expiry.

### Idempotency: two layers

1. **Ingest.** `transaction_id` is the primary key, and webhooks are written with
   `INSERT … ON CONFLICT DO NOTHING RETURNING`. However many copies arrive, and however they race,
   the database lets exactly one insert succeed. Duplicates are answered with a normal 202
   (`"duplicate": true`) and no errors, because a payment processor that gets a non-2xx keeps
   retrying.
2. **Processing.** A worker must *claim* a transaction before working on it. The claim is a single
   conditional `UPDATE … WHERE status='PROCESSING' AND (locked_until IS NULL OR locked_until <= now)`.
   It sets a random `lease_token` and a lease expiry, so only one claimer can win. Completion is
   `UPDATE … WHERE lease_token = <mine>`, so a worker whose lease was taken over cannot overwrite
   the result. This holds across several processes or instances, which the test suite checks
   with two app instances on one database.

### Meeting the 500ms response time

The handler only does one indexed insert, which takes single-digit milliseconds when the service
and database are in the same region. The slow part (30s) always happens after the response.

As a safeguard against database latency spikes, such as a serverless database waking from
idle, the insert runs as a shielded task with a deadline (`ACK_DEADLINE_MS`, default 300ms).
If the write hasn't finished by then, the service acknowledges anyway and the write completes
and retries in the background. This is a conscious trade-off: in that narrow window an ack is
sent before the row is durable. If the database is *failing* rather than slow and the error
arrives before the deadline, the service returns `503` without acknowledging, so the processor
redelivers. Idempotency makes that redelivery harmless.

### Reliability: no transaction is lost

- **Nothing is held only in memory.** A transaction is committed before processing starts. The
  in-process task is only a fast path; the database row is the source of truth.
- **Crash or restart mid-processing.** On shutdown, held leases are released right away. On a
  hard crash they expire after `processing_delay + LEASE_GRACE_SECONDS`. Either way the sweeper
  of a surviving or restarted instance claims the transaction again. It also runs immediately on
  startup.
- **Processing failure.** The attempt is recorded with its error and retried with exponential
  backoff (`RETRY_BACKOFF_SECONDS × 2^(n−1)`, capped at 10 minutes). After `MAX_ATTEMPTS` the
  status becomes `FAILED` instead of retrying forever.
- **Transient database errors** on ingest are retried before giving up.
- **Bounded concurrency.** `MAX_CONCURRENT_JOBS` limits in-flight processing per instance. Excess
  work stays in the table until there is capacity.

Delivery to the external call is at-least-once: a worker that freezes past its lease could overlap
with a re-claim. The lease token keeps the *stored result* single, and in a real system the
external call would carry `transaction_id` as its idempotency key.

### Project layout

```
app/
  main.py        app factory, lifespan (DB init, processor start/stop), timing middleware
  api.py         the three endpoints
  ingest.py      durable webhook acceptance with the ack deadline
  worker.py      background processor: lease, simulated external call, retries, sweeper
  repository.py  all SQL: idempotent insert, claim, complete, fail, release
  models.py      SQLAlchemy model (UTC timestamps on every backend)
  schemas.py     request/response models and validation
  db.py          engine creation, provider URL normalisation (sslmode → asyncpg)
  config.py      environment-based settings
tests/           pytest suite
scripts/         end-to-end smoke test for a live deployment
```

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local SQLite | Postgres connection string (Neon/Supabase/Render format accepted) |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external call duration |
| `ACK_DEADLINE_MS` | `300` | Max time the webhook waits for its DB write before acknowledging |
| `SWEEP_INTERVAL_SECONDS` | `5` | How often pending/orphaned transactions are picked up |
| `LEASE_GRACE_SECONDS` | `30` | Lease length beyond the processing delay |
| `MAX_ATTEMPTS` / `RETRY_BACKOFF_SECONDS` | `5` / `5` | Retry policy |
| `MAX_CONCURRENT_JOBS` | `500` | Per-instance processing concurrency |

### What I'd add for production

- Webhook signature verification (e.g. Razorpay's `X-Razorpay-Signature` HMAC)
- Alembic migrations instead of `create_all`
- Metrics on queue depth, processing latency and failures
- A dead-letter view for `FAILED` transactions
