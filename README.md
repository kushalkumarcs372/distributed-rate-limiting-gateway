# Distributed API Gateway with Adaptive Rate Limiting

A multi-node API gateway demonstrating core distributed systems primitives:
consistent hashing, sliding-window rate limiting, idempotent request handling,
and a custom weighted least-connections load balancer.

## Architecture

```
                        ┌─────────────────┐
   Clients  ───────────▶│  Load Balancer   │  (custom, weighted least-conn)
                        │   :8080          │
                        └────────┬─────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 ▼               ▼               ▼
           ┌──────────┐   ┌──────────┐    ┌──────────┐
           │gateway-1 │   │gateway-2 │    │gateway-3 │   (stateless FastAPI nodes)
           └────┬─────┘   └────┬─────┘    └────┬─────┘
                │              │               │
        ┌───────┴──────────────┴───────────────┴────────┐
        ▼                                                ▼
  ┌──────────┐                                    ┌──────────────┐
  │  Redis   │  sliding-window rate-limit counters │  Postgres    │  idempotency
  │          │  (shared across all gateway nodes)  │              │  keys + audit
  └──────────┘                                     └──────────────┘
```

## Why each component

| Component | Why this, not the alternative |
|---|---|
| **Redis** for rate limiting | Rate-limit checks are on the hot path (every request). Needs sub-ms shared state across nodes. Postgres would add too much latency here; Redis's atomic INCR/pipeline ops are exactly the right primitive. |
| **Sliding-window-counter algorithm** | Fixed-window rate limiting allows a 2x burst at window boundaries (measured: naive fixed window let through up to 20 requests at a boundary vs. our 12, against a limit of 10). Sliding log is accurate but O(n) memory per client. Sliding window counter is O(1) memory and closely approximates sliding log. |
| **Postgres** for idempotency keys | This data must survive a restart and needs real ACID semantics for the race where two identical retried requests land at the same instant. `INSERT ... ON CONFLICT DO NOTHING` gives us an atomic compare-and-set with durability, which a non-persistent Redis config can't guarantee. |
| **Consistent hashing** for client→shard routing | Naive `hash(client) % N` remaps ~80% of clients when you scale nodes up/down (measured: 79.7%). Consistent hashing with virtual nodes remaps only ~1/N of clients (measured: 20.9% when going 4→5 nodes, vs a theoretical 20%). This matters because remapping a client means losing its rate-limit history — a scaling event shouldn't reset everyone's limits. |
| **Custom load balancer (weighted least-connections)** | Round-robin keeps sending traffic to a node even if it's currently slow/overloaded, causing pile-up. Least-connections naturally routes around whichever node is currently busiest. Weighting lets heterogeneous node sizes get proportional traffic. |

## Request flow (`POST /api/action`)

1. Load balancer picks the gateway node with fewest active connections
2. Gateway computes which "shard" conceptually owns this client via the consistent hash ring (surfaced in `X-Owning-Shard` header — in a bigger deployment this would pick which Redis instance to query, here it's exposed for demo/explanation purposes since we run a single shared Redis)
3. Gateway checks Redis sliding-window counter for this client → 429 if over limit
4. If an `Idempotency-Key` header is present, gateway checks Postgres:
   - key not seen before → do the work, store the response
   - key already completed → return the **stored** response, does not redo the work
   - key currently in-flight (race) → 409, ask client to retry shortly
5. Response returned with `X-RateLimit-Remaining`, `X-Owning-Shard`, `X-Handled-By` headers

## Running it

```bash
docker compose up --build
```

Wait for all services healthy, then:

```bash
python3 loadtest/loadtest.py
```

This runs, in order:
1. **Rate limit demo** — hammers one client with 15 rapid requests against a limit of 10, shows exactly how many get 429'd
2. **Idempotency demo** — sends the same `Idempotency-Key` twice, shows the second response is a replay (`idempotent_replay: true`), not a re-execution
3. **Load test** — 50 simulated clients × 20 requests each through the load balancer, prints throughput and p50/p95/max latency

Check load balancer's live view of backend health/connections:
```bash
curl http://localhost:8080/lb/status
```

## Proving the consistent hashing claim

```bash
python3 -m pytest tests/test_consistent_hash.py -v -s
```
Prints the actual remap percentages for naive `% N` hashing vs. consistent hashing when a node is added — this is the number to quote in an interview.

## Proving the rate limiter's boundary behavior

```bash
python3 -m pytest tests/test_rate_limiter.py -v -s
```

## Things I'd extend if this weren't a 5-day project

- Redis Cluster (actual sharded Redis, not single instance) — the consistent hash ring would then really route to different Redis nodes, not just annotate a header
- Raft-based leader election instead of a single load balancer instance (currently the LB itself is a single point of failure)
- Circuit breaker on the LB→gateway path, not just passive health checks
- Real backend business logic instead of `simulate_backend_work`

## Another real bug: startup race condition across instances

When all 3 gateway containers start simultaneously, each independently
runs `CREATE TABLE IF NOT EXISTS idempotency_keys` against Postgres at
nearly the same instant. Postgres's `IF NOT EXISTS` check is **not
atomic across separate connections** — two sessions can both see "table
doesn't exist yet" before either commits, so the second one crashes with
a duplicate-key error on Postgres's internal `pg_type` catalog, not a
friendly "already exists" message. This took down `gateway-1` on a clean
`docker compose up` while `gateway-2`/`gateway-3` happened to win the
race.

**Fix:** wrapped table creation in a Postgres advisory lock
(`pg_advisory_lock`/`pg_advisory_unlock`) — a lightweight, DB-backed
mutex. Whichever instance starts first acquires the lock and creates the
table; the other two block on the lock, then see the table already
exists once it's their turn, and no-op safely.

This is a genuinely common real-world problem (multiple app replicas
racing on schema migration at deploy time) and a legitimate thing to
mention in an interview — the fix pattern (DB-backed lock for
one-time-setup coordination) generalizes well beyond this specific case.

## Diagnosing environment vs. app bottlenecks (real debugging story)

Initial load tests from Windows via `localhost:8080` showed throughput
stuck around 20-48 req/s regardless of which fix was applied. Systematic
isolation:

1. Single `curl` to a gateway container directly: **8-27ms** — healthy.
2. 1000 concurrent requests to `/health` (touches no Redis/Postgres) via
   the load balancer: still ~35-41 req/s — same ceiling as the real
   endpoint.
3. 1000 concurrent requests **bypassing the load balancer entirely**,
   straight to a gateway container: **still ~48 req/s** — same ceiling.

Since the bottleneck persisted even when neither the rate limiter, the
idempotency store, nor the load balancer were in the request path, the
app code was ruled out. What's common to all three tests: **Docker
Desktop's Windows port-forwarding (vpnkit/WSL2 bridge)**, which is a
documented weak point under high concurrent connection counts, even
though individual requests are fast.

**To measure real app throughput, bypass Windows port-forwarding by
running the load test from inside the Docker network:**

```bash
docker compose exec loadbalancer python3 loadtest/loadtest.py
```

This runs the same script but hits `localhost:8080` from *inside* the
`loadbalancer` container itself (loopback, no host↔WSL2 hairpin) and can
target `http://gateway-1:8000` directly via Docker's internal DNS. This
isolates true request-handling throughput from the host networking
limitation.

## A real bug we found under load testing (good interview material)

Initial load test (1000 requests, 100 concurrent) showed throughput of only
**20 req/s** and p50 latency of **4.7 seconds** — wildly worse than the
<50ms of actual simulated work per request.

**Root cause:** `redis-py` (sync client) and `psycopg2` are blocking
libraries. Calling them directly inside FastAPI's `async def` endpoints
blocks the entire event loop for the call's duration — so "concurrent"
requests were secretly being processed **one at a time**, not in parallel.
The 100-way concurrency in the load test was fake; everything serialized
through blocking I/O calls.

**Fix:** wrapped every blocking call (`rate_limiter.allow`, idempotency
store methods, simulated backend work) in `asyncio.to_thread(...)`, which
hands the blocking call to a worker thread and frees the event loop to
serve other requests while waiting. Also added a Postgres connection pool
(`ThreadedConnectionPool`) instead of opening a fresh connection per query,
and switched the load balancer to reuse one pooled `httpx.AsyncClient`
instead of creating a new one per proxied request.

The "more correct" long-term fix would be fully async libraries
(`redis.asyncio`, `asyncpg`) instead of thread-offloading sync ones —
noted in the extensions section below.

This is worth stating explicitly in an interview: it demonstrates you can
diagnose a real concurrency bug from load-test numbers, not just recite
"use async" as a slogan.

## Known limitations (be ready to say these out loud — it's more credible than pretending it's perfect)

- Load balancer is itself not distributed/HA (single instance) — a production version would need multiple LB instances behind DNS/anycast or a cloud LB
- Sliding-window-counter is an *approximation*, not exact — under adversarial traffic patterns it can be off by a bounded amount (this is a known, accepted tradeoff, not a bug)
- Single shared Redis instance in this demo means the consistent-hash ring's routing decision doesn't yet gate real infrastructure — see extensions above
