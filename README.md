# Distributed API Gateway with Adaptive Rate Limiting

A multi-node API gateway demonstrating core distributed systems primitives:
consistent hashing, sliding-window rate limiting, idempotent request handling,
and a custom weighted least-connections load balancer.

## Architecture

```
                        ┌─────────────────┐
   Clients  ───────────▶│  Load Balancer   │  (custom, weighted least-conn
                        │   :8080          │   + circuit breaker per backend)
                        └────────┬─────────┘
                                 │
                 ┌───────────────┼───────────────┐
                 ▼               ▼               ▼
           ┌──────────┐   ┌──────────┐    ┌──────────┐
           │gateway-1 │   │gateway-2 │    │gateway-3 │   (stateless FastAPI nodes)
           └────┬─────┘   └────┬─────┘    └────┬─────┘
                │              │               │
        ┌───────┴──────────────┴───────────────┴────────┐
        │  consistent hash ring picks ONE of 3 shards    │
        ▼                                                ▼
 ┌────────────────────────────┐                  ┌──────────────┐
 │ redis-shard-1 / -2 / -3    │  rate-limit       │  Postgres    │  idempotency
 │ (client-side sharded via   │  counters, per    │              │  keys + audit
 │  consistent hashing)       │  client's shard   │              │  + orders/inventory
 └────────────────────────────┘                   └──────────────┘
```

## Why each component

| Component | Why this, not the alternative |
|---|---|
| **Redis (client-side sharded across 3 instances)** for rate limiting | Rate-limit checks are on the hot path (every request). Needs sub-ms shared state. We deliberately did NOT implement real Redis Cluster protocol (16384 hash slots, gossip, MOVED/ASK redirects) — that's a much larger correctness undertaking. Instead, our own consistent hash ring picks ONE of 3 independent Redis instances per client — a legitimate, widely-used pattern (this is what Twemproxy/mcrouter do, and what many companies ran before Redis Cluster existed). Tradeoff accepted: no automatic replication/failover between shards, unlike real Redis Cluster. |
| **Sliding-window-counter algorithm** | Fixed-window rate limiting allows a 2x burst at window boundaries (measured: naive fixed window let through up to 20 requests at a boundary vs. our 10-12, against a limit of 10). Sliding log is accurate but O(n) memory per client. Sliding window counter is O(1) memory and closely approximates sliding log. |
| **Postgres** for idempotency keys AND for orders/inventory | This data must survive a restart and needs real ACID semantics for the race where two identical retried requests land at the same instant. `INSERT ... ON CONFLICT DO NOTHING` gives us an atomic compare-and-set with durability. Orders/inventory additionally need row-level locking (`SELECT ... FOR UPDATE`) so two concurrent orders for the same product can't both oversell stock. |
| **Consistent hashing** for client→Redis-shard routing | Naive `hash(client) % N` remaps ~80% of clients when you scale nodes up/down (measured: 79.5-79.7%). Consistent hashing with virtual nodes remaps only ~1/N of clients (measured: 20.9% when going 4→5 nodes, vs a theoretical 20%). This now genuinely determines which physical Redis instance a client's rate-limit data lives on — not just an annotated header. |
| **Custom load balancer (weighted least-connections + circuit breaker)** | Round-robin keeps sending traffic to a node even if it's currently slow/overloaded, causing pile-up. Least-connections naturally routes around whichever node is currently busiest. A circuit breaker adds fast reaction to *live request failures* on top of that — the passive health check only polls every 3s, so a circuit breaker (CLOSED → OPEN → HALF_OPEN) stops sending traffic to a failing backend immediately instead of waiting for the next poll. |

## Request flow (`POST /api/orders`)

1. Load balancer picks the gateway node with fewest active connections
   (skipping any backend whose circuit breaker is currently OPEN)
2. Gateway computes which Redis SHARD actually owns this client via the
   consistent hash ring (`X-Redis-Shard` header) — this is a real routing
   decision now, not just a demo annotation
3. Gateway checks that shard's Redis sliding-window counter → 429 if over limit
4. If an `Idempotency-Key` header is present, gateway checks Postgres:
   - key not seen before → do the work, store the response
   - key already completed → return the **stored** response, does not redo the work
   - key currently in-flight (race) → 409, ask client to retry shortly
5. "The work" is a real operation: place an order against limited
   inventory (`{"product_id": "widget-a", "quantity": 2}`), using
   `SELECT ... FOR UPDATE` row locking so concurrent orders for the same
   product can't oversell stock
6. Response returned with `X-RateLimit-Remaining`, `X-Redis-Shard`,
   `X-Handled-By` headers

Check current inventory: `GET /api/inventory`

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

## Testing the new features

**Redis sharding** — place a few orders as different clients and check
the `X-Redis-Shard` response header; different `X-Client-Id` values
should land on different shards:
```bash
curl -X POST http://localhost:8080/api/orders \
  -H "X-Client-Id: alice" -H "Content-Type: application/json" \
  -d '{"product_id":"widget-a","quantity":2}' -i
```

**Circuit breaker** — stop one gateway container and watch the LB stop
routing to it after 3 consecutive failures:
```bash
docker compose stop gateway-2
# then send a few requests and check:
curl http://localhost:8080/lb/status
# gateway-2 should show circuit_state: "OPEN" after a few failed attempts
docker compose start gateway-2
```

**Real inventory/orders** — place enough orders to exhaust stock and see
a real rejection (not a fake success):
```bash
curl http://localhost:8080/api/inventory  # see current stock
```

**Circuit breaker unit tests** (no Docker needed):
```bash
python3 -m pytest tests/test_circuit_breaker.py -v -s
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

## Extensions completed after the initial 5-day build

Originally listed as future work, then actually implemented:

- ✅ **Real Redis sharding** — 3 independent Redis instances, client-side
  sharded via the consistent hash ring (`gateway/redis_shard_router.py`).
  The ring's routing decision now genuinely determines which physical
  Redis instance holds a client's data.
- ✅ **Circuit breaker** on the load-balancer→gateway path
  (`loadbalancer/lb.py`) — CLOSED → OPEN → HALF_OPEN, reacting to live
  request failures immediately instead of waiting on the 3-second passive
  health check poll. Verified with unit tests (`tests/test_circuit_breaker.py`).
- ✅ **Real backend business logic** — order placement against limited
  inventory with row-level locking (`gateway/orders.py`), replacing the
  earlier `simulate_backend_work` placeholder.

### Deliberately NOT implemented: Raft-based leader election for the load balancer

The load balancer is still a single instance (a real single point of
failure). The "fix" would typically be Raft-based leader election across
multiple LB instances. This was deliberately left out rather than rushed:
a correct Raft implementation (election safety, log matching, leader
completeness) is a substantial distributed-systems project on its own —
easy to describe, notoriously easy to get subtly wrong (this is *why*
the Raft paper exists — the previous standard, Paxos, was widely
considered too hard to implement correctly). Shipping a rushed, likely-
incorrect Raft implementation and calling it "consensus" would be a
worse outcome than clearly stating the gap: in a real production setup,
you'd put 2+ LB instances behind a cloud load balancer, DNS-based
failover, or a coordination service like etcd/Consul (which already
implements Raft correctly) rather than hand-rolling it.

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

**Final confirmed measurement** (run from inside the Docker network,
bypassing Windows port-forwarding, after fixing both the blocking-I/O bug
and the startup race condition below):

```
1000 requests, 100 concurrent
Throughput:    101.8 req/s
Latency p50:   872.7ms
Latency p95:   1261.7ms
Latency max:   2320.4ms
Status codes:  {200: 503, 429: 497}
```

The 503/497 split on status codes is *expected, correct behavior* — 50
simulated clients × 20 requests each, rate limit = 10/client, so exactly
half get allowed and half get legitimately rate-limited. It is not an
error rate.

**Note on methodology:** an earlier diagnostic in this same run showed
"410 req/s direct to gateway-1" — that number is invalid and should be
ignored. It came from a test hitting `localhost:8001` from *inside* the
`loadbalancer` container, where port 8001 isn't reachable at all; the
requests failed instantly and a bug in the test script's exception
handling recorded those failures as fast successes. Caught by checking
"why is this suspiciously faster than everything else" rather than
trusting a good-looking number — worth mentioning as a debugging habit,
not hiding.

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

- Load balancer is itself not distributed/HA (single instance) — see the Raft discussion above for why this is a deliberate, explained gap rather than a rushed fix
- Sliding-window-counter is an *approximation*, not exact — under adversarial traffic patterns it can be off by a bounded amount (this is a known, accepted tradeoff, not a bug)
- Redis sharding here is client-side (our own hash ring picking which of 3 independent instances to use), not the real Redis Cluster protocol — no automatic replication or failover between shards if one goes down
- **The benchmark numbers earlier in this README (101.8 req/s etc.) were measured against the old `/api/action` + single-shared-Redis version, before the sharding/circuit-breaker/real-orders changes.** Re-run `docker compose exec loadbalancer python3 loadtest/loadtest.py` against the current code for an up-to-date number before quoting it anywhere.
