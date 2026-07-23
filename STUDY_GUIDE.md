# Study Guide: Distributed API Gateway

This is your prep document. Read it top to bottom once, then use the
interview Q&A section to test yourself without looking at the answers.

---

## Part 1: Docker & Containers, Explained Simply

### What is a container, really?

Think of a container as a **sealed box that has everything an application
needs to run** — the code, the exact Python version, the exact library
versions, and a mini operating system slice — all bundled together. When
you run that box on any machine (yours, a teammate's laptop, a cloud
server), it behaves identically, because nothing about it depends on what
happens to be installed on the host machine.

### Why didn't we install Postgres/Redis directly on your Windows machine?

A few concrete reasons:

1. **Version and config drift.** If you install Postgres directly on
   Windows, you get whatever config your installer set up, mixed with
   whatever else is on your machine. A container gives you a clean,
   known-identical Postgres every single time — same as what a teammate
   or a production server would run.

2. **Isolation.** Postgres and Redis both want to own certain ports and
   run background processes. Running them in containers means they can't
   conflict with anything else on your machine, and you can delete them
   completely (`docker compose down -v`) without leaving traces on your
   OS.

3. **This mirrors how real companies actually deploy.** Almost no
   production system today runs services installed directly on a bare
   server. They run in containers (Docker) managed by an orchestrator
   (Kubernetes, ECS, etc.). Building this way means what you built is
   architecturally close to how it would really be deployed — not a toy
   simplification.

4. **One command to stand up the whole system.** `docker compose up`
   brings up Postgres, Redis, 3 gateway instances, and the load balancer
   together, wired to talk to each other, in one shot. Manually installing
   and configuring 5 separate services and getting them to find each
   other would be much slower and much easier to get wrong.

### What is `docker-compose.yml` doing?

It's a recipe file that says: "here are the services I want running,
here's what each one needs (environment variables, which image to build,
which ports to expose), and here's how they depend on each other." When
you run `docker compose up`, Docker reads this file and builds/starts
everything it describes, on a private network where each service can
reach the others by name (e.g. `gateway-1` can be reached at that literal
hostname from other containers — that's Docker's internal DNS).

### What is each container in this project, in plain terms?

| Container | Plain-English job |
|---|---|
| `redis` | A super-fast in-memory data store. We use it to count how many requests each client has made recently (the rate limiter). |
| `postgres` | A traditional durable database. We use it to remember which requests we've already processed, so retries don't double-execute. |
| `gateway-1`, `gateway-2`, `gateway-3` | Three identical copies of our actual application logic (rate limiting + idempotency check + hand off to "backend"). Having 3 copies is the "distributed" part — if one dies, the other two still work. |
| `loadbalancer` | The single door clients knock on. It decides which of the 3 gateway copies should handle each incoming request. |

---

## Part 2: Architecture, System-Design Style

### The one-sentence pitch

*"A stateless, horizontally-scaled API gateway that enforces per-client
rate limits and safe request retries, using consistent hashing to
minimize disruption when the fleet scales up or down."*

### Why is each piece there? (the "why," not just the "what")

**Problem 1: If we run multiple gateway instances, how do we make sure
rate limits are enforced correctly across all of them, not just
per-instance?**
→ Answer: shared state in Redis. If gateway-1 and gateway-2 both checked
a client's request count using their own local memory, a client could
get 2x the allowed rate by spreading requests across both. Redis is the
single source of truth all instances check against.

**Problem 2: Clients retry requests on timeouts. How do we stop a retried
"charge $50" from charging twice?**
→ Answer: idempotency keys stored in Postgres. Client sends a unique key
with the request; we check "have I seen this key before?" before doing
anything. If yes and it's done, we return the same answer without
redoing the work.

**Problem 3: If we add or remove a gateway node, do all the clients'
rate-limit histories need to reset?**
→ Answer: no, and this is where consistent hashing comes in. Even though
our rate limit state actually lives in one shared Redis instance in this
project (not literally sharded), the *routing logic* is built the way a
real sharded system would need it: minimal disruption when the fleet
changes size.

**Problem 4: If one gateway node is slow, should we keep sending it equal
traffic?**
→ Answer: no — the load balancer routes to whichever node currently has
the fewest in-flight requests (least-connections), so a struggling node
naturally gets less new work piled onto it.

### The request's journey (say this out loud, it's your elevator pitch)

1. Client sends a request to the load balancer.
2. Load balancer picks the least-busy gateway node.
3. That gateway node checks Redis: "has this client gone over their rate
   limit?" If yes, reject immediately with a 429.
4. If an idempotency key is present, gateway checks Postgres: "have I
   done this exact request before?" If yes, return the stored answer
   instead of redoing the work.
5. Otherwise, do the work, save the result, respond.

---

## Part 3: Interview Q&A

Try answering each before reading the answer.

### Consistent Hashing

**Q: What problem does consistent hashing solve?**
A: When you have data or clients sharded across N nodes using
`hash(key) % N`, adding or removing a single node changes the result of
the modulo for almost every key, causing almost everything to remap to a
different node at once. Consistent hashing places nodes and keys on a
conceptual circle ("ring") so that only the keys that were closest to the
changed node get reassigned — everything else stays exactly where it
was.

**Q: What are "virtual nodes" and why do we use 150 of them per real
node?**
A: If you place just one point per physical node on the ring, the
distribution of keys across nodes can be very uneven purely by chance
(random hash placement). Giving each physical node many points
(virtual nodes) on the ring smooths this out, so load is spread evenly
even though the hash placement is still random.

**Q: What did you actually measure to prove this works?**
A: Went from 4 to 5 nodes. Naive `% N` hashing remapped ~79.5% of keys.
Consistent hashing remapped ~20.9%, matching the theoretical 1/5 = 20%
you'd expect when adding a 5th node to 4.

**Q: What's the time complexity of a lookup?**
A: O(log(N × vnodes)) — we binary-search (`bisect`) into a sorted list of
hash positions on the ring.

### Rate Limiting

**Q: Why not a simple fixed window (e.g. "reset every 60 seconds")?**
A: A client can send the full limit right at the end of one window, and
the full limit again right at the start of the next — getting 2x the
intended rate in a very short burst right at the boundary.

**Q: Why not track every single request timestamp (sliding log)?**
A: Perfectly accurate, but memory grows with every request per client —
doesn't scale.

**Q: What did you use instead, and why?**
A: Sliding window counter — an approximation that estimates the count in
the current sliding window using the current fixed window's count plus a
weighted fraction of the previous window's count. O(1) memory per
client, closely approximates the sliding log without its memory cost.

**Q: Is it perfectly accurate?**
A: No — it's a documented, accepted approximation. In testing, a limit
of 10 let through up to 11 requests near a boundary in one run. This is
a known tradeoff of the algorithm, not a bug.

**Q: Why Redis for this and not Postgres?**
A: Rate-limit checks happen on every single request — this is the
hottest of hot paths. Redis gives sub-millisecond reads/writes; adding a
relational DB round-trip here would meaningfully slow down every
request.

### Idempotency

**Q: What problem does an idempotency key solve?**
A: Network blips cause clients to retry requests. Without protection, a
retried "create order" or "charge card" executes the underlying action
twice. An idempotency key lets the server recognize "I've already done
this exact logical request" and return the original result instead of
repeating the side effect.

**Q: Why Postgres instead of Redis for this?**
A: This data must survive a restart (a non-persistent Redis setup would
lose in-flight idempotency state), and the "who gets there first" race
between two near-simultaneous identical requests needs real transactional
guarantees. `INSERT ... ON CONFLICT DO NOTHING` gives an atomic
compare-and-set with durability.

**Q: Walk me through the state machine.**
A: A key can be `new` (first time seen — do the work), `completed`
(already finished — return the stored response, don't redo the work), or
`in_progress` (another request with the same key is being handled right
now — return 409, ask the client to retry shortly).

### Load Balancing

**Q: Why least-connections instead of round-robin?**
A: Round-robin sends equal traffic regardless of how busy each node
currently is. If one node is slow, round-robin keeps piling new requests
onto it anyway, making it slower — a feedback loop in the wrong
direction. Least-connections naturally routes new requests toward
whichever node is currently freest.

**Q: What does "weighted" add?**
A: If nodes have different capacities (e.g. a bigger instance can handle
more concurrent work), you divide its connection count by its weight, so
a node with weight=2 looks "half as busy" as it actually is and gets
proportionally more traffic.

**Q: What's the load balancer's own single point of failure here, and
how would you fix it in production?**
A: The load balancer itself is a single instance — if it dies, everything
behind it is unreachable, even though the gateway nodes are fine. In
production you'd put multiple load balancer instances behind a cloud
load balancer, DNS round-robin, or an anycast IP so no single LB instance
is a hard dependency.

### Bugs found while building this (great material — shows real debugging, not just following a tutorial)

**Q: Tell me about a real bug you hit while building this.**
A (bug 1 — blocking I/O): "My load test showed only ~20 req/s with p50
latency of 4.7 seconds, even though each request only does about 10-50ms
of real work. I found that I was calling blocking libraries — `redis-py`
and `psycopg2` — directly inside `async def` FastAPI endpoints. A
blocking call inside an async function freezes the *entire event loop*
until it returns, so 100 'concurrent' requests were secretly being
processed one at a time. I fixed it by wrapping every blocking call in
`asyncio.to_thread()`, which offloads it to a worker thread and frees the
event loop to serve other requests while waiting."

A (bug 2 — startup race condition): "When all 3 gateway containers start
at the same moment, each one independently runs `CREATE TABLE IF NOT
EXISTS` against Postgres. That check isn't atomic across separate
connections — two containers can both see 'table doesn't exist yet'
before either commits, so the second one crashes with a duplicate-key
error on Postgres's internal catalog. I fixed it with a Postgres advisory
lock (`pg_advisory_lock`) so table creation is serialized across all
instances — whichever one starts first creates the table, the others
wait, then see it already exists and safely no-op."

**Q: How did you debug the throughput problem — walk me through your
process, not just the fix.**
A: "I didn't guess at fixes blindly — I isolated variables one at a time.
First I tested a single request directly to one gateway node (fast,
~10-20ms), which told me the app itself wasn't fundamentally broken.
Then I tested the lightweight `/health` endpoint — which touches no
Redis or Postgres at all — under the same concurrent load as the real
endpoint, and it was equally slow. That ruled out my rate limiter and
idempotency code as the cause. Then I bypassed the load balancer
entirely and hit a gateway directly under concurrent load, and it was
*still* slow, which ruled out the load balancer too. That process of
elimination pointed at something common to all of them — which turned
out to be Docker Desktop's Windows port-forwarding layer struggling under
many concurrent connections, confirmed by re-running the exact same test
from inside the Docker network instead of from Windows, where throughput
jumped roughly 3x."

**Q: Did you make any mistakes in your own testing/debugging?**
A: "Yes — one of my own diagnostic scripts had a bug. I wrote a test to
hit a port that wasn't actually reachable from where the test was
running, and my exception handling silently caught the connection
failure and recorded it as if it were a fast success. It produced a
number that looked *too good* — over 400 req/s when everything else was
capped around 100. That mismatch was the signal that something was
wrong with the test itself, not that I'd found a fast path. I flagged and
discarded that number rather than reporting it."

### General System Design

**Q: How would you scale this further?**
A: Actual sharded Redis (Redis Cluster) so the consistent hash ring
routes to genuinely different Redis instances, not one shared instance.
Multiple load balancer instances for high availability. Async-native
libraries (`redis.asyncio`, `asyncpg`) instead of thread-offloading sync
ones, for better efficiency at very high concurrency.

**Q: What would you do differently if you rebuilt this?**
A: Start with async libraries from day one instead of retrofitting
`asyncio.to_thread()` after finding the blocking-I/O bug under load
testing — it would have avoided that whole debugging detour, though
finding and fixing it taught the underlying lesson properly.

**Q: What's the actual measured performance?**
A: 101.8 requests/sec sustained, p50 latency 872.7ms, p95 1261.7ms, under
100 concurrent clients each retrying with idempotency keys — measured
from inside the Docker network to remove local-machine networking
overhead from the number. Rate limiting and idempotency both verified
working correctly under that same load (exact expected 503/497
allowed/blocked split).

---

## How to actually use this before an interview

Don't memorize this file. Read it once, then close it and try to explain
the "request's journey" section and the "bugs found" section out loud,
from memory, to yourself or a friend. If you get stuck on a piece,
that's exactly the piece to re-read. The goal is to be able to draw the
architecture diagram from memory and narrate it — that's what actually
gets tested in an interview, not the ability to recite this document.
