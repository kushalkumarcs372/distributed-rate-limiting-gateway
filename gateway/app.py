"""
API Gateway
-----------
Every incoming request goes through, in order:
  1. Consistent-hash routing check (which shard "owns" this client -- in a
     real multi-shard-Redis setup this decides which Redis instance to hit;
     here it's surfaced in the response so you can demo/explain it)
  2. Rate limiting (Redis sliding window)
  3. Idempotency check (Postgres) -- only for POST/PUT/PATCH (mutating ops)
  4. Forward to "backend" (mocked here as a simulated processing function)

Run with: uvicorn gateway.app:app --host 0.0.0.0 --port 8000
"""
import time
import random
import asyncio
import redis
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# BUG FOUND UNDER LOAD TEST (worth knowing for interviews):
# redis-py (sync client) and psycopg2 are BLOCKING libraries. Calling them
# directly inside an `async def` FastAPI endpoint blocks uvicorn's entire
# event loop for the duration of the call -- meaning concurrent requests
# were secretly being processed ONE AT A TIME instead of concurrently.
# Measured impact: 1000 requests that should take ~1-2s concurrently took
# 49.78s, with p50 latency of 4.7s for what is actually <50ms of real work.
# Fix: run every blocking call via asyncio.to_thread(), which hands it to
# a worker thread and frees the event loop to handle other requests while
# waiting. The "real" fix would be async libraries (redis.asyncio, asyncpg)
# but to_thread() is the pragmatic fix that doesn't require rewriting the
# rate limiter / idempotency modules.

from gateway import config
from gateway.rate_limiter import SlidingWindowRateLimiter
from gateway.idempotency import IdempotencyStore
from gateway.consistent_hash import ConsistentHashRing

app = FastAPI(title="Distributed API Gateway")


@app.on_event("startup")
async def _size_up_thread_pool():
    # WHY: asyncio.to_thread() uses Python's default executor, capped at
    # min(32, cpu_count + 4) threads out of the box. Each request makes up
    # to 4 sequential blocking calls (rate limit check, idempotency begin,
    # backend work, idempotency complete) -- with 100 concurrent requests
    # all competing for a handful of default threads, we still bottleneck
    # even after moving work off the event loop. Sizing this up removes
    # that ceiling; the real limit becomes Postgres pool size / Redis
    # connections, which is the actual resource we care about tuning.
    from concurrent.futures import ThreadPoolExecutor
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=128))

redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
rate_limiter = SlidingWindowRateLimiter(
    redis_client, limit=config.RATE_LIMIT, window_seconds=config.RATE_LIMIT_WINDOW_SECONDS
)
idempotency_store = IdempotencyStore(config.POSTGRES_DSN)
hash_ring = ConsistentHashRing(nodes=config.BACKEND_NODES)


def simulate_backend_work(client_id: str) -> dict:
    """Stand-in for a real backend call (e.g. 'create order', 'charge card')."""
    time.sleep(random.uniform(0.01, 0.05))
    return {"status": "processed", "client_id": client_id, "handled_by_node": config.NODE_ID}


@app.get("/health")
def health():
    return {"status": "ok", "node": config.NODE_ID}


@app.post("/api/action")
async def api_action(request: Request):
    client_id = request.headers.get("X-Client-Id")
    idempotency_key = request.headers.get("Idempotency-Key")

    if not client_id:
        raise HTTPException(400, "Missing X-Client-Id header")

    owning_shard = hash_ring.get_node(client_id)

    allowed, meta = await asyncio.to_thread(rate_limiter.allow, client_id)
    headers = {
        "X-RateLimit-Limit": str(meta["limit"]),
        "X-RateLimit-Remaining": str(meta["remaining"]),
        "X-Owning-Shard": owning_shard,
        "X-Handled-By": config.NODE_ID,
    }

    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"error": "rate_limit_exceeded", **meta},
            headers=headers,
        )

    if idempotency_key:
        state = await asyncio.to_thread(idempotency_store.try_begin, idempotency_key)

        if state == "completed":
            stored = await asyncio.to_thread(idempotency_store.get_stored_response, idempotency_key)
            return JSONResponse(
                status_code=stored["response_status"],
                content={**stored["response_body"], "idempotent_replay": True},
                headers=headers,
            )

        if state == "in_progress":
            return JSONResponse(
                status_code=409,
                content={"error": "request_already_in_progress"},
                headers=headers,
            )

        # state == "new" -> actually do the work
        result = await asyncio.to_thread(simulate_backend_work, client_id)
        await asyncio.to_thread(idempotency_store.complete, idempotency_key, result, 200)
        return JSONResponse(status_code=200, content=result, headers=headers)

    # no idempotency key provided -- process directly (not recommended for
    # mutating endpoints in production, but allowed here for demo/testing)
    result = await asyncio.to_thread(simulate_backend_work, client_id)
    return JSONResponse(status_code=200, content=result, headers=headers)
