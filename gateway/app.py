"""
API Gateway
-----------
Every incoming request goes through, in order:
  1. Consistent-hash routing: which Redis SHARD actually owns this client's
     rate-limit state (this is now a real routing decision, not decorative --
     see redis_shard_router.py)
  2. Rate limiting against that shard's Redis instance (sliding window)
  3. Idempotency check (Postgres) -- only for POST/PUT/PATCH (mutating ops)
  4. Forward to real backend logic: place an order against limited inventory

Run with: uvicorn gateway.app:app --host 0.0.0.0 --port 8000
"""
import asyncio
import time
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# BUG FOUND UNDER LOAD TEST (worth knowing for interviews):
# redis-py (sync client) and psycopg2 are BLOCKING libraries. Calling them
# directly inside an `async def` FastAPI endpoint blocks uvicorn's entire
# event loop for the duration of the call -- meaning concurrent requests
# were secretly being processed ONE AT A TIME instead of concurrently.
# Fix: run every blocking call via asyncio.to_thread(), which hands it to
# a worker thread and frees the event loop to handle other requests while
# waiting.

from gateway import config
from gateway.idempotency import IdempotencyStore
from gateway.redis_shard_router import ShardedRateLimiter
from gateway.orders import OrdersService
from gateway.metrics import REQUESTS, LATENCY, RATE_LIMIT_DECISIONS, IDEMPOTENCY_OUTCOMES

app = FastAPI(title="Distributed API Gateway")


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)
    start = time.perf_counter()
    response = await call_next(request)
    # Label by route TEMPLATE (e.g. /api/orders), not raw path, so arbitrary
    # URLs can't blow up the number of time series (label cardinality).
    route = request.scope.get("route")
    route_label = getattr(route, "path", "unmatched")
    REQUESTS.labels(request.method, route_label, str(response.status_code)).inc()
    LATENCY.labels(request.method, route_label).observe(time.perf_counter() - start)
    return response


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.on_event("startup")
async def _size_up_thread_pool():
    # WHY: asyncio.to_thread() uses Python's default executor, capped at
    # min(32, cpu_count + 4) threads out of the box. Sizing this up removes
    # that ceiling; the real limit becomes Postgres/Redis pool sizes, which
    # is the resource we actually want to be tuning.
    from concurrent.futures import ThreadPoolExecutor
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=128))


sharded_rate_limiter = ShardedRateLimiter(
    shard_names=config.REDIS_SHARDS,
    port=config.REDIS_PORT,
    limit=config.RATE_LIMIT,
    window_seconds=config.RATE_LIMIT_WINDOW_SECONDS,
)
idempotency_store = IdempotencyStore(config.POSTGRES_DSN)
orders_service = OrdersService(config.POSTGRES_DSN)


@app.get("/health")
def health():
    return {"status": "ok", "node": config.NODE_ID}


@app.get("/api/inventory")
async def get_inventory():
    snapshot = await asyncio.to_thread(orders_service.get_inventory_snapshot)
    return {"inventory": snapshot, "handled_by": config.NODE_ID}


@app.post("/api/orders")
async def place_order(request: Request):
    client_id = request.headers.get("X-Client-Id")
    idempotency_key = request.headers.get("Idempotency-Key")

    if not client_id:
        raise HTTPException(400, "Missing X-Client-Id header")

    body = await request.json() if await request.body() else {}
    product_id = body.get("product_id", "widget-a")
    quantity = int(body.get("quantity", 1))

    allowed, meta, shard = await asyncio.to_thread(sharded_rate_limiter.allow, client_id)
    RATE_LIMIT_DECISIONS.labels(shard, "allowed" if allowed else "blocked").inc()
    headers = {
        "X-RateLimit-Limit": str(meta["limit"]),
        "X-RateLimit-Remaining": str(meta["remaining"]),
        "X-Redis-Shard": shard,
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
        IDEMPOTENCY_OUTCOMES.labels({"completed": "replay"}.get(state, state)).inc()

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

        # state == "new" -> actually place the order
        result = await asyncio.to_thread(orders_service.place_order, client_id, product_id, quantity)
        status_code = 200 if result["status"] == "confirmed" else 409
        await asyncio.to_thread(idempotency_store.complete, idempotency_key, result, status_code)
        return JSONResponse(status_code=status_code, content=result, headers=headers)

    # no idempotency key -- process directly (not recommended for mutating
    # endpoints in production, but allowed here for demo/testing)
    result = await asyncio.to_thread(orders_service.place_order, client_id, product_id, quantity)
    status_code = 200 if result["status"] == "confirmed" else 409
    return JSONResponse(status_code=status_code, content=result, headers=headers)
