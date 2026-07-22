"""
Custom Load Balancer -- Weighted Least-Connections
----------------------------------------------------
WHY NOT ROUND-ROBIN:
Round-robin sends equal traffic to every node regardless of how busy it
currently is. If one node is slow (e.g. mid-GC-pause, or handling a heavy
request), round-robin keeps sending it new requests anyway, making the
slow node slower -- a pile-up.

WHY LEAST-CONNECTIONS:
Route each new request to whichever backend currently has the FEWEST
in-flight requests. Naturally self-balances around slow nodes.

WHY WEIGHTED:
Not all nodes are equal (bigger instance = handle more concurrent
requests). We divide active-connections by the node's weight so a node
with weight=2 is treated as if it has half as many connections as
it actually does -- meaning it gets proportionally more traffic.

This is a real reverse proxy: it forwards actual HTTP requests to the
chosen backend and streams the response back, using httpx.
"""
import asyncio
import time
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI(title="Load Balancer")


class Backend:
    def __init__(self, url: str, weight: int = 1):
        self.url = url
        self.weight = weight
        self.active_connections = 0
        self.healthy = True
        self.total_requests = 0
        self.failed_requests = 0

    def load_score(self) -> float:
        # lower score = more preferred
        return self.active_connections / self.weight


BACKENDS = [
    Backend("http://gateway-1:8000", weight=1),
    Backend("http://gateway-2:8000", weight=1),
    Backend("http://gateway-3:8000", weight=1),
]

_lock = asyncio.Lock()


async def pick_backend() -> Backend:
    async with _lock:
        healthy = [b for b in BACKENDS if b.healthy]
        if not healthy:
            # all marked unhealthy -- fail open and try everyone again
            healthy = BACKENDS
        chosen = min(healthy, key=lambda b: b.load_score())
        chosen.active_connections += 1
        return chosen


async def release_backend(backend: Backend, success: bool):
    async with _lock:
        backend.active_connections = max(0, backend.active_connections - 1)
        backend.total_requests += 1
        if not success:
            backend.failed_requests += 1


async def health_check_loop():
    """Background task: periodically pings each backend's /health endpoint."""
    while True:
        for backend in BACKENDS:
            try:
                resp = await http_client.get(f"{backend.url}/health", timeout=2.0)
                backend.healthy = resp.status_code == 200
            except Exception:
                backend.healthy = False
        await asyncio.sleep(3)


http_client: httpx.AsyncClient | None = None


@app.on_event("startup")
async def startup():
    global http_client
    # Reuse one client with a connection pool instead of opening a fresh
    # TCP connection to the backend on every single proxied request.
    http_client = httpx.AsyncClient(
        timeout=10.0,
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    asyncio.create_task(health_check_loop())


@app.on_event("shutdown")
async def shutdown():
    if http_client:
        await http_client.aclose()


@app.get("/lb/status")
async def status():
    return {
        b.url: {
            "healthy": b.healthy,
            "active_connections": b.active_connections,
            "total_requests": b.total_requests,
            "failed_requests": b.failed_requests,
        }
        for b in BACKENDS
    }


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    backend = await pick_backend()
    success = True
    try:
        body = await request.body()
        resp = await http_client.request(
            request.method,
            f"{backend.url}/{path}",
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            content=body,
            params=request.query_params,
        )
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={"X-Served-By-Backend": backend.url, **dict(resp.headers)},
        )
    except Exception as e:
        success = False
        return Response(content=f'{{"error": "backend_unreachable: {e}"}}',
                         status_code=502, media_type="application/json")
    finally:
        await release_backend(backend, success)
