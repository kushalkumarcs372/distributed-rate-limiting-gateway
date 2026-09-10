"""
Custom Load Balancer -- Weighted Least-Connections + Circuit Breaker
-----------------------------------------------------------------------
WHY NOT ROUND-ROBIN:
Round-robin sends equal traffic to every node regardless of how busy it
currently is. If one node is slow, round-robin keeps sending it new
requests anyway, making the slow node slower -- a pile-up.

WHY LEAST-CONNECTIONS:
Route each new request to whichever backend currently has the FEWEST
in-flight requests. Naturally self-balances around slow nodes.

WHY A CIRCUIT BREAKER ON TOP OF THE PASSIVE HEALTH CHECK:
The background health check (health_check_loop) only pings /health every
3 seconds -- up to 3 seconds of live traffic can still be routed to a
node that just started failing real requests, because the health check
hasn't caught up yet. A circuit breaker reacts to ACTUAL request failures
on the live path immediately, not on a polling delay:

  CLOSED     -- normal operation, requests flow through
  OPEN       -- after N consecutive failures, stop sending this backend
                traffic entirely for a cooldown period (fail fast instead
                of waiting for slow timeouts against a broken node)
  HALF_OPEN  -- after the cooldown, let exactly one trial request through;
                success -> CLOSED (fully trust it again), failure -> OPEN
                again with a fresh cooldown

This is the standard three-state pattern (as in Hystrix/resilience4j).
"""
import asyncio
import os
import time
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

app = FastAPI(title="Load Balancer")

FAILURE_THRESHOLD = 3       # consecutive failures before tripping to OPEN
OPEN_COOLDOWN_SECONDS = 10  # how long to wait before trying HALF_OPEN


class Backend:
    def __init__(self, url: str, weight: int = 1):
        self.url = url
        self.weight = weight
        self.active_connections = 0
        self.healthy = True  # from the passive background health check
        self.total_requests = 0
        self.failed_requests = 0

        # circuit breaker state
        self.circuit_state = "CLOSED"
        self.consecutive_failures = 0
        self.opened_at = None

    def load_score(self) -> float:
        # lower score = more preferred
        return self.active_connections / self.weight

    def circuit_allows_traffic(self) -> bool:
        if self.circuit_state == "CLOSED":
            return True
        if self.circuit_state == "OPEN":
            if time.time() - self.opened_at >= OPEN_COOLDOWN_SECONDS:
                self.circuit_state = "HALF_OPEN"
                return True  # allow exactly one trial request through
            return False
        if self.circuit_state == "HALF_OPEN":
            # Only one trial request should be in flight at a time in
            # HALF_OPEN. We approximate that by only letting it through
            # if nothing is currently active on this backend.
            return self.active_connections == 0
        return True

    def record_success(self):
        self.consecutive_failures = 0
        if self.circuit_state in ("HALF_OPEN", "OPEN"):
            self.circuit_state = "CLOSED"
            self.opened_at = None

    def record_failure(self):
        self.consecutive_failures += 1
        if self.circuit_state == "HALF_OPEN":
            # trial request failed -- reopen with a fresh cooldown
            self.circuit_state = "OPEN"
            self.opened_at = time.time()
        elif self.consecutive_failures >= FAILURE_THRESHOLD and self.circuit_state == "CLOSED":
            self.circuit_state = "OPEN"
            self.opened_at = time.time()


# Backends come from the environment so the same image works under Docker
# Compose (gateway-1..3) and Kubernetes (StatefulSet pod DNS names).
# Format: comma-separated URLs, optional weight suffix, e.g.
#   BACKEND_URLS="http://gateway-1:8000,http://gateway-2:8000@2"
_DEFAULT_BACKENDS = "http://gateway-1:8000,http://gateway-2:8000,http://gateway-3:8000"


def _parse_backends(spec: str) -> list[Backend]:
    backends = []
    for entry in filter(None, (e.strip() for e in spec.split(","))):
        url, _, weight = entry.partition("@")
        backends.append(Backend(url, weight=int(weight) if weight else 1))
    return backends


BACKENDS = _parse_backends(os.getenv("BACKEND_URLS", _DEFAULT_BACKENDS))

_lock = asyncio.Lock()


async def pick_backend() -> Backend | None:
    async with _lock:
        candidates = [b for b in BACKENDS if b.healthy and b.circuit_allows_traffic()]
        if not candidates:
            # Nothing is both health-check-passing AND circuit-closed.
            # Fail open onto whatever passes the health check, so we
            # don't wedge completely if the circuit breaker logic itself
            # is being overly cautious.
            candidates = [b for b in BACKENDS if b.healthy]
        if not candidates:
            return None  # truly nothing available
        chosen = min(candidates, key=lambda b: b.load_score())
        chosen.active_connections += 1
        return chosen


PROXIED = Counter("lb_proxied_requests_total", "Requests proxied per backend", ["backend", "outcome"])
BACKEND_ACTIVE = Gauge("lb_backend_active_connections", "In-flight requests per backend", ["backend"])
BACKEND_HEALTHY = Gauge("lb_backend_healthy", "1 if the passive health check passes", ["backend"])
BACKEND_CIRCUIT = Gauge("lb_backend_circuit_state", "0=CLOSED, 1=HALF_OPEN, 2=OPEN", ["backend"])
_CIRCUIT_CODES = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}


async def release_backend(backend: Backend, success: bool):
    PROXIED.labels(backend.url, "success" if success else "failure").inc()
    async with _lock:
        backend.active_connections = max(0, backend.active_connections - 1)
        backend.total_requests += 1
        if success:
            backend.record_success()
        else:
            backend.failed_requests += 1
            backend.record_failure()


async def health_check_loop():
    """Background task: periodically pings each backend's /health endpoint.
    This is the PASSIVE check -- separate from and slower than the circuit
    breaker's reaction to live request failures."""
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
            "circuit_state": b.circuit_state,
            "consecutive_failures": b.consecutive_failures,
            "active_connections": b.active_connections,
            "total_requests": b.total_requests,
            "failed_requests": b.failed_requests,
        }
        for b in BACKENDS
    }


_DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "dashboard.html")


@app.get("/dashboard")
async def dashboard():
    with open(_DASHBOARD_PATH, "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/")
async def root():
    # Redirect root to the dashboard for convenience
    return HTMLResponse(
        '<meta http-equiv="refresh" content="0; url=/dashboard">'
    )


@app.get("/lb/metrics")
async def lb_metrics():
    # Gauges are refreshed at scrape time from the live Backend objects.
    # Served under /lb/ so it can't collide with the catch-all proxy route
    # below, which would otherwise forward /metrics to a gateway node.
    for b in BACKENDS:
        BACKEND_ACTIVE.labels(b.url).set(b.active_connections)
        BACKEND_HEALTHY.labels(b.url).set(1 if b.healthy else 0)
        BACKEND_CIRCUIT.labels(b.url).set(_CIRCUIT_CODES.get(b.circuit_state, 0))
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    backend = await pick_backend()
    if backend is None:
        return Response(
            content='{"error": "no_backends_available", "reason": "all circuits open or unhealthy"}',
            status_code=503,
            media_type="application/json",
        )

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
        # A backend that responds but with 5xx counts as a circuit-breaker
        # failure too, not just connection-level errors.
        success = resp.status_code < 500
        return Response(
            content=resp.content,
            status_code=resp.status_code,
            headers={
                "X-Served-By-Backend": backend.url,
                "X-Circuit-State": backend.circuit_state,
                **dict(resp.headers),
            },
        )
    except Exception as e:
        success = False
        return Response(content=f'{{"error": "backend_unreachable: {e}"}}',
                         status_code=502, media_type="application/json")
    finally:
        await release_backend(backend, success)
