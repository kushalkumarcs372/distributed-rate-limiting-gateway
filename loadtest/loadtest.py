"""
Load test against the load balancer (http://localhost:8080).
Run AFTER `docker compose up --build`.

Usage:
    python3 loadtest/loadtest.py
"""
import asyncio
import time
import httpx
import statistics
import uuid

import os

# When run from Windows/host: hits localhost via Docker Desktop's port forwarding.
# When run from INSIDE the Docker network (see README "Diagnosing environment
# vs app bottlenecks" section): set these to the internal service names to
# bypass Windows/WSL2 port-forwarding entirely and measure true app throughput.
LB_URL = os.getenv("LB_URL", "http://localhost:8080")
GATEWAY_DIRECT_URL = os.getenv("GATEWAY_DIRECT_URL", "http://localhost:8001")
NUM_CLIENTS = 50
REQUESTS_PER_CLIENT = 20
CONCURRENCY = 100


async def one_request(client: httpx.AsyncClient, client_id: str, use_idempotency: bool):
    headers = {"X-Client-Id": client_id}
    if use_idempotency:
        headers["Idempotency-Key"] = str(uuid.uuid4())

    start = time.perf_counter()
    try:
        resp = await client.post(f"{LB_URL}/api/action", headers=headers, timeout=10.0)
        elapsed = time.perf_counter() - start
        return elapsed, resp.status_code
    except Exception:
        elapsed = time.perf_counter() - start
        return elapsed, -1


async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies = []
    status_counts = {}

    async def bound_request(client, client_id):
        async with sem:
            elapsed, status = await one_request(client, client_id, use_idempotency=True)
            latencies.append(elapsed)
            status_counts[status] = status_counts.get(status, 0) + 1

    async with httpx.AsyncClient() as client:
        tasks = []
        overall_start = time.perf_counter()
        for i in range(NUM_CLIENTS):
            client_id = f"loadtest-client-{i}"
            for _ in range(REQUESTS_PER_CLIENT):
                tasks.append(bound_request(client, client_id))
        await asyncio.gather(*tasks)
        overall_elapsed = time.perf_counter() - overall_start

    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    throughput = total_requests / overall_elapsed

    print("\n===== LOAD TEST RESULTS =====")
    print(f"Total requests:      {total_requests}")
    print(f"Total time:           {overall_elapsed:.2f}s")
    print(f"Throughput:           {throughput:.1f} req/s")
    print(f"Latency p50:          {statistics.median(latencies)*1000:.1f}ms")
    print(f"Latency p95:          {sorted(latencies)[int(len(latencies)*0.95)]*1000:.1f}ms")
    print(f"Latency max:          {max(latencies)*1000:.1f}ms")
    print(f"Status code breakdown: {status_counts}")
    print("==============================\n")


async def demo_rate_limit_blocking():
    """Hammer ONE client past its limit to show 429s kick in."""
    print("\n--- Demo: rate limit blocking (single client, 15 rapid requests, limit=10) ---")
    async with httpx.AsyncClient() as client:
        allowed, blocked = 0, 0
        for _ in range(15):
            resp = await client.post(
                f"{LB_URL}/api/action",
                headers={"X-Client-Id": "rate-limit-demo-client"},
            )
            if resp.status_code == 429:
                blocked += 1
            else:
                allowed += 1
        print(f"allowed={allowed}, blocked_with_429={blocked}")


async def demo_idempotency():
    """Send the SAME idempotency key twice -- second call should be a replay, not re-execute."""
    print("\n--- Demo: idempotency (same key sent twice) ---")
    key = str(uuid.uuid4())
    async with httpx.AsyncClient() as client:
        r1 = await client.post(
            f"{LB_URL}/api/action",
            headers={"X-Client-Id": "idempotency-demo-client", "Idempotency-Key": key},
        )
        r2 = await client.post(
            f"{LB_URL}/api/action",
            headers={"X-Client-Id": "idempotency-demo-client", "Idempotency-Key": key},
        )
        print(f"first call:  {r1.status_code} {r1.json()}")
        print(f"second call: {r2.status_code} {r2.json()}  <- note 'idempotent_replay': true")


async def control_test_health_endpoint():
    """
    Diagnostic: hammer the trivial /health endpoint (no Redis, no Postgres,
    no business logic -- just returns a static dict) through the SAME load
    balancer + network path as /api/action. If THIS is also slow, the
    bottleneck is network/Docker/proxy overhead, not our rate-limiter or
    idempotency code. If this is fast but /api/action is slow, the
    bottleneck really is in the app logic.
    """
    print("\n--- Control test: /health endpoint (no Redis/Postgres involved) ---")
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies = []

    async def hit(client):
        async with sem:
            start = time.perf_counter()
            try:
                await client.get(f"{LB_URL}/health", timeout=10.0)
            except Exception:
                pass
            latencies.append(time.perf_counter() - start)

    async with httpx.AsyncClient() as client:
        start = time.perf_counter()
        await asyncio.gather(*[hit(client) for _ in range(1000)])
        elapsed = time.perf_counter() - start

    print(f"1000 requests to /health in {elapsed:.2f}s "
          f"-> {1000/elapsed:.1f} req/s, p50={statistics.median(latencies)*1000:.1f}ms")


async def control_test_direct_to_gateway():
    """
    Diagnostic: hammer gateway-1 DIRECTLY on port 8001 (bypassing the load
    balancer entirely) with the same concurrency pattern as the other
    tests. If this is fast, the LB is the bottleneck. If this is ALSO
    slow, the bottleneck is Docker Desktop's port-forwarding under
    concurrent connections on Windows, not our application code.
    """
    print("\n--- Control test: gateway-1 DIRECT on :8001 (bypassing load balancer) ---")
    sem = asyncio.Semaphore(CONCURRENCY)
    latencies = []

    async def hit(client):
        async with sem:
            start = time.perf_counter()
            try:
                await client.get(f"{GATEWAY_DIRECT_URL}/health", timeout=10.0)
            except Exception:
                pass
            latencies.append(time.perf_counter() - start)

    async with httpx.AsyncClient() as client:
        start = time.perf_counter()
        await asyncio.gather(*[hit(client) for _ in range(1000)])
        elapsed = time.perf_counter() - start

    print(f"1000 requests direct to gateway-1 in {elapsed:.2f}s "
          f"-> {1000/elapsed:.1f} req/s, p50={statistics.median(latencies)*1000:.1f}ms")


if __name__ == "__main__":
    asyncio.run(control_test_direct_to_gateway())
    asyncio.run(control_test_health_endpoint())
    asyncio.run(demo_rate_limit_blocking())
    asyncio.run(demo_idempotency())
    asyncio.run(main())
