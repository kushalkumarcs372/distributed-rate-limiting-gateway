"""
Regression test for the check-then-act race in the rate limiter.

Many threads hit the SAME client at the same instant. With the old
GET-then-INCR implementation, several threads could read the same count
before any of them incremented, so more than `limit` requests got through.
With the Lua script, read + decide + increment is atomic, so exactly
`limit` requests are allowed no matter how the threads interleave.
"""
import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import fakeredis
from gateway.rate_limiter import SlidingWindowRateLimiter


def test_concurrent_requests_never_exceed_limit():
    r = fakeredis.FakeStrictRedis()
    limit = 10
    limiter = SlidingWindowRateLimiter(r, limit=limit, window_seconds=60)

    n_threads = 50
    barrier = threading.Barrier(n_threads)
    results = []
    results_lock = threading.Lock()

    def worker():
        barrier.wait()  # release all threads at once to maximise contention
        ok, _ = limiter.allow("hot-client")
        with results_lock:
            results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    allowed = sum(results)
    print(f"\n[atomic limiter] {allowed}/{n_threads} concurrent requests allowed (limit={limit})")
    assert allowed == limit


def test_remaining_header_counts_down_to_zero():
    r = fakeredis.FakeStrictRedis()
    limiter = SlidingWindowRateLimiter(r, limit=3, window_seconds=60)
    remaining = [limiter.allow("c")[1]["remaining"] for _ in range(3)]
    assert remaining == [2, 1, 0]
