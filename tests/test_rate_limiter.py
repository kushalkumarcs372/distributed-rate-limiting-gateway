import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import fakeredis
from gateway.rate_limiter import SlidingWindowRateLimiter


def test_allows_up_to_limit_then_blocks():
    r = fakeredis.FakeStrictRedis()
    limiter = SlidingWindowRateLimiter(r, limit=5, window_seconds=60)

    allowed_count = 0
    for _ in range(10):
        ok, meta = limiter.allow("client-1")
        if ok:
            allowed_count += 1

    print(f"\n[rate limiter] allowed {allowed_count}/10 requests with limit=5")
    assert allowed_count == 5


def test_different_clients_are_independent():
    r = fakeredis.FakeStrictRedis()
    limiter = SlidingWindowRateLimiter(r, limit=2, window_seconds=60)

    ok_a1, _ = limiter.allow("client-A")
    ok_a2, _ = limiter.allow("client-A")
    ok_a3, _ = limiter.allow("client-A")  # should be blocked
    ok_b1, _ = limiter.allow("client-B")  # fresh client, should be allowed

    assert ok_a1 and ok_a2
    assert not ok_a3
    assert ok_b1


def test_no_burst_at_window_boundary():
    """
    This is the core thing fixed-window rate limiting gets wrong:
    verify we don't allow 2x limit right at a window boundary.
    """
    r = fakeredis.FakeStrictRedis()
    limiter = SlidingWindowRateLimiter(r, limit=10, window_seconds=1)

    # burn the full limit near the end of window 0
    time.sleep(0.9)
    allowed_in_first_burst = 0
    for _ in range(10):
        ok, _ = limiter.allow("client-burst")
        if ok:
            allowed_in_first_burst += 1

    # cross into window 1 immediately, try to burst again
    time.sleep(0.15)
    allowed_in_second_burst = 0
    for _ in range(10):
        ok, _ = limiter.allow("client-burst")
        if ok:
            allowed_in_second_burst += 1

    total = allowed_in_first_burst + allowed_in_second_burst
    print(f"[rate limiter] total allowed across boundary: {total} (limit=10, "
          f"naive fixed-window would allow up to 20)")
    # sliding window should keep total well under 2x limit
    assert total < 16
