"""
Sliding Window Counter Rate Limiter (Redis-backed)
---------------------------------------------------
WHY NOT FIXED WINDOW:
Fixed window (e.g. "100 req per minute, reset at :00") allows a burst of
2x the limit right at the window boundary (100 requests at 11:59:59 +
100 more at 12:00:00 = 200 in 1 second). This is a well-known flaw.

WHY NOT SLIDING LOG (store every timestamp):
Perfectly accurate, but O(n) memory per client (one entry per request).
At scale this blows up Redis memory.

SLIDING WINDOW COUNTER (what we use):
Approximates a sliding log using O(1) memory per client. We keep two
fixed-window counters (current + previous) and weight the previous
window's count by how much of it still overlaps the sliding window.

    estimated_count = current_window_count +
                       previous_window_count * overlap_fraction

This is the same algorithm Cloudflare and Kong use in production.
"""
import time


class SlidingWindowRateLimiter:
    def __init__(self, redis_client, limit: int, window_seconds: int = 60):
        self.redis = redis_client
        self.limit = limit
        self.window = window_seconds

    def _keys(self, client_id: str, now: float):
        current_bucket = int(now // self.window)
        prev_bucket = current_bucket - 1
        return (
            f"rl:{client_id}:{current_bucket}",
            f"rl:{client_id}:{prev_bucket}",
            current_bucket,
        )

    def allow(self, client_id: str) -> tuple[bool, dict]:
        """
        Returns (allowed: bool, meta: dict) where meta has debug info
        useful for response headers (X-RateLimit-Remaining etc).
        """
        now = time.time()
        curr_key, prev_key, curr_bucket = self._keys(client_id, now)

        pipe = self.redis.pipeline()
        pipe.get(curr_key)
        pipe.get(prev_key)
        curr_count, prev_count = pipe.execute()

        curr_count = int(curr_count or 0)
        prev_count = int(prev_count or 0)

        elapsed_in_current = now - (curr_bucket * self.window)
        overlap_fraction = max(0.0, (self.window - elapsed_in_current) / self.window)

        estimated = curr_count + prev_count * overlap_fraction

        allowed = estimated < self.limit
        if allowed:
            pipe = self.redis.pipeline()
            pipe.incr(curr_key)
            pipe.expire(curr_key, self.window * 2)  # keep around for next window's calc
            pipe.execute()

        return allowed, {
            "estimated_count": round(estimated, 2),
            "limit": self.limit,
            "remaining": max(0, int(self.limit - estimated)),
            "window_seconds": self.window,
        }
