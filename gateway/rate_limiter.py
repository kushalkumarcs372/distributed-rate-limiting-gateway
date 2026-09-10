"""
Sliding Window Counter Rate Limiter (Redis-backed, atomic)
----------------------------------------------------------
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

WHY A LUA SCRIPT (race condition fix):
The first version did GET/GET in one round trip, decided in Python, then
INCR in a second round trip. Between those two round trips, other gateway
nodes (or other threads on this node) could read the same counts and ALL
decide "allowed" -- a classic check-then-act race that lets concurrent
requests slip past the limit. Redis runs a Lua script atomically (no other
command interleaves while it executes), so read + decide + increment now
happen as one indivisible step. See tests/test_rate_limiter_atomic.py.
"""
import time

# KEYS[1] = current window key, KEYS[2] = previous window key
# ARGV[1] = limit, ARGV[2] = overlap fraction of previous window, ARGV[3] = TTL seconds
# Returns {allowed (1/0), estimated count as a string}. The estimate is returned
# as a string because Redis truncates Lua numbers to integers in replies.
_SLIDING_WINDOW_LUA = """
local curr = tonumber(redis.call('GET', KEYS[1]) or '0')
local prev = tonumber(redis.call('GET', KEYS[2]) or '0')
local limit = tonumber(ARGV[1])
local overlap = tonumber(ARGV[2])
local estimated = curr + prev * overlap
if estimated < limit then
    redis.call('INCR', KEYS[1])
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]))
    return {1, tostring(estimated)}
end
return {0, tostring(estimated)}
"""


class SlidingWindowRateLimiter:
    def __init__(self, redis_client, limit: int, window_seconds: int = 60):
        self.redis = redis_client
        self.limit = limit
        self.window = window_seconds
        # register_script caches the script by SHA and uses EVALSHA,
        # falling back to EVAL automatically if Redis doesn't have it yet.
        self._script = self.redis.register_script(_SLIDING_WINDOW_LUA)

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

        elapsed_in_current = now - (curr_bucket * self.window)
        overlap_fraction = max(0.0, (self.window - elapsed_in_current) / self.window)

        allowed_flag, estimated_raw = self._script(
            keys=[curr_key, prev_key],
            args=[self.limit, overlap_fraction, self.window * 2],
        )
        allowed = int(allowed_flag) == 1
        estimated = float(estimated_raw)

        return allowed, {
            "estimated_count": round(estimated, 2),
            "limit": self.limit,
            "remaining": max(0, int(self.limit - estimated - (1 if allowed else 0))),
            "window_seconds": self.window,
        }
