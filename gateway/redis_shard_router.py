"""
Redis Shard Router
-------------------
Makes the consistent hash ring's routing decision REAL instead of decorative:
each client's rate-limit state actually lives on one specific Redis instance,
chosen by the hash ring. This is the piece that was missing before -- the
ring computed a shard name, but every client's data lived in one shared
Redis regardless. Now it genuinely matters which shard a client hashes to.

WHY CLIENT-SIDE SHARDING INSTEAD OF REAL REDIS CLUSTER:
Real Redis Cluster mode has its own protocol (16384 hash slots, gossip
between nodes, MOVED/ASK redirects). Implementing that correctly is a much
larger undertaking with real correctness pitfalls. Client-side sharding --
where the APPLICATION decides which of N independent Redis instances to
talk to -- is a legitimate, widely-used alternative (this is what Twemproxy/
mcrouter do, and what many companies ran before Redis Cluster existed). The
tradeoff we accept: if a shard's Redis instance goes down, only clients
hashed to that shard are affected (not everyone) -- but there's no automatic
failover/replication here, unlike real Redis Cluster with replicas.
"""
import redis
from gateway.consistent_hash import ConsistentHashRing
from gateway.rate_limiter import SlidingWindowRateLimiter


class ShardedRateLimiter:
    def __init__(self, shard_names: list[str], port: int, limit: int, window_seconds: int):
        self.ring = ConsistentHashRing(nodes=shard_names)
        self.redis_clients = {
            name: redis.Redis(host=name, port=port, decode_responses=True,
                               socket_connect_timeout=2, socket_timeout=2)
            for name in shard_names
        }
        self.limiters = {
            name: SlidingWindowRateLimiter(client, limit=limit, window_seconds=window_seconds)
            for name, client in self.redis_clients.items()
        }

    def shard_for(self, client_id: str) -> str:
        return self.ring.get_node(client_id)

    def allow(self, client_id: str):
        """Routes to the correct shard's limiter based on the hash ring, then
        checks that shard's Redis instance. Returns (allowed, meta, shard_name)."""
        shard = self.shard_for(client_id)
        allowed, meta = self.limiters[shard].allow(client_id)
        return allowed, meta, shard
