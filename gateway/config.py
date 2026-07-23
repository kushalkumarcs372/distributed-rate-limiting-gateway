import os

# REDIS SHARDING: instead of one shared Redis instance, we run 3 independent
# Redis instances and use our consistent hash ring to decide which shard
# owns which client's rate-limit data. This is "client-side sharding" --
# a real, widely-used pattern (this is what companies did before Redis
# Cluster existed, and what proxies like Twemproxy still do). We deliberately
# did NOT implement the real Redis Cluster protocol (hash slots, gossip,
# cluster redirects) -- that's a much bigger correctness undertaking, and
# getting it subtly wrong is worse than not claiming it at all.
REDIS_SHARDS = os.getenv("REDIS_SHARDS", "redis-shard-1,redis-shard-2,redis-shard-3").split(",")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://gateway:gateway@localhost:5432/gateway"
)

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

NODE_ID = os.getenv("NODE_ID", "gateway-1")
BACKEND_NODES = os.getenv("BACKEND_NODES", "gateway-1,gateway-2,gateway-3").split(",")
