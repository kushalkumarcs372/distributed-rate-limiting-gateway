import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://gateway:gateway@localhost:5432/gateway"
)

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

NODE_ID = os.getenv("NODE_ID", "gateway-1")
BACKEND_NODES = os.getenv("BACKEND_NODES", "gateway-1,gateway-2,gateway-3").split(",")
