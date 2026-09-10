"""
Prometheus metrics for the gateway nodes.

Exposed at GET /metrics on every gateway node and scraped by Prometheus
(see monitoring/prometheus.yml). Each node reports its own counters;
Prometheus attaches an `instance` label, so dashboards can show per-node
and cluster-wide views of the same series.
"""
from prometheus_client import Counter, Histogram

REQUESTS = Counter(
    "gateway_requests_total",
    "HTTP requests handled by this gateway node",
    ["method", "route", "status"],
)

LATENCY = Histogram(
    "gateway_request_duration_seconds",
    "End-to-end request latency inside the gateway",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

RATE_LIMIT_DECISIONS = Counter(
    "gateway_rate_limit_decisions_total",
    "Rate limiter outcomes, per Redis shard",
    ["shard", "decision"],  # decision: allowed | blocked
)

IDEMPOTENCY_OUTCOMES = Counter(
    "gateway_idempotency_outcomes_total",
    "Idempotency-Key handling outcomes",
    ["outcome"],  # new | replay | in_progress
)
