#!/usr/bin/env bash
# End-to-end smoke test against a running stack (Docker Compose or Kubernetes).
# Usage: scripts/smoke_test.sh [base_url]   (default http://localhost:8080)
set -euo pipefail
BASE="${1:-http://localhost:8080}"
CLIENT="smoke-$RANDOM-$RANDOM"
fail() { echo "FAIL: $*"; exit 1; }

echo "== waiting for load balancer at $BASE"
for i in $(seq 1 30); do
  curl -fs "$BASE/lb/status" >/dev/null && break
  sleep 2
  [ "$i" = 30 ] && fail "load balancer never became ready"
done

echo "== rate limiting: 15 requests for one client, limit is 10"
ok=0; limited=0
for i in $(seq 1 15); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/orders" \
    -H "X-Client-Id: $CLIENT" -H "Content-Type: application/json" \
    -d '{"product_id":"widget-a","quantity":1}')
  case "$code" in
    200|409) ok=$((ok+1)) ;;   # 409 = out of stock, still passed the limiter
    429) limited=$((limited+1)) ;;
    *) fail "unexpected status $code" ;;
  esac
done
echo "   passed limiter: $ok, rate-limited: $limited"
# Sliding-window counter is an approximation: if the run straddles a window
# boundary, the weighted estimate can admit one extra request.
[ "$ok" -ge 10 ] && [ "$ok" -le 11 ] || fail "expected 10 (or 11 across a window boundary) through the limiter, got $ok"
[ "$limited" -ge 4 ] || fail "expected rate limiting to kick in, got $limited 429s"

echo "== idempotency: same key twice must replay, not re-execute"
KEY="smoke-key-$RANDOM-$RANDOM"
CLIENT2="smoke2-$RANDOM-$RANDOM"
curl -s -X POST "$BASE/api/orders" -H "X-Client-Id: $CLIENT2" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" -d '{"product_id":"widget-a","quantity":1}' >/dev/null
second=$(curl -s -X POST "$BASE/api/orders" -H "X-Client-Id: $CLIENT2" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" -d '{"product_id":"widget-a","quantity":1}')
echo "$second" | grep -q '"idempotent_replay":true' || fail "second request was not a replay: $second"

echo "== metrics endpoint exposed"
curl -fs "$BASE/lb/metrics" | grep -q lb_backend_circuit_state || fail "LB metrics missing"

echo "ALL SMOKE TESTS PASSED"
