#!/usr/bin/env bash
set -euo pipefail
base_url="${1:-http://localhost:8080}"

# Wait for startup; fail on HTTP errors, timeout, or an unexpected response.
health="$(curl --silent --show-error --fail --retry 20 --retry-delay 1 --retry-connrefused --retry-all-errors --max-time 5 --retry-max-time 45 "$base_url/health")"
test "$health" = Healthy
root="$(curl --silent --show-error --fail --max-time 5 "$base_url/")"
test "$root" = '{"service":"Ordering.API"}'
echo 'HTTP integration checks passed: /health and /'
