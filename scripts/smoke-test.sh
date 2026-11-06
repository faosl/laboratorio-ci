#!/usr/bin/env bash
set -euo pipefail
base_url="${1:-http://localhost:8080}"
base_url="${base_url%/}"
body=$(mktemp)
trap 'rm -f "$body"' EXIT

fail() { echo "$*" >&2; exit 1; }
matches() { printf '%s' "$1" | cmp -s - "$body"; }

# Only startup transport failures and HTTP 503 are retryable. Bound every
# request by the remaining readiness budget; curl itself must not retry.
deadline=$(( $(date +%s) + 45 ))
echo 'Waiting up to 45s for readiness: /health'
while :; do
  remaining=$(( deadline - $(date +%s) ))
  (( remaining > 0 )) || fail 'Readiness timeout after 45s: /health'
  request_timeout=$remaining
  (( request_timeout <= 5 )) || request_timeout=5
  rc=0
  status=$(curl --silent --show-error --output "$body" --write-out '%{http_code}' \
    --connect-timeout "$request_timeout" --max-time "$request_timeout" \
    "$base_url/health") || rc=$?
  if (( rc != 0 )); then
    case "$rc" in
      7|28|52|56) echo "Readiness pending: /health transport error (curl $rc)" >&2 ;;
      *) fail "Readiness transport failure: /health (curl $rc)" ;;
    esac
  else
    case "$status" in
      200)
        matches Healthy || fail 'Unexpected readiness response: /health HTTP 200; expected exactly Healthy'
        (( $(date +%s) < deadline )) || fail 'Readiness timeout after 45s: /health'
        break ;;
      503) echo 'Readiness pending: /health HTTP 503' >&2 ;;
      *) fail "Readiness HTTP failure: /health HTTP $status" ;;
    esac
  fi
  (( $(date +%s) < deadline )) || fail 'Readiness timeout after 45s: /health'
  sleep 1
done

# Functional validation is a single request, only after readiness succeeds.
echo 'Application ready; checking / once'
rc=0
status=$(curl --silent --show-error --output "$body" --write-out '%{http_code}' \
  --max-time 5 "$base_url/") || rc=$?
(( rc == 0 )) || fail "Functional transport failure: / (curl $rc)"
[[ "$status" == 200 ]] || fail "Functional HTTP failure: / HTTP $status"
matches '{"service":"Ordering.API"}' || fail 'Unexpected functional response: / HTTP 200; expected exactly {"service":"Ordering.API"}'
echo 'HTTP integration checks passed: /health and /'
