#!/usr/bin/env bash
# Deterministic contract tests: fake HTTP, clock and sleep; no server or delays.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
state=$(mktemp -d)
trap 'rm -rf "$state"' EXIT
export state

date() { cat "$state/clock"; }
sleep() {
  [[ "$1" == 1 ]] || return 90
  echo "$(( $(cat "$state/clock") + 1 ))" > "$state/clock"
}
curl() {
  local output='' limit='' url='' n status=200 response=Healthy rc=0
  while (( $# )); do
    case "$1" in
      --output) output=$2; shift 2 ;;
      --write-out) [[ "$2" == '%{http_code}' ]] || return 91; shift 2 ;;
      --max-time) limit=$2; shift 2 ;;
      --connect-timeout) shift 2 ;;
      --silent|--show-error) shift ;;
      http://*) url=$1; shift ;;
      *) echo "Unexpected curl option: $1" >&2; return 92 ;;
    esac
  done
  [[ -n "$output" && -n "$limit" ]] || return 93
  echo "$url" >> "$state/requests"
  if [[ "$url" == */health ]]; then
    n=$(( $(cat "$state/health") + 1 ))
    echo "$n" > "$state/health"
    (( limit <= 5 && limit <= 45 - $(cat "$state/clock") )) || return 94
    case "$scenario" in
      delayed)
        case "$n" in
          1) rc=7 ;; 2) rc=28 ;; 3) rc=52 ;; 4) rc=56 ;; 5) status=503 ;;
        esac ;;
      timeout) status=503 ;;
      slow-timeout)
        echo "$(( $(cat "$state/clock") + limit ))" > "$state/clock"
        rc=28 ;;
      health-500) status=500 ;;
      health-404) status=404 ;;
      bad-health) response=Unhealthy ;;
      health-newline) response=$'Healthy\n' ;;
      transport-error) rc=60 ;;
    esac
  elif [[ "$url" == http://smoke.test/ ]]; then
    response='{"service":"Ordering.API"}'
    case "$scenario" in
      bad-root) response='{"service":"Other"}' ;;
      root-newline) response+=$'\n' ;;
      root-500) status=500 ;;
      root-503) status=503 ;;
      root-transport) rc=7 ;;
    esac
  else
    return 95
  fi
  printf '%s' "$response" > "$output"
  printf '%s' "$status"
  return "$rc"
}
export -f date sleep curl

run_case() {
  local expected_rc=$2 health_count=$3 root_count=$4 diagnostic=$5 actual_rc=0
  export scenario=$1
  printf '0\n' > "$state/clock"
  printf '0\n' > "$state/health"
  : > "$state/requests"
  bash "$script_dir/smoke-test.sh" http://smoke.test/ > "$state/log" 2>&1 || actual_rc=$?
  if [[ "$actual_rc" != "$expected_rc" ]] ||
    [[ "$(cat "$state/health")" != "$health_count" ]] ||
    [[ "$(grep -cx 'http://smoke.test/' "$state/requests" || true)" != "$root_count" ]] ||
    ! grep -Fq "$diagnostic" "$state/log"; then
    echo "FAIL: $scenario (exit $actual_rc)" >&2
    cat "$state/log" "$state/requests" >&2
    exit 1
  fi
  # All health checks must precede the sole functional request.
  if (( root_count )); then
    [[ "$(tail -n 1 "$state/requests")" == http://smoke.test/ ]]
  fi
  case "$scenario" in
    delayed) [[ "$(cat "$state/clock")" == 5 ]] ;;
    timeout|slow-timeout) [[ "$(cat "$state/clock")" == 45 ]] ;;
  esac
  echo "PASS: $scenario (health=$health_count, root=$root_count)"
}

run_case success 0 1 1 'HTTP integration checks passed'
run_case delayed 0 6 1 'HTTP integration checks passed'
run_case timeout 1 45 0 'Readiness timeout after 45s'
run_case slow-timeout 1 8 0 'Readiness timeout after 45s'
run_case health-500 1 1 0 'Readiness HTTP failure: /health HTTP 500'
run_case health-404 1 1 0 'Readiness HTTP failure: /health HTTP 404'
run_case bad-health 1 1 0 'Unexpected readiness response'
run_case health-newline 1 1 0 'Unexpected readiness response'
run_case transport-error 1 1 0 'Readiness transport failure'
run_case bad-root 1 1 1 'Unexpected functional response'
run_case root-newline 1 1 1 'Unexpected functional response'
run_case root-500 1 1 1 'Functional HTTP failure: / HTTP 500'
run_case root-503 1 1 1 'Functional HTTP failure: / HTTP 503'
run_case root-transport 1 1 1 'Functional transport failure'
