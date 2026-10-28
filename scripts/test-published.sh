#!/usr/bin/env bash
set -euo pipefail
ASPNETCORE_URLS=http://127.0.0.1:8080 dotnet publish/Ordering.API.dll > app.log 2>&1 &
app_pid=$!
trap 'kill "$app_pid" 2>/dev/null || true; wait "$app_pid" 2>/dev/null || true; cat app.log' EXIT
bash scripts/smoke-test.sh http://127.0.0.1:8080
