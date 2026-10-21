#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
check_project="generation-check-$(date +%s)-$$"
export COMPOSE_SUBNET="${CHECK_SUBNET:-10.246.82.0/24}"
export API_PORT="${CHECK_API_PORT:-18081}"
export PROVIDER_TIMEOUT=60
export WEBHOOK_SECRET=demo-secret-change-me
# Проверки используют только собственные контейнеры, сеть и новый том.
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        docker compose -p "$check_project" logs --tail 40 api alpha beta >&2 || true
    fi
    docker compose -p "$check_project" down -v --remove-orphans
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker compose -p "$check_project" up --build -d --wait api alpha beta
docker compose -p "$check_project" exec -T api python scripts/check_workers.py
docker compose -p "$check_project" run --build --rm tests ruff check .
docker compose -p "$check_project" run --rm tests ruff format --check .
docker compose -p "$check_project" run --rm tests pytest -q "$@"
