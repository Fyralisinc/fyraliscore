#!/usr/bin/env bash
# scripts/dev/m2-logs.sh — tail logs from the M2 dev stack.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-fyralis_dev}"
docker compose -f "${COMPOSE_FILE:-docker-compose.dev.yml}" logs -f "$@"
