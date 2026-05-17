#!/usr/bin/env bash
# scripts/dev/m2-down.sh — stop the M2 dev stack (preserves volumes).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-fyralis_dev}"
docker compose -f "${COMPOSE_FILE:-docker-compose.dev.yml}" down
