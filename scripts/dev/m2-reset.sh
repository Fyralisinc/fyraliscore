#!/usr/bin/env bash
# scripts/dev/m2-reset.sh — nuke volumes and restart the M2 dev stack.
# Use when Kafka state gets corrupted or topics need re-creation
# from scratch.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
docker compose -f "${COMPOSE_FILE:-docker-compose.dev.yml}" down -v
bash "$ROOT/scripts/dev/m2-up.sh"
