#!/usr/bin/env bash
# scripts/sandbox_down.sh — tear down the real-API ingestion sandbox.
#
#   scripts/sandbox_down.sh              # stop + remove containers
#   scripts/sandbox_down.sh --volumes    # also wipe pg/kafka/minio volumes
#                                        # (fresh DB next time — re-installs needed)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.sandbox.yml)

if [ "${1:-}" = "--volumes" ] || [ "${1:-}" = "-v" ]; then
  echo "Stopping sandbox and WIPING volumes (postgres / kafka / minio)..."
  "${COMPOSE[@]}" down --volumes --remove-orphans
  echo "Done. Note: a fresh DB means provider installs + seeded secrets are gone."
else
  echo "Stopping sandbox (volumes preserved)..."
  "${COMPOSE[@]}" down --remove-orphans
  echo "Done. Re-run scripts/sandbox_up.sh to resume with existing data."
fi
