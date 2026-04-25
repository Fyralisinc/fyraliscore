#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[lsob] Syncing workspace with uv…"
uv sync --all-packages

if command -v docker >/dev/null 2>&1; then
  echo "[lsob] Starting docker-compose services (postgres+pgvector, ollama, localstack)…"
  docker compose up -d
else
  echo "[lsob] docker not found — skipping docker-compose bringup." >&2
fi

if command -v ollama >/dev/null 2>&1; then
  echo "[lsob] Pulling Ollama embedding model…"
  ollama pull nomic-embed-text || true
else
  echo "[lsob] ollama CLI not found — fetch nomic-embed-text-v1.5 via the ollama container manually." >&2
fi

echo "[lsob] Running doctor…"
if uv run lsob doctor >/dev/null 2>&1; then
  uv run lsob doctor
else
  echo "[lsob] 'lsob doctor' not wired yet (Phase 1 Stream D). Skipping." >&2
fi

echo "[lsob] Bootstrap complete."
