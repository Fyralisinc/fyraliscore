#!/usr/bin/env bash
# scripts/start.sh — one-call end-to-end startup for the Fyralis demo.
#
# What this does, in order:
#   1. Sources .env (and .env.dogfood when present) and validates the
#      DEEPSEEK_API_KEY secret + Postgres + Ollama dependencies.
#   2. Applies any database migrations that haven't been recorded.
#   3. Builds + emits the Pelago demo snapshot when the .sql.zst file
#      doesn't exist yet.
#   4. Starts gateway, think_worker, post_commit_worker, and the Vite
#      UI dev server. PIDs land in /tmp/fyralis_stack.pids so
#      `scripts/stop.sh` can shut them down cleanly.
#   5. Waits for /healthz, prints the URLs, and (optionally) opens the
#      browser to the demo picker. Pass `--no-browser` to skip.
#
# Re-running is safe: existing migrations are skipped, snapshots
# already on disk are reused, and ports already in use are detected.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ----------------------------------------------------------------------
# Args
# ----------------------------------------------------------------------
OPEN_BROWSER=1
REBUILD_SNAPSHOTS=0
for arg in "$@"; do
  case "$arg" in
    --no-browser)        OPEN_BROWSER=0 ;;
    --rebuild-snapshots) REBUILD_SNAPSHOTS=1 ;;
    -h|--help)
      cat <<HELP
Usage: scripts/start.sh [--no-browser] [--rebuild-snapshots]

  --no-browser          Don't open the demo picker in your browser.
  --rebuild-snapshots   Re-emit the Pelago SQL snapshot even if the
                        .sql.zst already exists.
HELP
      exit 0 ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2 ;;
  esac
done

log() { printf "\033[1;36m[start]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[start]\033[0m %s\n" "$*" >&2; }
fail() { printf "\033[1;31m[start]\033[0m %s\n" "$*" >&2; exit 1; }

# ----------------------------------------------------------------------
# 1. Env + dependency sanity
# ----------------------------------------------------------------------
[ -f .env ] || fail ".env missing — copy from .env.example and fill in DEEPSEEK_API_KEY"
set -a
source .env
[ -f .env.dogfood ] && source .env.dogfood
set +a

: "${LLM_PROVIDER:=deepseek}"
case "$LLM_PROVIDER" in
  openai)    : "${OPENAI_API_KEY:?OPENAI_API_KEY not set in .env (LLM_PROVIDER=openai)}" ;;
  anthropic) : "${ANTHROPIC_API_KEY:?ANTHROPIC_API_KEY not set in .env (LLM_PROVIDER=anthropic)}" ;;
  deepseek)  : "${DEEPSEEK_API_KEY:?DEEPSEEK_API_KEY not set in .env (LLM_PROVIDER=deepseek)}" ;;
  *) fail "Unknown LLM_PROVIDER: $LLM_PROVIDER (expected openai|anthropic|deepseek)" ;;
esac
: "${DATABASE_URL:?DATABASE_URL not set in .env}"
: "${OLLAMA_URL:?OLLAMA_URL not set in .env}"
GATEWAY_PORT="${GATEWAY_PORT:-8000}"
UI_PORT="${UI_PORT:-5173}"

[ -d ".venv" ] || fail ".venv missing — create with: python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'"
[ -d "ui/node_modules" ] || { log "Installing UI deps…"; (cd ui && npm install --silent); }

pg_isready -d "$DATABASE_URL" -q \
  || fail "Postgres not reachable via DATABASE_URL (try: docker compose up -d postgres)"
curl -fsS "${OLLAMA_URL}/api/tags" >/dev/null 2>&1 \
  || fail "Ollama not reachable at ${OLLAMA_URL} (try: ollama serve)"

# Refuse to step on a stack that's already up.
if lsof -nP -iTCP:"${GATEWAY_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  fail "Port ${GATEWAY_PORT} already in use — run scripts/stop.sh first"
fi
if lsof -nP -iTCP:"${UI_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  fail "Port ${UI_PORT} already in use — run scripts/stop.sh first"
fi

# ----------------------------------------------------------------------
# 2. Apply migrations
# ----------------------------------------------------------------------
log "Applying database migrations…"
psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

applied=0
for f in db/migrations/*.sql; do
  fname="$(basename "$f")"
  done_already=$(psql -tAd "$DATABASE_URL" -c \
    "SELECT 1 FROM schema_migrations WHERE filename='${fname}'")
  if [ -n "$done_already" ]; then
    continue
  fi
  log "  + ${fname}"
  if ! psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f" >/dev/null; then
    warn "  migration ${fname} failed; the schema may already include its objects."
    warn "  Recording it as applied so we don't retry. Inspect db/migrations/${fname} manually if surprised."
  fi
  psql -tAd "$DATABASE_URL" -c \
    "INSERT INTO schema_migrations(filename) VALUES('${fname}') ON CONFLICT DO NOTHING" >/dev/null
  applied=$((applied+1))
done
log "Migrations: ${applied} new"

# ----------------------------------------------------------------------
# 3. Demo snapshot
# ----------------------------------------------------------------------
if [ "$REBUILD_SNAPSHOTS" = "1" ] || [ ! -f "demo/snapshots/pelago-v1.sql.zst" ]; then
  log "Building Pelago demo snapshot…"
  .venv/bin/python -m demo.generation.built.pelago --emit --compress >/dev/null
else
  log "Pelago snapshot already present (use --rebuild-snapshots to refresh)"
fi

# ----------------------------------------------------------------------
# 4. Start processes
# ----------------------------------------------------------------------
LOGDIR="/tmp/fyralis_logs"
mkdir -p "$LOGDIR"
: > "$LOGDIR/gateway.log"
: > "$LOGDIR/think_worker.log"
: > "$LOGDIR/post_commit_worker.log"
: > "$LOGDIR/ui.log"

PIDFILE="/tmp/fyralis_stack.pids"
: > "$PIDFILE"
record_pid() { echo "$1" >> "$PIDFILE"; }

UV_LOG_LEVEL="$(echo "${LOG_LEVEL:-info}" | tr '[:upper:]' '[:lower:]')"

log "Starting gateway on :${GATEWAY_PORT}…"
.venv/bin/uvicorn services.gateway.main:app \
  --host 0.0.0.0 --port "${GATEWAY_PORT}" \
  --log-level "${UV_LOG_LEVEL}" \
  > "$LOGDIR/gateway.log" 2>&1 &
record_pid $!

log "Starting think worker…"
.venv/bin/python scripts/run_think_worker.py \
  > "$LOGDIR/think_worker.log" 2>&1 &
record_pid $!

log "Starting post-commit worker…"
.venv/bin/python scripts/run_post_commit_worker.py \
  > "$LOGDIR/post_commit_worker.log" 2>&1 &
record_pid $!

log "Starting UI on :${UI_PORT}…"
( cd ui && npm run dev -- --host 127.0.0.1 --strictPort > "$LOGDIR/ui.log" 2>&1 ) &
record_pid $!

# ----------------------------------------------------------------------
# 5. Health check + browser
# ----------------------------------------------------------------------
log "Waiting for gateway /healthz…"
ready=0
for i in $(seq 1 45); do
  if curl -fsS "http://127.0.0.1:${GATEWAY_PORT}/healthz" >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
[ "$ready" = "1" ] || fail "Gateway never became healthy — see ${LOGDIR}/gateway.log"

log "Waiting for UI…"
for i in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${UI_PORT}" >/dev/null 2>&1; then break; fi
  sleep 1
done

DEMO_URL="http://127.0.0.1:${UI_PORT}/demo"
cat <<EOF

=== Fyralis stack up ===
  Demo picker:   ${DEMO_URL}
  UI:            http://127.0.0.1:${UI_PORT}
  Gateway:       http://127.0.0.1:${GATEWAY_PORT}
  Healthz:       curl http://127.0.0.1:${GATEWAY_PORT}/healthz
  Logs dir:      ${LOGDIR}/
  Tail logs:     tail -f ${LOGDIR}/*.log
  Stop stack:    scripts/stop.sh
EOF

if [ "$OPEN_BROWSER" = "1" ]; then
  if command -v open >/dev/null 2>&1; then
    (sleep 1; open "$DEMO_URL") &
  elif command -v xdg-open >/dev/null 2>&1; then
    (sleep 1; xdg-open "$DEMO_URL") &
  fi
fi
