#!/usr/bin/env bash
# scripts/dogfood_up.sh — brings up the full Company OS dogfood stack
#
# The real architecture is a single gateway app (not six separate services).
# Processes started:
#   - gateway         (uvicorn services.gateway.main:app on :8000)
#   - think_worker    (services.think.worker.ThinkWorker)
#   - post_commit_worker (services.think.post_commit.process_batch loop)
#   - topology_sweeper (latent relationship-field refresh loop)
#   - ui              (vite dev server on :5173)
#
# The gateway spawns the GRT scheduler and realtime dispatcher in-process;
# the SIM router is mounted when GATEWAY_MOUNT_SIM=1 (default in dev).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ---- Env ------------------------------------------------------------
if [ ! -f .env ]; then
  echo "ERROR: .env not found (LLM credentials live there)."
  exit 1
fi
set -a
# Base env first, dogfood overrides last.
source .env
if [ -f .env.dogfood ]; then source .env.dogfood; fi
set +a

case "${LLM_PROVIDER:-deepseek}" in
  deepseek)
    [ -n "${DEEPSEEK_API_KEY:-${LLM_API_KEY:-}}" ] \
      || { echo "ERROR: DEEPSEEK_API_KEY or LLM_API_KEY not set"; exit 1; }
    ;;
  openai)
    [ -n "${OPENAI_API_KEY:-${LLM_API_KEY:-}}" ] \
      || { echo "ERROR: OPENAI_API_KEY or LLM_API_KEY not set"; exit 1; }
    ;;
  anthropic)
    [ -n "${ANTHROPIC_API_KEY:-${LLM_API_KEY:-}}" ] \
      || { echo "ERROR: ANTHROPIC_API_KEY or LLM_API_KEY not set"; exit 1; }
    ;;
  codex)
    CODEX_AUTH_PATH="${CODEX_AUTH_FILE:-${CODEX_HOME:-$HOME/.codex}/auth.json}"
    if [ "${CODEX_TRANSPORT:-auto}" = "responses" ]; then
      [ -n "${CODEX_API_KEY:-${OPENAI_API_KEY:-${LLM_API_KEY:-}}}" ] \
        || { echo "ERROR: Codex Responses auth missing; set CODEX_API_KEY/OPENAI_API_KEY/LLM_API_KEY"; exit 1; }
    else
      [ -n "${CODEX_API_KEY:-${OPENAI_API_KEY:-${LLM_API_KEY:-}}}" ] \
        || [ -f "$CODEX_AUTH_PATH" ] \
        || { echo "ERROR: Codex auth missing; set CODEX_API_KEY/OPENAI_API_KEY/LLM_API_KEY or run codex login"; exit 1; }
    fi
    ;;
  *)
    echo "ERROR: Unsupported LLM_PROVIDER=${LLM_PROVIDER}"
    exit 1
    ;;
esac

# ---- Sanity checks --------------------------------------------------
pg_isready >/dev/null || { echo "ERROR: Postgres not running"; exit 1; }
curl -fsS "${OLLAMA_URL}/api/tags" >/dev/null 2>&1 \
  || { echo "ERROR: Ollama not reachable at ${OLLAMA_URL}"; exit 1; }

if [ ! -d ".venv" ]; then
  echo "ERROR: .venv not found. Create with python3 -m venv .venv && pip install -e '.[dev]'"
  exit 1
fi

# ---- Log directory --------------------------------------------------
LOGDIR="/tmp/company_os_logs"
mkdir -p "$LOGDIR"
: > "$LOGDIR/gateway.log"
: > "$LOGDIR/think_worker.log"
: > "$LOGDIR/post_commit_worker.log"
: > "$LOGDIR/topology_sweeper.log"
: > "$LOGDIR/ui.log"

PIDS=()

# ---- Start services -------------------------------------------------
PY=".venv/bin/python"
UVICORN=".venv/bin/uvicorn"

echo "Starting gateway on :${GATEWAY_PORT}..."
# uvicorn wants lowercase log-level; lowercase LOG_LEVEL before passing.
UVICORN_LOG_LEVEL="$(echo "${LOG_LEVEL:-info}" | tr '[:upper:]' '[:lower:]')"
"$UVICORN" services.gateway.main:app \
  --host 0.0.0.0 --port "${GATEWAY_PORT}" \
  --log-level "${UVICORN_LOG_LEVEL}" \
  > "$LOGDIR/gateway.log" 2>&1 &
PIDS+=($!)

# Workers share the same DB pool config via env, separate processes so
# a crash in one doesn't take down the gateway.
echo "Starting think worker..."
"$PY" scripts/run_think_worker.py > "$LOGDIR/think_worker.log" 2>&1 &
PIDS+=($!)

echo "Starting post_commit worker..."
"$PY" scripts/run_post_commit_worker.py > "$LOGDIR/post_commit_worker.log" 2>&1 &
PIDS+=($!)

echo "Starting topology sweeper..."
"$PY" scripts/run_topology_sweeper.py > "$LOGDIR/topology_sweeper.log" 2>&1 &
PIDS+=($!)

# ---- UI -------------------------------------------------------------
echo "Starting UI (vite) on :5173..."
( cd ui && npm run dev > "$LOGDIR/ui.log" 2>&1 & echo $! ) > /tmp/dogfood_ui.pid
UI_PID=$(cat /tmp/dogfood_ui.pid)
PIDS+=("$UI_PID")

# ---- Record PIDs ----------------------------------------------------
printf "%s\n" "${PIDS[@]}" > /tmp/company_os_dogfood.pids

# ---- Health check ---------------------------------------------------
echo ""
echo "Waiting for gateway /healthz..."
for i in $(seq 1 30); do
  if curl -fsS "http://localhost:${GATEWAY_PORT}/healthz" >/dev/null 2>&1; then
    echo "  gateway OK"
    break
  fi
  sleep 1
  if [ "$i" = "30" ]; then
    echo "  gateway did NOT become healthy in 30s — check $LOGDIR/gateway.log"
  fi
done

cat <<EOF

=== Company OS dogfood stack up ===
  Gateway:         http://localhost:${GATEWAY_PORT}
  Main UI:         http://localhost:5173
  Slack simulator: http://localhost:${GATEWAY_PORT}/simulation/slack_ui/
  Healthz:         curl http://localhost:${GATEWAY_PORT}/healthz
  Logs:            $LOGDIR/
  Tail all:        scripts/dogfood_logs.sh
  Inspect state:   scripts/dogfood_inspect.sh
  Stop:            scripts/dogfood_down.sh

PIDs written to /tmp/company_os_dogfood.pids
EOF
