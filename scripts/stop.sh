#!/usr/bin/env bash
# scripts/stop.sh — stop the stack started by scripts/start.sh.
# Sends SIGTERM, waits 2s, then SIGKILL anything that survives. Also
# kills the entire process group so vite/uvicorn child processes die
# with their parents. Free of dependencies on the PID file (falls back
# to pattern-matching on the process list if /tmp/fyralis_stack.pids
# is missing — so a Ctrl-C'd start is still cleanable).
set -uo pipefail

PIDFILE="/tmp/fyralis_stack.pids"

stop_pid() {
  local pid="$1"
  [ -z "$pid" ] && return 0
  kill -0 "$pid" 2>/dev/null || return 0
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
}
force_pid() {
  local pid="$1"
  [ -z "$pid" ] && return 0
  kill -0 "$pid" 2>/dev/null || return 0
  kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
}

if [ -f "$PIDFILE" ]; then
  while IFS= read -r pid; do stop_pid "$pid"; done < "$PIDFILE"
  sleep 2
  while IFS= read -r pid; do force_pid "$pid"; done < "$PIDFILE"
  rm -f "$PIDFILE"
else
  # Fallback path: pattern-match the things start.sh launches.
  pkill -TERM -f "uvicorn services.gateway.main:app" 2>/dev/null || true
  pkill -TERM -f "scripts/run_think_worker.py"        2>/dev/null || true
  pkill -TERM -f "scripts/run_post_commit_worker.py"  2>/dev/null || true
  pkill -TERM -f "vite --host 127.0.0.1 --strictPort" 2>/dev/null || true
  sleep 2
  pkill -KILL -f "uvicorn services.gateway.main:app" 2>/dev/null || true
  pkill -KILL -f "scripts/run_think_worker.py"        2>/dev/null || true
  pkill -KILL -f "scripts/run_post_commit_worker.py"  2>/dev/null || true
  pkill -KILL -f "vite --host 127.0.0.1 --strictPort" 2>/dev/null || true
fi

echo "Stack stopped."
