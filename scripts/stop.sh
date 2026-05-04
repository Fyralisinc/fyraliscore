#!/usr/bin/env bash
# scripts/stop.sh — stop the stack started by scripts/start.sh.
# Two-pass shutdown: first by recorded PIDs (if the PID file exists),
# then always by pattern-match. The pattern pass is unconditional because
# npm → sh → node/vite all share the shell's process group rather than
# their own, so killing by process group (-$pid) doesn't reach the vite
# grandchild. The pkill sweep catches those escaped children reliably.
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
fi

# Always pattern-kill to catch child processes (node/vite) that escape
# the PID-based signal above.
pkill -TERM -f "uvicorn services.gateway.main:app" 2>/dev/null || true
pkill -TERM -f "scripts/run_think_worker.py"        2>/dev/null || true
pkill -TERM -f "scripts/run_post_commit_worker.py"  2>/dev/null || true
pkill -TERM -f "vite --host 127.0.0.1 --strictPort" 2>/dev/null || true
sleep 2
pkill -KILL -f "uvicorn services.gateway.main:app" 2>/dev/null || true
pkill -KILL -f "scripts/run_think_worker.py"        2>/dev/null || true
pkill -KILL -f "scripts/run_post_commit_worker.py"  2>/dev/null || true
pkill -KILL -f "vite --host 127.0.0.1 --strictPort" 2>/dev/null || true

echo "Stack stopped."
