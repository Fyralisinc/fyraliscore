#!/usr/bin/env bash
# scripts/dogfood_down.sh — stop every process started by dogfood_up.sh
set -uo pipefail

PIDFILE=/tmp/company_os_dogfood.pids
if [ ! -f "$PIDFILE" ]; then
  echo "No PID file at $PIDFILE; stack may not be running."
  exit 0
fi

while IFS= read -r pid; do
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    # Kill the whole process group — vite / uvicorn spawn children.
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  fi
done < "$PIDFILE"

sleep 2

# Force-kill anything still hanging on.
while IFS= read -r pid; do
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
done < "$PIDFILE"

rm -f "$PIDFILE" /tmp/dogfood_ui.pid
echo "Stack stopped."
