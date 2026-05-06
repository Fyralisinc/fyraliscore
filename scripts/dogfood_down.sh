#!/usr/bin/env bash
# scripts/dogfood_down.sh — thin wrapper around scripts/stop.sh.
#
# Historical name. stop.sh is the canonical shutdown script; it tears down
# the same processes, with a fallback to pattern-matching when no PID file
# is present (useful after a Ctrl-C'd start).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "[dogfood_down] forwarding to scripts/stop.sh"
exec "$ROOT/scripts/stop.sh" "$@"
