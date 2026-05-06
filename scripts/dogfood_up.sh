#!/usr/bin/env bash
# scripts/dogfood_up.sh — thin wrapper around scripts/start.sh.
#
# Historical name. start.sh is the canonical entry point; it brings up the
# same gateway + workers + UI stack with provider-aware env validation,
# port-conflict checks, demo snapshot generation, and browser auto-open.
#
# This wrapper is kept so existing muscle memory and docs still work.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "[dogfood_up] forwarding to scripts/start.sh"
exec "$ROOT/scripts/start.sh" "$@"
