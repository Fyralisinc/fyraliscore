#!/usr/bin/env bash
# ============================================================================
# uninstall.sh — tear down a tenant deployment stood up by install.sh
# ----------------------------------------------------------------------------
#   uninstall.sh [--volumes] <agent-bundle-dir>
#   uninstall.sh [--volumes] --project <compose-project-name>
#
# Resolves the compose project name (from the bundle manifest, or given directly)
# and runs `docker compose down` on the deployment overlay. By default volumes
# are KEPT (the agent's buffered heartbeats + the data-plane Postgres survive a
# reinstall). Pass --volumes to also delete them.
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CP_DIR="$(cd "$HERE/.." && pwd)"
OVERLAY="$HERE/deployment.compose.yml"

PY_CANDIDATES=(
  "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  "$(command -v python3 || true)"
  "$(command -v python || true)"
)
PY=""
for c in "${PY_CANDIDATES[@]}"; do
  if [ -n "$c" ] && [ -x "$c" ]; then PY="$c"; break; fi
done

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  elif command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
  else
    echo "ERROR: neither 'docker compose' nor 'docker-compose' is available." >&2
    return 127
  fi
}

WITH_VOLUMES=0
PROJECT=""
BUNDLE_DIR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --volumes|-v) WITH_VOLUMES=1; shift ;;
    --project) PROJECT="$2"; shift 2 ;;
    -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown flag: $1" >&2; exit 2 ;;
    *) BUNDLE_DIR="$1"; shift ;;
  esac
done

# Resolve project name from the bundle manifest if not given directly.
if [ -z "$PROJECT" ]; then
  if [ -z "$BUNDLE_DIR" ]; then
    echo "ERROR: provide an <agent-bundle-dir> or --project <name>." >&2
    exit 2
  fi
  if [ ! -f "$BUNDLE_DIR/bundle.json" ]; then
    echo "ERROR: no bundle.json in $BUNDLE_DIR (cannot resolve project name)." >&2
    exit 2
  fi
  if [ -z "$PY" ]; then
    echo "ERROR: no python to read bundle.json; pass --project <name> instead." >&2
    exit 3
  fi
  PROJECT="$("$PY" - "$BUNDLE_DIR/bundle.json" <<'PYEOF'
import json, sys
m = json.load(open(sys.argv[1]))
dep = m["deployment_id"].lower()
slug = "".join(c if (c.isalnum() or c == "-") else "-" for c in dep).strip("-")
print(f"fyralis-dp-{slug}")
PYEOF
)"
fi

echo "Tearing down deployment project: $PROJECT"

# Prefer the rendered env-file (so the overlay's required ${...} vars resolve);
# fall back to interpolating just the project name via env if it is absent.
ENV_FILE="$HERE/.deployment.env"
DOWN_ARGS=(-f "$OVERLAY")
if [ -f "$ENV_FILE" ]; then
  DOWN_ARGS+=(--env-file "$ENV_FILE")
fi
DOWN_ARGS+=(-p "$PROJECT" down --remove-orphans)
if [ "$WITH_VOLUMES" -eq 1 ]; then
  DOWN_ARGS+=(--volumes)
  echo "  (also removing volumes: agent buffer + data-plane state)"
fi

# `down -p <project>` works even without the env-file because down does not need
# the service definitions resolved; but supplying placeholders keeps interpolation
# warnings quiet on older compose.
FYRALIS_TENANT_ID="${FYRALIS_TENANT_ID:-_}" \
FYRALIS_DEPLOYMENT_ID="${FYRALIS_DEPLOYMENT_ID:-_}" \
FYRALIS_AUTH_PROXY_URL="${FYRALIS_AUTH_PROXY_URL:-_}" \
FYRALIS_BUNDLE_DIR="${FYRALIS_BUNDLE_DIR:-${BUNDLE_DIR:-/tmp}}" \
  compose "${DOWN_ARGS[@]}"

echo "Done. Project '$PROJECT' is down."
if [ "$WITH_VOLUMES" -eq 0 ]; then
  echo "(volumes kept — rerun with --volumes to delete buffered state)"
fi
