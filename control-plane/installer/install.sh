#!/usr/bin/env bash
# ============================================================================
# install.sh — WS-INSTALLER: stand up ONE tenant deployment from an agent bundle
# ----------------------------------------------------------------------------
#   install.sh [--dry-run] [--no-register] [--no-up] <agent-bundle-dir>
#
# Steps:
#   1. VALIDATE the bundle (bundle_lib: required files, manifest, cert SAN
#      round-trip (C1), trust-root, signed license/config verify (I6), license
#      not expired). --dry-run STOPS here with a clear PASS/FAIL.
#   2. RENDER a per-deployment .env from the bundle manifest (the ${...} vars the
#      deployment overlay is parameterized by).
#   3. REGISTER the deployment via the console/onboarding REST API if reachable
#      (POST /api/v1/register) — best-effort, never blocks the install (I3).
#   4. LAUNCH the deployment overlay: data plane + boundary + agent.
#   5. PRINT next steps.
#
# This is the MINIMAL LOCAL installer. Production = Helm/Terraform (see README).
# ============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CP_DIR="$(cd "$HERE/.." && pwd)"
OVERLAY="$HERE/deployment.compose.yml"

# Prefer the project venv python (has cryptography + pydantic + pyyaml); fall
# back to whatever python3 is on PATH.
PY_CANDIDATES=(
  "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  "$(command -v python3 || true)"
  "$(command -v python || true)"
)
PY=""
for c in "${PY_CANDIDATES[@]}"; do
  if [ -n "$c" ] && [ -x "$c" ]; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "ERROR: no python interpreter found (need python3 with 'cryptography')." >&2
  exit 3
fi

# docker compose detection (plugin v2 preferred; legacy docker-compose accepted).
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

DRY_RUN=0
DO_REGISTER=1
DO_UP=1
BUNDLE_DIR=""

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --no-register) DO_REGISTER=0; shift ;;
    --no-up) DO_UP=0; shift ;;
    -h|--help) usage 0 ;;
    -*) echo "unknown flag: $1" >&2; usage 2 ;;
    *) BUNDLE_DIR="$1"; shift ;;
  esac
done

if [ -z "$BUNDLE_DIR" ]; then
  echo "ERROR: missing <agent-bundle-dir>." >&2
  usage 2
fi
if [ ! -d "$BUNDLE_DIR" ]; then
  echo "ERROR: bundle dir not found: $BUNDLE_DIR" >&2
  exit 2
fi
BUNDLE_DIR="$(cd "$BUNDLE_DIR" && pwd)"

echo "=========================================================="
echo " Fyralis installer — bundle: $BUNDLE_DIR"
echo "=========================================================="

# --- 1) VALIDATE ----------------------------------------------------------
echo
echo "[1/5] Validating agent bundle ..."
if ! "$PY" "$HERE/validate_bundle.py" "$BUNDLE_DIR"; then
  echo "ERROR: bundle validation FAILED — refusing to install." >&2
  exit 1
fi

if [ "$DRY_RUN" -eq 1 ]; then
  echo
  echo "[dry-run] bundle is VALID. Stopping before render/register/launch."
  exit 0
fi

# --- 2) RENDER .env -------------------------------------------------------
echo
echo "[2/5] Rendering deployment env ..."
ENV_FILE="$HERE/.deployment.env"
# validate_bundle re-runs the validation (cheap) and emits the sourceable env.
"$PY" "$HERE/validate_bundle.py" "$BUNDLE_DIR" --print-env --control-plane-dir "$CP_DIR" \
  | grep -E '^export ' | sed 's/^export //' > "$ENV_FILE"
echo "  wrote $ENV_FILE:"
sed 's/^/    /' "$ENV_FILE"

# pull a couple of values for messaging / registration
get_env() { grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2- | sed "s/^'//; s/'$//"; }
TENANT_ID="$(get_env FYRALIS_TENANT_ID)"
DEPLOYMENT_ID="$(get_env FYRALIS_DEPLOYMENT_ID)"
REGION="$(get_env FYRALIS_REGION)"
CONSOLE_URL="$(get_env FYRALIS_CONSOLE_URL)"
DEPLOYMENT_NAME="$(get_env FYRALIS_DEPLOYMENT_NAME)"

# --- 3) REGISTER (best-effort) -------------------------------------------
echo
if [ "$DO_REGISTER" -eq 1 ]; then
  echo "[3/5] Registering deployment with console ($CONSOLE_URL) ..."
  REG_BODY="$("$PY" - "$TENANT_ID" "$REGION" <<'PYEOF'
import json, sys
tenant, region = sys.argv[1], sys.argv[2]
print(json.dumps({"tenant_id": tenant, "region": region, "plan": "enterprise"}))
PYEOF
)"
  if command -v curl >/dev/null 2>&1; then
    if curl -fsS --max-time 5 -X POST "$CONSOLE_URL/api/v1/register" \
         -H 'Content-Type: application/json' -d "$REG_BODY" 2>/dev/null; then
      echo "  registration accepted."
    else
      echo "  console unreachable — proceeding anyway (the agent will register/heartbeat" \
           "when it dials home; I3: data plane does not depend on the control plane)."
    fi
  else
    echo "  curl not found — skipping HTTP registration (agent dial-home will register)."
  fi
else
  echo "[3/5] Skipping registration (--no-register)."
fi

# --- 4) LAUNCH ------------------------------------------------------------
echo
if [ "$DO_UP" -eq 1 ]; then
  echo "[4/5] Launching deployment overlay ($DEPLOYMENT_NAME) ..."
  echo "  validating overlay config ..."
  if ! compose -f "$OVERLAY" --env-file "$ENV_FILE" config >/dev/null; then
    echo "ERROR: overlay config invalid (see above)." >&2
    exit 1
  fi
  echo "  bringing services up (data plane + boundary + agent) ..."
  compose -f "$OVERLAY" --env-file "$ENV_FILE" up -d
  echo "  services:"
  compose -f "$OVERLAY" --env-file "$ENV_FILE" ps
else
  echo "[4/5] Skipping launch (--no-up). Overlay + env are rendered and ready."
fi

# --- 5) NEXT STEPS --------------------------------------------------------
echo
echo "[5/5] Done."
cat <<EOF

=========================================================
 Deployment ready
   tenant:        $TENANT_ID
   deployment_id: $DEPLOYMENT_ID
   region:        $REGION
   project:       $DEPLOYMENT_NAME

 Next steps:
   * Watch the boundary collector egress metrics outbound to the auth-proxy:
       docker compose -f $OVERLAY --env-file $ENV_FILE logs -f boundary
   * Watch the agent dial home + heartbeat (outbound-only, I2):
       docker compose -f $OVERLAY --env-file $ENV_FILE logs -f agent
   * The deployment should appear in the fleet console:
       $CONSOLE_URL/   (GET /api/v1/deployments)
   * Tear down:
       $HERE/uninstall.sh $BUNDLE_DIR        # or: --project $DEPLOYMENT_NAME

 NOTE: this is the MINIMAL LOCAL installer (single host). The production path is
 Helm/Terraform — see installer/README.md.
=========================================================
EOF
