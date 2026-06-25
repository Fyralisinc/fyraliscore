#!/usr/bin/env bash
# =============================================================================
# demo-dataplane/flow-second-tenant.sh TENANT [REGION] [PLAN]
# -----------------------------------------------------------------------------
# Onboard a SECOND demo tenant against the already-running control plane and
# start a boundary collector that pushes the golden-12 stub metrics under that
# tenant's identity — making the cross-tenant ISOLATION demo reproducible after
# a fresh `make bootstrap` (which only onboards the first tenant, "acme").
#
# What it does (mirrors bootstrap.sh's acme flow, for a 2nd tenant):
#   1. onboard TENANT against the RUNNING console (http://localhost:8080) with
#      the I4 bearer token  -> mints cert + license + signed bundle, registers
#      the deployment in the live fleet, writes the tenant's cert into the
#      shared ca/tenant_registry.json the auth-proxy reads.
#   2. stage _runtime/<TENANT>/ (license trio + client cert/key) the collector mounts.
#   3. relax the gitignored demo registry + client key (0644) so the non-root
#      proxy/collector containers can read them; restart the proxy so it picks
#      up the new tenant's cert fingerprint.
#   4. docker run a boundary collector (otel-collector-contrib) that scrapes the
#      shared demo-dataplane stub and remote-writes THROUGH the mTLS auth-proxy
#      with THIS tenant's client cert — the proxy injects X-Scope-OrgID from the
#      verified cert SAN, so the metrics land in the tenant's own Mimir org (I4).
#
# Re-runnable: re-running for the same tenant reconciles (offboard + re-onboard
# if PARTIAL) and recreates the collector.
#
#   ./demo-dataplane/flow-second-tenant.sh globex eu-west
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # -> control-plane/
cd "$HERE"

TENANT="${1:?usage: flow-second-tenant.sh TENANT [REGION] [PLAN]}"
REGION="${2:-eu-west}"
PLAN="${3:-standard}"
CONSOLE_URL="${CONSOLE_URL:-http://localhost:8080}"
PROJECT_NET="${PROJECT_NET:-fyralis-control-plane_cp-net}"
DATAPLANE_NET="${DATAPLANE_NET:-dataplane-net}"
COLLECTOR_IMAGE="otel/opentelemetry-collector-contrib:0.105.0"

PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python" ]]; then
    PYTHON_BIN="/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  else PYTHON_BIN="python3"; fi
fi

say() { printf '\033[1;36m[2nd-tenant]\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }

TOKEN_FILE="$HERE/_runtime/secrets/console_ingest_token"
[[ -s "$TOKEN_FILE" ]] || { echo "missing $TOKEN_FILE — run ./bootstrap.sh first" >&2; exit 1; }
TOKEN="$(tr -d '\n' < "$TOKEN_FILE")"

# --- 1. onboard against the RUNNING console ---------------------------------
say "onboarding '$TENANT' (region=$REGION plan=$PLAN) against $CONSOLE_URL …"
OUT="$(CONSOLE_INGEST_TOKEN="$TOKEN" "$PYTHON_BIN" onboarding/onboard.py \
  --tenant "$TENANT" --region "$REGION" --plan "$PLAN" \
  --console-url "$CONSOLE_URL" --console-token "$TOKEN" --json)"
echo "$OUT"
read -r BUNDLE_DIR DEPLOYMENT_ID < <("$PYTHON_BIN" - <<'PY' "$OUT"
import json, sys
text = sys.argv[1]; dec = json.JSONDecoder(); obj = None
for i, ch in enumerate(text):
    if ch == "{":
        try: obj, _ = dec.raw_decode(text[i:]); break
        except json.JSONDecodeError: continue
if obj is None: sys.exit("could not parse onboard JSON result")
print(obj["bundle_dir"], obj["deployment_id"])
PY
)
[[ -n "$BUNDLE_DIR" && -d "$BUNDLE_DIR" ]] || { echo "onboard produced no bundle dir" >&2; exit 1; }
ok "onboarded $TENANT -> $DEPLOYMENT_ID (bundle: $BUNDLE_DIR)"

# --- 2. stage _runtime/<TENANT>/ --------------------------------------------
say "staging _runtime/$TENANT/ …"
RT="$HERE/_runtime/$TENANT"
mkdir -p "$RT"
cp -f "$BUNDLE_DIR/${TENANT}.license.json"               "$RT/license.json"
cp -f "$BUNDLE_DIR/${TENANT}.license.json.sig"           "$RT/license.json.sig"
cp -f "$BUNDLE_DIR/${TENANT}.license.json.manifest.json" "$RT/license.json.manifest.json"
cp -f "$BUNDLE_DIR/cert/${TENANT}.crt"                   "$RT/client.crt"
cp -f "$BUNDLE_DIR/cert/${TENANT}.key"                   "$RT/client.key"

# --- 3. relax demo permissions + refresh the proxy's registry view ----------
chmod 0644 "$RT/client.key" 2>/dev/null || true
chmod 0644 "$HERE/ca/tenant_registry.json" 2>/dev/null || true
say "restarting auth-proxy to pick up $TENANT's cert fingerprint …"
docker compose -f docker-compose.control-plane.yml restart auth-proxy >/dev/null
sleep 3

# --- 4. start the tenant's boundary collector -------------------------------
say "starting boundary collector cp-boundary-$TENANT …"
docker rm -f "cp-boundary-$TENANT" >/dev/null 2>&1 || true
docker run -d --name "cp-boundary-$TENANT" --restart unless-stopped \
  --network "$PROJECT_NET" \
  -e FYRALIS_TENANT_ID="$TENANT" \
  -e FYRALIS_DEPLOYMENT_ID="$DEPLOYMENT_ID" \
  -e FYRALIS_REGION="$REGION" \
  -e FYRALIS_TELEMETRY_TIER=T1 \
  -e FYRALIS_AUTH_PROXY_URL="https://auth-proxy:8443" \
  -v "$HERE/demo-dataplane/boundary-collector.demo.yaml:/etc/otelcol/config.yaml:ro" \
  -v "$HERE/_runtime/ca:/etc/fyralis/ca:ro" \
  -v "$RT:/etc/fyralis/agent:ro" \
  "$COLLECTOR_IMAGE" --config=/etc/otelcol/config.yaml >/dev/null
# scrape the shared demo-dataplane stub (on dataplane-net) AND reach the proxy (on cp-net)
docker network connect "$DATAPLANE_NET" "cp-boundary-$TENANT"
ok "cp-boundary-$TENANT up — pushing golden-12 under org '$TENANT' via mTLS proxy"
echo
ok "DONE. '$TENANT' should have series in Mimir within ~30s. Verify isolation with:"
echo "  docker exec cp-grafana sh -c \"wget -qO- --header='X-Scope-OrgID: $TENANT' 'http://mimir:9009/prometheus/api/v1/query?query=count(up)'\""
