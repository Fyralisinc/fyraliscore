#!/usr/bin/env bash
# =============================================================================
# control-plane/bootstrap.sh — the FIRST-RUN one-command entry for the Fyralis
# BYOC control plane. Run this and a CTO has a live, testable control plane.
#
#   ./bootstrap.sh              # full: trust roots + demo onboard + stack up
#   ./bootstrap.sh --no-docker  # trust roots + demo onboard + python smoke only
#   ./bootstrap.sh --help
#
# WHAT IT DOES (idempotent — safe to re-run)
#   1. Generate the CA (ca/bootstrap_ca.py)                      -> ca/pki/*
#   2. Generate + ACTIVATE the CP signing key (signing/keygen.py --activate)
#                                                                -> signing/trust_root.json
#      (private key stays gitignored under signing/keys/)
#   3. Mint the auth-proxy server cert (auth-proxy/gen_server_cert.py)
#                                                                -> auth-proxy/tls/{server.crt,server.key}
#   4. ONBOARD the demo tenant "acme" (onboarding/onboard.py, embedded console)
#      -> a signed bundle (cert + license + agent-config) under
#         onboarding/bundles/<deployment_id>/ (gitignored), then copy the runtime
#         material the compose mounts into ./_runtime/ (gitignored):
#           _runtime/agent/license.json[.sig|.manifest.json]   (agent license, I6)
#           _runtime/agent/client.crt / client.key             (boundary mTLS cert)
#           _runtime/ca/ca.crt                                 (CA chain for the proxy)
#      and write a .env so compose picks up the real deployment id.
#   5. (default) docker compose up -d, wait for health, print the URLs.
#
# The data plane is a DEMO STUB (demo-dataplane/) — in production the installer
# points the boundary collector at the REAL data plane. See README.md.
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

COMPOSE_FILE="docker-compose.control-plane.yml"
RUNTIME_DIR="$HERE/_runtime"
TENANT="${TENANT:-acme}"
REGION="${REGION:-us-east-1}"
PLAN="${PLAN:-standard}"
DATAPLANE_NET="dataplane-net"

NO_DOCKER=0
for arg in "$@"; do
  case "$arg" in
    --no-docker) NO_DOCKER=1 ;;
    -h|--help)
      sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "unknown arg: $arg (try --help)" >&2; exit 2 ;;
  esac
done

# --- pick a python (prefer the repo dev venv) --------------------------------
PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python" ]]; then
    PYTHON_BIN="/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

say() { printf '\033[1;36m[bootstrap]\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
warn(){ printf '\033[1;33m[warn]\033[0m %s\n' "$*"; }

say "python: $PYTHON_BIN"

# ============================================================================ #
# 1. CA                                                                        #
# ============================================================================ #
CA_CHAIN="$HERE/ca/pki/ca-chain.crt"
if [[ -f "$HERE/ca/pki/keys/root.key" ]]; then
  ok "CA already present ($CA_CHAIN) — skipping bootstrap_ca"
else
  say "generating the Fyralis CA (root + intermediate) …"
  "$PYTHON_BIN" ca/bootstrap_ca.py --pki-dir "$HERE/ca/pki"
  ok "CA chain at $CA_CHAIN"
fi

# ============================================================================ #
# 2. signing key + trust root                                                  #
# ============================================================================ #
TRUST_ROOT="$HERE/signing/trust_root.json"
if [[ -f "$TRUST_ROOT" ]] && "$PYTHON_BIN" -c "import json,sys; d=json.load(open('$TRUST_ROOT')); sys.exit(0 if d.get('active_key_id') else 1)" 2>/dev/null; then
  ok "signing trust root already active ($TRUST_ROOT) — skipping keygen"
else
  say "generating + ACTIVATING the CP signing key …"
  "$PYTHON_BIN" signing/keygen.py --activate
  ok "trust root at $TRUST_ROOT (private key stays gitignored under signing/keys/)"
fi

# ============================================================================ #
# 3. auth-proxy server cert                                                    #
# ============================================================================ #
PROXY_CRT="$HERE/auth-proxy/tls/server.crt"
PROXY_KEY="$HERE/auth-proxy/tls/server.key"
if [[ -f "$PROXY_CRT" && -f "$PROXY_KEY" ]]; then
  ok "auth-proxy server cert already present — skipping"
else
  say "minting the auth-proxy server cert (SANs: localhost 127.0.0.1 auth-proxy) …"
  "$PYTHON_BIN" auth-proxy/gen_server_cert.py \
    --pki-dir "$HERE/ca/pki" \
    --out-dir "$HERE/auth-proxy/tls" \
    --san localhost --san 127.0.0.1 --san auth-proxy
  # gen_server_cert writes <name>.crt/.key; normalize to server.crt/server.key.
  if [[ ! -f "$PROXY_CRT" ]]; then
    crt="$(ls -1 "$HERE/auth-proxy/tls/"*.crt 2>/dev/null | head -1 || true)"
    key="$(ls -1 "$HERE/auth-proxy/tls/"*.key 2>/dev/null | head -1 || true)"
    [[ -n "$crt" ]] && cp -f "$crt" "$PROXY_CRT"
    [[ -n "$key" ]] && cp -f "$key" "$PROXY_KEY"
  fi
  ok "auth-proxy server cert at $PROXY_CRT"
fi
# The proxy container runs as a non-root uid (10001) and bind-mounts this key
# read-only; a 0600 host key (owned by the operator) is unreadable inside the
# container. This is a gitignored DEV/DEMO cert, so relax it to group/other
# read so the containerized proxy can load it. (In prod the key is delivered via
# a secrets manager with matching container ownership, not a host bind-mount.)
[[ -f "$PROXY_KEY" ]] && chmod 0644 "$PROXY_KEY" 2>/dev/null || true

# ============================================================================ #
# 3b. console write token (CONSOLE_INGEST_TOKEN, I4)                            #
# ============================================================================ #
# The console's WRITE endpoints (register/heartbeat/delete) require a bearer
# token (I4) — without one anything on cp-net could forge fleet state. Generate
# one ONCE into a gitignored runtime path; the compose passes it to the console
# (env) and bind-mounts the file into the agent (AGENT_CONSOLE_TOKEN_FILE), and
# onboarding stamps it into the agent bundle's agent-config.json.
mkdir -p "$RUNTIME_DIR/secrets"
TOKEN_FILE="$RUNTIME_DIR/secrets/console_ingest_token"
if [[ -s "$TOKEN_FILE" ]]; then
  ok "console ingest token already present ($TOKEN_FILE) — reusing"
else
  say "generating the console ingest token (CONSOLE_INGEST_TOKEN, I4) …"
  "$PYTHON_BIN" -c "import secrets; print(secrets.token_urlsafe(32))" > "$TOKEN_FILE"
  # The agent container (uid 10010) bind-mounts this read-only; relax so it can
  # read the gitignored dev/demo token (in prod it is a secrets-manager secret).
  chmod 0644 "$TOKEN_FILE" 2>/dev/null || true
  ok "console ingest token at $TOKEN_FILE"
fi
CONSOLE_INGEST_TOKEN="$(tr -d '\n' < "$TOKEN_FILE")"

# ============================================================================ #
# 4. onboard the demo tenant "acme" -> bundle -> _runtime/                     #
# ============================================================================ #
mkdir -p "$RUNTIME_DIR/agent" "$RUNTIME_DIR/ca"
ENV_FILE="$HERE/.env"

# Is the demo tenant already onboarded? (a license already staged in _runtime)
if [[ -f "$RUNTIME_DIR/agent/license.json" && -f "$RUNTIME_DIR/agent/client.crt" && -f "$ENV_FILE" ]]; then
  ok "demo tenant '$TENANT' already onboarded — runtime material staged in _runtime/"
else
  say "onboarding demo tenant '$TENANT' (region=$REGION plan=$PLAN, embedded console) …"
  # onboard.py prints step logs AND the --json result to stdout, so extract the
  # trailing balanced JSON object (raw_decode from the last top-level '{').
  ONBOARD_OUT="$(CONSOLE_INGEST_TOKEN="$CONSOLE_INGEST_TOKEN" "$PYTHON_BIN" onboarding/onboard.py \
    --tenant "$TENANT" --region "$REGION" --plan "$PLAN" \
    --console-token "$CONSOLE_INGEST_TOKEN" \
    --embedded-console --json)"
  echo "$ONBOARD_OUT"

  read -r BUNDLE_DIR DEPLOYMENT_ID < <("$PYTHON_BIN" - <<'PY' "$ONBOARD_OUT"
import json, sys
text = sys.argv[1]
dec = json.JSONDecoder()
obj = None
for i, ch in enumerate(text):
    if ch == "{":
        try:
            obj, _ = dec.raw_decode(text[i:])
            break
        except json.JSONDecodeError:
            continue
if obj is None:
    sys.exit("could not parse onboard JSON result")
print(obj["bundle_dir"], obj["deployment_id"])
PY
)
  [[ -n "$BUNDLE_DIR" && -d "$BUNDLE_DIR" ]] || { echo "onboard did not produce a bundle dir" >&2; exit 1; }

  say "staging runtime material from $BUNDLE_DIR -> _runtime/ …"
  # License trio (named <tenant>.license.json in the bundle; compose mounts plain license.json).
  cp -f "$BUNDLE_DIR/${TENANT}.license.json"               "$RUNTIME_DIR/agent/license.json"
  cp -f "$BUNDLE_DIR/${TENANT}.license.json.sig"           "$RUNTIME_DIR/agent/license.json.sig"
  cp -f "$BUNDLE_DIR/${TENANT}.license.json.manifest.json" "$RUNTIME_DIR/agent/license.json.manifest.json"
  # Tenant client cert/key for the boundary collector's mTLS to the proxy.
  cp -f "$BUNDLE_DIR/cert/${TENANT}.crt" "$RUNTIME_DIR/agent/client.crt"
  cp -f "$BUNDLE_DIR/cert/${TENANT}.key" "$RUNTIME_DIR/agent/client.key"
  # CA chain the collector uses to verify the proxy server cert.
  cp -f "$CA_CHAIN" "$RUNTIME_DIR/ca/ca.crt"
  # The boundary collector container runs non-root and bind-mounts these
  # read-only; relax the demo client key so the container can load it (see the
  # auth-proxy key note above — gitignored dev/demo material).
  chmod 0644 "$RUNTIME_DIR/agent/client.key" 2>/dev/null || true

  # The auth-proxy container (uid 10001) bind-mounts ca/tenant_registry.json
  # read-only to resolve cert-fingerprint -> tenant + revocation status. The CA
  # tooling writes it 0600 owned by the operator, which the container user can't
  # read (=> 403 registry_read_error on every push). Relax the demo registry so
  # the containerized proxy can read it (it is the public revocation list, not a
  # secret). In prod the registry is delivered with matching container ownership.
  chmod 0644 "$HERE/ca/tenant_registry.json" 2>/dev/null || true

  # Write a .env so compose binds the agent / demo-dataplane / collector to the
  # REAL deployment id the onboard minted.
  cat > "$ENV_FILE" <<EOF
# Generated by bootstrap.sh — demo tenant binding. Safe to delete + re-bootstrap.
AGENT_TENANT_ID=$TENANT
AGENT_DEPLOYMENT_ID=$DEPLOYMENT_ID
AGENT_REGION=$REGION
AGENT_TELEMETRY_TIER=T1
# Console write token (I4) — the console requires it on register/heartbeat/delete;
# the agent reads it from the mounted token file (AGENT_CONSOLE_TOKEN_FILE).
CONSOLE_INGEST_TOKEN=$CONSOLE_INGEST_TOKEN
EOF
  ok "onboarded $TENANT -> $DEPLOYMENT_ID; runtime staged; wrote .env"
fi

# Ensure CONSOLE_INGEST_TOKEN is in .env even when the tenant was already
# onboarded on a prior run (the .env block above only runs in the else branch).
if [[ -f "$ENV_FILE" ]] && ! grep -q '^CONSOLE_INGEST_TOKEN=' "$ENV_FILE"; then
  printf 'CONSOLE_INGEST_TOKEN=%s\n' "$CONSOLE_INGEST_TOKEN" >> "$ENV_FILE"
  ok "appended CONSOLE_INGEST_TOKEN to existing .env"
fi

# ============================================================================ #
# 5. bring the stack up (unless --no-docker)                                   #
# ============================================================================ #
if [[ "$NO_DOCKER" -eq 1 ]]; then
  say "--no-docker: skipping docker. Running the python e2e smoke instead …"
  if [[ -f "tests/e2e_smoke.py" ]]; then
    "$PYTHON_BIN" tests/e2e_smoke.py || warn "smoke reported issues (see above)"
  else
    warn "tests/e2e_smoke.py not found — skipping smoke"
  fi
  ok "no-docker bootstrap complete (CA + signing + demo onboard + smoke)."
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found on PATH — re-run with --no-docker for the offline path." >&2
  exit 1
fi

# The data-plane network is declared external in the compose; create it idempotently.
if ! docker network inspect "$DATAPLANE_NET" >/dev/null 2>&1; then
  say "creating external docker network '$DATAPLANE_NET' …"
  docker network create "$DATAPLANE_NET" >/dev/null
fi

say "bringing the control plane up (docker compose up -d --build) …"
docker compose -f "$COMPOSE_FILE" up -d --build

# --- wait for the operator-facing surfaces to be healthy --------------------
say "waiting for core services to become healthy (console + grafana + mimir) …"
wait_http() {
  local name="$1" url="$2" tries="${3:-60}"
  local i=0
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    i=$((i+1))
    if [[ "$i" -ge "$tries" ]]; then warn "$name not ready after $((tries*2))s ($url)"; return 1; fi
    sleep 2
  done
  ok "$name ready ($url)"
}
# Mimir + cp-prometheus are cp-net-ONLY now (no host ports, I4) — probe them from
# INSIDE the network via the self-obs exporter container rather than the host.
wait_incluster() {
  local name="$1" url="$2" tries="${3:-60}"
  local i=0
  until docker compose -f "$COMPOSE_FILE" exec -T cp-self-obs-exporter \
        python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('$url',timeout=3).status==200 else 1)" \
        >/dev/null 2>&1; do
    i=$((i+1))
    if [[ "$i" -ge "$tries" ]]; then warn "$name not ready after $((tries*2))s ($url, in-cluster)"; return 1; fi
    sleep 2
  done
  ok "$name ready ($url, in-cluster)"
}
# Operator-facing surfaces are host-published — probe them on the host.
wait_http "Console" "http://localhost:8080/healthz"        60 || true
wait_http "Grafana" "http://localhost:3000/api/health"     90 || true
# Internal stores are cp-net-only — probe by service name from within the network.
wait_incluster "Mimir" "http://mimir:9009/ready"           90 || true
wait_incluster "CP self-obs Prometheus" "http://cp-prometheus:9090/-/healthy" 60 || true

cat <<EOF

╔══════════════════════════════════════════════════════════════════════════╗
║  Fyralis BYOC control plane is UP.                                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Operator Console : http://localhost:8080      (fleet registry + health) ║
║  Grafana          : http://localhost:3000      (fleet + per-customer +   ║
║                       admin / ${GF_ADMIN_USER:-admin} : ${GF_ADMIN_PASSWORD:-fyralis-operator})              ║
║                       Control-Plane folder -> CP self-obs watchdog       ║
║  Internal stores  : cp-net-only (Mimir/Loki/Prometheus have NO host port,║
║                       I4) — reach them via Grafana / the auth-proxy.      ║
║  Demo tenant      : ${TENANT}  (golden-12 metrics flowing via the boundary    ║
║                       collector -> mTLS auth-proxy -> Mimir)             ║
╚══════════════════════════════════════════════════════════════════════════╝

Next:
  make logs            tail the stack
  make smoke           run the end-to-end smoke (tests/e2e_smoke.py)
  make onboard TENANT=globex REGION=eu-west   onboard another tenant
  make down            stop the stack
EOF
