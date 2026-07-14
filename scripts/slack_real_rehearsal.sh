#!/usr/bin/env bash
# Drive the local BYOC sandbox far enough to install real Slack and verify
# historical/live observations through the same gateway API the onboarding UI uses.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="prepare"
SKIP_STACK=0
OPEN_BROWSER=0
START_UI=0
ENSURE_NGROK=0
WAIT_SECONDS=180
POLL_SECONDS=5

usage() {
  cat <<'EOF'
Usage:
  scripts/slack_real_rehearsal.sh [prepare|wait|synthetic-proof] [options]

Modes:
  prepare          Start/check the sandbox, mint a local UI session, and print
                   the real Slack OAuth install URL + callback/webhook URLs.
  wait             Poll gateway /observations?source=slack until Slack rows land.
  synthetic-proof  Run the fully local Slack DM demo, then show gateway rows.

Options:
  --skip-stack     Do not run scripts/sandbox_up.sh; assume gateway is already up.
  --ensure-ngrok   Start/reuse ngrok, update public callback/webhook URLs in
                   .env.sandbox, then run the sandbox with the current tunnel.
  --open           Open the Slack install URL in the default browser when possible.
  --start-ui       Build and start the Next UI preview on UI_PORT (default 3003).
  --wait-seconds N Observation polling timeout for wait/synthetic-proof.

Environment:
  GATEWAY_URL      Default: http://localhost:8000
  UI_PORT          Default: 3003
  UI_URL           Default: http://localhost:$UI_PORT

Required local env values are read from .env then .env.sandbox:
  COMPANY_OS_TENANT_ID, COMPANY_OS_CEO_ACTOR_ID, AUTH_BOOTSTRAP_SECRET,
  SLACK_CLIENT_ID, SLACK_CLIENT_SECRET, SLACK_SIGNING_SECRET,
  SLACK_REDIRECT_URI, OAUTH_STATE_HMAC_KEY, SANDBOX_PUBLIC_URL

No Slack tokens are printed. The bearer token printed by this script is a local
Fyralis gateway session token; paste it only into your local onboarding UI.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    prepare|wait|synthetic-proof)
      MODE="$1"
      shift
      ;;
    --skip-stack)
      SKIP_STACK=1
      shift
      ;;
    --ensure-ngrok)
      ENSURE_NGROK=1
      shift
      ;;
    --open)
      OPEN_BROWSER=1
      shift
      ;;
    --start-ui)
      START_UI=1
      shift
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:?missing value for --wait-seconds}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8000}"
UI_PORT="${UI_PORT:-3003}"
UI_URL="${UI_URL:-http://localhost:${UI_PORT}}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.sandbox.yml)

hr() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }
warn() { printf '\033[1;33mWARN:\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

load_local_env() {
  [ -f .env ] || die ".env not found"
  [ -f .env.sandbox ] || die ".env.sandbox not found; run: cp .env.sandbox.example .env.sandbox && edit it"

  # Local operator files are expected to be shell-compatible KEY=VALUE files.
  # shellcheck disable=SC1091
  set -a
  . ./.env
  . ./.env.sandbox
  set +a
}

require_env() {
  local missing=()
  for name in "$@"; do
    if [ -z "${!name:-}" ]; then
      missing+=("$name")
    fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    printf 'Missing required env values:\n' >&2
    printf '  %s\n' "${missing[@]}" >&2
    exit 1
  fi
}

json_get() {
  "$PYTHON_BIN" -c '
import json
import sys
key = sys.argv[1]
data = json.load(sys.stdin)
value = data
for part in key.split("."):
    value = value[part]
print(value)
' "$1"
}

wait_gateway() {
  hr "Gateway health"
  local ok=""
  for _ in $(seq 1 60); do
    if curl -fsS "${GATEWAY_URL}/healthz" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 2
  done
  [ -n "$ok" ] || die "gateway did not become healthy at ${GATEWAY_URL}"
  echo "gateway OK: ${GATEWAY_URL}"
}

start_stack_if_needed() {
  if [ "$SKIP_STACK" -eq 1 ]; then
    wait_gateway
    return
  fi
  hr "Starting local real-API sandbox"
  scripts/sandbox_up.sh
}

discover_ngrok_public_url() {
  "$PYTHON_BIN" - <<'PY'
import json
import urllib.request

try:
    with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=5) as r:
        data = json.load(r)
except Exception:
    raise SystemExit(1)

for tunnel in data.get("tunnels", []):
    public_url = tunnel.get("public_url", "")
    config = tunnel.get("config") or {}
    addr = str(config.get("addr", ""))
    if public_url.startswith("https://") and (addr.endswith(":8000") or addr == "http://localhost:8000"):
        print(public_url.rstrip("/"))
        raise SystemExit(0)

for tunnel in data.get("tunnels", []):
    public_url = tunnel.get("public_url", "")
    if public_url.startswith("https://"):
        print(public_url.rstrip("/"))
        raise SystemExit(0)

raise SystemExit(1)
PY
}

update_sandbox_public_url() {
  local public_url="$1"
  "$PYTHON_BIN" - "$public_url" <<'PY'
from pathlib import Path
import sys

path = Path(".env.sandbox")
public_url = sys.argv[1].rstrip("/")
mapping = {
    "SANDBOX_PUBLIC_URL": public_url,
    "SLACK_REDIRECT_URI": f"{public_url}/integrations/slack/callback",
    "DISCORD_REDIRECT_URI": f"{public_url}/integrations/discord/callback",
    "NOTION_REDIRECT_URI": f"{public_url}/integrations/notion/callback",
}

lines = path.read_text(encoding="utf-8").splitlines()
out = []
seen = set()
for line in lines:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        out.append(line)
        continue
    key = line.split("=", 1)[0].strip()
    if key in mapping:
        out.append(f"{key}={mapping[key]}")
        seen.add(key)
    else:
        out.append(line)

missing = [key for key in mapping if key not in seen]
if missing:
    out.append("")
    out.append("# Updated by scripts/slack_real_rehearsal.sh --ensure-ngrok")
    for key in missing:
        out.append(f"{key}={mapping[key]}")

path.write_text("\n".join(out) + "\n", encoding="utf-8")
PY
  SANDBOX_PUBLIC_URL="$public_url"
  SLACK_REDIRECT_URI="${public_url}/integrations/slack/callback"
  DISCORD_REDIRECT_URI="${public_url}/integrations/discord/callback"
  NOTION_REDIRECT_URI="${public_url}/integrations/notion/callback"
  export SANDBOX_PUBLIC_URL SLACK_REDIRECT_URI DISCORD_REDIRECT_URI NOTION_REDIRECT_URI
}

ensure_ngrok_tunnel() {
  [ "$ENSURE_NGROK" -eq 1 ] || return 0
  require_cmd ngrok
  hr "Ensure public ngrok tunnel"

  local public_url=""
  public_url="$(discover_ngrok_public_url || true)"
  if [ -z "$public_url" ]; then
    nohup ngrok http 8000 --log=stdout > "${ROOT}/.fyralis-ngrok.log" 2>&1 &
    echo $! > "${ROOT}/.fyralis-ngrok.pid"
    for _ in $(seq 1 30); do
      public_url="$(discover_ngrok_public_url || true)"
      [ -n "$public_url" ] && break
      sleep 1
    done
  fi

  if [ -z "$public_url" ]; then
    warn "ngrok did not expose a tunnel. Check .fyralis-ngrok.log."
    return 1
  fi

  update_sandbox_public_url "$public_url"
  echo "ngrok public URL: ${SANDBOX_PUBLIC_URL}"
  echo "updated .env.sandbox public callback/webhook URLs"
}

verify_public_tunnel_health() {
  [ "$ENSURE_NGROK" -eq 1 ] || return 0
  if curl -fsS -m 10 "${SANDBOX_PUBLIC_URL}/healthz" >/dev/null 2>&1; then
    echo "public tunnel OK: ${SANDBOX_PUBLIC_URL}/healthz"
    return 0
  fi
  warn "public tunnel is not returning gateway /healthz yet: ${SANDBOX_PUBLIC_URL}/healthz"
  warn "Slack OAuth and Events API need this URL to be reachable."
}

mint_session() {
  hr "Mint local Fyralis UI session"
  local body
  body="$(printf '{"actor_id":"%s","tenant_id":"%s","ttl_seconds":86400}' \
    "$COMPANY_OS_CEO_ACTOR_ID" "$COMPANY_OS_TENANT_ID")"
  local response
  response="$(curl -fsS -X POST "${GATEWAY_URL}/auth/session" \
    -H "content-type: application/json" \
    -H "X-Bootstrap-Secret: ${AUTH_BOOTSTRAP_SECRET}" \
    -d "$body")"
  FYRALIS_BEARER_TOKEN="$(printf '%s' "$response" | json_get token)"
  export FYRALIS_BEARER_TOKEN
  echo "session minted for tenant ${COMPANY_OS_TENANT_ID}"
}

slack_install_url() {
  hr "Generate real Slack OAuth install URL"
  local headers body_file status
  headers="$(mktemp)"
  body_file="$(mktemp)"
  status="$(curl -sS -o "$body_file" -D "$headers" -w '%{http_code}' \
    -H "Authorization: Bearer ${FYRALIS_BEARER_TOKEN}" \
    "${GATEWAY_URL}/integrations/slack/install" || true)"
  if [ "$status" != "302" ] && [ "$status" != "307" ]; then
    echo "Install endpoint returned HTTP ${status}:" >&2
    cat "$body_file" >&2
    rm -f "$headers" "$body_file"
    exit 1
  fi
  SLACK_INSTALL_URL="$(
    "$PYTHON_BIN" - "$headers" <<'PY'
import sys
from email.parser import Parser

with open(sys.argv[1], "r", encoding="utf-8", errors="replace") as f:
    raw = f.read()

# curl -D may include multiple response blocks; use the last block with headers.
blocks = [b for b in raw.replace("\r\n", "\n").split("\n\n") if b.strip()]
headers = Parser().parsestr(blocks[-1].split("\n", 1)[1] if "\n" in blocks[-1] else "")
print(headers.get("location", ""))
PY
  )"
  rm -f "$headers" "$body_file"
  [ -n "$SLACK_INSTALL_URL" ] || die "Slack install Location header was empty"
  export SLACK_INSTALL_URL
  echo "Slack install URL:"
  echo "$SLACK_INSTALL_URL"
}

write_handoff_file() {
  mkdir -p "${ROOT}/.fyralis"
  cat > "${ROOT}/.fyralis/slack-real-handoff.txt" <<EOF
Generated: $(date -Iseconds)

Slack install URL:
${SLACK_INSTALL_URL}

Slack app URLs:
OAuth redirect URL: ${SLACK_REDIRECT_URI}
Events request URL: ${SANDBOX_PUBLIC_URL}/webhooks/slack/events

Onboarding UI:
${UI_URL}/onboarding/ingestion-health

UI connection values:
Gateway API base: ${GATEWAY_URL}
Bearer token: ${FYRALIS_BEARER_TOKEN}
Source: Slack

After Slack approval:
scripts/slack_real_rehearsal.sh wait --skip-stack --wait-seconds 300
EOF
  echo "handoff written: .fyralis/slack-real-handoff.txt"
}

print_real_slack_instructions() {
  hr "Real Slack setup handoff"
  cat <<EOF
Use these exact URLs in the Slack app configuration:
  OAuth redirect URL: ${SLACK_REDIRECT_URI}
  Events request URL: ${SANDBOX_PUBLIC_URL}/webhooks/slack/events
  Local gateway:       ${GATEWAY_URL}

Then:
  1. Open the Slack install URL printed above and approve the app.
  2. Invite the Fyralis Slack app to one or more test channels if you want
     channel history backfill from those channels.
  3. Post one new message in a connected channel or DM after install to test live.
  4. Run:
       scripts/slack_real_rehearsal.sh wait --skip-stack

Open the onboarding UI:
  ${UI_URL}/onboarding/ingestion-health

Use:
  Gateway API base: ${GATEWAY_URL}
  Bearer token:     ${FYRALIS_BEARER_TOKEN}
  Source:           Slack

Historical observations start from the Slack OAuth callback because it writes
provider_installations + onboarding_triggers. Live observations start when the
Slack Events API delivers to /webhooks/slack/events.
EOF
}

open_browser_if_requested() {
  [ "$OPEN_BROWSER" -eq 1 ] || return 0
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$SLACK_INSTALL_URL" >/dev/null 2>&1 || true
  elif command -v open >/dev/null 2>&1; then
    open "$SLACK_INSTALL_URL" >/dev/null 2>&1 || true
  else
    warn "no browser opener found; open the Slack URL manually"
  fi
}

start_ui_if_requested() {
  [ "$START_UI" -eq 1 ] || return 0
  require_cmd npm
  hr "Starting onboarding UI preview"
  (
    cd ui
    NEXT_PUBLIC_FYRALIS_API_BASE="$GATEWAY_URL" npm run build
    NEXT_PUBLIC_FYRALIS_API_BASE="$GATEWAY_URL" npm run preview -- -p "$UI_PORT" \
      > "${ROOT}/.fyralis-ui-preview.log" 2>&1 &
    echo $! > "${ROOT}/.fyralis-ui-preview.pid"
  )
  echo "UI preview starting at ${UI_URL}; logs: .fyralis-ui-preview.log"
}

fetch_observation_count() {
  local response
  response="$(curl -fsS \
    -H "Authorization: Bearer ${FYRALIS_BEARER_TOKEN}" \
    "${GATEWAY_URL}/observations?source=slack&limit=50")"
  printf '%s' "$response" | "$PYTHON_BIN" -c '
import json
import sys
data = json.load(sys.stdin)
items = data.get("items") or []
print(len(items))
'
}

show_recent_observations() {
  curl -fsS \
    -H "Authorization: Bearer ${FYRALIS_BEARER_TOKEN}" \
    "${GATEWAY_URL}/observations?source=slack&limit=10" \
  | "$PYTHON_BIN" -c '
import json
import sys
data = json.load(sys.stdin)
items = data.get("items") or []
print(f"gateway returned {len(items)} Slack observation(s)")
for item in items[:10]:
    text = (item.get("content_text") or "").replace("\n", " ")
    if len(text) > 96:
        text = text[:93] + "..."
    print("- {} {} {}".format(item.get("occurred_at"), item.get("source_channel"), text))
'
}

wait_for_observations() {
  hr "Poll gateway-backed Slack observations"
  local elapsed=0 count=0
  while [ "$elapsed" -le "$WAIT_SECONDS" ]; do
    count="$(fetch_observation_count || echo 0)"
    if [ "$count" -gt 0 ]; then
      show_recent_observations
      return 0
    fi
    printf 'waiting for Slack observations... %ss/%ss\r' "$elapsed" "$WAIT_SECONDS"
    sleep "$POLL_SECONDS"
    elapsed=$((elapsed + POLL_SECONDS))
  done
  echo
  warn "no Slack observations visible yet"
  cat <<EOF
Check:
  ${COMPOSE[*]} exec gateway python scripts/sandbox_inspect.py
  ${COMPOSE[*]} logs -f gateway source_onboarding shard_fetch normalizer observation_writer

Common causes:
  - Slack app was not approved or callback did not return to ${SLACK_REDIRECT_URI:-the configured redirect URI}.
  - Bot was not invited to a private/test channel, so channel history is empty.
  - No live message was posted after Events API was configured.
  - ngrok/public URL changed but .env.sandbox or Slack app URLs were not updated.
EOF
  return 1
}

run_synthetic_proof() {
  hr "Run fully local Slack proof"
  GATEWAY_URL="$GATEWAY_URL" TENANT_ID="$COMPANY_OS_TENANT_ID" scripts/slack_dm_demo.sh
  wait_for_observations
}

main() {
  require_cmd curl
  require_cmd "$PYTHON_BIN"
  load_local_env
  require_env \
    COMPANY_OS_TENANT_ID \
    COMPANY_OS_CEO_ACTOR_ID \
    AUTH_BOOTSTRAP_SECRET \
    SLACK_CLIENT_ID \
    SLACK_CLIENT_SECRET \
    SLACK_SIGNING_SECRET \
    SLACK_REDIRECT_URI \
    OAUTH_STATE_HMAC_KEY \
    SANDBOX_PUBLIC_URL

  case "$MODE" in
    prepare)
      require_cmd docker
      ensure_ngrok_tunnel
      start_stack_if_needed
      verify_public_tunnel_health
      mint_session
      slack_install_url
      write_handoff_file
      start_ui_if_requested
      print_real_slack_instructions
      open_browser_if_requested
      ;;
    wait)
      wait_gateway
      mint_session
      wait_for_observations
      ;;
    synthetic-proof)
      wait_gateway
      mint_session
      run_synthetic_proof
      ;;
    *)
      die "unsupported mode: $MODE"
      ;;
  esac
}

main
