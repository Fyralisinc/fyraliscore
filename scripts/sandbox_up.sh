#!/usr/bin/env bash
# scripts/sandbox_up.sh — bring up the local real-API ingestion sandbox.
#
# Runs the full ingestion data plane locally (docker-compose) under prod
# guards, ready to ingest from the REAL GitHub / Slack / Discord APIs. A
# host-side ngrok tunnel (you start it separately) gives the gateway a
# public HTTPS URL for webhooks + OAuth callbacks. Gmail is skipped.
#
# Prereqs: docker compose; .env (real creds + MASTER_KEK); .env.sandbox
# (copied from .env.sandbox.example and filled in). See
# docs/ingestion/sandbox-real-api-runbook.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.sandbox.yml)

# ---- Preflight ------------------------------------------------------
[ -f .env ]         || { echo "ERROR: .env not found (real creds + MASTER_KEK live there)."; exit 1; }
[ -f .env.sandbox ] || { echo "ERROR: .env.sandbox not found. Run: cp .env.sandbox.example .env.sandbox && edit it."; exit 1; }

if grep -q 'YOUR-NGROK-SUBDOMAIN' .env.sandbox; then
  echo "WARNING: .env.sandbox still has the placeholder ngrok host (YOUR-NGROK-SUBDOMAIN)."
  echo "         OAuth redirect URIs will be wrong until you set SANDBOX_PUBLIC_URL +"
  echo "         SLACK_REDIRECT_URI + DISCORD_REDIRECT_URI to your real ngrok URL."
  echo "         (Fine for a first boot to verify the stack; required before OAuth installs.)"
fi

# ---- Bring up -------------------------------------------------------
echo "Building + starting the sandbox stack (gmail/ui/edge/think parked under --profile full)..."
"${COMPOSE[@]}" up -d --build --remove-orphans

# ---- Wait for gateway ----------------------------------------------
echo "Waiting for gateway /healthz on http://localhost:8000 ..."
ok=""
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8000/healthz >/dev/null 2>&1; then ok=1; echo "  gateway OK"; break; fi
  sleep 2
done
if [ -z "$ok" ]; then
  echo "  gateway did NOT become healthy. Check: ${COMPOSE[*]} logs gateway"
  exit 1
fi

# ---- Seed the sandbox tenant (CEO actor + personas) -----------------
echo "Seeding sandbox tenant (idempotent)..."
"${COMPOSE[@]}" exec -T gateway python scripts/seed_dogfood_tenant.py || {
  echo "  WARNING: tenant seed failed — you can re-run it manually (see runbook)."; }

cat <<'EOF'

=== Real-API ingestion sandbox is UP ===
  Gateway (local):  http://localhost:8000/healthz
  Compose:          docker compose -f docker-compose.yml -f docker-compose.sandbox.yml ...

NEXT STEPS (see docs/ingestion/sandbox-real-api-runbook.md):
  1. Start the public tunnel (separate terminal):
         ngrok http 8000           # or: ngrok http --domain=<your-static> 8000
     Copy the https URL.
  2. If it changed, set SANDBOX_PUBLIC_URL + the two *_REDIRECT_URI in
     .env.sandbox to that URL, update the 3 provider apps' webhook/redirect
     URLs, then re-run scripts/sandbox_up.sh (picks up env changes).
  3. Drive OAuth installs (§ runbook 4) → backfill runs automatically.
  4. Post a live message/issue → watch it land.
  5. Check results:  docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
                       exec gateway python scripts/sandbox_inspect.py

  Logs:  docker compose -f docker-compose.yml -f docker-compose.sandbox.yml logs -f gateway normalizer observation_writer
  Stop:  scripts/sandbox_down.sh          (add --volumes to wipe DB/kafka/minio)
EOF
