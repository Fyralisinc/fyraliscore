#!/usr/bin/env bash
# scripts/setup.sh — one-command bootstrap for a fresh clone of fyraliscore.
#
# What this does, in order:
#   1. Prompts you for an LLM provider (OpenAI / Anthropic / DeepSeek) and
#      its API key, then writes a working .env from .env.example.
#   2. Verifies host prerequisites (docker, python3.11+, node, npm, psql, curl).
#   3. Brings up Postgres + Ollama via docker compose and waits for them
#      to be ready (also ensures the nomic-embed-text model is pulled).
#   4. Creates a Python venv and installs runtime + dev dependencies.
#   5. Applies all DB migrations.
#   6. Seeds the dogfood tenant (CEO + personas).
#   7. Installs UI dependencies.
#   8. Hands off to scripts/start.sh, which boots the gateway, workers,
#      and Vite dev server.
#
# Re-running is safe: every step is idempotent.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ----------------------------------------------------------------------
# Args
# ----------------------------------------------------------------------
START_AFTER_SETUP=1
NO_BROWSER=0
for arg in "$@"; do
  case "$arg" in
    --no-start)   START_AFTER_SETUP=0 ;;
    --no-browser) NO_BROWSER=1 ;;
    -h|--help)
      cat <<HELP
Usage: scripts/setup.sh [--no-start] [--no-browser]

  --no-start     Stop after setup; don't run scripts/start.sh.
  --no-browser   Forwarded to scripts/start.sh — don't open the demo picker.
HELP
      exit 0 ;;
    *)
      echo "Unknown arg: $arg" >&2
      exit 2 ;;
  esac
done

log()  { printf "\033[1;36m[setup]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[setup]\033[0m %s\n" "$*" >&2; }
fail() { printf "\033[1;31m[setup]\033[0m %s\n" "$*" >&2; exit 1; }

lc() { echo "$1" | tr '[:upper:]' '[:lower:]'; }

# ----------------------------------------------------------------------
# 1. Interactive .env
# ----------------------------------------------------------------------
write_env=1
if [ -f .env ]; then
  read -r -p "[setup] .env already exists. Overwrite? [y/N] " resp
  case "$(lc "${resp:-}")" in
    y|yes) ;;
    *) write_env=0; log "Keeping existing .env" ;;
  esac
fi

if [ "$write_env" = "1" ]; then
  [ -f .env.example ] || fail ".env.example missing — are you in the repo root?"

  echo
  echo "Choose your LLM provider:"
  echo "  1) OpenAI"
  echo "  2) Anthropic"
  echo "  3) DeepSeek"
  while :; do
    read -r -p "Enter 1, 2, or 3: " choice
    case "$choice" in
      1) provider="openai";    model="gpt-4o-mini";     key_var="OPENAI_API_KEY";    break ;;
      2) provider="anthropic"; model="claude-sonnet-4-5"; key_var="ANTHROPIC_API_KEY"; break ;;
      3) provider="deepseek";  model="deepseek-chat";   key_var="DEEPSEEK_API_KEY";  break ;;
      *) echo "  Please enter 1, 2, or 3." ;;
    esac
  done

  while :; do
    read -r -s -p "Enter your ${key_var}: " api_key; echo
    [ -n "$api_key" ] && break
    echo "  Key must be non-empty."
  done

  log "Writing .env (LLM_PROVIDER=${provider}, LLM_MODEL=${model})"
  cp .env.example .env
  PROVIDER="$provider" MODEL="$model" KEY_VAR="$key_var" API_KEY="$api_key" \
  python3 - <<'PY'
import os, re, shlex, pathlib
path = pathlib.Path(".env")
text = path.read_text()
def setline(name, raw_value):
    global text
    value = shlex.quote(raw_value)
    pat = re.compile(rf'^{re.escape(name)}=.*$', re.MULTILINE)
    if pat.search(text):
        text = pat.sub(lambda _: f'{name}={value}', text)
    else:
        text += f'\n{name}={value}\n'
setline("LLM_PROVIDER", os.environ["PROVIDER"])
setline("LLM_MODEL",    os.environ["MODEL"])
setline(os.environ["KEY_VAR"], os.environ["API_KEY"])
path.write_text(text)
PY
fi

# Source the .env we just wrote (or the existing one) so subsequent steps
# see DATABASE_URL, OLLAMA_URL, etc.
set -a
source .env
[ -f .env.dogfood ] && source .env.dogfood
set +a

# ----------------------------------------------------------------------
# 2. Host prerequisites
# ----------------------------------------------------------------------
need() { command -v "$1" >/dev/null 2>&1 || fail "Missing prerequisite: $1 ($2)"; }
need docker  "install Docker Desktop or docker-ce + docker compose plugin"
need node    "install Node.js 20+"
need npm     "install Node.js 20+"
need psql    "install postgresql client (e.g. brew install postgresql@16, or apt-get install postgresql-client)"
need curl    "install curl"

if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  fail "docker compose v2 plugin not found"
fi

docker info >/dev/null 2>&1 \
  || fail "Docker daemon not running — start Docker Desktop / 'sudo systemctl start docker'"

pick_python() {
  for cand in python3.11 python3.12 python3.13 python3; do
    command -v "$cand" >/dev/null 2>&1 || continue
    ver="$("$cand" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)" || continue
    major="${ver%%.*}"; minor="${ver##*.}"
    if [ "$major" = "3" ] && [ "$minor" -ge 11 ]; then
      echo "$cand"; return 0
    fi
  done
  return 1
}
PYTHON="$(pick_python)" || fail "Python 3.11+ not found — install python3.11 (e.g. brew install python@3.11)"
log "Using Python: $PYTHON ($("$PYTHON" --version))"

# ----------------------------------------------------------------------
# 3. Postgres + Ollama via docker compose
# ----------------------------------------------------------------------
log "Bringing up Postgres + Ollama via $COMPOSE…"
$COMPOSE up -d postgres ollama

log "Waiting for Postgres on localhost:5432…"
for i in $(seq 1 60); do
  if pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
    log "  Postgres ready."
    break
  fi
  sleep 1
  if [ "$i" = "60" ]; then
    fail "Postgres did not become ready in 60s — check '$COMPOSE logs postgres'"
  fi
done

log "Waiting for Ollama at ${OLLAMA_URL}…"
for i in $(seq 1 90); do
  if curl -fsS "${OLLAMA_URL}/api/tags" >/dev/null 2>&1; then
    log "  Ollama ready."
    break
  fi
  sleep 1
  if [ "$i" = "90" ]; then
    fail "Ollama not reachable at ${OLLAMA_URL} after 90s — check '$COMPOSE logs ollama'"
  fi
done

EMBED_MODEL="${OLLAMA_EMBED_MODEL:-nomic-embed-text}"
log "Verifying ${EMBED_MODEL} is available (cold start can take a minute)…"
have_model=0
for i in $(seq 1 180); do
  if curl -fsS "${OLLAMA_URL}/api/tags" 2>/dev/null | grep -q "$EMBED_MODEL"; then
    have_model=1; break
  fi
  sleep 1
done
if [ "$have_model" != "1" ]; then
  warn "${EMBED_MODEL} not present yet — pulling explicitly."
  $COMPOSE exec -T ollama ollama pull "$EMBED_MODEL" \
    || fail "Failed to pull $EMBED_MODEL"
fi
log "  ${EMBED_MODEL} ready."

# ----------------------------------------------------------------------
# 4. Python venv + dependencies
# ----------------------------------------------------------------------
if [ ! -d .venv ]; then
  log "Creating Python venv at .venv with $PYTHON…"
  "$PYTHON" -m venv .venv
fi
log "Installing/updating Python dependencies (may take a minute)…"
.venv/bin/pip install -U pip
.venv/bin/pip install -e ".[dev]"

# ----------------------------------------------------------------------
# 5. DB migrations
# ----------------------------------------------------------------------
log "Applying database migrations…"
psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

applied=0
for f in db/migrations/*.sql; do
  fname="$(basename "$f")"
  done_already=$(psql -tAd "$DATABASE_URL" -c \
    "SELECT 1 FROM schema_migrations WHERE filename='${fname}'")
  if [ -n "$done_already" ]; then
    continue
  fi
  log "  + ${fname}"
  if ! psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f" >/dev/null; then
    warn "  migration ${fname} failed; the schema may already include its objects."
    warn "  Recording it as applied so we don't retry. Inspect db/migrations/${fname} if surprised."
  fi
  psql -tAd "$DATABASE_URL" -c \
    "INSERT INTO schema_migrations(filename) VALUES('${fname}') ON CONFLICT DO NOTHING" >/dev/null
  applied=$((applied+1))
done
log "Migrations: ${applied} new"

# ----------------------------------------------------------------------
# 6. Seed dogfood tenant
# ----------------------------------------------------------------------
log "Seeding dogfood tenant…"
.venv/bin/python scripts/seed_dogfood_tenant.py

# ----------------------------------------------------------------------
# 7. UI dependencies
# ----------------------------------------------------------------------
if [ ! -d ui/node_modules ]; then
  log "Installing UI dependencies…"
  ( cd ui && npm install --silent )
else
  log "UI deps already installed (delete ui/node_modules to reinstall)"
fi

# ----------------------------------------------------------------------
# 8. Hand off to start.sh
# ----------------------------------------------------------------------
log "Setup complete."
if [ "$START_AFTER_SETUP" = "1" ]; then
  start_args=()
  [ "$NO_BROWSER" = "1" ] && start_args+=(--no-browser)
  log "Launching scripts/start.sh…"
  exec scripts/start.sh "${start_args[@]}"
else
  cat <<EOF

Next steps:
  ./scripts/start.sh              # boot the full stack
  ./scripts/start.sh --no-browser # …without opening a browser
EOF
fi
