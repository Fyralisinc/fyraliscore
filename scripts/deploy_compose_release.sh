#!/usr/bin/env bash
set -euo pipefail

PREVIOUS_SHA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --previous-sha)
      PREVIOUS_SHA="${2:-}"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${PREVIOUS_SHA}" ]]; then
  echo "--previous-sha is required" >&2
  exit 2
fi

COMPOSE=(docker compose)
HEALTH_URL="${DEPLOY_HEALTH_URL:-http://localhost:8000/healthz}"
HEALTH_TIMEOUT_S="${DEPLOY_HEALTH_TIMEOUT_S:-60}"
SERVICE_HEALTH_TIMEOUT_S="${DEPLOY_SERVICE_HEALTH_TIMEOUT_S:-90}"
CANARY_TIMEOUT_S="${DEPLOY_CANARY_TIMEOUT_S:-60}"
CANARY_CONTAINER_NAME="${DEPLOY_CANARY_CONTAINER_NAME:-fyralis_gateway_canary}"

log() {
  echo "==> $*"
}

wait_gateway_health() {
  timeout "${HEALTH_TIMEOUT_S}" bash -c \
    'until curl -sf "$0" >/dev/null; do sleep 3; done' \
    "${HEALTH_URL}"
}

wait_service_health() {
  local service="$1"
  local deadline=$((SECONDS + SERVICE_HEALTH_TIMEOUT_S))
  local container_id status

  while true; do
    container_id="$("${COMPOSE[@]}" ps -q "${service}" 2>/dev/null || true)"
    if [[ -z "${container_id}" ]]; then
      status="missing"
    else
      status="$(docker inspect \
        --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
        "${container_id}" 2>/dev/null || echo "missing")"
    fi

    case "${status}" in
      healthy|none)
        return 0
        ;;
      unhealthy)
        return 1
        ;;
    esac

    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 3
  done
}

rollback() {
  local reason="$1"
  echo "${reason}; rolling back to ${PREVIOUS_SHA}" >&2
  git reset --hard "${PREVIOUS_SHA}"
  "${COMPOSE[@]}" up -d --build --remove-orphans
  wait_gateway_health || true
  "${COMPOSE[@]}" ps
  exit 1
}

cleanup_canary() {
  docker rm -f "${CANARY_CONTAINER_NAME}" >/dev/null 2>&1 || true
}

run_gateway_canary() {
  if [[ "${DEPLOY_GATEWAY_CANARY:-1}" != "1" ]]; then
    log "Skipping gateway canary because DEPLOY_GATEWAY_CANARY!=1"
    return 0
  fi

  log "Starting no-traffic gateway canary"
  cleanup_canary
  "${COMPOSE[@]}" run -d \
    --name "${CANARY_CONTAINER_NAME}" \
    --no-deps \
    -e GATEWAY_START_GRT_SCHEDULER=0 \
    gateway >/dev/null

  if ! timeout "${CANARY_TIMEOUT_S}" bash -c \
    'until docker exec "$0" python -c '"'"'import urllib.request,sys; sys.exit(0 if urllib.request.urlopen("http://localhost:8000/healthz", timeout=2).status == 200 else 1)'"'"'; do sleep 3; done' \
    "${CANARY_CONTAINER_NAME}"; then
    docker logs --tail=200 "${CANARY_CONTAINER_NAME}" >&2 || true
    cleanup_canary
    return 1
  fi

  cleanup_canary
}

rollout_services() {
  if [[ -n "${DEPLOY_WORKER_ROLLOUT_SERVICES:-}" ]]; then
    tr ', ' '\n' <<< "${DEPLOY_WORKER_ROLLOUT_SERVICES}" | sed '/^$/d'
    return 0
  fi

  python3 - <<'PY'
from services.platform.runtime.process_manifest import production_processes

for process in production_processes():
    service = process.compose_service
    if service and service != "gateway" and process.has_healthcheck:
        print(service)
PY
}

run_slo_gate() {
  if [[ "${DEPLOY_RUN_PRODUCT_SLO_GATE:-1}" != "1" ]]; then
    log "Skipping product SLO gate because DEPLOY_RUN_PRODUCT_SLO_GATE!=1"
    return 0
  fi

  python3 scripts/check_product_slo_gate.py \
    --prometheus-url "${PRODUCT_SLO_GATE_PROMETHEUS_URL:-http://localhost:9090}" \
    --wait-seconds "${PRODUCT_SLO_GATE_WAIT_SECONDS:-120}" \
    --interval-seconds "${PRODUCT_SLO_GATE_INTERVAL_SECONDS:-15}" \
    --error-burn-max "${PRODUCT_SLO_GATE_ERROR_BURN_MAX:-2}" \
    --latency-burn-max "${PRODUCT_SLO_GATE_LATENCY_BURN_MAX:-2}"
}

log "Pulling images and building release"
"${COMPOSE[@]}" pull
"${COMPOSE[@]}" build

run_gateway_canary || rollback "Gateway canary failed"

log "Promoting gateway"
"${COMPOSE[@]}" up -d --no-deps gateway
wait_gateway_health || rollback "Gateway failed health after promotion"

if [[ "${DEPLOY_WORKER_ROLLOUT:-1}" == "1" ]]; then
  while IFS= read -r service; do
    [[ -n "${service}" ]] || continue
    log "Rolling ${service}"
    "${COMPOSE[@]}" up -d --no-deps "${service}"
    wait_service_health "${service}" || rollback "${service} failed health during rollout"
  done < <(rollout_services)
else
  log "Skipping health-gated worker rollout because DEPLOY_WORKER_ROLLOUT!=1"
fi

log "Reconciling compose project and removing orphans"
"${COMPOSE[@]}" up -d --remove-orphans
wait_gateway_health || rollback "Gateway failed health after final compose reconciliation"

log "Checking product SLO gate"
run_slo_gate || rollback "Product SLO gate failed after deploy"

log "Deployment complete"
"${COMPOSE[@]}" ps
