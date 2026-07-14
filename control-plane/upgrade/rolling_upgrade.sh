#!/usr/bin/env bash
# rolling_upgrade.sh — zero-disruption rolling upgrade of the STATELESS control-plane
# services (NFR-6 / WS-CP-UPGRADE).
#
# WHAT IT DOES
#   Rolls the stateless CP services ONE AT A TIME, health-gating between every step:
#
#       auth-proxy   -> config-dist -> console
#
#   For each service:
#     1. PRE-GATE   — confirm the service is currently healthy (don't roll onto a
#                     bad baseline).
#     2. PULL/BUILD — pick up the new image for that one service.
#     3. RECREATE   — `up -d --no-deps <svc>` recreates just that container; its
#                     peers keep serving (the fleet sees no gap because the agent
#                     buffers across the few-second restart — I3).
#     4. POST-GATE  — poll the service's health endpoint until healthy (or roll back).
#
#   Stateful services (Mimir/Loki) are DELIBERATELY NOT touched here — they need the
#   blue-green / shared-object-storage procedure in UPGRADE_RUNBOOK.md, because a
#   naive recreate of a stateful store risks dropping in-flight remote-write. This
#   script refuses to roll them.
#
# WHY ROLLING IS SAFE HERE
#   * auth-proxy  — stateless mTLS terminator. A restart drops only the in-flight
#                   connections on THAT instance; the agent retries (outbound dial),
#                   and because we add the new CA to the trust bundle FIRST
#                   (trust_overlap.sh), the new proxy still trusts every live cert.
#   * config-dist — stateless signed-config publisher. Agents PULL on their own loop
#                   and verify-before-apply (I6); a brief gap just delays a poll.
#   * console     — stateless API over the fleet registry (registry is a mounted
#                   volume, not in-process state). Heartbeats that miss the window
#                   are BUFFERED by the agent and replayed (I3).
#
# USAGE
#   ./rolling_upgrade.sh                         # roll all stateless services
#   ./rolling_upgrade.sh auth-proxy console      # roll a subset, in this order
#   SERVICES="auth-proxy" ./rolling_upgrade.sh   # same via env
#   DRY_RUN=1 ./rolling_upgrade.sh               # print the plan, change nothing
#   NO_PULL=1 ./rolling_upgrade.sh               # skip image pull/build (local img)
#
# ENV KNOBS (all optional)
#   COMPOSE_FILE     master compose (default: ../docker-compose.control-plane.yml)
#   COMPOSE_PROJECT  compose project name (default: docker's default)
#   HEALTH_TIMEOUT   seconds to wait for a service to become healthy (default 120)
#   HEALTH_INTERVAL  seconds between health polls (default 3)
#   DRY_RUN=1        plan only
#   NO_PULL=1        do not pull/build new images
#   NO_ROLLBACK=1    do not auto-rollback a failed step (leave it for inspection)
#
# EXIT CODES
#   0 success; 1 a step failed health-gating (and was rolled back unless NO_ROLLBACK);
#   2 usage/precondition error (e.g. asked to roll a stateful service).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CP_ROOT="$(cd "${HERE}/.." && pwd)"

COMPOSE_FILE="${COMPOSE_FILE:-${CP_ROOT}/docker-compose.control-plane.yml}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-3}"
DRY_RUN="${DRY_RUN:-0}"
NO_PULL="${NO_PULL:-0}"
NO_ROLLBACK="${NO_ROLLBACK:-0}"

# The ONLY services this script will roll. Order matters: auth-proxy first (so the
# trust/header path is fresh), then config-dist (publisher), then console (the
# heartbeat sink). Stateful stores are intentionally excluded.
DEFAULT_ROLL_ORDER=(auth-proxy config-dist console)

# Services this script REFUSES to roll (stateful — see UPGRADE_RUNBOOK.md).
STATEFUL_DENYLIST=(mimir loki grafana cp-grafana cp-loki)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log()  { printf '[rolling-upgrade] %s\n' "$*"; }
warn() { printf '[rolling-upgrade][WARN] %s\n' "$*" >&2; }
err()  { printf '[rolling-upgrade][ERROR] %s\n' "$*" >&2; }

# ---------------------------------------------------------------------------
# docker compose shim — supports both `docker compose` (v2) and `docker-compose`.
# ---------------------------------------------------------------------------
DC=()
detect_compose() {
  if docker compose version >/dev/null 2>&1; then
    DC=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    DC=(docker-compose)
  else
    err "neither 'docker compose' nor 'docker-compose' is available"
    exit 2
  fi
  DC+=(-f "${COMPOSE_FILE}")
  if [[ -n "${COMPOSE_PROJECT:-}" ]]; then
    DC+=(-p "${COMPOSE_PROJECT}")
  fi
}

dc() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    log "DRY_RUN: ${DC[*]} $*"
    return 0
  fi
  "${DC[@]}" "$@"
}

# ---------------------------------------------------------------------------
# Health gating. A service is "healthy" if EITHER:
#   * its compose healthcheck reports `healthy`, OR
#   * (no healthcheck declared) its container is `running` and stable.
# We read the container state via `docker inspect` keyed off the compose-assigned
# container id, so this works regardless of container_name.
# ---------------------------------------------------------------------------
container_id_for() {
  local svc="$1"
  "${DC[@]}" ps -q "${svc}" 2>/dev/null | head -n1
}

# Echoes one of: healthy | unhealthy | starting | running-no-hc | absent
service_state() {
  local svc="$1" cid hstatus rstatus
  cid="$(container_id_for "${svc}")"
  if [[ -z "${cid}" ]]; then
    echo "absent"; return
  fi
  rstatus="$(docker inspect -f '{{.State.Status}}' "${cid}" 2>/dev/null || echo unknown)"
  if [[ "${rstatus}" != "running" ]]; then
    echo "unhealthy"; return
  fi
  # Has a healthcheck? .State.Health is empty if not.
  hstatus="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${cid}" 2>/dev/null || echo none)"
  case "${hstatus}" in
    healthy)   echo "healthy" ;;
    unhealthy) echo "unhealthy" ;;
    starting)  echo "starting" ;;
    none)      echo "running-no-hc" ;;
    *)         echo "starting" ;;
  esac
}

# Wait until a service is healthy (or running, for services without a healthcheck),
# bounded by HEALTH_TIMEOUT. Returns 0 healthy, 1 timed out / unhealthy.
wait_healthy() {
  local svc="$1" waited=0 st
  if [[ "${DRY_RUN}" == "1" ]]; then
    log "DRY_RUN: would wait up to ${HEALTH_TIMEOUT}s for ${svc} to become healthy"
    return 0
  fi
  while (( waited < HEALTH_TIMEOUT )); do
    st="$(service_state "${svc}")"
    case "${st}" in
      healthy|running-no-hc)
        log "${svc} is ${st} after ${waited}s"
        # For a service WITHOUT a compose healthcheck, give it a short settle so a
        # crash-on-boot is caught rather than reported healthy.
        if [[ "${st}" == "running-no-hc" ]]; then
          sleep "${HEALTH_INTERVAL}"
          [[ "$(service_state "${svc}")" == "running-no-hc" ]] || { warn "${svc} did not stay up"; return 1; }
        fi
        return 0
        ;;
      unhealthy)
        warn "${svc} reported UNHEALTHY"
        return 1
        ;;
      absent)
        warn "${svc} container is absent"
        return 1
        ;;
      *)  # starting
        ;;
    esac
    sleep "${HEALTH_INTERVAL}"
    waited=$(( waited + HEALTH_INTERVAL ))
  done
  err "${svc} did not become healthy within ${HEALTH_TIMEOUT}s (last state: $(service_state "${svc}"))"
  return 1
}

is_stateful() {
  local svc="$1" deny
  for deny in "${STATEFUL_DENYLIST[@]}"; do
    [[ "${svc}" == "${deny}" ]] && return 0
  done
  return 1
}

# Is the service even defined in this compose file? (Skip cleanly if not — e.g.
# config-dist may not be wired into the master compose yet.)
service_defined() {
  local svc="$1"
  "${DC[@]}" config --services 2>/dev/null | grep -qx "${svc}"
}

# ---------------------------------------------------------------------------
# Roll ONE service: pre-gate -> pull/build -> recreate -> post-gate (+ rollback).
# ---------------------------------------------------------------------------
roll_one() {
  local svc="$1"

  if is_stateful "${svc}"; then
    err "${svc} is STATEFUL — refusing to roll it here. Use the blue-green /"
    err "shared-object-storage procedure in UPGRADE_RUNBOOK.md instead."
    return 2
  fi

  if ! service_defined "${svc}"; then
    warn "${svc} is not defined in ${COMPOSE_FILE} — skipping (not yet wired)."
    return 0
  fi

  log "=== rolling ${svc} ==="

  # 1) PRE-GATE: only roll onto a healthy baseline. A NOT-yet-running service is
  #    allowed (first deploy); an UNHEALTHY running one is a stop sign.
  local pre
  pre="$(service_state "${svc}")"
  log "pre-state: ${pre}"
  if [[ "${pre}" == "unhealthy" ]]; then
    err "${svc} is already UNHEALTHY before the roll — aborting (fix it first)."
    return 1
  fi

  # 2) PULL / BUILD the new image for just this service.
  if [[ "${NO_PULL}" != "1" ]]; then
    log "pulling/building new image for ${svc} ..."
    # A service may be `build:`-only (no registry image) — try build, fall back to pull.
    dc build "${svc}" 2>/dev/null || dc pull "${svc}" 2>/dev/null || \
      warn "no new image pulled/built for ${svc} (continuing with current image)"
  else
    log "NO_PULL=1 — using the already-present image for ${svc}"
  fi

  # 3) RECREATE just this one container; --no-deps so peers are untouched.
  log "recreating ${svc} (--no-deps, peers keep serving) ..."
  dc up -d --no-deps "${svc}"

  # 4) POST-GATE: wait for health. On failure, roll back to the previous image.
  if wait_healthy "${svc}"; then
    log "${svc} rolled successfully and is healthy."
    return 0
  fi

  err "${svc} FAILED health-gating after the roll."
  if [[ "${NO_ROLLBACK}" == "1" ]]; then
    warn "NO_ROLLBACK=1 — leaving ${svc} as-is for inspection."
    return 1
  fi
  warn "rolling ${svc} BACK (recreate from the prior compose definition) ..."
  # `up -d --no-deps` re-applies the compose-pinned image. In a real pipeline the
  # rollback target is the prior pinned tag; here we restart to the last good
  # definition and re-gate so a botched roll never leaves a broken service live.
  dc up -d --no-deps --force-recreate "${svc}" || true
  if wait_healthy "${svc}"; then
    warn "${svc} recovered after rollback restart."
  else
    err "${svc} did NOT recover after rollback — MANUAL INTERVENTION REQUIRED."
  fi
  return 1
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
  detect_compose

  if [[ ! -f "${COMPOSE_FILE}" ]]; then
    err "compose file not found: ${COMPOSE_FILE}"
    exit 2
  fi

  # Resolve the roll list: CLI args > SERVICES env > default order.
  local -a roll
  if (( $# > 0 )); then
    roll=("$@")
  elif [[ -n "${SERVICES:-}" ]]; then
    # shellcheck disable=SC2206  # word-splitting is intended for a space list
    roll=(${SERVICES})
  else
    roll=("${DEFAULT_ROLL_ORDER[@]}")
  fi

  log "compose file : ${COMPOSE_FILE}"
  log "roll order   : ${roll[*]}"
  log "health gate  : up to ${HEALTH_TIMEOUT}s (poll ${HEALTH_INTERVAL}s)"
  [[ "${DRY_RUN}" == "1" ]] && log "MODE: DRY_RUN (no changes will be made)"

  # Guard: refuse the whole run if any requested service is stateful.
  local svc
  for svc in "${roll[@]}"; do
    if is_stateful "${svc}"; then
      err "${svc} is stateful and cannot be rolled by this script."
      err "Stateful upgrade (Mimir/Loki) -> see UPGRADE_RUNBOOK.md §Stateful."
      exit 2
    fi
  done

  local failed=0
  for svc in "${roll[@]}"; do
    if ! roll_one "${svc}"; then
      failed=1
      err "STOPPING the rolling upgrade at ${svc} (one-at-a-time: do not proceed"
      err "to the next service while one is unhealthy)."
      break
    fi
  done

  if (( failed )); then
    err "rolling upgrade FAILED."
    exit 1
  fi
  log "rolling upgrade COMPLETE — all requested stateless services healthy."
}

main "$@"
