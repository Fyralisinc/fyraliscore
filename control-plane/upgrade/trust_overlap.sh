#!/usr/bin/env bash
# trust_overlap.sh — operator front-end for the CA trust-OVERLAP workflow (FR-A5).
#
# Wraps upgrade/trust_bundle.py to perform the non-disruptive CA-rotation dance and
# reload the auth-proxy at the right moment, so in-flight agent mTLS NEVER breaks.
#
# THE ORDERING (this is the whole point — do it in THIS order):
#   add    : ADD the new CA to the trust bundle      => proxy now trusts {old,new}
#   reload : roll the auth-proxy so it loads the bundle (zero-drop, one instance)
#   ...     issue new-CA leaves; rotate agents at their own pace ...
#   remove : once every agent presents a new-CA leaf, DROP the old CA => {new}
#   reload : roll the auth-proxy again
#
#   `add` and its reload MUST happen BEFORE you start issuing/ rotating to the new
#   CA. `remove` MUST happen only AFTER every active agent has rotated. Reversing
#   either order is exactly the disruption this tooling prevents.
#
# COMMANDS
#   add     --new-ca <pem>            append a CA to the bundle (idempotent), then sign
#   remove  --root-cn "<CN>"          drop a retired CA by its root subject CN
#   list                              show every trust anchor in the bundle
#   verify  [--leaf <pem>]            parse the bundle (+ optionally prove a leaf chains)
#   reload                            roll JUST the auth-proxy to load the new bundle
#
# ENV / FLAGS
#   BUNDLE          trust bundle path (default ../ca/pki/ca-chain.crt)
#   SIGN=0          skip signing the bundle after a write (default: sign, I6)
#   PYTHON          python interpreter (default: repo dev venv, else python3)
#   --no-reload     (add/remove) do NOT auto-reload the proxy; you reload manually
#
# EXAMPLES
#   ./trust_overlap.sh add --new-ca ../ca/pki-new/ca-chain.crt
#   ./trust_overlap.sh verify --leaf /tmp/acme-leaf.crt
#   ./trust_overlap.sh remove --root-cn "Fyralis Root CA"      # the OLD one
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CP_ROOT="$(cd "${HERE}/.." && pwd)"

BUNDLE="${BUNDLE:-${CP_ROOT}/ca/pki/ca-chain.crt}"
SIGN="${SIGN:-1}"

PYTHON_BIN="${PYTHON:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python" ]]; then
    PYTHON_BIN="/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

TB="${HERE}/trust_bundle.py"
ROLL="${HERE}/rolling_upgrade.sh"

log()  { printf '[trust-overlap] %s\n' "$*"; }
err()  { printf '[trust-overlap][ERROR] %s\n' "$*" >&2; }

usage() {
  sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

sign_flag() {
  [[ "${SIGN}" == "1" ]] && echo "--sign" || echo ""
}

reload_proxy() {
  if [[ ! -x "${ROLL}" && ! -f "${ROLL}" ]]; then
    err "rolling_upgrade.sh not found at ${ROLL}; reload the auth-proxy manually."
    return 1
  fi
  log "reloading auth-proxy (rolling, one instance) so it loads the new bundle ..."
  SERVICES="auth-proxy" bash "${ROLL}"
}

cmd_add() {
  local new_ca="" do_reload=1
  while (( $# )); do
    case "$1" in
      --new-ca) new_ca="$2"; shift 2 ;;
      --no-reload) do_reload=0; shift ;;
      *) err "unknown flag for add: $1"; usage 2 ;;
    esac
  done
  [[ -n "${new_ca}" ]] || { err "add requires --new-ca <pem>"; usage 2; }
  [[ -f "${new_ca}" ]] || { err "new CA file not found: ${new_ca}"; exit 2; }

  log "OVERLAP step 1/2: adding new CA ${new_ca} to ${BUNDLE}"
  # shellcheck disable=SC2046  # sign_flag intentionally expands to 0 or 1 word
  "${PYTHON_BIN}" "${TB}" add "${BUNDLE}" --add-ca "${new_ca}" $(sign_flag)
  log "bundle now trusts BOTH the old and new CA (overlap window open)."
  "${PYTHON_BIN}" "${TB}" list "${BUNDLE}"
  if (( do_reload )); then
    reload_proxy
    log "OVERLAP step 2/2 done: auth-proxy reloaded; in-flight agents unaffected."
  else
    log "--no-reload: remember to roll the auth-proxy before issuing new-CA leaves."
  fi
}

cmd_remove() {
  local root_cn="" fp="" do_reload=1
  while (( $# )); do
    case "$1" in
      --root-cn) root_cn="$2"; shift 2 ;;
      --fingerprint) fp="$2"; shift 2 ;;
      --no-reload) do_reload=0; shift ;;
      *) err "unknown flag for remove: $1"; usage 2 ;;
    esac
  done
  if [[ -z "${root_cn}" && -z "${fp}" ]]; then
    err "remove requires --root-cn <CN> or --fingerprint <hex>"; usage 2
  fi

  log "RETIRE step: removing the old CA from ${BUNDLE}"
  log "  (run this ONLY after every active agent presents a new-CA leaf)"
  local args=(remove "${BUNDLE}")
  [[ -n "${root_cn}" ]] && args+=(--match-root-cn "${root_cn}")
  [[ -n "${fp}" ]] && args+=(--match-fingerprint "${fp}")
  # shellcheck disable=SC2046
  "${PYTHON_BIN}" "${TB}" "${args[@]}" $(sign_flag)
  "${PYTHON_BIN}" "${TB}" list "${BUNDLE}"
  if (( do_reload )); then
    reload_proxy
    log "old CA retired and auth-proxy reloaded."
  else
    log "--no-reload: roll the auth-proxy to finish retiring the old CA."
  fi
}

cmd_list()   { "${PYTHON_BIN}" "${TB}" list "${BUNDLE}"; }

cmd_verify() {
  local leaf=""
  while (( $# )); do
    case "$1" in
      --leaf) leaf="$2"; shift 2 ;;
      *) err "unknown flag for verify: $1"; usage 2 ;;
    esac
  done
  if [[ -n "${leaf}" ]]; then
    "${PYTHON_BIN}" "${TB}" verify "${BUNDLE}" --leaf "${leaf}"
  else
    "${PYTHON_BIN}" "${TB}" verify "${BUNDLE}"
  fi
}

main() {
  (( $# )) || usage 0
  local cmd="$1"; shift || true
  case "${cmd}" in
    add)     cmd_add "$@" ;;
    remove)  cmd_remove "$@" ;;
    list)    cmd_list "$@" ;;
    verify)  cmd_verify "$@" ;;
    reload)  reload_proxy ;;
    -h|--help|help) usage 0 ;;
    *) err "unknown command: ${cmd}"; usage 2 ;;
  esac
}

main "$@"
