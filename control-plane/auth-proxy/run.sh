#!/usr/bin/env bash
# run.sh — start the Fyralis tenant auth proxy (mTLS termination, X-Scope-OrgID).
#
# The proxy needs four pieces of material; all are overridable via env:
#   * AUTH_PROXY_CA_CHAIN        intermediate+root chain that VERIFIES client certs
#                               (default: ../ca/pki/ca-chain.crt)
#   * AUTH_PROXY_TENANT_REGISTRY fingerprint->tenant revocation registry (C1)
#                               (default: ../ca/tenant_registry.json)
#   * AUTH_PROXY_TLS_CERT        the proxy's OWN server cert (what it presents)
#   * AUTH_PROXY_TLS_KEY         the proxy's OWN server key
#
# …plus AUTH_PROXY_LISTEN_PORT (default 8443) and AUTH_PROXY_UPSTREAM_URL
# (default http://mimir:9009).
#
# Usage:
#   ./run.sh                       # use env / defaults
#   AUTH_PROXY_TLS_CERT=... AUTH_PROXY_TLS_KEY=... ./run.sh
#
# Prereqs: run ../ca/bootstrap_ca.py once to create the CA chain, and issue at
# least one tenant cert with ../ca/issue_cert.py so the registry is populated.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CP_ROOT="$(cd "${HERE}/.." && pwd)"

# Pick a Python: prefer an explicit PYTHON, else the repo dev venv, else PATH.
PYTHON_BIN="${PYTHON:-}"
if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python" ]]; then
    PYTHON_BIN="/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

export AUTH_PROXY_CA_CHAIN="${AUTH_PROXY_CA_CHAIN:-${CP_ROOT}/ca/pki/ca-chain.crt}"
export AUTH_PROXY_TENANT_REGISTRY="${AUTH_PROXY_TENANT_REGISTRY:-${CP_ROOT}/ca/tenant_registry.json}"
export AUTH_PROXY_LISTEN_HOST="${AUTH_PROXY_LISTEN_HOST:-0.0.0.0}"
export AUTH_PROXY_LISTEN_PORT="${AUTH_PROXY_LISTEN_PORT:-8443}"
export AUTH_PROXY_UPSTREAM_URL="${AUTH_PROXY_UPSTREAM_URL:-http://mimir:9009}"

echo "[auth-proxy] python:    ${PYTHON_BIN}"
echo "[auth-proxy] ca-chain:  ${AUTH_PROXY_CA_CHAIN}"
echo "[auth-proxy] registry:  ${AUTH_PROXY_TENANT_REGISTRY}"
echo "[auth-proxy] tls cert:  ${AUTH_PROXY_TLS_CERT:-<unset! set AUTH_PROXY_TLS_CERT>}"
echo "[auth-proxy] listen:    ${AUTH_PROXY_LISTEN_HOST}:${AUTH_PROXY_LISTEN_PORT}"
echo "[auth-proxy] upstream:  ${AUTH_PROXY_UPSTREAM_URL}"

exec "${PYTHON_BIN}" "${HERE}/proxy.py"
