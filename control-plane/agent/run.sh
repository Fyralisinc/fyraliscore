#!/usr/bin/env sh
# run.sh — launch the outbound-only Fyralis data-plane agent.
#
# The agent runs IN THE CUSTOMER VPC. It only ever reaches OUT to the console
# (I2: no inbound listener). Configure it entirely through AGENT_* env vars; the
# defaults point at files inside this directory so a fresh checkout + a generated
# license/VERSION is immediately runnable.
#
# Required / commonly-set env:
#   AGENT_CONSOLE_URL    base URL of the vendor console (default https://console:8080)
#   AGENT_TENANT_ID      this deployment's tenant     (default acme)
#   AGENT_DEPLOYMENT_ID  this deployment's id         (default acme-use1-0001)
#   AGENT_REGION         deployment region            (default us-east-1)
#   AGENT_TELEMETRY_TIER T1|T2|T3                      (default T1)
#   AGENT_LICENSE_PATH   signed license bundle        (default ./license.json)
#   AGENT_TRUST_ROOT     signing/trust_root.json      (default ../signing/trust_root.json)
#   AGENT_VERSION_FILE   plain-text VERSION file       (default ./VERSION)
#   AGENT_HEALTHZ_URL    local data-plane /healthz    (default http://127.0.0.1:8088/healthz)
#   AGENT_INTERVAL_S     seconds between heartbeats   (default 30)
#   AGENT_BUFFER_PATH    durable un-sent-heartbeat queue (default ./buffer.jsonl)
#
# Usage:
#   ./run.sh                 # run the daemon loop forever
#   ./run.sh selftest        # run the end-to-end self-test scenario and exit
set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python3}"

cd "$HERE"

if [ "${1:-}" = "selftest" ]; then
    exec "$PYTHON" selftest.py
fi

exec "$PYTHON" agent.py
