#!/usr/bin/env bash
# scripts/slack_dm_demo.sh — drive Slack human↔human DM ingestion end-to-end.
#
# One command: install a consenting user -> backfill historical DMs + a group DM
# + a couple channel messages -> emit live DM/MPIM/edit events -> print status.
# Everything lands in Postgres on the `observations` table (source_channel =
# 'slack:message'); watch it in pgAdmin (localhost:5434, db/user/pass company_os)
# with the query printed at the end.
#
# Usage:
#   scripts/slack_dm_demo.sh [USER_ID] [LIVE_COUNT]
#     USER_ID     consenting user's Slack id (default: U_ALICE)
#     LIVE_COUNT  number of live events to emit (default: 6)
#
# Env:
#   GATEWAY_URL  gateway base (default: http://localhost:8000)
#   TENANT_ID    X-Tenant-Id (default: 00000000-0000-0000-0000-000000000001,
#                the dev DEFAULT_TENANT_ID the gateway already uses)
set -euo pipefail

USER_ID="${1:-U_ALICE}"
LIVE_COUNT="${2:-6}"
GATEWAY_URL="${GATEWAY_URL:-http://localhost:8000}"
TENANT_ID="${TENANT_ID:-00000000-0000-0000-0000-000000000001}"
H_TENANT="X-Tenant-Id: ${TENANT_ID}"

# Pretty-print JSON if jq is available, else raw.
pp() { if command -v jq >/dev/null 2>&1; then jq .; else cat; fi; }
hr() { printf '\n\033[1;36m== %s ==\033[0m\n' "$1"; }

hr "1/4  install consenting user ${USER_ID}  (tenant ${TENANT_ID})"
curl -fsS -X POST "${GATEWAY_URL}/slack/${USER_ID}/install" -H "${H_TENANT}" | pp

hr "2/4  backfill historical DMs + group DM + channel messages"
curl -fsS -X POST "${GATEWAY_URL}/slack/${USER_ID}/backfill" \
  -H "${H_TENANT}" -H 'content-type: application/json' \
  -d '{"count": 6, "seed": 0}' | pp

hr "3/4  emit ${LIVE_COUNT} live events (message.im / message.mpim / message_changed)"
for seq in $(seq 1 "${LIVE_COUNT}"); do
  curl -fsS -X POST "${GATEWAY_URL}/slack/${USER_ID}/live/emit" \
    -H "${H_TENANT}" -H 'content-type: application/json' \
    -d "{\"seq\": ${seq}}" \
  | (command -v jq >/dev/null 2>&1 \
       && jq -c '{seq: '"${seq}"', via: .delivered_via, type: .event.channel_type, subtype: .event.subtype, ch: .event.channel}' \
       || cat)
done

hr "4/4  status (DM vs channel observation counts + recent rows)"
curl -fsS "${GATEWAY_URL}/slack/${USER_ID}/status" -H "${H_TENANT}" | pp

cat <<EOF

\033[1;32mDone.\033[0m  View results in pgAdmin — connect to:
  Host: localhost   Port: 5434   DB/User/Password: company_os

Then run:
  SELECT
    CASE WHEN content->>'channel_type' IN ('im','mpim') THEN 'DM/MPIM'
         WHEN left(external_id,1) IN ('D','G')          THEN 'DM/MPIM (by id)'
         ELSE 'channel' END           AS surface,
    content->>'channel_type'          AS channel_type,
    content->>'subtype'               AS subtype,
    count(*)                          AS n
  FROM observations
  WHERE source_channel = 'slack:message'
  GROUP BY 1,2,3 ORDER BY 1;
EOF
