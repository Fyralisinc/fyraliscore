#!/usr/bin/env bash
# =====================================================================
# scripts/slack_dm_worker_demo.sh — production worker-fetch DM backfill demo.
#
# Runs human↔human Slack DM ingestion through the GENUINE backfill worker chain
# (planner → fetcher → raw-tier/S3 → Kafka → normalizer → observation_writer),
# in spammer mode, landing observations you can watch in pgAdmin (:5434).
#
# This is the worker-chain counterpart of scripts/slack_dm_demo.sh (which drives
# the inline gateway console). Same observations, different path: this exercises
# SlackUserClient + the slack_dm_window planner/fetcher shards via the real Kafka
# workers, with the synthetic spammer serving the DM reads.
#
#   scripts/slack_dm_worker_demo.sh            # default user U_ALICE, 6 msgs/DM
#
# Requires the base+sandbox stack already up (scripts/sandbox_up.sh). It is
# non-destructive: it rebuilds the app image with the new code, provisions the
# per-source Kafka lanes, recreates the two Kafka consumer workers, then runs a
# one-shot producer. To restore the workers to baseline afterwards, re-run
# scripts/sandbox_up.sh.
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.sandbox.yml)
PGEXec=(docker exec company_os_postgres psql -U company_os -d company_os -tA)
TENANT="${SLACK_DM_DEMO_TENANT:-00000000-0000-0000-0000-0000000000d3}"
USER_ID="${1:-U_ALICE}"
PER="${2:-6}"

echo "==> [1/6] Building app image with the worker-fetch DM code..."
"${COMPOSE[@]}" build shard_fetch normalizer observation_writer >/tmp/slackdm_build.log 2>&1 \
  || { echo "build failed; tail:"; tail -30 /tmp/slackdm_build.log; exit 1; }

echo "==> [2/6] Provisioning per-source Kafka lanes (ingestion.raw.slack, ...)..."
"${COMPOSE[@]}" run --rm --no-deps kafka-init >/tmp/slackdm_kafka.log 2>&1 || true
docker exec company_os_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep -E 'ingestion\.(raw|normalized)\.slack' || {
    echo "slack lanes missing after provision; tail:"; tail -20 /tmp/slackdm_kafka.log; }

echo "==> [3/6] Recreating Kafka consumer workers (new code + re-subscribe)..."
"${COMPOSE[@]}" up -d --no-deps --force-recreate normalizer observation_writer \
  >/tmp/slackdm_consumers.log 2>&1
sleep 5  # let the consumers join the (now-existing) slack lanes

echo "==> [4/6] Running the worker-fetch DM backfill (real planner+fetcher → Kafka)..."
"${COMPOSE[@]}" run --rm --no-deps \
  -e COMPANY_OS_ENV=dev \
  -e PYTHONPATH=/app \
  -e SLACK_DM_DEMO_TENANT="${TENANT}" \
  -e SLACK_DM_DEMO_USER="${USER_ID}" \
  -e SLACK_DM_DEMO_PER="${PER}" \
  shard_fetch python scripts/slack_dm_worker_fetch.py | tee /tmp/slackdm_fetch.out
echo
grep -o 'SLACK_DM_WORKER_FETCH_RESULT .*' /tmp/slackdm_fetch.out | sed 's/^SLACK_DM_WORKER_FETCH_RESULT //' \
  | python3 -m json.tool 2>/dev/null || true

echo "==> [5/6] Waiting for the Kafka workers to land DM observations..."
deadline=$(( SECONDS + 90 ))
last=0
while (( SECONDS < deadline )); do
  n=$("${PGEXec[@]}" -c "SELECT count(*) FROM observations WHERE tenant_id='${TENANT}' AND source_channel='slack:message';" 2>/dev/null || echo 0)
  n=${n:-0}
  if (( n > last )); then echo "    observations so far: ${n}"; last=$n; fi
  # Expect 3 im * PER + 1 mpim * 4 + 1 channel * 2.
  if (( n >= 18 )); then break; fi
  sleep 3
done

echo "==> [6/6] Result — DM vs channel breakdown for tenant ${TENANT}:"
"${PGEXec[@]}" -F$'\t' -c "
  SELECT COALESCE(content->>'channel_type','channel') AS channel_type,
         count(*) AS n
    FROM observations
   WHERE tenant_id='${TENANT}' AND source_channel='slack:message'
   GROUP BY 1 ORDER BY 1;" 2>/dev/null || true

echo
echo "View in pgAdmin (host localhost:5434, db/user/pass = company_os):"
cat <<SQL
  -- DM messages between two co-workers (landed by the worker chain):
  SELECT content->>'channel' AS dm_channel,
         array_agg(DISTINCT content->>'user' ORDER BY content->>'user') AS participants,
         count(*) AS messages
    FROM observations
   WHERE tenant_id = '${TENANT}'
     AND source_channel = 'slack:message'
     AND content->>'channel_type' = 'im'
   GROUP BY content->>'channel'
   HAVING count(DISTINCT content->>'user') >= 2
   ORDER BY messages DESC;
SQL
