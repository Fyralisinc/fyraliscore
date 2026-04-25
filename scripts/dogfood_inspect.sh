#!/usr/bin/env bash
# scripts/dogfood_inspect.sh — one-shot dogfood tenant state dump.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

set -a
if [ -f .env ]; then source .env; fi
if [ -f .env.dogfood ]; then source .env.dogfood; fi
set +a

TID="${COMPANY_OS_TENANT_ID:?unset}"

echo "=== Tenant state (tenant_id=$TID) ==="
psql -d company_os -c "
SELECT
  (SELECT count(*) FROM observations WHERE tenant_id = '$TID') as observations,
  (SELECT count(*) FROM models WHERE tenant_id = '$TID' AND status='active') as active_models,
  (SELECT count(*) FROM models WHERE tenant_id = '$TID' AND status='archived') as archived_models,
  (SELECT count(*) FROM commitments WHERE tenant_id = '$TID') as commitments,
  (SELECT count(*) FROM goals WHERE tenant_id = '$TID') as goals,
  (SELECT count(*) FROM decisions WHERE tenant_id = '$TID') as decisions,
  (SELECT count(*) FROM think_runs WHERE tenant_id = '$TID' AND started_at > now() - interval '1 hour') as thinks_last_hour,
  (SELECT count(*) FROM think_trigger_queue WHERE tenant_id = '$TID') as trigger_queue_depth;
"

echo ""
echo "=== Recent Think activity (last 24h) ==="
psql -d company_os -c "
SELECT trigger_kind, status, count(*), round(avg(llm_latency_ms)) as avg_llm_ms
FROM think_runs
WHERE tenant_id = '$TID' AND started_at > now() - interval '24 hours'
GROUP BY trigger_kind, status
ORDER BY trigger_kind, status;
"

echo ""
echo "=== Recent LLM render activity (last 24h) ==="
psql -d company_os -c "
SELECT render_kind, outcome, count(*),
       sum(llm_cost_usd)::numeric(10,6) as total_usd,
       round(avg(latency_total_ms)) as avg_ms
FROM view_render_costs
WHERE tenant_id = '$TID' AND computed_at > now() - interval '24 hours'
GROUP BY render_kind, outcome
ORDER BY render_kind;
"

echo ""
echo "=== View cache age ==="
psql -d company_os -c "
SELECT cache_key, cached_at, now() - cached_at as age
FROM view_ceo_cache
WHERE tenant_id = '$TID'
ORDER BY cached_at DESC;
"

echo ""
echo "=== Active models (top 5 by confidence) ==="
psql -d company_os -c "
SELECT
  substring(id::text, 1, 8) as id,
  proposition_kind,
  confidence::numeric(3,2),
  left(proposition::text, 80) as proposition_head
FROM models
WHERE tenant_id = '$TID' AND status='active'
ORDER BY confidence DESC
LIMIT 5;
"
