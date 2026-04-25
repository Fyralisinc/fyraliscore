#!/usr/bin/env bash
# scripts/reset_dogfood_tenant_data.sh — purge synthetic data for the dogfood tenant.
# Leaves persona actors + identity mappings intact (they're "foundation"), so the
# next scenario run doesn't need a re-seed.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

set -a
if [ -f .env ]; then source .env; fi
if [ -f .env.dogfood ]; then source .env.dogfood; fi
set +a

TID="${COMPANY_OS_TENANT_ID:?unset}"

echo "Resetting synthetic data for tenant $TID..."
.venv/bin/python -m simulation.reset --tenant "$TID" --confirm

echo ""
echo "Clearing view_ceo_cache (forces next /view/ceo/home to re-render)..."
psql -d company_os -c "DELETE FROM view_ceo_cache WHERE tenant_id = '$TID';"

echo ""
echo "Cleaning non-synthetic observations left over from prior integration probes..."
psql -d company_os -c "DELETE FROM observations WHERE tenant_id = '$TID';"

echo ""
echo "Done. Stack stays up; just restart the scheduler by POSTing /view/ceo/force-refresh"
echo "after injecting new signals, or the next scheduled tick will refresh naturally."
