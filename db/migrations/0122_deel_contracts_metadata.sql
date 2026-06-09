-- =====================================================================
-- 0122_deel_contracts_metadata.sql — add the contract metadata columns
-- the deel install path writes/reads but 0098 never created.
-- =====================================================================
-- Phase-2 integration-hardening fix (finding #4, CRITICAL).
--
-- `deel_contracts` (created in 0098_deel.sql) is missing two columns that
-- LIVE code already references:
--
--   * services/ingest/integrations/deel/onboarding.py::finalize_install
--     INSERTs (and ON CONFLICT UPDATEs) `contract_name` / `contract_type`.
--     finalize_install is the real install surface — it is called from the
--     deel OAuth finalize (oauth.py) and the finance connect wizard
--     (finance_router.py). Every real install with >= 1 contract therefore
--     raised UndefinedColumnError at install time.
--   * services/app/gateway/finance_router.py SELECTs `contract_name` from
--     deel_contracts for the install-status console.
--
-- The all-25 synthetic gate never hit this because it seeds deel through the
-- fetcher path (make_deel / fetch_page_deel), not finalize_install — a clean
-- example of synthetic-mock drift masking a real-install failure.
--
-- Additive + idempotent (ADD COLUMN IF NOT EXISTS); re-running is a no-op.
-- No backfill needed: both columns are nullable and populated on next upsert.
-- =====================================================================

BEGIN;

ALTER TABLE deel_contracts
  -- Human-readable contract name (Deel `name`); optional metadata used by the
  -- finance install console and the handler's observation text.
  ADD COLUMN IF NOT EXISTS contract_name TEXT,
  -- Contract type (Deel `type`, e.g. eor / contractor / ...); optional.
  ADD COLUMN IF NOT EXISTS contract_type TEXT;

COMMIT;
