-- 0195_fair_ingestion_scheduling.sql
--
-- Durable fairness watermarks and lookup indexes for contract-driven
-- ingestion scheduling.  Candidate selection remains in PostgreSQL so every
-- worker replica observes the same tenant/installation service history.

BEGIN;

ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS reconcile_last_claimed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS source_onboarding_runs_fair_tenant_idx
    ON source_onboarding_runs
       (tenant_id, reconcile_last_claimed_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS source_onboarding_runs_fair_installation_idx
    ON source_onboarding_runs
       (tenant_id, installation_row_id,
        reconcile_last_claimed_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS onboarding_shards_fair_tenant_idx
    ON onboarding_shards (tenant_id, started_at DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS onboarding_shards_fair_installation_idx
    ON onboarding_shards
       (tenant_id, installation_row_id, started_at DESC NULLS LAST)
    WHERE installation_row_id IS NOT NULL;

COMMIT;
