-- =====================================================================
-- 0046_ingestion_failures.sql
--   Ingestion LLD §1.3 — mirror of `ingestion.dlq` Kafka topic;
--   queryable surface for ops.
-- =====================================================================
-- One row per failed ingestion event. UPSERT semantics on
-- (tenant_id, source, raw_s3_key, failure_kind) are enforced in
-- application code (LLD §1.3 column justifications): a UNIQUE
-- constraint would over-restrict the cases where raw_s3_key is NULL
-- (pre-fetch rate-limit exhaustion, reconciliation gap, etc.).
--
-- The `failure_kind` CHECK enumerates the failure modes from LLD §8.
-- New kinds require both a migration (widen the CHECK) and a catalog
-- entry — deliberate friction to keep the kinds finite.
--
-- Constitution alignment:
--   §I — per-feature substrate, bounded to ingestion.
--   §II — additive; idempotent.
--   §III — tenant_id FK, RLS, tenant-prefixed partial index for the
--          ops-hot "unresolved by tenant+source" query.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS ingestion_failures (
    id                  UUID         PRIMARY KEY,             -- uuid7() app-side
    tenant_id           UUID         NOT NULL
                                     REFERENCES tenants(id),
    source              TEXT         NOT NULL
                                     CHECK (source IN ('slack','github','discord','gmail')),
    failure_kind        TEXT         NOT NULL
                                     CHECK (failure_kind IN (
                                       'normalizer_parse_error',
                                       'observation_insert_error',
                                       'rate_limit_exhausted',
                                       's3_put_failure',
                                       'kafka_publish_failure',
                                       'fetcher_terminal_error',
                                       'reconciliation_gap_unresolved',
                                       'oauth_revoked_mid_run'
                                     )),
    raw_s3_key          TEXT,                                 -- pointer to raw body (if available)
    onboarding_shard_id UUID         REFERENCES onboarding_shards(id),  -- NULL for steady-state
    error_summary       TEXT         NOT NULL,                -- short single-line summary
    error_context       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    attempt_count       INTEGER      NOT NULL DEFAULT 1,
    first_seen_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ,
    resolution_kind     TEXT         CHECK (resolution_kind IS NULL OR resolution_kind IN (
                                       'replayed', 'discarded', 'auto_recovered', 'manual_recovered'
                                     )),
    resolved_by         TEXT
);

CREATE INDEX IF NOT EXISTS ingestion_failures_tenant_source_unresolved_idx
    ON ingestion_failures (tenant_id, source, last_seen_at DESC)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS ingestion_failures_failure_kind_idx
    ON ingestion_failures (failure_kind, last_seen_at DESC)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS ingestion_failures_shard_idx
    ON ingestion_failures (onboarding_shard_id)
    WHERE onboarding_shard_id IS NOT NULL;

ALTER TABLE ingestion_failures ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_failures FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON ingestion_failures;
CREATE POLICY tenant_isolation ON ingestion_failures
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
