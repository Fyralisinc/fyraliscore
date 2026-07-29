-- =====================================================================
-- 0200_source_renewal_jobs.sql
--
-- Durable, exact-installation scheduling for bounded credential and watch
-- renewal.  This is intentionally source-neutral: provider-specific expiry,
-- token, watch, and resource state stay in their existing source tables.
--
-- A job is identified by the complete source/tenant/installation/target
-- tuple.  ``target_key`` is an opaque non-secret resource selector (for
-- example a watch row UUID or the literal ``installation``), never a provider
-- payload or credential.  Owner/version leases fence all terminal and retry
-- writes, so an expired worker cannot commit after another worker takes over.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS source_renewal_jobs (
    source_id                    TEXT        NOT NULL
                                         REFERENCES ingestion_source_catalog (id),
    tenant_id                    UUID        NOT NULL
                                         REFERENCES tenants (id) ON DELETE CASCADE,
    -- Installation tables are source-specific, so a polymorphic FK cannot be
    -- expressed here.  Runtime callables must prove this UUID belongs to the
    -- declared source and tenant before any provider operation.
    installation_id              UUID        NOT NULL,
    target_key                   TEXT        NOT NULL,
    state                        TEXT        NOT NULL DEFAULT 'pending'
                                         CHECK (state IN (
                                             'pending',
                                             'leased',
                                             'retry_scheduled',
                                             'reauthorization_required',
                                             'manual_reconciliation_required'
                                         )),
    next_attempt_at              TIMESTAMPTZ,
    attempt_count                BIGINT      NOT NULL DEFAULT 0
                                         CHECK (attempt_count >= 0),
    last_attempt_at              TIMESTAMPTZ,
    last_success_at              TIMESTAMPTZ,
    -- Provider-returned expiry metadata only.  No token or secret reference
    -- belongs in this table; source installation tables remain the authority.
    expires_at                   TIMESTAMPTZ,
    -- A bounded, controlled Fyralis code only.  The schema deliberately has
    -- no free-form error/detail column because provider errors can contain
    -- sensitive response material.
    last_error_code              TEXT,
    reauthorization_required_at  TIMESTAMPTZ,
    -- A non-retryable/unknown-side-effect provider outcome is intentionally
    -- terminal.  It requires source-specific reconciliation or operator
    -- repair; it must never be converted into a normal cadence retry.
    manual_reconciliation_required_at TIMESTAMPTZ,
    lease_owner                  TEXT,
    lease_version                BIGINT      NOT NULL DEFAULT 0
                                         CHECK (lease_version >= 0),
    lease_expires_at             TIMESTAMPTZ,
    -- Set immediately before an unsafe provider-side operation begins. If a
    -- worker loses its lease before it can settle that operation, recovery
    -- must require reconciliation rather than repeat a potentially rotated
    -- credential or newly-created watch channel.
    provider_call_started_at     TIMESTAMPTZ,
    last_claimed_at              TIMESTAMPTZ,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, tenant_id, installation_id, target_key),
    CHECK (
        length(source_id) BETWEEN 1 AND 64
        AND btrim(source_id) = source_id
    ),
    CHECK (
        length(target_key) BETWEEN 1 AND 1024
        AND btrim(target_key) = target_key
    ),
    CHECK (
        last_error_code IS NULL
        OR last_error_code ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'
    ),
    CHECK (
        lease_owner IS NULL
        OR (
            length(lease_owner) BETWEEN 1 AND 256
            AND btrim(lease_owner) = lease_owner
        )
    ),
    CHECK (
        (state = 'leased') = (
            lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL
        )
    ),
    CHECK (
        (
            state IN (
                'reauthorization_required',
                'manual_reconciliation_required'
            )
            AND next_attempt_at IS NULL
        )
        OR (
            state NOT IN (
                'reauthorization_required',
                'manual_reconciliation_required'
            )
            AND next_attempt_at IS NOT NULL
        )
    ),
    CHECK (
        (state = 'manual_reconciliation_required') = (
            manual_reconciliation_required_at IS NOT NULL
        )
    )
);

-- 0200 is intentionally safe to replay against databases where an earlier
-- draft of this migration created the table without the manual-reconciliation
-- terminal state.  Rebuild only the state-dependent checks; the other column
-- constraints remain untouched.  The named replacement checks make future
-- upgrades deterministic rather than depending on PostgreSQL-generated names.
ALTER TABLE source_renewal_jobs
    ADD COLUMN IF NOT EXISTS manual_reconciliation_required_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS provider_call_started_at TIMESTAMPTZ;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
          FROM pg_constraint
         WHERE conrelid = 'source_renewal_jobs'::regclass
           AND contype = 'c'
           AND pg_get_constraintdef(oid) ILIKE '%state%'
    LOOP
        EXECUTE format(
            'ALTER TABLE source_renewal_jobs DROP CONSTRAINT %I',
            constraint_name
        );
    END LOOP;
END $$;

ALTER TABLE source_renewal_jobs
    ADD CONSTRAINT source_renewal_jobs_state_check
        CHECK (state IN (
            'pending',
            'leased',
            'retry_scheduled',
            'reauthorization_required',
            'manual_reconciliation_required'
        )),
    ADD CONSTRAINT source_renewal_jobs_lease_state_check
        CHECK (
            (state = 'leased') = (
                lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT source_renewal_jobs_schedule_state_check
        CHECK (
            (
                state IN (
                    'reauthorization_required',
                    'manual_reconciliation_required'
                )
                AND next_attempt_at IS NULL
            )
            OR (
                state NOT IN (
                    'reauthorization_required',
                    'manual_reconciliation_required'
                )
                AND next_attempt_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT source_renewal_jobs_manual_reconciliation_state_check
        CHECK (
            (state = 'manual_reconciliation_required') = (
                manual_reconciliation_required_at IS NOT NULL
            )
        );

COMMENT ON TABLE source_renewal_jobs IS
    'Source-neutral, exact tenant+installation+target renewal schedule. Stores metadata and controlled codes only; provider credentials remain in source installation tables.';
COMMENT ON COLUMN source_renewal_jobs.target_key IS
    'Opaque non-secret renewal target within one exact installation, such as a watch resource UUID or the literal installation selector.';
COMMENT ON COLUMN source_renewal_jobs.lease_version IS
    'Monotonic fenced lease generation. A stale owner cannot complete, defer, or mark reauthorization after takeover.';
COMMENT ON COLUMN source_renewal_jobs.last_error_code IS
    'Bounded Fyralis-controlled error code only; provider response text and secret material are forbidden.';
COMMENT ON COLUMN source_renewal_jobs.manual_reconciliation_required_at IS
    'Timestamp of a terminal unknown/unsafe provider outcome. Explicit source repair must call resume_renewal_job before this exact target can run again.';
COMMENT ON COLUMN source_renewal_jobs.provider_call_started_at IS
    'Fenced marker set before an unsafe provider operation. An expired lease with this marker transitions to manual reconciliation instead of being retried.';

-- The due index gives a future periodic worker an installation-fair order
-- without needing a provider-specific queue.  ``last_claimed_at`` is updated
-- only when a lease is won; a large backfill cannot permanently starve a
-- quieter installation in the same tenant.
CREATE INDEX IF NOT EXISTS source_renewal_jobs_due_fair_idx
    ON source_renewal_jobs (
        tenant_id,
        last_claimed_at NULLS FIRST,
        installation_id,
        source_id,
        next_attempt_at,
        target_key
    )
    WHERE state IN ('pending', 'retry_scheduled');

CREATE INDEX IF NOT EXISTS source_renewal_jobs_lease_expiry_idx
    ON source_renewal_jobs (lease_expires_at)
    WHERE state = 'leased';

-- Renewal rows contain tenant and installation metadata.  Fail closed when a
-- caller has not bound app.current_tenant; every runtime helper does so inside
-- its own short transaction before querying or mutating this table.
ALTER TABLE source_renewal_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_renewal_jobs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON source_renewal_jobs;
CREATE POLICY tenant_isolation
    ON source_renewal_jobs
    USING (
        tenant_id = NULLIF(
            current_setting('app.current_tenant', true),
            ''
        )::uuid
    )
    WITH CHECK (
        tenant_id = NULLIF(
            current_setting('app.current_tenant', true),
            ''
        )::uuid
    );

COMMIT;
