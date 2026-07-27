-- ============================================================================
-- 0199_ingestion_event_replica_attributions.sql
--
-- Durable, exact event-to-replica ownership for isolated ingestion trials.
--
-- The source-certification load contract counts accepted work items, not
-- process-lifetime workflow claims.  Kafka redelivery also means that more than
-- one process may see the same event.  This ledger gives each event exactly one
-- durable owner: the first replica whose INSERT commits.  A later redelivery
-- may increment delivery_count, but it cannot replace that owner or change the
-- event's tenant, installation, or operation identity.
--
-- This table is metadata-only.  It must never contain source payloads,
-- provider identifiers, URLs, credentials, or observation content.
--
-- Retention is bounded at the schema boundary: callers normally use seven
-- days, and no row may live longer than thirty days from its first claim.
-- Runtime cleanup deletes an exact trial namespace after evidence capture and
-- can sweep expired rows per tenant as a fallback.
-- ============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS ingestion_event_replica_attributions (
    trial_namespace  TEXT        NOT NULL,
    source           TEXT        NOT NULL
                                REFERENCES ingestion_source_catalog (id),
    event_id         TEXT        NOT NULL,
    tenant_id        UUID        NOT NULL
                                REFERENCES tenants (id) ON DELETE CASCADE,
    -- Installation tables are source-specific, so a cross-table foreign key is
    -- not possible.  The future adapter must pass the exact installation row
    -- UUID selected for the offered item.
    installation_id  UUID        NOT NULL,
    operation_id     TEXT        NOT NULL,
    -- Immutable first durable owner.  Cross-replica Kafka redelivery does not
    -- rewrite this value.
    replica_id       TEXT        NOT NULL,
    delivery_count   BIGINT      NOT NULL DEFAULT 1
                                CHECK (delivery_count >= 1),
    first_recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at       TIMESTAMPTZ NOT NULL
                                DEFAULT (now() + INTERVAL '7 days'),
    PRIMARY KEY (trial_namespace, source, event_id),
    CHECK (
        length(trial_namespace) BETWEEN 1 AND 200
        AND btrim(trial_namespace) = trial_namespace
    ),
    CHECK (
        length(event_id) BETWEEN 1 AND 512
        AND btrim(event_id) = event_id
    ),
    CHECK (
        length(operation_id) BETWEEN 1 AND 256
        AND btrim(operation_id) = operation_id
    ),
    CHECK (
        length(replica_id) BETWEEN 1 AND 256
        AND btrim(replica_id) = replica_id
    ),
    CHECK (last_seen_at >= first_recorded_at),
    CHECK (
        expires_at > first_recorded_at
        AND expires_at <= first_recorded_at + INTERVAL '30 days'
    )
);

COMMENT ON TABLE ingestion_event_replica_attributions IS
    'Metadata-only event ledger for isolated ingestion trials. The primary key owns one event per trial/source; replica_id is the immutable first durable owner. Seven-day default retention, thirty-day hard maximum.';
COMMENT ON COLUMN ingestion_event_replica_attributions.installation_id IS
    'Exact UUID of the source-specific installation row selected by the trial adapter; no polymorphic SQL foreign key is possible.';
COMMENT ON COLUMN ingestion_event_replica_attributions.replica_id IS
    'First replica to commit the durable event claim. Replays increment delivery_count without changing ownership.';

CREATE INDEX IF NOT EXISTS ingestion_event_replica_attributions_owner_idx
    ON ingestion_event_replica_attributions (
        trial_namespace,
        source,
        replica_id
    );

CREATE INDEX IF NOT EXISTS ingestion_event_replica_attributions_scope_idx
    ON ingestion_event_replica_attributions (
        tenant_id,
        trial_namespace,
        source,
        installation_id
    );

CREATE INDEX IF NOT EXISTS ingestion_event_replica_attributions_expiry_idx
    ON ingestion_event_replica_attributions (tenant_id, expires_at);

-- Although rows are diagnostic metadata, tenant and installation identifiers
-- are still tenant-scoped.  Keep the table fail-closed when tenant context is
-- absent; the runtime helper binds app.current_tenant inside the caller's
-- transaction for every write/read/delete.
ALTER TABLE ingestion_event_replica_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_event_replica_attributions FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation
    ON ingestion_event_replica_attributions;
CREATE POLICY tenant_isolation
    ON ingestion_event_replica_attributions
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
