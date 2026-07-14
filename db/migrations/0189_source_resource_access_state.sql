-- 0189_source_resource_access_state.sql
--
-- Metadata-only source resource access state. Used by source onboarding UI
-- probes to detect permission transitions such as Discord private channels
-- becoming readable after an admin grants the bot role access.
--
-- This table stores identifiers, bounded status strings, counters, and
-- timestamps only. It must not store raw source payloads, messages, URLs,
-- credentials, prompts, embeddings, or customer record contents.

CREATE TABLE IF NOT EXISTS source_resource_access_state (
    tenant_id UUID NOT NULL REFERENCES tenants(id),
    source TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    permission_status TEXT NOT NULL CHECK (
        permission_status IN (
            'ready',
            'missing_access',
            'needs_admin',
            'not_selected',
            'unknown'
        )
    ),
    observation_count INTEGER NOT NULL DEFAULT 0 CHECK (observation_count >= 0),
    last_probe_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ready_replay_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        tenant_id,
        source,
        installation_id,
        resource_kind,
        resource_id
    )
);

CREATE INDEX IF NOT EXISTS source_resource_access_state_lookup_idx
    ON source_resource_access_state (
        tenant_id,
        source,
        installation_id,
        permission_status,
        updated_at DESC
    );

ALTER TABLE source_resource_access_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_resource_access_state FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON source_resource_access_state;
CREATE POLICY tenant_isolation ON source_resource_access_state
    USING (
        NULLIF(current_setting('app.current_tenant', true), '') IS NULL
        OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        NULLIF(current_setting('app.current_tenant', true), '') IS NULL
        OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
    );
