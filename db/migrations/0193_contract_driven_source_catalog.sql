-- =====================================================================
-- 0193_contract_driven_source_catalog.sql
--   Canonical persisted source membership and durable fetch scheduling.
--
-- Code owns executable source behavior. This lookup table is the database
-- membership boundary used by foreign keys, replacing the four copied
-- source-membership constraints that had to be widened for every connector.
-- Persisted source strings do not change.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS ingestion_source_catalog (
    id                   TEXT        PRIMARY KEY,
    ui_slug              TEXT        NOT NULL UNIQUE,
    aliases              TEXT[]      NOT NULL DEFAULT '{}'::text[],
    historical_supported BOOLEAN     NOT NULL,
    data_plane           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ingestion_source_catalog (
    id, ui_slug, aliases, historical_supported, data_plane
) VALUES
    ('ashby',           'ashby',            '{}',                                  TRUE,  TRUE),
    ('aws',             'aws',              '{}',                                  TRUE,  TRUE),
    ('brex',            'brex',             '{}',                                  TRUE,  TRUE),
    ('carta',           'carta',            '{}',                                  TRUE,  TRUE),
    ('deel',            'deel',             '{}',                                  TRUE,  TRUE),
    ('discord',         'discord',          '{}',                                  TRUE,  TRUE),
    ('facebook_pages',  'facebook-pages',   '{"facebook-pages","facebook"}',        TRUE,  TRUE),
    ('figma',           'figma',            '{}',                                  TRUE,  TRUE),
    ('fireflies',       'fireflies',        '{}',                                  TRUE,  TRUE),
    ('github',          'github',           '{}',                                  TRUE,  TRUE),
    ('gmail',           'gmail',            '{}',                                  TRUE,  TRUE),
    ('google_calendar', 'google-calendar',  '{"google-calendar","calendar"}',       TRUE,  TRUE),
    ('google_drive',    'google-drive',     '{"google-drive","drive"}',             TRUE,  TRUE),
    ('grafana',         'grafana',          '{}',                                  TRUE,  TRUE),
    ('gusto',           'gusto',            '{}',                                  TRUE,  TRUE),
    ('hibob',           'hibob',            '{"hi-bob"}',                          TRUE,  TRUE),
    ('jira',            'jira',             '{}',                                  TRUE,  TRUE),
    ('linkedin',        'linkedin',         '{"linked-in"}',                       TRUE,  TRUE),
    ('mercury',         'mercury',          '{}',                                  TRUE,  TRUE),
    ('miro',            'miro',             '{}',                                  TRUE,  TRUE),
    ('notion',          'notion',           '{}',                                  TRUE,  TRUE),
    ('quickbooks',      'quickbooks',       '{"quick-books","qbo"}',                TRUE,  TRUE),
    ('ramp',            'ramp',             '{}',                                  TRUE,  TRUE),
    ('signal',          'signal',           '{}',                                  TRUE,  TRUE),
    ('slack',           'slack',            '{}',                                  TRUE,  TRUE),
    ('telegram',        'telegram',         '{}',                                  TRUE,  TRUE),
    ('whatsapp',        'whatsapp',         '{"whats-app"}',                       FALSE, TRUE)
ON CONFLICT (id) DO UPDATE SET
    ui_slug = EXCLUDED.ui_slug,
    aliases = EXCLUDED.aliases,
    historical_supported = EXCLUDED.historical_supported,
    data_plane = EXCLUDED.data_plane,
    updated_at = now();

-- Durable retry scheduling and owner/version-aware shard leases. A worker
-- that receives RetryLater writes next_attempt_at and relinquishes ownership;
-- no process is expected to sleep while holding the shard.
ALTER TABLE onboarding_shards
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retry_reason TEXT,
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- One onboarding run now represents exactly one source installation. Existing
-- terminal rows remain readable with NULL during cutover; contract-only
-- workers reject NULL on every new request and persist the UUID on both the
-- source run and all of its shards.
ALTER TABLE source_onboarding_runs
    ADD COLUMN IF NOT EXISTS installation_row_id UUID;
ALTER TABLE onboarding_shards
    ADD COLUMN IF NOT EXISTS installation_row_id UUID;

CREATE INDEX IF NOT EXISTS source_onboarding_runs_installation_idx
    ON source_onboarding_runs (tenant_id, source, installation_row_id);
CREATE INDEX IF NOT EXISTS onboarding_shards_installation_idx
    ON onboarding_shards (tenant_id, source, installation_row_id);

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_attempt_count_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_attempt_count_check
    CHECK (attempt_count >= 0) NOT VALID;
ALTER TABLE onboarding_shards
    VALIDATE CONSTRAINT onboarding_shards_attempt_count_check;

CREATE INDEX IF NOT EXISTS onboarding_shards_due_retry_idx
    ON onboarding_shards (next_attempt_at, recency_score DESC)
    WHERE state IN ('pending', 'in_progress');

CREATE INDEX IF NOT EXISTS onboarding_shards_lease_expiry_idx
    ON onboarding_shards (lease_expires_at)
    WHERE state = 'in_progress';

-- Replace each duplicated source CHECK with a single catalog FK. PostgreSQL
-- has no ADD CONSTRAINT IF NOT EXISTS, so guard each addition explicitly.
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'source_onboarding_runs_source_catalog_fk'
          AND conrelid = 'source_onboarding_runs'::regclass
    ) THEN
        ALTER TABLE source_onboarding_runs
            ADD CONSTRAINT source_onboarding_runs_source_catalog_fk
            FOREIGN KEY (source) REFERENCES ingestion_source_catalog(id)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'onboarding_shards_source_catalog_fk'
          AND conrelid = 'onboarding_shards'::regclass
    ) THEN
        ALTER TABLE onboarding_shards
            ADD CONSTRAINT onboarding_shards_source_catalog_fk
            FOREIGN KEY (source) REFERENCES ingestion_source_catalog(id)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ingestion_failures_source_catalog_fk'
          AND conrelid = 'ingestion_failures'::regclass
    ) THEN
        ALTER TABLE ingestion_failures
            ADD CONSTRAINT ingestion_failures_source_catalog_fk
            FOREIGN KEY (source) REFERENCES ingestion_source_catalog(id)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'onboarding_triggers_source_catalog_fk'
          AND conrelid = 'onboarding_triggers'::regclass
    ) THEN
        ALTER TABLE onboarding_triggers
            ADD CONSTRAINT onboarding_triggers_source_catalog_fk
            FOREIGN KEY (source) REFERENCES ingestion_source_catalog(id)
            NOT VALID;
    END IF;
END
$$;

ALTER TABLE source_onboarding_runs
    VALIDATE CONSTRAINT source_onboarding_runs_source_catalog_fk;
ALTER TABLE onboarding_shards
    VALIDATE CONSTRAINT onboarding_shards_source_catalog_fk;
ALTER TABLE ingestion_failures
    VALIDATE CONSTRAINT ingestion_failures_source_catalog_fk;
ALTER TABLE onboarding_triggers
    VALIDATE CONSTRAINT onboarding_triggers_source_catalog_fk;

COMMIT;
