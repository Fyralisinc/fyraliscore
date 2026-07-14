-- 0226_figma_design_artifacts.sql
--
-- Durable source-document artifacts.  The raw ingestion bucket is short-lived
-- transport evidence; these rows index customer-visible source snapshots (for
-- the first use case, a Figma GET /v1/files/{key} document JSON response).
--
-- Observation content carries only a safe blob id + integrity metadata.  The
-- private S3 bucket/key stay in `blobs`, and the observation relationship is
-- modeled separately because `observations` is range partitioned and cannot be
-- the target of a normal foreign key.

BEGIN;

CREATE TABLE IF NOT EXISTS blobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  storage_provider TEXT NOT NULL CHECK (storage_provider IN ('s3')),
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  content_type TEXT NOT NULL,
  content_encoding TEXT,
  size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
  status TEXT NOT NULL DEFAULT 'ready'
    CHECK (status IN ('pending', 'ready', 'failed', 'deleted')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Content-addressed objects deduplicate only within a tenant.  The same
  -- bytes in different tenants must never share catalog ownership.
  UNIQUE (tenant_id, content_hash),
  -- Supports the composite artifact-link FK below, so a link cannot pair an
  -- observation in one tenant with a blob catalog row owned by another.
  UNIQUE (id, tenant_id)
);

CREATE INDEX IF NOT EXISTS blobs_tenant_status_idx
  ON blobs (tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS observation_artifacts (
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Application-enforced reference: observations is a partitioned table whose
  -- primary identity includes occurred_at, so PostgreSQL cannot safely expose
  -- a simple UUID FK here.
  observation_id UUID NOT NULL,
  blob_id UUID NOT NULL,
  artifact_kind TEXT NOT NULL CHECK (length(artifact_kind) > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, observation_id, blob_id, artifact_kind),
  -- Enforce tenant ownership at the database boundary.  A tenant-scoped
  -- observation link must never point at another tenant's durable object.
  FOREIGN KEY (blob_id, tenant_id)
    REFERENCES blobs (id, tenant_id)
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS observation_artifacts_blob_idx
  ON observation_artifacts (tenant_id, blob_id);

-- Snapshot high-water state lets a later planner skip a second full document
-- download when Figma reports the same file version.
ALTER TABLE figma_files
  ADD COLUMN IF NOT EXISTS snapshot_version TEXT,
  ADD COLUMN IF NOT EXISTS snapshot_blob_id UUID,
  ADD COLUMN IF NOT EXISTS last_snapshot_at TIMESTAMPTZ;

ALTER TABLE blobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE blobs FORCE ROW LEVEL SECURITY;
ALTER TABLE observation_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE observation_artifacts FORCE ROW LEVEL SECURITY;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['blobs', 'observation_artifacts']
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS %I_tenant_isolation ON %I', t, t);
    EXECUTE format(
      'CREATE POLICY %I_tenant_isolation ON %I '
      'USING (tenant_id = current_setting(''app.current_tenant'', true)::uuid) '
      'WITH CHECK (tenant_id = current_setting(''app.current_tenant'', true)::uuid)',
      t, t
    );
  END LOOP;
END $$;

COMMIT;
