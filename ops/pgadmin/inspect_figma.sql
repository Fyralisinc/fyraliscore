-- Fyralis Figma OAuth + design snapshot inspection
--
-- Run this file in pgAdmin against the `company_os` database after applying
-- migrations. It intentionally selects metadata only: OAuth access tokens,
-- refresh tokens, webhook passcodes, and S3 object keys are never displayed.

-- 1. OAuth connection state and first-sync health.
SELECT
  fi.id AS installation_id,
  fi.tenant_id,
  fi.auth_kind,
  fi.oauth_user_id,
  fi.connection_state,
  fi.token_expires_at,
  fi.team_id,
  fi.disabled_at,
  fi.created_at,
  COUNT(ff.id) FILTER (WHERE ff.state = 'active') AS active_files,
  COUNT(ff.id) FILTER (WHERE ff.state <> 'active') AS inactive_files
FROM figma_installations fi
LEFT JOIN figma_files ff ON ff.figma_installation_id = fi.id
GROUP BY fi.id
ORDER BY fi.created_at DESC;

-- 2. Explicit Fyralis file allowlist and snapshot state.
SELECT
  ff.figma_installation_id,
  ff.file_key,
  ff.file_name,
  ff.project_name,
  ff.state,
  ff.snapshot_version,
  ff.snapshot_blob_id,
  ff.last_synced_at,
  ff.last_error
FROM figma_files ff
ORDER BY ff.created_at DESC, ff.file_name NULLS LAST;

-- 3. The observation that proves a design snapshot landed.
SELECT
  o.id AS observation_id,
  o.tenant_id,
  o.occurred_at,
  o.source_channel,
  o.kind,
  o.external_id,
  o.content_text,
  o.content -> 'source_locator' AS source_locator,
  o.content -> 'artifacts' AS artifacts,
  o.content -> 'projection' AS projection
FROM observations o
WHERE o.source_channel IN ('figma:file_snapshot', 'figma:event')
ORDER BY o.occurred_at DESC
LIMIT 100;

-- 4. Durable artifact catalog and its observation links.
SELECT
  b.id AS blob_id,
  b.tenant_id,
  b.storage_provider,
  b.content_hash,
  b.content_type,
  b.content_encoding,
  b.size_bytes,
  b.status,
  b.created_at,
  oa.observation_id,
  oa.artifact_kind,
  o.source_channel
FROM blobs b
LEFT JOIN observation_artifacts oa ON oa.blob_id = b.id
LEFT JOIN observations o
  ON o.id = oa.observation_id AND o.tenant_id = oa.tenant_id
WHERE o.source_channel LIKE 'figma:%'
ORDER BY b.created_at DESC;

-- 5. Onboarding run/shard proof when a connection is still syncing.
SELECT
  os.source,
  os.shard_kind,
  os.state,
  os.observations_seen,
  os.last_error,
  os.created_at,
  os.completed_at
FROM onboarding_shards os
WHERE os.source = 'figma'
ORDER BY os.created_at DESC;
