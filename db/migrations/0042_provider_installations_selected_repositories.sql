-- =====================================================================
-- 0042_provider_installations_selected_repositories.sql
--   IN-13: per-installation repository allowlist for GitHub Apps
-- =====================================================================
-- GitHub App installations carry a per-installation repository allowlist
-- that the customer admin sets at install time and mutates via
-- `installation_repositories` webhook events. NULL means "all
-- repositories" (the admin granted the App org-wide access). A JSONB
-- array of `<owner>/<repo>` full-name strings means an explicit
-- selection.
--
-- Idempotent. Additive. Default NULL preserves existing rows' semantics
-- as "all repositories" (no prior selection was recorded). Slack /
-- Discord / Linear / Stripe rows ignore this column; only the GitHub
-- webhook router branch (IN-13) reads it.
--
-- Constitution alignment:
--   §I   — `provider_installations` is the IN-07 cross-cutting side
--          table; this column adds a per-row property, not a new
--          Foundation.
--   §II  — additive (ADD COLUMN IF NOT EXISTS), idempotent. No
--          destructive change; no staged plan required.
--   §III — `provider_installations` already enables RLS +
--          tenant_isolation policy + tenant_id FK + tenant-prefixed
--          index (migration 0039). The new column inherits all three.
-- =====================================================================

BEGIN;

ALTER TABLE provider_installations
    ADD COLUMN IF NOT EXISTS selected_repositories JSONB DEFAULT NULL;

COMMENT ON COLUMN provider_installations.selected_repositories IS
    'IN-13 GitHub: NULL = all-repositories grant; JSONB array of '
    '"<owner>/<repo>" strings = explicit selection. NULL for non-github '
    'rows.';

COMMIT;
