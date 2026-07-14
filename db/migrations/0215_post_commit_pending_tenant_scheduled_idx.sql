BEGIN;

CREATE INDEX IF NOT EXISTS post_commit_pending_tenant_scheduled_idx
    ON pending_post_commit_actions (tenant_id, scheduled_at)
    WHERE processed_at IS NULL
      AND dead_lettered_at IS NULL;

COMMIT;
