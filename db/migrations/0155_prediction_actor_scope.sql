-- 0155_prediction_actor_scope.sql
--
-- Add an explicit actor scope for Forecasts predictions that do not point at a
-- substrate target. Target-linked predictions remain governed by the target
-- entity's access-control decision; targetless rows need their own creator /
-- scope metadata so the Forecasts routes can filter them safely.

BEGIN;

ALTER TABLE predictions
  ADD COLUMN IF NOT EXISTS created_by_actor_id UUID
    REFERENCES actors(id) DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE predictions
  ADD COLUMN IF NOT EXISTS scope_actors UUID[] NOT NULL DEFAULT '{}'::uuid[];

CREATE INDEX IF NOT EXISTS idx_pred_tenant_created_by_actor
  ON predictions (tenant_id, created_by_actor_id)
  WHERE created_by_actor_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pred_tenant_scope_actors
  ON predictions USING GIN (scope_actors);

COMMIT;
