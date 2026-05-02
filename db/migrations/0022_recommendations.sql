-- =====================================================================
-- 0022_recommendations.sql — Stage 1 decision support (CEO action list)
-- =====================================================================
-- Adds the schema support that the `recommendation` proposition kind
-- needs without disturbing existing kinds:
--
--   * `target_actor_id` — generated column extracted from
--     proposition->>'target_actor_id' for recommendation-kind rows;
--     NULL for every other kind. Powers the action-list query.
--
--   * `caused_act_change_id` — Act-layer entity id that an acted-upon
--     recommendation produced. Set by the act handler at archive time;
--     NULL for everything else. Audit-trail link.
--
--   * Hot-path partial index for the action-list ranker so the
--     `(tenant_id, target_actor_id, status='active', proposition_kind=
--     'recommendation')` query stays under 100ms with up to a few
--     thousand active recommendations.
--
-- New archive_reason values (`acted_upon`, `dismissed_by_user`,
-- `situation_resolved`) require no DB change — `models.archive_reason`
-- is a free TEXT column; the Pydantic Literal in lib/shared/types.py
-- is the only enforcement point.
-- =====================================================================

BEGIN;

-- Generated column extracting target_actor_id from the proposition
-- JSONB. Cast guarded by the kind discriminator so non-recommendation
-- kinds without that JSON field never trip the cast.
ALTER TABLE models
  ADD COLUMN IF NOT EXISTS target_actor_id UUID
    GENERATED ALWAYS AS (
      CASE
        WHEN proposition->>'kind' = 'recommendation'
         AND proposition->>'target_actor_id' IS NOT NULL
        THEN (proposition->>'target_actor_id')::uuid
        ELSE NULL
      END
    ) STORED;

-- Free-form column populated by the recommendation act handler with
-- the resulting Act-layer entity id (commitment / goal / decision /
-- resource that was created or transitioned).
ALTER TABLE models
  ADD COLUMN IF NOT EXISTS caused_act_change_id UUID;

-- Hot-path index: (tenant, target_actor) over active recommendations.
-- The ranker sorts client-side by impact * confidence, so a covering
-- index isn't required — keep it small + selective.
CREATE INDEX IF NOT EXISTS recommendations_active_idx
  ON models (tenant_id, target_actor_id, created_at DESC)
  WHERE proposition_kind = 'recommendation' AND status = 'active';

COMMIT;
