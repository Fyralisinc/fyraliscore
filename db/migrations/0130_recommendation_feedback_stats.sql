-- 0130_recommendation_feedback_stats.sql
--
-- Close the CEO recommendation feedback loop. The product already emits
-- recommendation_acted_upon / recommendation_dismissed state changes; this
-- table stores the bounded, decaying aggregate that ranking can consume.

BEGIN;

CREATE TABLE IF NOT EXISTS recommendation_feedback_stats (
  tenant_id UUID NOT NULL,
  target_actor_id UUID NOT NULL,
  pattern_key TEXT NOT NULL,
  acted_count INTEGER NOT NULL DEFAULT 0 CHECK (acted_count >= 0),
  dismissed_count INTEGER NOT NULL DEFAULT 0 CHECK (dismissed_count >= 0),
  last_acted_at TIMESTAMPTZ,
  last_dismissed_at TIMESTAMPTZ,
  last_reason TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, target_actor_id, pattern_key)
);

CREATE INDEX IF NOT EXISTS recommendation_feedback_stats_actor_idx
  ON recommendation_feedback_stats (tenant_id, target_actor_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS recommendation_feedback_stats_pattern_idx
  ON recommendation_feedback_stats (tenant_id, pattern_key);

COMMIT;
