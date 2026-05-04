-- 0027_model_watchers.sql
--
-- Per-actor "watch for revision" subscriptions on recommendation cards.
--
-- When an LLM-issued recommendation card carries a falsifier
-- ("I'd revise if X happens"), the user can subscribe to be notified
-- if the predicate fires. The substrate just stores the subscription;
-- the T2 cascade work that detects predicate firing lands later and
-- will UPDATE `fired_at`. UI surfaces the row as a "Watching" state.
--
-- Lifecycle:
--   created_at  — INSERT
--   fired_at    — set by the cascade when the predicate detects a fire
--   cleared_at  — set when the recommendation is archived OR the user
--                 cancels the watch (DELETE endpoint). A re-subscribe
--                 reactivates the row by clearing both fired_at and
--                 cleared_at (see ON CONFLICT in the repo).
--
-- Why no FK to models(id): the substrate is event-sourced; many
-- existing tables (card_conversations etc) carry a `_id` reference
-- without an FK so background recompactions / replays don't fight the
-- constraint. We follow the same convention here.

CREATE TABLE IF NOT EXISTS model_watchers (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          UUID        NOT NULL,
    -- The recommendation Model id the watcher is attached to.
    recommendation_id  UUID        NOT NULL,
    -- The actor (user) who created the watch.
    actor_id           UUID        NOT NULL,
    -- Stable predicate id from the falsifier, e.g.
    -- "falsifier:<rec-uuid>:cluster_fade".
    predicate          TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Non-null when the cascade detects the predicate firing.
    fired_at           TIMESTAMPTZ,
    -- Non-null when the recommendation is archived OR the user cancels.
    cleared_at         TIMESTAMPTZ,
    -- One watch per actor per predicate. Re-watch does ON CONFLICT
    -- DO UPDATE (reset fired_at/cleared_at) — see repo.
    UNIQUE (tenant_id, actor_id, predicate)
);

-- Read path: aggregator fetches watches by (tenant, recommendation_ids)
-- to fan out is_watched onto Today cards.
CREATE INDEX IF NOT EXISTS model_watchers_tenant_rec_idx
    ON model_watchers (tenant_id, recommendation_id);
