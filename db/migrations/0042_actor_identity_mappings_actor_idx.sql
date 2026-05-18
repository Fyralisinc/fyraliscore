-- =====================================================================
-- 0042_actor_identity_mappings_actor_idx.sql — reverse lookup index
-- =====================================================================
-- The PK on actor_identity_mappings is (source_channel, source_actor_ref),
-- which optimizes the forward path (channel + external id -> actor_id)
-- used during ingestion. The reverse direction — "what identities does
-- this actor have?" — is unindexed and falls back to a full table scan.
-- That reverse query runs on every actor-detail render, ingest-time
-- merge check, and identity reconciliation pass, so the cost compounds
-- as the table grows.
--
-- Plain CREATE INDEX (not CONCURRENTLY) because migrations are wrapped
-- in a transaction by the migration runner; the table is small enough
-- at current scale that this completes well within startup.

CREATE INDEX IF NOT EXISTS actor_identity_mappings_actor_id_idx
    ON actor_identity_mappings (actor_id);
