-- 0030_audit_events.sql
--
-- PR 1 (Q5): per-Model audit chain.
--
-- See services/think/SUBSTRATE_SEMANTICS.md "Q5 — Audit chain" for the
-- canonical decision. Every Model state transition emits an
-- audit_events row inside the same transaction as the mutation. Direct
-- consumers query `get_audit_chain(model_id)` (services/think/audit.py)
-- for the full ordered history.
--
-- Distinct from `reconciliation_events` (migration 0029) and from
-- observations.kind='state_change'. The three serve different purposes:
--   * reconciliation_events — decision history for the reconciler,
--     keyed by (model_id_a, model_id_b). Not Model-history shaped.
--   * observations(kind='state_change') — signal-shaped event log;
--     downstream consumers (cascade, NOTIFY subscribers) read this.
--   * audit_events — per-Model state transitions, with previous_state
--     / new_state / changed_fields. Keyed by model_id; this is what
--     `get_audit_chain(model_id)` returns.
--
-- Reversal-of-reversal preservation: A → B → A produces three distinct
-- rows. The third event sets `re_asserts_event_id` to the event_id of
-- the original (first) event whose new_state matches.
--
-- Reconciliation-merge: when two Models merge (PR 4 introduces the
-- two-Model-then-merge case; PR 1 includes the schema), the merge
-- event has cause_type='reconciliation_merge' and source_model_ids
-- populated with the IDs of the merged-from Models. `get_audit_chain`
-- walks this array transitively to return the union of source chains.
--
-- Foreign-key behavior:
--   * `model_id` → `models.id` ON DELETE CASCADE — if a Model is hard-
--      deleted (rare; archives don't cascade), its audit chain goes
--      with it. Audit rows that reference archived Models persist
--      because archive does not DELETE the Model row.
--   * `re_asserts_event_id` → `audit_events(event_id)` ON DELETE SET
--      NULL — the linkage is lossy if the original event is purged.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
-- Wrapped in BEGIN/COMMIT by lib/shared/migrations.py per the harness
-- contract; an explicit BEGIN here would conflict with the wrapper.
--
-- ROLLBACK
-- --------
-- DROP INDEX IF EXISTS audit_events_model_chain_idx;
-- DROP INDEX IF EXISTS audit_events_tenant_time_idx;
-- DROP INDEX IF EXISTS audit_events_source_models_idx;
-- DROP INDEX IF EXISTS audit_events_re_asserts_idx;
-- DROP TABLE IF EXISTS audit_events;

CREATE TABLE IF NOT EXISTS audit_events (
  event_id BIGSERIAL PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Pointer to the cause: typically an observation.id (state_change,
  -- signal) or a think_runs.id. Not FK-enforced because triggers and
  -- observations may be pruned independently.
  cause_id UUID,
  -- Discriminator describing the kind of state transition. Vocabulary
  -- defined in services/think/audit.py. CHECK keeps unknown values out
  -- so we don't drift silently — every new cause type is an explicit
  -- PR.
  cause_type TEXT NOT NULL CHECK (
    cause_type IN (
      'create',
      'archive',
      'field_update',
      'confidence_update',
      'reconciliation_merge'
    )
  ),
  -- Pre-transition snapshot. NULL only for the chain's first event
  -- (cause_type='create'). Excludes embedding to keep rows compact;
  -- callers needing embedding history must walk by cause_id back to
  -- the underlying observation.
  previous_state JSONB,
  -- Post-transition snapshot. NOT NULL for all events.
  new_state JSONB NOT NULL,
  -- Names of columns that changed. Empty for cause_type='create'
  -- (everything is "new"). For 'reconciliation_merge' this is the set
  -- of fields that ended up different on the surviving Model.
  changed_fields TEXT[] NOT NULL DEFAULT '{}',
  -- Reversal-of-reversal pointer. NULL unless this event's new_state
  -- matches a prior event's new_state on the same model_id, in which
  -- case it points to that earlier event_id. Set by
  -- audit.find_re_assertable_event() at emit time.
  re_asserts_event_id BIGINT REFERENCES audit_events(event_id)
    ON DELETE SET NULL,
  -- Source Model IDs. Non-NULL only for cause_type='reconciliation_merge'.
  -- PR 1: empty array for current-code single-pass auto_merge (the
  -- candidate was never inserted, so no source Model exists). PR 4
  -- (LLM second-pass) populates this with both source Models when the
  -- two-Model-then-merge case fires.
  source_model_ids UUID[] NOT NULL DEFAULT '{}'
);

-- Primary access pattern: get_audit_chain(model_id) ordered by occurred_at.
CREATE INDEX IF NOT EXISTS audit_events_model_chain_idx
  ON audit_events (model_id, occurred_at);

-- Per-tenant audit dashboards (e.g. "show me all reconciliation merges
-- in the last 24h for tenant X").
CREATE INDEX IF NOT EXISTS audit_events_tenant_time_idx
  ON audit_events (tenant_id, occurred_at);

-- Source-Model walk for reconciliation-merge chain union. GIN over the
-- UUID array supports `WHERE source_model_ids && ARRAY[...]` lookups.
CREATE INDEX IF NOT EXISTS audit_events_source_models_idx
  ON audit_events USING gin (source_model_ids);

-- Reverse re_asserts lookup: "show me all events that re-assert me".
CREATE INDEX IF NOT EXISTS audit_events_re_asserts_idx
  ON audit_events (re_asserts_event_id)
  WHERE re_asserts_event_id IS NOT NULL;
