-- 0033_topology_events.sql
--
-- S3 of the Self-Organizing Substrate plan — phase events and the T6
-- trigger surface. Sits on top of S2 (migration 0032) which created
-- the topo_embedding layer + materialized neighborhoods. S2 left
-- arrangement observable but not consequential. S3 makes it
-- consequential by:
--
--   1. Recording phase events (emergence, dissolution, split, merge,
--      drift) whenever the neighborhood detector finds the cluster
--      structure changed.
--   2. Enqueuing a T6 Think trigger per phase event so the LLM can
--      name the cluster, surface a recommendation, or otherwise
--      ratify the structural shift.
--   3. Wiring topology context into the LLM prompt and into Pathway F
--      retrieval (no schema changes needed for those — they live on
--      top of S2's `topo_embedding`, `model_neighborhoods`, and
--      `model_neighborhood_membership`).
--
-- T6 piggybacks on the existing `think_trigger_queue` (migration 0004)
-- — that table's `trigger_kind` is plain TEXT with no CHECK, so adding
-- a new kind requires no migration there. Only this events table is
-- new.
--
-- Why a separate table (rather than a payload-only enqueue):
--   - Phase events are first-class observables. CEO view, audit log,
--     and post-hoc analytics all want a stable "list of structural
--     transitions per tenant by occurred_at" — much easier to query
--     against a dedicated table than to dig through trigger queue
--     payloads.
--   - The T6 trigger is one consumer among several (UI, replay,
--     analytics). Decoupling the event log from the trigger enqueue
--     means the queue becomes a join, not the source of truth.
--   - Naming the event (`named_signature`) is computed once and
--     stored on the event, not recomputed on every read.
--
-- Idempotent (CREATE TABLE / INDEX IF NOT EXISTS).

BEGIN;

-- ---------------------------------------------------------------------
-- topology_events — durable log of phase transitions
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS topology_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  -- Phase-event taxonomy. Closed set; the detector maps community
  -- diffs to one of these:
  --   emergence    : a new community with no member intersection
  --                  with any prior active neighborhood.
  --   dissolution  : a previously-active neighborhood is no longer
  --                  present (its members fragmented or exited).
  --   split        : one prior neighborhood whose members are now
  --                  divided across ≥2 new communities.
  --   merge        : ≥2 prior neighborhoods whose members coalesced
  --                  into one new community.
  --   drift        : the same matched neighborhood survived but its
  --                  membership / centroid moved enough to warrant
  --                  re-naming. Optional in v1; threshold-controlled.
  kind TEXT NOT NULL CHECK (kind IN (
    'emergence', 'dissolution', 'split', 'merge', 'drift'
  )),
  -- The neighborhood the event is "about". For emergence/drift this
  -- is the surviving / new neighborhood id. For dissolution it is
  -- the dissolved neighborhood id. For split it is one of the new
  -- neighborhoods (the largest by membership). For merge it is the
  -- new combined neighborhood id.
  neighborhood_id UUID,
  -- Predecessor neighborhoods (one for emergence/drift, ≥1 for
  -- merge, exactly 1 for split, exactly 1 for dissolution).
  predecessor_neighborhood_ids UUID[],
  -- For split events: the sibling new neighborhoods (the cluster
  -- sister-of-this).
  sibling_neighborhood_ids UUID[],
  -- Membership snapshot — the model_ids the event is about. Stored
  -- denormalized so the T6 dispatcher can hand the LLM the exact
  -- members at event time even if the neighborhood drifts later.
  member_model_ids UUID[] NOT NULL,
  -- Magnitude: a single scalar capturing "how big is this event".
  --   emergence   : new-community size
  --   dissolution : old-community size
  --   split       : 1 - (largest-share / total) — closer to 1 = more even split
  --   merge       : combined size
  --   drift       : Jaccard distance from prior membership (0..1)
  -- The detector decides; consumers may threshold on this to
  -- prioritize triggers.
  magnitude FLOAT,
  -- Heuristic name for the event (e.g. "engineering velocity",
  -- "Q3 commitments"). Computed at event-write time from the
  -- members' propositions / scopes by lib/topology/naming.py.
  -- The LLM may overwrite this on the corresponding
  -- model_neighborhoods row via T6.
  named_signature TEXT,
  -- Optional structured payload for consumer-specific extensions.
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Set when the T6 dispatch (or any other consumer) acknowledges
  -- the event. v1 dispatcher: neighborhood_detector worker enqueues
  -- T6 in the same transaction as the event INSERT, so this stays
  -- NULL until the dispatcher logs completion. Future consumers can
  -- maintain their own per-event cursor instead of relying on this
  -- column.
  processed_at TIMESTAMPTZ,
  CHECK (cardinality(member_model_ids) >= 0)
);

-- Hot path: list recent phase events for a tenant (CEO view, audit).
CREATE INDEX IF NOT EXISTS topology_events_tenant_recent_idx
  ON topology_events (tenant_id, occurred_at DESC);

-- Pending T6 dispatch: unprocessed events.
CREATE INDEX IF NOT EXISTS topology_events_pending_idx
  ON topology_events (tenant_id, occurred_at)
  WHERE processed_at IS NULL;

-- Reverse lookup: events that touched a given neighborhood.
CREATE INDEX IF NOT EXISTS topology_events_neighborhood_idx
  ON topology_events (neighborhood_id)
  WHERE neighborhood_id IS NOT NULL;

COMMIT;
