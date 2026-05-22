-- 0034_topology_relocate.sql
--
-- S4 of the Self-Organizing Substrate plan — `relocate` claim_op +
-- topological cascade with bounded fan-out. The capstone: arrangement
-- becomes a first-class diff operation, closing the loop where
-- reasoning can deliberately reposition a Model in the topology
-- substrate (and propagate that change with bounded depth/fan-out).
--
-- What changes
-- ------------
--
--   1. `topology_events.kind` CHECK extended with 'relocate'. A
--      relocate is a phase event of a different shape than the
--      structural ones (emergence/dissolution/split/merge/drift
--      describe how COMMUNITIES changed; relocate describes how a
--      single Model's POSITION changed deliberately). It belongs in
--      the same log because:
--        - Operators want one timeline of "what moved when".
--        - The CEO view query is `SELECT * FROM topology_events
--          ORDER BY occurred_at DESC` regardless of kind.
--        - Rollback / replay is uniform.
--      Magnitude semantics for 'relocate':  L2 distance from
--      previous topo to new topo (≥ 0). Payload semantics:
--        {
--          "target_kind": "model_id" | "vector" | "neighborhood",
--          "target_ref":  "<uuid or list[float] truncated>",
--          "reason":      "<string from claim_op>",
--          "applied_by_diff_id": "<trigger uuid, optional>",
--          "cascade_enqueued": <int>
--        }
--      The `member_model_ids` column for a relocate event is
--      populated with `[model_id]` (singleton list) so consumers
--      that group events by member touch get the relocate too.
--
-- T6 dispatch behavior for 'relocate' is the same as for any other
-- topology event. The neighborhood_detector worker is NOT the
-- producer here — the applier writes the event in the same
-- transaction as the topo write. Whether T6 fires for a relocate
-- depends on a future configuration knob (default OFF in this
-- migration: relocates are deliberate human/LLM acts, the LLM
-- already saw enough context to make the decision; piling another
-- T6 on top would be redundant). The neighborhood detector skips
-- relocate events when computing T6 enqueue (see
-- services/workers/neighborhood_detector/worker.py).
--
-- Idempotent (DROP + ADD CONSTRAINT IF EXISTS).

BEGIN;

-- 1. Drop and re-add the kind CHECK with the extended set.
ALTER TABLE topology_events
  DROP CONSTRAINT IF EXISTS topology_events_kind_check;

ALTER TABLE topology_events
  ADD CONSTRAINT topology_events_kind_check
  CHECK (kind IN (
    'emergence', 'dissolution', 'split', 'merge', 'drift', 'relocate'
  ));

COMMIT;
