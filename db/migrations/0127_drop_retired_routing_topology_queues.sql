-- Drop retired execution-routing and accepted-topology queue tables.
--
-- These tables had no production writers/readers:
--   * signal_routing_decisions belonged to the unused execution routing gate.
--   * topo_dirty_queue belonged to the retired accepted-memory topology queue.
--
-- The live paths are think_trigger_queue, inquiry_sessions, topology_events,
-- and model_edges.

DROP TABLE IF EXISTS signal_routing_decisions;
DROP TABLE IF EXISTS topo_dirty_queue;
