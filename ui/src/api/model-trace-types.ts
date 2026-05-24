// TypeScript counterparts to services/model_trace/router.py response
// types. The router emits one TraceStep dataclass per node in a chain;
// adjacency endpoints emit the same shape, flat.

export type TraceDirection = "back" | "forward";

export type TraceStep = {
  id: string;
  kind: string;
  label: string;
  summary: string;
  ts: string | null;
  via_edge_kind: string | null;
  extra?: Record<string, unknown>;
};

export type TraceChain = {
  node_id: string;
  direction: TraceDirection;
  max_depth: number;
  chain: TraceStep[];
};

export type AdjacencyResponse = {
  node_id: string;
  items: TraceStep[];
};
