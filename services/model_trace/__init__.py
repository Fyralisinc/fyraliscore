"""services/model_trace — evidence-chain traversal over model_edges.

Backs the Model page's Trace Back / Trace Forward controls (spec §2.4).
A trace walks the active edge graph for a tenant and returns the
ordered chain of Models that either led to (back) or follow from
(forward) the selected node.

  * `repo.trace_back(conn, tenant_id, node_id, max_depth=4)`
      Walks evidence edges in the supports / instance_of /
      contributes_to_resolution direction toward the originating
      observation. Returns the chain Observation → Claim → Pattern →
      Belief → Recommendation/Delta ending at this node.

  * `repo.trace_forward(conn, tenant_id, node_id, max_depth=4)`
      Walks forward to surface the chain this node enables: Node →
      Recommendation → Commitment impact → Customer / revenue impact.

The router exposes three GET endpoints under `/v1/model/{node_id}`:
trace, supports, depends_on.
"""
from services.model_trace.repo import (
    TraceStep,
    trace_back,
    trace_forward,
)

__all__ = ["TraceStep", "trace_back", "trace_forward"]
