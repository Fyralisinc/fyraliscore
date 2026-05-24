"""Topology services.

Active topology now means the latent relationship field: a
consequence-sensitive discovery layer that proposes relationship and
situation candidates before typed edges exist.

Legacy accepted-memory topology tables may still exist for map/history
compatibility, but the Python runtime path lives in `field.py`.
"""

from .field import (
    ImpactSignature,
    LatentTopologyService,
    TopologyGenerationResult,
    TopologySweepReport,
    TopologyScore,
    impact_signature,
    impact_signature_from_row,
)
from .eval_harness import (
    ExpectedPair,
    ExpectedSituation,
    TopologyEvalReport,
    run_topology_eval,
)

__all__ = [
    "ExpectedPair",
    "ExpectedSituation",
    "ImpactSignature",
    "LatentTopologyService",
    "TopologyGenerationResult",
    "TopologyEvalReport",
    "TopologySweepReport",
    "TopologyScore",
    "impact_signature",
    "impact_signature_from_row",
    "run_topology_eval",
]
