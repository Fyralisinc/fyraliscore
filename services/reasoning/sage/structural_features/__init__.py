"""services.reasoning.sage.structural_features — Phase 5 structural feature store.

Re-exports the public surface:
  * `ModelStructuralFeatures`, `EdgeStructuralFeatures` — row types
  * `StructuralFeaturesRepo` — asyncpg repo
  * `compute_model_features`, `compute_edge_features` — pure async compute
  * `recompute_features_for_tenant` — job entrypoint
"""

from services.reasoning.sage.structural_features.compute import (
    compute_edge_features,
    compute_model_features,
)
from services.reasoning.sage.structural_features.job import recompute_features_for_tenant
from services.reasoning.sage.structural_features.repo import StructuralFeaturesRepo
from services.reasoning.sage.structural_features.types import (
    EdgeStructuralFeatures,
    ModelStructuralFeatures,
    StructuralEdge,
)

__all__ = [
    "EdgeStructuralFeatures",
    "ModelStructuralFeatures",
    "StructuralEdge",
    "StructuralFeaturesRepo",
    "compute_edge_features",
    "compute_model_features",
    "recompute_features_for_tenant",
]
