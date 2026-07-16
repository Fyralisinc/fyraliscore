"""Fail-closed propagation for authoritative grounding corrections."""

from services.domain.correction_propagation.projections import (
    ProjectionCorrectionAdapter,
    ProjectionCorrectionFenceReport,
)
from services.domain.correction_propagation.relations import (
    RelationCorrectionAdapter,
    RelationCorrectionFenceReport,
)
from services.domain.correction_propagation.service import (
    CorrectionPropagationService,
    DirectCorrectionFenceReport,
)

__all__ = [
    "CorrectionPropagationService",
    "DirectCorrectionFenceReport",
    "ProjectionCorrectionAdapter",
    "ProjectionCorrectionFenceReport",
    "RelationCorrectionAdapter",
    "RelationCorrectionFenceReport",
]
