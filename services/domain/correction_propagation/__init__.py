"""Fail-closed propagation for authoritative grounding corrections."""

from services.domain.correction_propagation.service import (
    CorrectionPropagationService,
    DirectCorrectionFenceReport,
)

__all__ = ["CorrectionPropagationService", "DirectCorrectionFenceReport"]
