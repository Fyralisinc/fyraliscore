"""SAGE inquiry-trace gap-filler surfaces (Phase 1).

Re-exports the row models + repo classes for the three Phase-1 tables
introduced in migration 0049:

  * retrieval_plans         — RetrievalPlanRow / RetrievalPlansRepo
  * omitted_evidence        — OmittedEvidenceRow / OmittedEvidenceRepo
  * inquiry_outcome_events  — OutcomeEventRow / OutcomeEventsRepo

See `fyralis-sage-synthesis-self-evolution.md` (Phase 1, §15.1, §7.3)
for the role each table plays in the self-evolution loop.
"""
from __future__ import annotations

from services.sage.inquiry_traces.repo import (
    OmittedEvidenceRepo,
    OutcomeEventsRepo,
    RetrievalPlansRepo,
    SageInquiryTraceRepoError,
)
from services.sage.inquiry_traces.types import (
    OMISSION_REASONS,
    OUTCOME_EVENT_TYPES,
    OmittedEvidenceRow,
    OutcomeEventRow,
    RetrievalPlanRow,
)


__all__ = [
    "OMISSION_REASONS",
    "OUTCOME_EVENT_TYPES",
    "OmittedEvidenceRepo",
    "OmittedEvidenceRow",
    "OutcomeEventRow",
    "OutcomeEventsRepo",
    "RetrievalPlanRow",
    "RetrievalPlansRepo",
    "SageInquiryTraceRepoError",
]
