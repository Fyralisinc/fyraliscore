"""Runtime execution routing for Fyralis.

The execution package owns the cheap production gate in front of deeper
retrieval and Think work. It is intentionally deterministic first: the
first rollout records auditable shadow decisions without changing the
existing ingestion -> T1 Think behavior.
"""

from .contracts import (
    DecisionStatus,
    RoutingDecision,
    SignalEnvelope,
    SignalRefType,
    SignalRoute,
)
from .routing import (
    decide_route,
    record_routing_decision,
    routing_decision_status_from_env,
    should_record_routing_decisions,
)
from .inquiry import (
    EvidenceCard,
    Hypothesis,
    InquiryConfig,
    InquiryQuestion,
    InquiryResult,
    RetrievalAction,
    SufficiencyVerdict,
    execution_retrieval_engine,
    inquiry_enabled,
    retrieve_for_execution,
    run_inquiry_retrieval,
)

__all__ = [
    "DecisionStatus",
    "RoutingDecision",
    "SignalEnvelope",
    "SignalRefType",
    "SignalRoute",
    "decide_route",
    "record_routing_decision",
    "routing_decision_status_from_env",
    "should_record_routing_decisions",
    "EvidenceCard",
    "Hypothesis",
    "InquiryConfig",
    "InquiryQuestion",
    "InquiryResult",
    "RetrievalAction",
    "SufficiencyVerdict",
    "execution_retrieval_engine",
    "inquiry_enabled",
    "retrieve_for_execution",
    "run_inquiry_retrieval",
]
