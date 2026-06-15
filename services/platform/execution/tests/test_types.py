from __future__ import annotations

from services.platform import execution
from services.platform.execution import inquiry
from services.platform.execution import types as inquiry_types


def test_inquiry_public_types_keep_legacy_identity() -> None:
    assert inquiry.EvidenceCard is inquiry_types.EvidenceCard
    assert inquiry.Hypothesis is inquiry_types.Hypothesis
    assert inquiry.InquiryQuestion is inquiry_types.InquiryQuestion
    assert inquiry.InquiryResult is inquiry_types.InquiryResult
    assert inquiry.RetrievalAction is inquiry_types.RetrievalAction
    assert inquiry.RetrievalActionPath is inquiry_types.RetrievalActionPath
    assert inquiry.SignalRoute is inquiry_types.SignalRoute
    assert inquiry.SufficiencyVerdict is inquiry_types.SufficiencyVerdict


def test_execution_package_exports_canonical_public_types() -> None:
    assert execution.EvidenceCard is inquiry_types.EvidenceCard
    assert execution.InquiryConfig.__module__ == "services.platform.execution.config"
    assert execution.InquiryResult is inquiry_types.InquiryResult
    assert execution.LearnedRetrievalMotif is inquiry_types.LearnedRetrievalMotif
    assert execution.ModelRelevance is inquiry_types.ModelRelevance
    assert execution.QuestionPolicySignal is inquiry_types.QuestionPolicySignal
