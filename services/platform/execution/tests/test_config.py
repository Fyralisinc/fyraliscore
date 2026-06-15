from __future__ import annotations

from services.platform.execution.config import InquiryConfig as CanonicalInquiryConfig
from services.platform.execution.inquiry import InquiryConfig as InquiryModuleConfig


def test_inquiry_config_legacy_import_reexports_canonical_type() -> None:
    assert InquiryModuleConfig is CanonicalInquiryConfig


def test_inquiry_config_from_env_normalizes_tunable_values(monkeypatch) -> None:
    monkeypatch.setenv("INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", "bad-mode")
    monkeypatch.setenv("INQUIRY_FOCUSED_INDEX_ENABLED", "false")
    monkeypatch.setenv("INQUIRY_SEMANTIC_HYBRID_LEXICAL_MAX_CANDIDATES", "0")

    config = CanonicalInquiryConfig.from_env()

    assert config.context_packet_evidence_mode == "model_first"
    assert config.focused_index_enabled is False
    assert config.semantic_hybrid_lexical_max_candidates == 1
