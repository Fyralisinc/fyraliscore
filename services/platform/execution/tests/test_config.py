from __future__ import annotations

from services.platform.execution.config import InquiryConfig as CanonicalInquiryConfig
from services.platform.execution.inquiry import InquiryConfig as InquiryModuleConfig


def test_inquiry_config_legacy_import_reexports_canonical_type() -> None:
    assert InquiryModuleConfig is CanonicalInquiryConfig


def test_inquiry_config_from_env_normalizes_tunable_values(monkeypatch) -> None:
    monkeypatch.setenv("INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", "bad-mode")
    monkeypatch.setenv("INQUIRY_FOCUSED_INDEX_ENABLED", "false")
    monkeypatch.setenv("INQUIRY_SEMANTIC_HYBRID_LEXICAL_MAX_CANDIDATES", "0")
    monkeypatch.setenv("INQUIRY_TEMPORAL_NEARBY_WINDOW_DAYS", "0")
    monkeypatch.setenv("INQUIRY_TEMPORAL_BROAD_WINDOW_DAYS", "14")
    monkeypatch.setenv("INQUIRY_TEMPORAL_BROAD_FALLBACK_MIN_RECORDS", "5")
    monkeypatch.setenv("INQUIRY_ADAPTIVE_QUESTION_BUDGET_ENABLED", "false")
    monkeypatch.setenv("INQUIRY_ADAPTIVE_STRONG_CONTEXT_QUESTION_LIMIT", "0")
    monkeypatch.setenv("INQUIRY_ADAPTIVE_STRONG_CONTEXT_MIN_EVIDENCE", "9")
    monkeypatch.setenv("INQUIRY_ADAPTIVE_STRONG_CONTEXT_MIN_MODELS", "4")

    config = CanonicalInquiryConfig.from_env()

    assert config.context_packet_evidence_mode == "model_first"
    assert config.focused_index_enabled is False
    assert config.semantic_hybrid_lexical_max_candidates == 1
    assert config.temporal_nearby_window_days == 1
    assert config.temporal_broad_window_days == 14
    assert config.temporal_broad_fallback_min_records == 5
    assert config.adaptive_question_budget_enabled is False
    assert config.adaptive_strong_context_question_limit == 1
    assert config.adaptive_strong_context_min_evidence == 9
    assert config.adaptive_strong_context_min_models == 4
