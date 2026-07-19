"""Configuration for adaptive inquiry execution."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, float(raw))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _env_literal(name: str, default: str, allowed: set[str]) -> str:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().lower()
    if value in allowed:
        return value
    return default


def _env_csv_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or default


@dataclass(frozen=True, slots=True)
class InquiryConfig:
    learned_policy_enabled: bool = True
    max_rounds: int = 2
    questions_per_round: int = 3
    evidence_reservoir_limit: int = 500
    fast_path_evidence_limit: int = 50
    candidate_model_limit: int = 160
    result_model_limit: int = 64
    action_model_budget_limit: int = 24
    action_observation_budget_limit: int = 24
    relevance_min_score: float = 0.30
    relevance_weak_signal_min_score: float = 0.44
    relevance_broad_signal_min_score: float = 0.24
    relevance_score_cliff: float = 0.18
    relevance_min_material_models: int = 3
    reasoning_packet_token_budget: int = 24000
    context_packet_evidence_mode: str = "model_first"
    temporal_window_days: int = 30
    temporal_nearby_window_days: int = 3
    temporal_broad_window_days: int = 30
    temporal_broad_fallback_min_records: int = 2
    semantic_budget: int = 30
    semantic_terms_fallback_min_models: int = 3
    semantic_hybrid_lexical_enabled: bool = True
    semantic_hybrid_lexical_max_candidates: int = 24
    semantic_hybrid_lexical_terms: int = 8
    semantic_hybrid_lexical_per_term_limit: int = 12
    focused_index_enabled: bool = True
    focused_index_terms: int = 12
    focused_index_max_candidates: int = 48
    focused_index_scope_candidates: int = 18
    retrieval_motifs_enabled: bool = True
    retrieval_motif_min_successes: int = 1
    retrieval_motif_max_actions: int = 5
    retrieval_motif_match_threshold: float = 0.34
    reflective_rules_enabled: bool = True
    reflective_rules_shadow_only: bool = False
    reflective_rule_limit: int = 5
    reflective_rule_match_threshold: float = 0.42
    reflective_rule_score_boost: float = 0.12
    read_prep_parallel_enabled: bool = True
    round_action_pipeline_enabled: bool = True
    question_action_parallel_enabled: bool = True
    question_action_parallelism: int = 6
    structural_max_hops: int = 2
    structural_read_fanout_enabled: bool = False
    structural_read_fanout_min_seeds: int = 16
    structural_read_fanout_chunk_size: int = 8
    model_edge_max_hops: int = 2
    llm_question_planning_enabled: bool = True
    utility_governor_enabled: bool = True
    utility_governor_planner_skip_threshold: float = 0.68
    adaptive_question_budget_enabled: bool = True
    adaptive_strong_context_question_limit: int = 2
    adaptive_strong_context_min_evidence: int = 18
    adaptive_strong_context_min_models: int = 8
    llm_question_temperature: float = 0.0
    llm_question_max_tokens: int = 900
    sage_reader_enabled: bool = True
    sage_reader_row_cache_enabled: bool = True
    sage_reader_shared_substrate_enabled: bool = True
    sage_reader_parallel_enabled: bool = True
    sage_reader_parallelism: int = 2
    sage_reader_gate_broad_actions: bool = True
    sage_retrieval_policy_enabled: bool = True
    sage_retrieval_policy_shadow_mode: bool = False
    sage_retrieval_policy_semantic_budget_floor: int = 8
    persist_full_sage_reader_notes: bool = False
    persist: bool = True
    planner_profile: str = "default"
    llm_question_planning_trigger_kinds: tuple[str, ...] = ("T1",)
    question_primitive_weights: dict[str, float] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "InquiryConfig":
        return cls(
            learned_policy_enabled=_env_bool(
                "INQUIRY_LEARNED_POLICY_ENABLED",
                True,
            ),
            planner_profile=os.environ.get(
                "INQUIRY_PLANNER_PROFILE",
                "default",
            ).strip()
            or "default",
            llm_question_planning_trigger_kinds=_env_csv_tuple(
                "INQUIRY_LLM_QUESTION_PLANNING_TRIGGER_KINDS",
                ("T1",),
            ),
            max_rounds=int(os.environ.get("INQUIRY_MAX_ROUNDS", "2")),
            questions_per_round=int(os.environ.get("INQUIRY_QUESTIONS_PER_ROUND", "3")),
            evidence_reservoir_limit=int(
                os.environ.get("INQUIRY_EVIDENCE_RESERVOIR_LIMIT", "500")
            ),
            fast_path_evidence_limit=int(
                os.environ.get("INQUIRY_FAST_PATH_EVIDENCE_LIMIT", "50")
            ),
            candidate_model_limit=int(
                os.environ.get("INQUIRY_CANDIDATE_MODEL_LIMIT", "160")
            ),
            result_model_limit=int(os.environ.get("INQUIRY_RESULT_MODEL_LIMIT", "64")),
            relevance_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_MIN_SCORE", "0.30")
            ),
            relevance_weak_signal_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_WEAK_SIGNAL_MIN_SCORE", "0.44")
            ),
            relevance_broad_signal_min_score=float(
                os.environ.get("INQUIRY_RELEVANCE_BROAD_SIGNAL_MIN_SCORE", "0.24")
            ),
            relevance_score_cliff=float(
                os.environ.get("INQUIRY_RELEVANCE_SCORE_CLIFF", "0.18")
            ),
            relevance_min_material_models=int(
                os.environ.get("INQUIRY_RELEVANCE_MIN_MATERIAL_MODELS", "3")
            ),
            action_model_budget_limit=int(
                os.environ.get("INQUIRY_ACTION_MODEL_BUDGET_LIMIT", "24")
            ),
            action_observation_budget_limit=int(
                os.environ.get("INQUIRY_ACTION_OBSERVATION_BUDGET_LIMIT", "24")
            ),
            reasoning_packet_token_budget=int(
                os.environ.get("INQUIRY_REASONING_PACKET_TOKENS", "24000")
            ),
            context_packet_evidence_mode=_env_literal(
                "INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE",
                "model_first",
                {"all", "model_first", "models_only"},
            ),
            temporal_window_days=int(
                os.environ.get("INQUIRY_TEMPORAL_WINDOW_DAYS", "30")
            ),
            temporal_nearby_window_days=_env_int(
                "INQUIRY_TEMPORAL_NEARBY_WINDOW_DAYS",
                3,
                minimum=1,
            ),
            temporal_broad_window_days=_env_int(
                "INQUIRY_TEMPORAL_BROAD_WINDOW_DAYS",
                int(os.environ.get("INQUIRY_TEMPORAL_WINDOW_DAYS", "30")),
                minimum=1,
            ),
            temporal_broad_fallback_min_records=_env_int(
                "INQUIRY_TEMPORAL_BROAD_FALLBACK_MIN_RECORDS",
                2,
                minimum=1,
            ),
            semantic_budget=int(os.environ.get("INQUIRY_SEMANTIC_BUDGET", "30")),
            semantic_terms_fallback_min_models=_env_int(
                "INQUIRY_SEMANTIC_TERMS_FALLBACK_MIN_MODELS",
                3,
                minimum=1,
            ),
            semantic_hybrid_lexical_enabled=_env_bool(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_ENABLED", True
            ),
            semantic_hybrid_lexical_max_candidates=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_MAX_CANDIDATES",
                24,
                minimum=1,
            ),
            semantic_hybrid_lexical_terms=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_TERMS",
                8,
                minimum=1,
            ),
            semantic_hybrid_lexical_per_term_limit=_env_int(
                "INQUIRY_SEMANTIC_HYBRID_LEXICAL_PER_TERM_LIMIT",
                12,
                minimum=1,
            ),
            focused_index_enabled=_env_bool("INQUIRY_FOCUSED_INDEX_ENABLED", True),
            focused_index_terms=_env_int(
                "INQUIRY_FOCUSED_INDEX_TERMS",
                12,
                minimum=1,
            ),
            focused_index_max_candidates=_env_int(
                "INQUIRY_FOCUSED_INDEX_MAX_CANDIDATES",
                48,
                minimum=1,
            ),
            focused_index_scope_candidates=_env_int(
                "INQUIRY_FOCUSED_INDEX_SCOPE_CANDIDATES",
                18,
                minimum=1,
            ),
            retrieval_motifs_enabled=_env_bool(
                "INQUIRY_RETRIEVAL_MOTIFS_ENABLED", True
            ),
            retrieval_motif_min_successes=_env_int(
                "INQUIRY_RETRIEVAL_MOTIF_MIN_SUCCESSES",
                1,
                minimum=0,
            ),
            retrieval_motif_max_actions=_env_int(
                "INQUIRY_RETRIEVAL_MOTIF_MAX_ACTIONS",
                5,
                minimum=1,
            ),
            retrieval_motif_match_threshold=float(
                os.environ.get("INQUIRY_RETRIEVAL_MOTIF_MATCH_THRESHOLD", "0.34")
            ),
            reflective_rules_enabled=_env_bool(
                "INQUIRY_REFLECTIVE_RULES_ENABLED", True
            ),
            reflective_rules_shadow_only=_env_bool(
                "INQUIRY_REFLECTIVE_RULES_SHADOW_ONLY", False
            ),
            reflective_rule_limit=_env_int(
                "INQUIRY_REFLECTIVE_RULE_LIMIT",
                5,
                minimum=0,
            ),
            reflective_rule_match_threshold=float(
                os.environ.get("INQUIRY_REFLECTIVE_RULE_MATCH_THRESHOLD", "0.42")
            ),
            reflective_rule_score_boost=float(
                os.environ.get("INQUIRY_REFLECTIVE_RULE_SCORE_BOOST", "0.12")
            ),
            read_prep_parallel_enabled=_env_bool(
                "INQUIRY_READ_PREP_PARALLEL_ENABLED", True
            ),
            round_action_pipeline_enabled=_env_bool(
                "INQUIRY_ROUND_ACTION_PIPELINE_ENABLED", True
            ),
            question_action_parallel_enabled=_env_bool(
                "INQUIRY_QUESTION_ACTION_PARALLEL_ENABLED", True
            ),
            question_action_parallelism=_env_int(
                "INQUIRY_QUESTION_ACTION_PARALLELISM",
                6,
                minimum=1,
            ),
            structural_max_hops=int(os.environ.get("INQUIRY_STRUCTURAL_MAX_HOPS", "2")),
            structural_read_fanout_enabled=os.environ.get(
                "INQUIRY_STRUCTURAL_READ_FANOUT_ENABLED",
                "0",
            )
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            structural_read_fanout_min_seeds=int(
                os.environ.get("INQUIRY_STRUCTURAL_READ_FANOUT_MIN_SEEDS", "16")
            ),
            structural_read_fanout_chunk_size=int(
                os.environ.get("INQUIRY_STRUCTURAL_READ_FANOUT_CHUNK_SIZE", "8")
            ),
            model_edge_max_hops=int(os.environ.get("INQUIRY_MODEL_EDGE_MAX_HOPS", "2")),
            llm_question_planning_enabled=os.environ.get(
                "INQUIRY_LLM_QUESTION_PLANNING_ENABLED",
                "1",
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off"},
            utility_governor_enabled=_env_bool(
                "INQUIRY_UTILITY_GOVERNOR_ENABLED",
                True,
            ),
            utility_governor_planner_skip_threshold=_env_float(
                "INQUIRY_UTILITY_GOVERNOR_PLANNER_SKIP_THRESHOLD",
                0.68,
                minimum=0.0,
            ),
            adaptive_question_budget_enabled=_env_bool(
                "INQUIRY_ADAPTIVE_QUESTION_BUDGET_ENABLED",
                True,
            ),
            adaptive_strong_context_question_limit=_env_int(
                "INQUIRY_ADAPTIVE_STRONG_CONTEXT_QUESTION_LIMIT",
                2,
                minimum=1,
            ),
            adaptive_strong_context_min_evidence=_env_int(
                "INQUIRY_ADAPTIVE_STRONG_CONTEXT_MIN_EVIDENCE",
                18,
                minimum=1,
            ),
            adaptive_strong_context_min_models=_env_int(
                "INQUIRY_ADAPTIVE_STRONG_CONTEXT_MIN_MODELS",
                8,
                minimum=1,
            ),
            llm_question_temperature=float(
                os.environ.get("INQUIRY_LLM_QUESTION_TEMPERATURE", "0.0")
            ),
            llm_question_max_tokens=int(
                os.environ.get("INQUIRY_LLM_QUESTION_MAX_TOKENS", "900")
            ),
            sage_reader_enabled=os.environ.get("SAGE_READER_ENABLED", "1")
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_reader_row_cache_enabled=os.environ.get(
                "SAGE_READER_ROW_CACHE_ENABLED", "1"
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_reader_shared_substrate_enabled=os.environ.get(
                "SAGE_READER_SHARED_SUBSTRATE_ENABLED", "1"
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_reader_parallel_enabled=os.environ.get(
                "SAGE_READER_PARALLEL_ENABLED", "1"
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_reader_parallelism=max(
                1,
                int(os.environ.get("SAGE_READER_PARALLELISM", "2")),
            ),
            sage_reader_gate_broad_actions=os.environ.get(
                "SAGE_READER_GATE_BROAD_ACTIONS", "1"
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_retrieval_policy_enabled=os.environ.get(
                "SAGE_RETRIEVAL_POLICY_ENABLED", "1"
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off", ""},
            sage_retrieval_policy_shadow_mode=os.environ.get(
                "SAGE_RETRIEVAL_POLICY_SHADOW_MODE", "0"
            )
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            sage_retrieval_policy_semantic_budget_floor=_env_int(
                "SAGE_RETRIEVAL_POLICY_SEMANTIC_BUDGET_FLOOR",
                8,
                minimum=1,
            ),
            persist_full_sage_reader_notes=os.environ.get(
                "SAGE_READER_PERSIST_FULL_NOTES", "0"
            )
            .strip()
            .lower()
            in {"1", "true", "yes", "on"},
            persist=os.environ.get("INQUIRY_PERSIST", "1").strip().lower()
            not in {"0", "false", "no", "off"},
        )


__all__ = ["InquiryConfig"]
