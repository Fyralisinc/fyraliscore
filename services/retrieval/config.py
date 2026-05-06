"""
services/retrieval/config.py — RetrievalConfig dataclass.

Source: RETRIEVAL-DESIGN-AUDIT §11 item 5 (magic numbers configurable).
Implementation plan: AUDIT-FIXES-IMPLEMENTATION-PLAN §2 RA-5.

Houses every retrieval-layer tunable in one place. Loaded once at
process start from environment variables (with sensible defaults), so
operators can tune behavior without code changes.

ENV var convention: each field maps to `RETRIEVAL_<FIELD_UPPERCASE>`.
Bools: case-insensitive {true,1,yes,on} = True; everything else False.
Ints/floats: parsed via int()/float(); a parse error logs a warning
and falls back to the default.

Module-level `CONFIG` is the canonical singleton — most callers should
import that. Tests instantiate fresh `RetrievalConfig(...)` instances
and pass them in explicitly.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, fields
from typing import Any


_log = logging.getLogger(__name__)


_TRUE_STRINGS = {"true", "1", "yes", "on", "y", "t"}


def _env_str_literal(
    name: str, default: str, allowed: set[str],
) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in allowed:
        return v
    _log.warning(
        "RetrievalConfig: env %s=%r not in %s; using default %s",
        name, raw, allowed, default,
    )
    return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_STRINGS


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        _log.warning(
            "RetrievalConfig: env %s=%r failed int parse; using default %d",
            name, raw, default,
        )
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        _log.warning(
            "RetrievalConfig: env %s=%r failed float parse; using default %s",
            name, raw, default,
        )
        return default


@dataclass
class RetrievalConfig:
    """All retrieval tunables. Plan §2 RA-5 spec is canonical."""

    # ---- Pathway A (structural) ----
    structural_k_per_entity: int = 5
    # Audit suggested sibling expansion; off by default per plan.
    structural_sibling_expansion_enabled: bool = False
    # Bound on graph hops in pathway A.
    structural_max_hops: int = 2

    # ---- Pathway B (semantic) ----
    semantic_k: int = 20
    # HNSW ef_search bumped from typical 40 to 80 (audit §3 arg 4).
    semantic_hnsw_ef_search: int = 80

    # ---- Pathway C (temporal) ----
    temporal_window_minutes: int = 60
    # RA-5 fix for audit §4 arg 2: include observations where the
    # actor is in entities_mentioned, not only as author_id.
    temporal_include_entity_mentions: bool = True

    # ---- Pathway D (predictions) ----
    prediction_eval_window_past_hours: int = 24
    prediction_eval_window_future_days: int = 7

    # ---- Assembler ----
    # VERIFY against actual LLM limits (audit §7 arg 3); 100K is the
    # current default.
    context_budget_tokens: int = 100_000
    mmr_lambda_diversity: float = 0.5
    # Follow-up FU-1 (RA-4 wire): when True, `assemble_context` will run
    # MMR (token-budgeted) over the Models bucket. Default False so the
    # integration stays opt-in until the token-budget semantics are
    # validated against production LLM context sizes.
    assembler_use_mmr: bool = False

    # ---- Scoring ----
    # Follow-up FU-1 (RA-3 wire): primary retrieve's merge + rank uses
    # either the legacy linear-weighted-sum ("linear") or reciprocal-
    # rank-fusion ("rrf"). RRF is the new default (audit §6 arg 1).
    # Override via env `RETRIEVAL_SCORING_MODE=linear` for rollback.
    scoring_mode: str = "rrf"

    # ---- Second-pass ----
    second_pass_sparse_threshold: int = 5
    second_pass_bridge_confidence_threshold: float = 0.7

    # ---- Maintenance ----
    activation_pruning_threshold_days: int = 30
    activation_pruning_min_value: float = 0.1

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "RetrievalConfig":
        """Load from os.environ (or a supplied dict, e.g. for tests).

        Each field N maps to RETRIEVAL_<N_uppercase>.
        """
        # Snapshot env if provided; otherwise read live os.environ via helpers.
        if env is not None:
            # Temporarily install env values so the _env_* helpers work
            # uniformly. We restore at the end.
            saved = {k: os.environ.get(k) for k in env}
            os.environ.update(env)
            try:
                return cls.from_env(env=None)
            finally:
                for k, v in saved.items():
                    if v is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = v

        kwargs: dict[str, Any] = {}
        for f in fields(cls):
            envname = "RETRIEVAL_" + f.name.upper()
            default = f.default
            if f.type is int or f.type == "int":
                kwargs[f.name] = _env_int(envname, int(default))
            elif f.type is float or f.type == "float":
                kwargs[f.name] = _env_float(envname, float(default))
            elif f.type is bool or f.type == "bool":
                kwargs[f.name] = _env_bool(envname, bool(default))
            elif f.name == "scoring_mode":
                # Follow-up FU-1: string literal with allowed values.
                kwargs[f.name] = _env_str_literal(
                    envname, str(default), {"linear", "rrf"},
                )
            else:
                kwargs[f.name] = default
        return cls(**kwargs)


# Process-wide singleton. Lazy-initialized so test harnesses can swap
# RETRIEVAL_* env vars before the first import is observed.
CONFIG: RetrievalConfig = RetrievalConfig.from_env()


def reload_config() -> RetrievalConfig:
    """Re-read env vars and replace the module-level CONFIG. Returns
    the new value. Mainly for tests that mutate os.environ at runtime."""
    global CONFIG
    CONFIG = RetrievalConfig.from_env()
    return CONFIG


__all__ = ["RetrievalConfig", "CONFIG", "reload_config"]
