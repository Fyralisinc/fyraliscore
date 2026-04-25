"""services/rendering/ — prose layer.

Takes structured substrate state and produces voice-compliant prose
(greeting, card bodies, query labels, conversation turns, close line)
matching company-os-design.md §8 voice and §9 visual system.

Entry points:
    RenderingService (core.py) — orchestrates prompt → LLM → voice_rules → retry.
    voice_rules.py — post-LLM rule-based validators.
    prompts/ — one module per rendering type; each exports build_prompt().
    api.py — FastAPI routes wrapping the service.
"""
from __future__ import annotations

from .voice_rules import (  # noqa: F401
    RULES,
    Severity,
    Violation,
    VoiceRule,
    check_all,
)
