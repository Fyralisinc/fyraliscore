"""services/rendering/prompts/ — prompt library.

One module per rendering type. Each exports `build_prompt(request) ->
PromptPair` where PromptPair is a (system, user) string tuple. System
message imports the shared voice rules summary + type-specific few-shots.
User message wraps the structured input and the required HTML output
contract (span classes per CONTRACTS.md §5).
"""
from __future__ import annotations

from .base import PromptPair, voice_system_block  # noqa: F401
