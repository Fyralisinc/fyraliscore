"""Load the pairwise rubric and compute a stable prompt hash.

The hash is surfaced on every `JudgeResult` so downstream consumers can
detect prompt drift and refuse to mix results across rubric versions.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

_PROMPT_PATH = (
    Path(__file__).parent.parent / "prompts" / "pairwise_rubric.md"
)


def load_prompt_template() -> str:
    """Return the rubric template contents as a UTF-8 string."""
    return _PROMPT_PATH.read_text(encoding="utf-8")


def prompt_hash(template: str | None = None) -> str:
    """SHA-256 of the rubric template (64-char hex)."""
    text = template if template is not None else load_prompt_template()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


__all__ = ["load_prompt_template", "prompt_hash"]
