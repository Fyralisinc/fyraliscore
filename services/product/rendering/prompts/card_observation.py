"""prompts/card_observation.py — observation card body.

Reference: company-os-design.md §10.4 Observation card. Single paragraph
with inline numbers; the consequence phrase rendered in
<span class="serif-hot">; expansions handled elsewhere.
"""
from __future__ import annotations

from ..contracts import RenderCardRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import OBSERVATION_CARD_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_OBS_EXTRA_RULES = """\
Observation-card-body rules:
- One paragraph. Two to four sentences.
- Name the consequence phrase in <span class="serif-hot">. Exactly once.
- Wrap confidence transitions, counts, and money amounts in <span class="n">.
  Pre-format: "0.81 \u2192 0.54", "$487K", "11 times", "21 days".
- Use <span class="hl"> once to name the asymmetry if there is one
  ("zero mentions", "the silence itself is the signal").
- Reference at least one specific name (customer, person, cite like
  m-2841 / c-187). Generic card bodies are rejected.
- No preamble. Start with the subject; the tag already says "Observation".
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(OBSERVATION_CARD_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderCardRequest) -> PromptPair:
    assert request.kind == "observation", "card_observation prompt expects kind='observation'"
    system = voice_system_block(
        kind="observation card body (one paragraph)",
        extra_rules=_OBS_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_summary = summarize_snapshot(
        request.substrate_state, founder=request.founder_context
    )

    focus_lines = []
    for key, val in (request.card_focus or {}).items():
        focus_lines.append(f"  {key}: {val}")
    focus_block = "\n".join(focus_lines) if focus_lines else "  (use most salient model/resource from state)"

    user = (
        "Render an observation card body for this state. Produce only the "
        "inline HTML for the card body. No <p> wrapper.\n\n"
        f"STATE:\n{snap_summary}\n\n"
        f"CARD FOCUS:\n{focus_block}\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
