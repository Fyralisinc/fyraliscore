"""prompts/card_decision.py — decision card body.

Reference: company-os-design.md §10.4 Decision card. Two-column layout:
a one-sentence claim on the left + mono chips on the right. This
prompt renders the claim prose; the chips (deadline, at-stake) are
structured fields the UI composes from the card_focus.

Voice: the emphasized clause (e.g. "extend the renewal window") is
wrapped in <span class="serif"> — serif italic accent, not hot. The
decision card is warm-tagged, not hot.
"""
from __future__ import annotations

from ..contracts import RenderCardRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import DECISION_CARD_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_DEC_EXTRA_RULES = """\
Decision-card-body rules (CONTRACTS.md §5, Rev-2 Change 3):
- Wrap your entire output in <div class="card-content">…</div>.
- Inside, put the claim prose in <p class="dec-text">…</p>:
  * One or two sentences. The founder reads this in 5 seconds.
  * If the decision is binary, use the pattern
    "Do A, or <span class=\"serif\">do B</span>." — exactly one
    emphasized clause in the serif span.
  * End with a release phrase if drafts or prep are ready:
    "Drafts for both paths are ready." / "I've prepped both sides."
- After the <p class="dec-text">, emit a <div class="dec-chips">
  block containing the deadline + stakes chips. Example shape:
    <div class="dec-chips">
      <span class="dec-chip hot">decide by <b>Thu 24 Apr</b></span>
      <span class="dec-chip">at stake <b>$487K</b></span>
    </div>
- The .card-content, .dec-text, .dec-chips wrappers are required;
  Agent-UI attaches the two-column layout via these hooks.
- Do not use <span class="serif-hot"> here; hot is for observation cards.
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(DECISION_CARD_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderCardRequest) -> PromptPair:
    assert request.kind == "decision", "card_decision prompt expects kind='decision'"
    system = voice_system_block(
        kind="decision card body (claim + options + deadline)",
        extra_rules=_DEC_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_summary = summarize_snapshot(
        request.substrate_state, founder=request.founder_context
    )
    focus_lines = [f"  {k}: {v}" for k, v in (request.card_focus or {}).items()]
    focus_block = "\n".join(focus_lines) if focus_lines else "  (use most salient decision)"

    user = (
        "Render a decision card body. Produce only the inline HTML. No <p> "
        "wrapper. The UI positions chips separately; your output is the claim "
        "prose only, including any inline deadline/stakes references.\n\n"
        f"STATE:\n{snap_summary}\n\n"
        f"CARD FOCUS:\n{focus_block}\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
