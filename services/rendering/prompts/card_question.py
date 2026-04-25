"""prompts/card_question.py — question card body.

Reference: company-os-design.md §10.4 Question card. Italic serif body
for the question; a second short paragraph ("why I'm asking") of
sans-serif context. The question is soft-tagged.

Register per §8 "Question voice — italic serif, slightly longer, more
patient. Probes rather than declares." The question probes; it does not
demand.
"""
from __future__ import annotations

from ..contracts import RenderCardRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import QUESTION_CARD_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_Q_EXTRA_RULES = """\
Question-card-body rules:
- Output structure: one question sentence (italic serif register),
  followed by a short prose context sentence. The UI styles them;
  you produce the HTML.
- The question sentence is the one that PROBES. Never a declaration.
  Shape: "Is X a real bet, or is it there because Y?" / "Are you
  planning to engage g-42, or did you let it sit because Z?"
- The context sentence names the pattern: the specific asymmetry
  making the system ask. Use <span class="hl"> once on the key clause
  ("I can tell you're visiting it; I can't tell you what visiting means").
- Specifics mandatory: name the goal, the Model, the Resource, the
  count of days. Generic questions are rejected.
- Do not use serif / serif-hot spans here; the UI already sets the
  italic serif face for the question body.
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(QUESTION_CARD_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderCardRequest) -> PromptPair:
    assert request.kind == "question", "card_question prompt expects kind='question'"
    system = voice_system_block(
        kind="question card body (one probe + one context sentence)",
        extra_rules=_Q_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_summary = summarize_snapshot(
        request.substrate_state, founder=request.founder_context
    )
    focus_lines = [f"  {k}: {v}" for k, v in (request.card_focus or {}).items()]
    focus_block = "\n".join(focus_lines) if focus_lines else "  (use most salient standing question)"

    user = (
        "Render a question card body. Produce only the inline HTML. The first "
        "sentence is the question; the second is the short context. Separate "
        "them with a blank line (\\n\\n) only — no <br> or <p>.\n\n"
        f"STATE:\n{snap_summary}\n\n"
        f"CARD FOCUS:\n{focus_block}\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
