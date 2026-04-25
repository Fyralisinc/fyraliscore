"""prompts/conversation_turn.py — render a conversation turn response body.

Reference: company-os-design.md §11. A turn is the system's response to
an arbitrary founder query. It lives under the close line as a stacked
block until the founder dismisses it.

Structure: two to four short paragraphs. Use cites to m-*, c-*, obs-*,
g-*, r-* plainly (no spans). Use <span class="n"> for numbers and
confidence transitions. Use <em> for italicised falsifier phrases or
drafted-voice snippets (the drafted-communication register is the
founder's voice, not the system's).
"""
from __future__ import annotations

import json

from ..contracts import ConversationTurn, RenderConversationTurnRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import CONVERSATION_TURN_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_TURN_EXTRA_RULES = """\
Conversation-turn rules (CONTRACTS.md §5, Rev-2 Change 3):
- Wrap your entire response in <div class="t-body">…</div>. Agent-UI
  attaches turn-body styling via this hook; it must be present.
- Two to four short paragraphs inside the wrapper. Separate with
  blank lines (\\n\\n).
- Each paragraph earns its length. If the answer is one paragraph,
  ship one paragraph.
- Cites (entity references like m-2841, c-187, obs-88412) go inside
  <span class="t-id">…</span>. State-change / event kinds go inside
  <span class="t-kind">…</span>.
- Inline source attributions (e.g. "Alice — Sun 03:12" or
  "linear webhook — Sat 19:03") go inside <span class="cite">…</span>.
- Secondary / parenthetical prose goes inside <span class="note">…</span>.
- Numbers, confidence transitions, and money in <span class="n">.
- <em> is allowed inside a turn for: (a) falsifier text, (b) drafted
  email / Slack messages in the founder's voice. Keep drafts short
  and in the founder's observed register.
- Uncertainty voice: when you don't have a grounded answer, say
  "I don't have a grounded answer to that yet" and name what would
  unblock it. Do not fabricate.
- No "In summary", "In conclusion", or other meta-framing. The last
  paragraph is just the last paragraph.
- If the founder's message conflicts with the substrate, disagree plainly:
  "The substrate reads this differently: <specific reason>."
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(CONVERSATION_TURN_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def _format_history(history: list[ConversationTurn]) -> str:
    if not history:
        return "(no prior turns)"
    lines = []
    for t in history:
        lines.append(f"  [{t.role}]: {t.text}")
    return "\n".join(lines)


def build_prompt(request: RenderConversationTurnRequest) -> PromptPair:
    system = voice_system_block(
        kind="conversation turn (2-4 short paragraphs)",
        extra_rules=_TURN_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_block = (
        summarize_snapshot(request.substrate_state, founder=request.founder_context)
        if request.substrate_state is not None
        else "(no snapshot provided)"
    )
    retrieval_block = json.dumps(request.retrieval_context or {}, indent=2, default=str)
    history_block = _format_history(request.conversation_history)

    user = (
        f"QUERY FROM FOUNDER:\n{request.query}\n\n"
        f"PRIOR TURNS:\n{history_block}\n\n"
        f"RETRIEVAL CONTEXT:\n{retrieval_block}\n\n"
        f"STATE SNAPSHOT:\n{snap_block}\n\n"
        "Render the response. Only inline HTML; paragraphs separated by "
        "blank lines. No <p> wrappers.\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
