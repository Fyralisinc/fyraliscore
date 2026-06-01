"""prompts/greeting.py — greeting prompt builder.

Shape mandated by company-os-design.md §10.2:
  - One paragraph.
  - Four beats on active days (greeting, the one thing, the one decision,
    the release phrase). Two beats on quiet days (greeting, what the
    company is doing).
  - Exactly one <span class="serif"> emphasis on the consequence phrase
    when there is one.

Register is the one demonstrated on the Acme Tuesday page:
  "Good morning. One thing is worth your attention before the day starts —
   Acme's renewal is **structurally unsafe** as of Sunday, and revenue
   hasn't caught it yet. One decision is on you by Thursday. Everything
   else is handled."
"""
from __future__ import annotations

from ..contracts import RenderGreetingRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import GREETING_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_GREETING_EXTRA_RULES = """\
Greeting-specific rules:
- Exactly one paragraph. No lists. No headings.
- Open with a time-appropriate greeting phrase (NOT a literal timestamp):
  early_morning/morning \u2192 "Good morning."
  afternoon             \u2192 "Afternoon." or an in-motion opener ("Between meetings?").
  evening               \u2192 "Evening."
  late                  \u2192 "Late check-in." or similar — acknowledges the hour.
- Four beats on active days: (1) greeting, (2) the one thing worth
  attention, (3) the one decision (if any) on the founder by a specific
  date, (4) the release phrase ("Everything else is handled." or similar).
- Two beats on quiet days: (1) greeting, (2) "Nothing consequential
  since yesterday; the company is running at normal metabolism." —
  the quiet register is a legitimate outcome, not a failure.
- At most ONE <span class="serif">PHRASE</span> in the whole paragraph.
  Use it on the consequence phrase. Do not wrap a name or number in it.
- Do not use <span class="serif-hot">; that class is for observation cards.
- If you use <span class="n"> at all, wrap only pre-formatted numbers.
- Reference specific customers / people / cites when active. Never
  generic "things are happening".
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(GREETING_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderGreetingRequest) -> PromptPair:
    system = voice_system_block(
        kind="greeting (one paragraph, four beats on active days)",
        extra_rules=_GREETING_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_summary = summarize_snapshot(
        request.substrate_state, founder=request.founder_context
    )

    user = (
        "Render the greeting for this state. Produce only the inline HTML "
        "for the greeting paragraph. No surrounding <p>, no wrapper tags.\n\n"
        "STATE:\n" + snap_summary + "\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
