"""prompts/card_reasoning.py — expanded-card reasoning + evidence.

Gate 4b fix (COMPANY-OS-UI-BUILD-PLAN §14, Weeks 7-8 closeout deferral).

The Week-6 demo capture showed `cards[].expanded.reasoning_html` landing as
a GRT-synthesised placeholder (`"Pattern surfaced from recent substrate
activity."`) and `evidence[]` rows carrying empty `<span class='serif'></span>`
stubs. This prompt asks RND to compose the expanded content from the
card's subject, body context, and the structured EvidenceRef rows GRT
already gathers.

Output contract (strict JSON object, single shape):

    {
      "reasoning_html": "<one paragraph HTML fragment>",
      "evidence": [
        {"label": "Alice — Sat 22:41",
         "body_html": "<short prose with <span class=\"cite\">…</span> + <span class=\"note\">…</span>>"},
        ...
      ]
    }

Reasoning HTML uses `.serif` / `.hl` / `.n` spans per the §5 contract.
Evidence bodies use `.cite` (inline evidence citation) and `.note`
(secondary/parenthetical prose) — these are the two Rev-2-added span
classes. Labels are short — "actor — Day HH:MM" when the data is there,
falling back to "signal N" in the adapter when not.

Exemplar: the Acme Tuesday observation-card expanded drawer from
company-os.html lines 928-950 and company-os-design.md §10.4. We lift
the structure (reasoning paragraph → evidence rows) and match register
rather than verbatim copy.
"""
from __future__ import annotations

import json

from ..contracts import EvidenceRef, RenderCardReasoningRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block


_REASONING_EXTRA_RULES = """\
Card-reasoning rules:
- Output a single JSON object with EXACTLY two keys: "reasoning_html"
  and "evidence". No prose around it. No code fences. No other keys.
- "reasoning_html" is ONE paragraph (two to four sentences) that
  explains WHY this situation is consequential. Reference specific
  actors / numbers / dates / cite codes from supporting_evidence.
  Never generic ("pattern surfaced from recent substrate activity" is
  exactly what to avoid).
- Inside "reasoning_html", use the span set from the global HTML contract:
  <span class="serif"> ... </span>       — single surgical phrase, rare.
  <span class="hl"> ... </span>          — one asymmetry clause.
  <span class="n"> ... </span>           — numbers, confidence transitions, money.
  Inline cite codes (m-2841, c-187, obs-...) bare, no span.
- "evidence" is a list (1-6 entries). Each entry is
  {"label": "<actor> \u2014 <Day HH:MM>", "body_html": "<HTML fragment>"}.
  If actor or timestamp is missing, fall back to "signal 1" etc.
- Each evidence body_html MUST include at least one <span class="cite">
  AND should include a <span class="note"> where there is a
  secondary / parenthetical clause. Keep bodies short (one sentence).
- Never produce exclamation marks, emoji, or marketing language. Stay
  in the voice.
- Do not invent facts. Only restate what is in card_body_context or
  supporting_evidence. When an evidence row has no excerpt, summarise
  it from the actor/channel/kind fields instead of fabricating a quote.
"""


_EXEMPLAR_OUT = {
    "reasoning_html": (
        "Model <b>m-2841</b> (\u201cAcme renews Q3\u201d) carried a falsifier: "
        "<span class=\"note\">two or more contracted deliverables slip past 15 April</span>. "
        "That falsifier fired Saturday when <b>c-187</b> transitioned to "
        "<span class=\"serif\">Blocked</span> and Alice re-estimated "
        "<b>c-203</b> from <span class=\"n\">2 days</span> to "
        "<span class=\"n\">10 days</span>. Neither transition reached the "
        "CEO channel; revenue has <span class=\"hl\">zero mentions</span> of "
        "the <span class=\"n\">$487K</span> at risk."
    ),
    "evidence": [
        {
            "label": "linear webhook \u2014 Sat 19:03",
            "body_html": (
                "c-187 transitioned to <span class=\"cite\">Blocked "
                "\u2014 Sat 19:03</span>; rate-limiter SLA missed."
            ),
        },
        {
            "label": "Alice \u2014 Sat 22:41",
            "body_html": (
                "Alice re-estimated c-203 from <span class=\"n\">2d</span> to "
                "<span class=\"n\">~10d</span> "
                "<span class=\"cite\">Alice \u2014 Sat 22:41</span>. "
                "<span class=\"note\">Not escalated.</span>"
            ),
        },
        {
            "label": "m-2841 \u2014 Sun 03:12",
            "body_html": (
                "Confidence on m-2841 moved <span class=\"n\">0.81 \u2192 0.54</span> "
                "<span class=\"cite\">falsifier fired \u2014 Sun 03:12</span>."
            ),
        },
    ],
}


_EXEMPLAR_IN = (
    "card_kind: observation\n"
    "card_subject: Acme renewal\n"
    "card_body_context: \"Acme\u2019s renewal is <span class=\\\"serif-hot\\\">structurally "
    "unsafe</span>. Confidence dropped <span class=\\\"n\\\">0.81 \u2192 0.54</span> after two "
    "contracted deliverables slipped. Revenue at risk: <span class=\\\"n\\\">$487K</span>.\"\n"
    "supporting_evidence:\n"
    "  - actor=linear webhook channel=linear t=Sat 19:03 kind=state_change "
    "excerpt='c-187 InProgress \u2192 Blocked' cite_id=obs-88412\n"
    "  - actor=Alice channel=slack_eng t=Sat 22:41 kind=slack "
    "excerpt='re-estimates c-203 from 2d to 10d' cite_id=obs-88430\n"
    "  - actor=system channel=think t=Sun 03:12 kind=update "
    "excerpt='m-2841 0.81 \u2192 0.54; falsifier fired' cite_id=m-2841\n"
)


def _format_evidence(evs: list[EvidenceRef]) -> str:
    if not evs:
        return "  (no supporting evidence rows — the model should still "\
               "produce one reasoning paragraph grounded in card_body_context.)"
    lines: list[str] = []
    for i, e in enumerate(evs, 1):
        ts = ""
        if e.t is not None:
            try:
                ts = e.t.strftime("%a %H:%M")
            except Exception:
                ts = str(e.t)
        parts = [f"  - [{i}]"]
        if e.actor:
            parts.append(f"actor={e.actor}")
        if e.channel:
            parts.append(f"channel={e.channel}")
        if ts:
            parts.append(f"t={ts}")
        if e.kind:
            parts.append(f"kind={e.kind}")
        if e.cite_id:
            parts.append(f"cite_id={e.cite_id}")
        if e.excerpt:
            parts.append(f"excerpt={e.excerpt!r}")
        lines.append(" ".join(parts))
    return "\n".join(lines)


def build_prompt(request: RenderCardReasoningRequest) -> PromptPair:
    """Build the (system, user) prompt pair for card-reasoning rendering.

    System: voice + HTML contract + reasoning-specific rules + one
    worked exemplar (Acme Tuesday observation-card expanded drawer).

    User: the structured inputs — card kind, subject, body context,
    supporting evidence rows.
    """
    system_base = voice_system_block(
        kind="expanded card reasoning (one paragraph + evidence list)",
        extra_rules=_REASONING_EXTRA_RULES,
    )
    exemplar_block = (
        f"{FEW_SHOT_HEADER}\n\n"
        f"EXAMPLE (Acme Tuesday observation-card expanded drawer)\n"
        f"INPUT:\n{_EXEMPLAR_IN}\n"
        f"OUTPUT:\n{json.dumps(_EXEMPLAR_OUT, ensure_ascii=False)}"
    )
    system = f"{system_base}\n\n{exemplar_block}"

    ev_block = _format_evidence(request.supporting_evidence)
    user = (
        "Render the expanded card drawer (reasoning + evidence) for "
        f"this state. Return ONLY the JSON object described in the rules.\n\n"
        f"card_kind: {request.card_kind}\n"
        f"card_subject: {request.card_subject}\n"
        f"card_body_context: {request.card_body_context!r}\n"
        f"supporting_evidence:\n{ev_block}\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
