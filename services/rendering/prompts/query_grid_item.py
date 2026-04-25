"""prompts/query_grid_item.py — render the labels for the 6-chip query grid.

Reference: company-os-design.md §10.3. Six pre-loaded queries. Each is:
  - a terse label (conversational, second-person to the system)
  - an icon (chosen by Agent-GRT, passed through)
  - an optional tag (urgent / relevant / 2 min / evergreen)
  - a hot flag (tied to today's live situation)

We render the label only. The chip is then composed by the caller
with the metadata that came in on the spec.

Unlike the other prompts, the output is PLAIN TEXT — not HTML. The UI
renders chips; no inline spans are used inside a chip label.
"""
from __future__ import annotations

import json

from ..contracts import RenderQueryGridRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import QUERY_GRID_EXEMPLARS
from .snapshot_summary import summarize_snapshot


_Q_EXTRA_RULES = """\
Query-grid-chip-label rules:
- PLAIN TEXT. Never HTML. No spans, no tags. The chip is its own
  visual element; the label is one line of text.
- Second person to the system. "Show me why X". "Draft a brief for Y".
  "Which of my beliefs are least supported?".
- No trailing period. Chips read as button-like commands.
- Length: 4 to 10 words typical; 12 hard cap.
- Situation-tied chips (hot=true) reference the specific subject.
  Evergreen chips (hot=false) are standing patterns.
- The six chips together cover distinct intents. Do not duplicate.
- Output: a JSON array of the final labels, ONE label per spec, IN
  THE SAME ORDER as the spec list. No prose, no wrapper keys.
  Example: ["Show me why Acme became unsafe", "Draft a brief for Monica", ...]
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(QUERY_GRID_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderQueryGridRequest) -> PromptPair:
    system = voice_system_block(
        kind="query-grid chip labels (6 chips; plain-text labels only)",
        extra_rules=_Q_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    snap_summary = summarize_snapshot(
        request.substrate_state, founder=request.founder_context
    )

    specs_block = json.dumps(
        [
            {
                "id": s.id,
                "icon": s.icon,
                "hot": s.hot,
                "tag": s.tag,
                "intent": s.intent,
            }
            for s in request.specs
        ],
        indent=2,
    )

    user = (
        "Render a chip label for each spec. Return a JSON array of labels "
        "in the same order as the input specs.\n\n"
        f"STATE:\n{snap_summary}\n\n"
        f"SPECS:\n{specs_block}\n\n"
        "OUTPUT (JSON array of label strings):"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
