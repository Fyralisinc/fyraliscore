"""prompts/close_line.py — render the close line body.

Reference: company-os-design.md §10.5. A single horizontal strip below
the cards. Left: one sentence that releases the founder ("That's the
signal. You can go."). Right: three mono metadata items composed by
the UI from the metadata structure the service returns.

The close line is deliberately the most formulaic surface in the
product. It says the same thing on quiet days and on noisy days. Its
job is release, not novelty. We therefore give the LLM minimal
discretion here — but we still render it through the service for two
reasons: (1) voice consistency checks still run; (2) context-awareness
may, in future, subtly vary the phrasing at different times of day
without breaking the register.
"""
from __future__ import annotations

from ..contracts import RenderCloseLineRequest
from .base import FEW_SHOT_HEADER, PromptPair, voice_system_block
from .exemplars import CLOSE_LINE_EXEMPLARS


_CLOSE_EXTRA_RULES = """\
Close-line rules:
- Plain text, no HTML. The UI bolds the release phrase itself.
- One short sentence followed by the release phrase.
- Register is CANONICAL. Default: "That's the signal. You can go."
  Acceptable variants: "That's what the substrate sees. You can go."
  "Nothing else demands you. You can go." "Done for now. You can go."
- NEVER "Have a great day" / "Good luck!" / "Stay awesome". Those
  are marketing closures; ours is an operational release.
- Length: 4 to 10 words hard cap.
- Output the single line only. No quotes, no prefix, no trailing metadata.
"""


def _format_exemplars() -> str:
    blocks = [FEW_SHOT_HEADER]
    for i, ex in enumerate(CLOSE_LINE_EXEMPLARS, 1):
        blocks.append(
            f"EXAMPLE {i} ({ex.situation})\n"
            f"INPUT:\n{ex.input_summary}\n"
            f"OUTPUT:\n{ex.html_output}"
        )
    return "\n\n".join(blocks)


def build_prompt(request: RenderCloseLineRequest) -> PromptPair:
    system = voice_system_block(
        kind="close line (one short sentence)",
        extra_rules=_CLOSE_EXTRA_RULES,
    ) + "\n\n" + _format_exemplars()

    # The close line input is minimal. We pass the metadata for
    # context (so future variants can reference 'three external moves'
    # if the model ever wants to), but the default output stays terse.
    user = (
        "Render the close line for today. Plain text, one line.\n\n"
        f"signals_watched_count: {request.signals_watched_count}\n"
        f"external_moves: {request.external_moves}\n"
        f"calibration_pct: {request.calibration_pct}\n\n"
        "OUTPUT:"
    )
    return PromptPair(system=system, user=user)


__all__ = ["build_prompt"]
