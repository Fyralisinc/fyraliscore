"""Deterministic source-coordinate helpers for explicit mention detection."""

from __future__ import annotations

import re


def locate_explicit_surface_spans(
    content_text: str,
    candidate_surface: str,
) -> tuple[tuple[int, int], ...]:
    """Locate full-token, case-insensitive surface occurrences exactly."""

    tokens = candidate_surface.split()
    if not tokens:
        return ()
    body = r"\s+".join(re.escape(token) for token in tokens)
    if tokens[0][0].isalnum():
        body = rf"(?<!\w){body}"
    if tokens[-1][-1].isalnum():
        body = rf"{body}(?!\w)"
    return tuple(
        (match.start(), match.end())
        for match in re.finditer(body, content_text, flags=re.IGNORECASE)
    )


__all__ = ["locate_explicit_surface_spans"]
