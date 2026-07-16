"""Shared conservative phrase-scope classification for entity grounding."""

from __future__ import annotations


def phrase_requires_context(phrase: str) -> bool:
    """Return whether a surface form is unsafe as tenant-global exact memory."""

    normalized = " ".join(phrase.casefold().split())
    tokens = {
        token.strip(".,!?;:()[]{}\"'")
        for token in normalized.split()
    }
    context_words = {
        "it",
        "this",
        "that",
        "these",
        "those",
        "they",
        "them",
        "he",
        "she",
        "we",
        "same",
        "again",
        "here",
        "there",
        "above",
        "former",
        "latter",
    }
    return bool(tokens & context_words) or normalized.startswith("the ")


__all__ = ["phrase_requires_context"]
