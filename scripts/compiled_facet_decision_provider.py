"""Generic deterministic producer for the active BatchMemoryDecisionSet contract."""

from __future__ import annotations

from collections import defaultdict
import json
import re
from typing import Any

from lib.llm.provider import LLMConfig, LLMProvider


_FACET = re.compile(r"\[FACET subject=([a-z0-9_-]+) value=([a-z0-9_-]+)\]", re.I)
_SUMMARY = re.compile(
    r"(?:natural|summary)[=:]\s*([a-z0-9_-]+) evidence facets:\s*"
    r"([a-z0-9_, -]+)", re.I)
_CANDIDATE = re.compile(
    r"<candidate>\s*(.*?)\s*</candidate>", re.I | re.S
)
_MODEL_CARD = re.compile(
    r"id=([0-9a-f-]{36})[^\n]*?(?:natural|summary)[=:]\s*"
    r"([a-z0-9_-]+) evidence facets:\s*([a-z0-9_, -]+)", re.I
)
_CANDIDATE_ID = re.compile(r"candidate_id:\s*\"?([^\"\s]+)", re.I)


class CompiledFacetDecisionProvider(LLMProvider):
    """Compress visible facets without receiving hidden truth or judge rules."""

    def __init__(self):
        super().__init__(LLMConfig(
            provider="deterministic", api_key="none",
            model="compiled-facet-decision-producer-v1"))
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        del temperature, max_tokens, schema_hint
        self.calls.append({"system": system, "user": user})
        grouped: dict[str, set[str]] = defaultdict(set)
        for subject, facet in _FACET.findall(user):
            grouped[subject.lower()].add(facet.lower())
        consumed_model_ids: list[str] = []
        for model_id, subject, raw_facets in _MODEL_CARD.findall(user):
            consumed_model_ids.append(model_id)
            grouped[subject.lower()].update(
                facet.strip().lower() for facet in raw_facets.split(",")
                if facet.strip())
        # Also accept model-card renderings without an id for format tolerance;
        # they contribute content but cannot be claimed as referenced lineage.
        for subject, raw_facets in _SUMMARY.findall(user):
            grouped[subject.lower()].update(
                facet.strip().lower() for facet in raw_facets.split(",")
                if facet.strip())
        candidate_blocks: list[tuple[str, str]] = []
        for block in _CANDIDATE.findall(user):
            match = _CANDIDATE_ID.search(block)
            if match:
                candidate_blocks.append((match.group(1).rstrip(","), block.lower()))
        decisions = []
        unused_subjects = sorted(grouped)
        for candidate_id, block in candidate_blocks:
            bound = next((subject for subject in unused_subjects if subject in block), None)
            subject = bound or (unused_subjects[0] if unused_subjects else None)
            if subject is not None:
                unused_subjects.remove(subject)
                facets = sorted(grouped[subject])
                decisions.append({
                    "candidate_id": candidate_id, "decision": "accept",
                    "operation": "claim", "confidence": min(.85, .65 + .04 * len(facets)),
                    "claim_role": "fact",
                    "claim_text": f"{subject} evidence facets: " + ", ".join(facets),
                    "reason": "Generic compression of visible current and prior Model facets.",
                })
            else:
                decisions.append({
                    "candidate_id": candidate_id, "decision": "reject",
                    "operation": "no_op", "confidence": .5, "claim_role": "fact",
                    "reason": "No unmatched visible facet group remains; no edge warranted.",
                })
        lineage = ", ".join(sorted(set(consumed_model_ids)))
        return json.dumps({
            "decisions": decisions,
            "reasoning_trace": (
                "Generic compiled compression of visible facet groups. "
                + (f"Consumed prior Models: {lineage}." if lineage else "No prior Model consumed.")
            ),
        })


__all__ = ["CompiledFacetDecisionProvider"]
