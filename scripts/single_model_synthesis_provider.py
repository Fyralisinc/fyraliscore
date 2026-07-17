"""Generic same-subject synthesis provider for active batch-memory evaluation."""

from __future__ import annotations

from collections import defaultdict
import json
import re

from lib.llm.provider import LLMConfig, LLMProvider

_FACET = re.compile(r"\[FACET subject=([a-z0-9_-]+) value=([a-z0-9_-]+)\]", re.I)
_MODEL_CARD = re.compile(r"<model>\s*(.*?)\s*</model>", re.I | re.S)
_MODEL_ID = re.compile(r"\bid:\s*([0-9a-f-]{36})", re.I)
_MODEL_NATURAL = re.compile(
    r"\bnatural:\s*([a-z0-9_-]+) evidence facets:\s*([^\n<]+)", re.I
)
_CANDIDATE = re.compile(r"<candidate>\s*(.*?)\s*</candidate>", re.I | re.S)
_CID = re.compile(r"candidate_id:\s*\"?([^\"\s]+)", re.I)


class SingleModelSynthesisProvider(LLMProvider):
    """Combine current facets only with visible same-subject Model cards."""

    def __init__(self) -> None:
        super().__init__(LLMConfig(provider="deterministic", api_key="none",
            model="single-model-synthesis-provider-v1"))

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        del system, temperature, max_tokens, schema_hint
        grouped = defaultdict(set)
        current = set()
        for subject, facet in _FACET.findall(user):
            grouped[subject.lower()].add(facet.lower())
            current.add(subject.lower())
        lineage = defaultdict(set)
        for card in _MODEL_CARD.findall(user):
            model_match = _MODEL_ID.search(card)
            natural_match = _MODEL_NATURAL.search(card)
            if not model_match or not natural_match:
                continue
            model_id = model_match.group(1)
            subject, facets = natural_match.groups()
            subject = subject.lower()
            if subject in current:
                grouped[subject].update(x.strip().lower() for x in facets.split(",") if x.strip())
                lineage[subject].add(model_id)
        subjects = sorted(current)
        decisions = []
        for block in _CANDIDATE.findall(user):
            cid = _CID.search(block)
            if not cid:
                continue
            subject = next((value for value in subjects if value in block.lower()), None)
            subject = subject or (subjects.pop(0) if subjects else None)
            if subject and subject in subjects:
                subjects.remove(subject)
            if subject:
                facets = sorted(grouped[subject])
                if lineage[subject]:
                    claim_text = (
                        f"cross batch synthesized pattern for {subject}; integrated evidence: "
                        + ", ".join(facets)
                    )
                else:
                    claim_text = f"{subject} evidence facets: " + ", ".join(facets)
                decisions.append({"candidate_id": cid.group(1).rstrip(","),
                    "decision": "accept", "operation": "claim", "confidence": .85,
                    "claim_role": "pattern",
                    "claim_text": claim_text,
                    **({"model_id": next(iter(lineage[subject]))}
                       if len(lineage[subject]) == 1 else {}),
                    "reason": "Same-subject synthesis from current evidence and prior Model cards: "
                    + ",".join(sorted(lineage[subject]))})
            else:
                decisions.append({"candidate_id": cid.group(1).rstrip(","),
                    "decision": "reject", "operation": "no_op", "confidence": .5,
                    "claim_role": "fact", "reason": "No current subject remains."})
        return json.dumps({"decisions": decisions,
            "reasoning_trace": "Generic same-subject synthesis; no hidden truth supplied."})


__all__ = ["SingleModelSynthesisProvider"]
