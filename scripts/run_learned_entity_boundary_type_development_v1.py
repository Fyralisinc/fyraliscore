#!/usr/bin/env python3
"""Run a deterministic bounded proof on boundary/type development corpus v1."""

from __future__ import annotations

import asyncio
import argparse
import json
import os
import re
from pathlib import Path
import sys
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate  # noqa: E402
from lib.evaluation.entity_extraction_gold import (  # noqa: E402
    GoldMention, GoldSignal, PredictedMention, evaluate_gold_entity_extraction,
)
from lib.llm.provider import (  # noqa: E402
    build_provider, close_codex_app_server_client, set_response_cache,
)
from services.domain.entity_grounding.learned_discovery import (  # noqa: E402
    PersistedSignalText, discover_batch_mentions,
)
from tests.evaluation.learned_entity_discovery_boundary_type_development_v1 import (  # noqa: E402
    DEVELOPMENT_CORPUS, EVIDENCE_CLASS, VERSION,
)

OUTPUT = Path("/tmp/learned_entity_boundary_type_development_v1.json")
_NAMED = re.compile(r"\b(?:[A-Z][\w-]+(?:\s+[A-Z][\w-]+)*)\s+(?:workstream|rollout|migration|launch|transition)\b")
_CODE = re.compile(r"\b[A-Z][A-Z0-9]*-[A-Z0-9]+\b")


class DeterministicDiscoveryProvider:
    """Generic source-only detector; deliberately emits short workstream spans."""

    async def structured(self, *, user, schema, **kwargs):
        del kwargs
        mentions = []
        for row in json.loads(user)["signals"]:
            text = row["content_text"]
            covered = []
            for match in _NAMED.finditer(text):
                full = match.group(0)
                suffix = full.rsplit(" ", 1)[1].casefold()
                # Simulate the observed learned-boundary miss only for the
                # explicit ontology word. Other work-form boundaries remain a
                # provider decision and are emitted intact.
                short = full[:-(len(suffix) + 1)] if suffix == "workstream" else full
                # Prefix-style "Workstream X" is intentionally outside this regex.
                start = match.start()
                mentions.append({"signal_id": row["signal_id"], "surface": short,
                    "span_start": start, "span_end": start + len(short),
                    "entity_type": "workstream", "confidence": .94, "abstain": False})
                covered.append((match.start(), match.end()))
            for match in _CODE.finditer(text):
                if any(a <= match.start() < b for a, b in covered):
                    continue
                before = text[max(0, match.start()-20):match.start()].casefold()
                typed = next((t for t in ("goal", "decision", "commitment") if re.search(rf"{t}\s*$", before)), None)
                mentions.append({"signal_id": row["signal_id"], "surface": match.group(0),
                    "span_start": match.start(), "span_end": match.end(),
                    "entity_type": typed or "other", "confidence": .92, "abstain": False})
        return schema.model_validate({"mentions": mentions})


async def main(*, provider_kind: str) -> None:
    signals = [GoldSignal(signal_id=r["signal_id"], batch_id=r["batch_id"], source_type=r["source_type"], text=r["text"], slack_context="threaded" if r["source_type"] == "slack" else "not_slack") for r in DEVELOPMENT_CORPUS]
    gold = [GoldMention(mention_id=m["mention_id"], signal_id=r["signal_id"], start=m["start"], end=m["end"], entity_type=m["entity_type"], canonical_referent=None) for r in DEVELOPMENT_CORPUS for m in r["gold"]]
    predictions = []
    type_caps = []
    verified_candidates = []
    if provider_kind == "codex":
        os.environ.update({"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server", "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0"})
        set_response_cache(None)
        provider = build_provider()
    else:
        provider = DeterministicDiscoveryProvider()
    try:
        for batch_id in sorted({r["batch_id"] for r in DEVELOPMENT_CORPUS}):
            rows = [r for r in DEVELOPMENT_CORPUS if r["batch_id"] == batch_id]
            result = await discover_batch_mentions(provider=provider, signals=tuple(PersistedSignalText(UUID(r["signal_id"]), r["source_type"], r["text"]) for r in rows))
            for c in result.candidates:
                verified_candidates.append({
                    "signal_id": str(c.signal_id), "surface": c.surface,
                    "start": c.span_start, "end": c.span_end,
                    "entity_type": c.entity_type, "confidence": c.confidence,
                    "type_confidence": c.type_confidence, "fate": c.fate.value,
                    "reason_codes": list(c.reason_codes),
                })
                if c.fate is EntityMentionDetectionFate.DETECTED:
                    predictions.append(PredictedMention(prediction_id=f"p-{len(predictions)+1}", signal_id=str(c.signal_id), start=c.span_start, end=c.span_end, entity_type=c.entity_type, confidence=c.confidence, canonical_referent=None, candidate_fate=c.fate.value))
                if c.type_confidence != c.confidence:
                    type_caps.append({"surface": c.surface, "type": c.entity_type, "detection_confidence": c.confidence, "type_confidence": c.type_confidence})
    finally:
        if provider_kind == "codex":
            await close_codex_app_server_client()
    metrics = evaluate_gold_entity_extraction(signals=signals, gold_mentions=gold, predictions=predictions).model_dump(mode="json")
    artifact = {"schema_version": VERSION, "evidence_class": EVIDENCE_CLASS, "development_only": True, "generalization_claim_permitted": False, "provider": provider_kind, "signal_count": len(signals), "batch_count": 2, "metrics": metrics, "verified_candidates": verified_candidates, "ambiguous_code_type_caps": type_caps}
    output = OUTPUT.with_name(f"{OUTPUT.stem}_{provider_kind}.json")
    output.write_text(json.dumps(artifact, indent=2) + "\n")
    print(output)
    print(json.dumps({"overall": metrics["overall"], "ambiguous_code_type_caps": type_caps}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", choices=("deterministic", "codex"), default="deterministic")
    args = parser.parse_args()
    asyncio.run(main(provider_kind=args.provider))
