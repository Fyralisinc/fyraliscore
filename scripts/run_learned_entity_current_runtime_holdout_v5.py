#!/usr/bin/env python3
"""Seal, then execute once, the current-runtime entity holdout v5.

Protocol:
1. Commit this runner, its corpus, and every runtime source named below.
2. Run ``--seal``. This makes no provider call and writes an immutable receipt.
3. Run ``--execute`` once. Drift, retries, prior artifacts, or extra calls fail.

There is intentionally no recovery or rerun mode.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)
from lib.llm.provider import (
    LLMUsageAggregator,
    build_provider,
    close_codex_app_server_client,
    set_response_cache,
    using_usage_aggregator,
)
from services.domain.entity_grounding.learned_discovery import (
    _DISCOVERY_SYSTEM_PROMPT,
    PersistedSignalText,
    discover_batch_mentions,
)
from tests.evaluation.learned_entity_current_runtime_holdout_v5 import (
    FROZEN_CORPUS_V5,
    FROZEN_SHA256_V5,
    METADATA,
    computed_sha256_v5,
)

ARTIFACT_DIR = Path("/tmp/learned_entity_current_runtime_holdout_v5")
PRECALL_RECEIPT = ARTIFACT_DIR / "precall_receipt.json"
EXECUTION_RECEIPT = ARTIFACT_DIR / "execution_receipt.json"
CHECKPOINT = ARTIFACT_DIR / "checkpoint.json"
REPORT = ARTIFACT_DIR / "report.json"
RUNTIME_SOURCE_PATHS = (
    "scripts/run_learned_entity_current_runtime_holdout_v5.py",
    "tests/evaluation/learned_entity_current_runtime_holdout_v5.py",
    "services/domain/entity_grounding/learned_discovery.py",
    "lib/contracts/entity_mentions.py",
    "lib/evaluation/entity_extraction_gold.py",
    "lib/llm/provider.py",
)
PROVIDER_CONFIG = {
    "LLM_PROVIDER": "codex",
    "CODEX_TRANSPORT": "app-server",
    "CODEX_MODEL": "gpt-5.4",
    "LLM_MAX_RETRIES": "0",
    "response_cache": None,
    "temperature": 0.0,
    "max_tokens_formula": "min(4096, 256 + 160 * len(signals))",
}
SLACK_CONTEXT_MAP = {
    "standalone": "standalone",
    "thread_reply": "threaded",
    "thread_reply_delayed": "threaded",
    "cross_thread_reference": "cross_thread",
    "temporal_sequence": "temporally_distributed",
    "channel_followup": "temporally_distributed",
    "cross_channel_temporal": "temporally_distributed",
    "not_slack": "not_slack",
}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _git(*args: str) -> str:
    return subprocess.check_output(
        ("git", *args), cwd=ROOT, text=True, stderr=subprocess.STDOUT
    ).strip()


def _source_digests() -> dict[str, str]:
    return {path: _sha((ROOT / path).read_bytes()) for path in RUNTIME_SOURCE_PATHS}


def _validate_corpus() -> dict[str, tuple[dict, ...]]:
    if computed_sha256_v5() != FROZEN_SHA256_V5:
        raise SystemExit("frozen v5 corpus digest mismatch")
    batches = {
        f"v5-batch-{index}": tuple(
            row for row in FROZEN_CORPUS_V5
            if row["batch_id"] == f"v5-batch-{index}"
        )
        for index in range(1, 4)
    }
    if any(len(rows) != 8 for rows in batches.values()):
        raise SystemExit("v5 requires three genuine eight-signal batches")
    if any(sum(bool(row["gold"]) for row in rows) != 4 for rows in batches.values()):
        raise SystemExit("v5 requires four positive and four negative signals per batch")
    return batches


def seal() -> None:
    _validate_corpus()
    if ARTIFACT_DIR.exists() and any(ARTIFACT_DIR.iterdir()):
        raise SystemExit("v5 artifacts already exist; seal/execution/rerun refused")
    dirty_runtime = [
        path for path in RUNTIME_SOURCE_PATHS
        if subprocess.run(
            ("git", "diff", "--quiet", "HEAD", "--", path), cwd=ROOT
        ).returncode != 0
    ]
    if dirty_runtime:
        raise SystemExit(f"runtime sources must be committed before seal: {dirty_runtime}")
    commit = _git("rev-parse", "HEAD")
    receipt = {
        "schema_version": "entity-current-runtime-precall-receipt-v1",
        "status": "sealed_before_first_provider_call",
        "benchmark": METADATA["benchmark"],
        "evidence_class": METADATA["evidence_class"],
        "sealed_unix_seconds": time.time(),
        "provider_execution_count_before_seal": 0,
        "prior_execution_artifacts": [],
        "allowed_execution_count": 1,
        "reruns_allowed": 0,
        "git_commit": commit,
        "corpus_sha256": FROZEN_SHA256_V5,
        "runtime_source_sha256": _source_digests(),
        "prompt_contract_sha256": _sha(_DISCOVERY_SYSTEM_PROMPT.encode("utf-8")),
        "provider_config": PROVIDER_CONFIG,
        "batch_contract": {"batch_count": 3, "signals_per_batch": 8},
    }
    _atomic_json(PRECALL_RECEIPT, receipt)
    print(json.dumps({
        "sealed": str(PRECALL_RECEIPT),
        "precall_receipt_sha256": _sha(PRECALL_RECEIPT.read_bytes()),
        "git_commit": commit,
    }, indent=2))


class _CaptureProvider:
    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.call_count = 0
        self.response: Any | None = None
        self.error: dict[str, str] | None = None
        self.request: dict[str, Any] | None = None

    async def structured(
        self, *, system: str, user: str, schema: type[BaseModel],
        temperature: float, max_tokens: int,
    ) -> Any:
        self.call_count += 1
        self.request = {
            "system_sha256": _sha(system.encode("utf-8")),
            "user_sha256": _sha(user.encode("utf-8")),
            "schema": f"{schema.__module__}.{schema.__qualname__}",
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            self.response = await self.delegate.structured(
                system=system, user=user, schema=schema,
                temperature=temperature, max_tokens=max_tokens,
            )
            return self.response
        except Exception as exc:
            self.error = {"type": type(exc).__name__, "message": str(exc)}
            raise


def _validate_precall() -> tuple[dict[str, Any], dict[str, tuple[dict, ...]]]:
    batches = _validate_corpus()
    if not PRECALL_RECEIPT.exists():
        raise SystemExit("missing pre-call receipt; execute refused")
    if any(path.exists() for path in (EXECUTION_RECEIPT, CHECKPOINT, REPORT)):
        raise SystemExit("execution artifact exists; rerun refused")
    receipt = json.loads(PRECALL_RECEIPT.read_text(encoding="utf-8"))
    expected = {
        "status": "sealed_before_first_provider_call",
        "provider_execution_count_before_seal": 0,
        "allowed_execution_count": 1,
        "reruns_allowed": 0,
        "corpus_sha256": FROZEN_SHA256_V5,
        "runtime_source_sha256": _source_digests(),
        "prompt_contract_sha256": _sha(_DISCOVERY_SYSTEM_PROMPT.encode("utf-8")),
        "provider_config": PROVIDER_CONFIG,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise SystemExit(f"pre-call receipt drift: {key}")
    if receipt.get("git_commit") != _git("rev-parse", "HEAD"):
        raise SystemExit("HEAD differs from pre-call committed runtime")
    return receipt, batches


def _score(predictions: list[PredictedMention]) -> dict[str, Any]:
    signals = [GoldSignal(
        signal_id=row["signal_id"], batch_id=row["batch_id"],
        source_type=row["source_type"], text=row["text"],
        slack_context=SLACK_CONTEXT_MAP[row["slack_context"]],
    ) for row in FROZEN_CORPUS_V5]
    gold = [GoldMention(
        mention_id=item["mention_id"], signal_id=row["signal_id"],
        start=item["start"], end=item["end"], entity_type=item["entity_type"],
    ) for row in FROZEN_CORPUS_V5 for item in row["gold"]]
    report = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
    ).model_dump(mode="json")
    types = sorted({item.entity_type for item in gold})
    sources = sorted({item.source_type for item in signals})
    report["by_entity_type"] = {
        name: evaluate_gold_entity_extraction(
            signals=signals,
            gold_mentions=[item for item in gold if item.entity_type == name],
            predictions=[item for item in predictions if item.entity_type == name],
        ).overall.model_dump(mode="json") for name in types
    }
    report["by_source_type"] = {
        name: evaluate_gold_entity_extraction(
            signals=[item for item in signals if item.source_type == name],
            gold_mentions=[item for item in gold if next(
                signal for signal in signals if signal.signal_id == item.signal_id
            ).source_type == name],
            predictions=[item for item in predictions if next(
                signal for signal in signals if signal.signal_id == item.signal_id
            ).source_type == name],
        ).overall.model_dump(mode="json") for name in sources
    }
    return report


async def execute() -> None:
    receipt, batches = _validate_precall()
    _atomic_json(EXECUTION_RECEIPT, {
        "schema_version": "entity-current-runtime-execution-receipt-v1",
        "status": "running",
        "attempt": 1,
        "started_unix_seconds": time.time(),
        "precall_receipt_sha256": _sha(PRECALL_RECEIPT.read_bytes()),
    })
    os.environ.update({key: value for key, value in PROVIDER_CONFIG.items()
                       if isinstance(value, str)})
    set_response_cache(None)
    provider = build_provider()
    predictions: list[PredictedMention] = []
    batch_runs: list[dict[str, Any]] = []
    try:
        for batch_id, rows in batches.items():
            signals = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows)
            capture = _CaptureProvider(provider)
            usage = LLMUsageAggregator()
            started = time.perf_counter()
            with using_usage_aggregator(usage):
                result = await discover_batch_mentions(provider=capture, signals=signals)
            detected = [item for item in result.candidates
                        if item.fate is EntityMentionDetectionFate.DETECTED]
            for candidate in detected:
                predictions.append(PredictedMention(
                    prediction_id=f"v5-{len(predictions)+1:04d}",
                    signal_id=str(candidate.signal_id),
                    start=candidate.span_start, end=candidate.span_end,
                    entity_type=candidate.entity_type,
                    confidence=candidate.confidence,
                    candidate_fate=candidate.fate.value,
                ))
            raw = capture.response.model_dump(mode="json") if capture.response else None
            candidates = [{
                "signal_id": str(item.signal_id), "surface": item.surface,
                "start": item.span_start, "end": item.span_end,
                "entity_type": item.entity_type, "confidence": item.confidence,
                "type_confidence": item.type_confidence, "fate": item.fate.value,
                "reason_codes": list(item.reason_codes),
            } for item in result.candidates]
            run = {
                "batch_id": batch_id, "signal_count": len(rows),
                "structured_calls_observed": capture.call_count,
                "request_contract": capture.request,
                "raw_structured_output": raw,
                "raw_proposal_count": len(raw.get("mentions", [])) if raw else 0,
                "verified_candidates": candidates,
                "terminal_fate_count": len(candidates),
                "mode": result.mode, "error": capture.error or result.provider_error,
                "latency_seconds": time.perf_counter() - started,
                "usage": {"calls": usage.call_count,
                          "input_tokens": usage.total_input_tokens,
                          "output_tokens": usage.total_output_tokens},
            }
            if capture.call_count != 1:
                run["error"] = f"expected one structured call, observed {capture.call_count}"
            if run["raw_proposal_count"] != run["terminal_fate_count"]:
                run["error"] = "raw proposals lack one terminal verified fate"
            batch_runs.append(run)
            _atomic_json(CHECKPOINT, {
                "schema_version": "entity-current-runtime-checkpoint-v1",
                "precall_receipt_sha256": _sha(PRECALL_RECEIPT.read_bytes()),
                "completed_batch_count": len(batch_runs), "batch_runs": batch_runs,
            })
            if run["error"]:
                raise RuntimeError(f"{batch_id} failed: {run['error']}")

        metrics = _score(predictions)
        negative_ids = {row["signal_id"] for row in FROZEN_CORPUS_V5 if not row["gold"]}
        dirty = sorted({item.signal_id for item in predictions} & negative_ids)
        raw_count = sum(row["raw_proposal_count"] for row in batch_runs)
        fate_count = sum(row["terminal_fate_count"] for row in batch_runs)
        gold_count = sum(len(row["gold"]) for row in FROZEN_CORPUS_V5)
        exact_count = metrics["overall"]["exact_match_count"]
        report = {
            "schema_version": "learned-entity-current-runtime-holdout-v5",
            "evidence_class": METADATA["evidence_class"],
            "precall_receipt_sha256": _sha(PRECALL_RECEIPT.read_bytes()),
            "precommit_commit": receipt["git_commit"],
            "frozen_corpus_sha256": FROZEN_SHA256_V5,
            "runtime_source_sha256": receipt["runtime_source_sha256"],
            "prompt_contract_sha256": receipt["prompt_contract_sha256"],
            "provider_config": receipt["provider_config"],
            "batch_only": True, "batch_count": 3, "signal_count": 24,
            "gold_count": gold_count, "batch_runs": batch_runs,
            "metrics": metrics,
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty),
                "rate": (len(negative_ids) - len(dirty)) / len(negative_ids),
                "dirty_signal_ids": dirty,
            },
            "fate_coverage": {
                "raw_proposal_count": raw_count,
                "terminal_candidate_fate_count": fate_count,
                "terminal_candidate_fate_rate": fate_count / raw_count if raw_count else 1.0,
                "gold_mentions_with_exact_detected_fate": exact_count,
                "gold_mention_detected_fate_rate": exact_count / gold_count,
                "candidate_fates": dict(Counter(
                    item["fate"] for run in batch_runs for item in run["verified_candidates"]
                )),
            },
            "protocol": {
                "execution_attempts": 1, "retries": 0,
                "precall_runtime_sources_bound": True,
                "precall_prompt_contract_bound": True,
                "raw_outputs_preserved": True, "per_batch_checkpoints": True,
            },
            "proof_boundary": [
                "literal mention discovery and role-grounded typing only",
                "aliases are separate written mentions; canonical linking is not claimed",
                "implicit references must abstain; implicit resolution is not claimed",
                "bounded synthetic normalized signals; no connector or open-world claim",
            ],
        }
        _atomic_json(REPORT, report)
        _atomic_json(EXECUTION_RECEIPT, {
            "schema_version": "entity-current-runtime-execution-receipt-v1",
            "status": "completed", "attempt": 1,
            "precall_receipt_sha256": _sha(PRECALL_RECEIPT.read_bytes()),
            "checkpoint_sha256": _sha(CHECKPOINT.read_bytes()),
            "report_sha256": _sha(REPORT.read_bytes()),
            "completed_unix_seconds": time.time(),
        })
        print(json.dumps({
            "report": str(REPORT), "overall": metrics["overall"],
            "negative_cleanliness": report["negative_cleanliness"],
            "fate_coverage": report["fate_coverage"],
        }, indent=2))
    except Exception as exc:
        current = json.loads(EXECUTION_RECEIPT.read_text(encoding="utf-8"))
        current.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        _atomic_json(EXECUTION_RECEIPT, current)
        raise
    finally:
        await close_codex_app_server_client()


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--seal", action="store_true")
    mode.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.seal:
        seal()
    else:
        asyncio.run(execute())


if __name__ == "__main__":
    main()
