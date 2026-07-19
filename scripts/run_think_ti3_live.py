#!/usr/bin/env python3
"""Execute the bounded TI3 experiment through the pinned Codex CLI provider."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.llm.provider import (  # noqa: E402
    LLMConfig,
    _codex_transport,
    build_provider,
    get_response_cache,
    using_receipt_sink,
)
from lib.llm.telemetry import InMemoryLLMReceiptSink  # noqa: E402
from lib.contracts.kernel import canonical_sha256  # noqa: E402
from services.evaluation.epistemic_repair.think_ti3_experiment import (  # noqa: E402
    CaptureRequest,
    ProviderAttempt,
    run_ti3_experiment,
)
from services.reasoning.think.compiled_reasoning import (  # noqa: E402
    BatchMemoryDecisionSet,
)
from services.reasoning.think.synthesis_contract import (  # noqa: E402
    SynthesisDecisionEnvelope,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--quality-tolerance", type=float, default=0.03)
    parser.add_argument("--max-concurrency", type=int, default=3)
    return parser.parse_args()


def _assert_response_cache_disabled() -> None:
    if get_response_cache() is not None:
        raise RuntimeError("TI3 live execution forbids the response cache")


def _accepted_cognition_binding(event) -> tuple[dict, str, dict]:
    """Bind provider raw text and parsed semantic object as distinct representations."""
    payload = dict(event.payload)
    if payload.get("parse_outcome") != "accepted":
        raise RuntimeError("TI3 cognition event parse outcome is not accepted")
    if canonical_sha256(payload) != event.content_digest:
        raise RuntimeError("TI3 cognition event content digest mismatch")
    structured = payload.get("structured_text")
    if not isinstance(structured, str):
        raise RuntimeError("TI3 raw response is not text")
    text_digest = canonical_sha256(structured)
    if payload.get("raw_digest") != text_digest:
        raise RuntimeError("TI3 cognition raw text digest mismatch")
    parsed = json.loads(structured)
    if not isinstance(parsed, dict):
        raise RuntimeError("TI3 accepted response is not a JSON object")
    return parsed, text_digest, payload


async def _main() -> int:
    args = _arguments()
    base = LLMConfig.from_env()
    if base.provider != "codex" or _codex_transport() != "cli":
        raise SystemExit("TI3 live execution requires provider=codex and transport=cli")
    if base.model != "gpt-5.3-codex-spark":
        raise SystemExit("TI3 live execution requires gpt-5.3-codex-spark")
    _assert_response_cache_disabled()

    async def capture(request: CaptureRequest) -> ProviderAttempt:
        _assert_response_cache_disabled()
        if request.model != base.model:
            raise ValueError("capture request model differs from pinned provider")
        config = replace(
            base,
            max_retries=0,
            reasoning_effort=request.effort,
            circuit_breaker_name=f"codex:ti3:{request.effort}",
        )
        provider = build_provider(config)
        schema = (
            BatchMemoryDecisionSet
            if request.schema_name == "BatchMemoryDecisionSet"
            else SynthesisDecisionEnvelope
        )
        sink = InMemoryLLMReceiptSink()
        with using_receipt_sink(sink):
            await provider.structured(
                system=request.system_prompt,
                user=request.user_prompt,
                schema=schema,
                logical_call_id=request.attempt_id,
                max_attempts=1,
                deadline_s=300,
                max_tokens=4096,
                cognitive_purpose="main_synthesis",
                cognition_versions={
                    "prompt_policy_version": (
                        "legacy-compiled-v1"
                        if request.interface == "legacy_isolated"
                        else "dossier-schema-v1"
                    ),
                    "provider_schema_version": request.schema_name,
                    "compiler_version": "ti2-v1",
                    "routing_policy_version": "ti3-v1",
                    "model": request.model,
                    "effort": request.effort,
                },
            )
        raw_events = [
            row
            for row in sink.cognition_events
            if row.logical_call_id == request.attempt_id
            and row.stage == "raw_provider_response"
            and row.payload.get("parse_outcome") == "accepted"
        ]
        if len(raw_events) != 1:
            raise RuntimeError("TI3 attempt requires exactly one accepted raw response")
        raw_decision, cognition_text_digest, cognition_payload = (
            _accepted_cognition_binding(raw_events[0])
        )
        attempts = [
            row for row in sink.attempts if row.logical_call_id == request.attempt_id
        ]
        logical = [
            row for row in sink.logical_calls if row.logical_call_id == request.attempt_id
        ]
        if len(logical) != 1 or logical[0].outcome != "success":
            raise RuntimeError("TI3 logical receipt is incomplete")
        if len(attempts) != 1 or attempts[0].outcome != "success":
            raise RuntimeError("TI3 requires one successful physical attempt")
        if attempts[0].retry_scheduled:
            raise RuntimeError("TI3 retries are forbidden")
        if attempts[0].usage_exactness != "reported":
            raise RuntimeError("TI3 requires reported exact usage")
        if attempts[0].model != request.model or logical[0].model != request.model:
            raise RuntimeError("TI3 receipt model mismatch")
        latency_ms = max(
            0.0,
            (logical[0].ended_at - logical[0].started_at).total_seconds() * 1000,
        )
        return ProviderAttempt(
            raw_decision=raw_decision,
            input_tokens=sum(row.input_tokens for row in attempts),
            output_tokens=sum(row.output_tokens for row in attempts),
            latency_ms=latency_ms,
            cost_usd=sum(row.cost_usd for row in attempts),
            validation_status="not_run", apply_status="not_run",
            partial_write_count=None, validator_applier_failure_count=None,
            attempt_id=request.attempt_id,
            model=attempts[0].model,
            effort=request.effort,
            prompt_digest=request.prompt_digest,
            schema_digest=request.schema_digest,
            physical_attempt_ids=[attempts[0].physical_attempt_id],
            physical_attempt_count=1, physical_outcomes=[attempts[0].outcome],
            logical_outcome_id=logical[0].logical_call_id, logical_outcome_count=1,
            logical_outcome=logical[0].outcome, parse_outcome="accepted",
            cognition_event_digest=raw_events[0].content_digest,
            cognition_event_payload=cognition_payload,
            cognition_raw_text_digest=cognition_text_digest,
            accepted_raw_digest=canonical_sha256(raw_decision),
            usage_exactness=attempts[0].usage_exactness,
            provider=attempts[0].provider,
            provider_config_effort_digest=canonical_sha256({
                "provider": attempts[0].provider, "model": attempts[0].model,
                "effort": request.effort,
            }),
        )

    artifact = await run_ti3_experiment(
        output_root=args.output_root,
        run_id=args.run_id,
        provider=capture,
        commit=args.commit,
        quality_tolerance=args.quality_tolerance,
        max_concurrency=args.max_concurrency,
        historical_atlas_baseline=None,
    )
    print(json.dumps(artifact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
