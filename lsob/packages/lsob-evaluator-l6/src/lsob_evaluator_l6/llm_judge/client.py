"""Core judge clients and orchestration.

`JudgeClient` is the minimal Protocol any backend must implement. `MockJudge`
is a hermetic deterministic client used in tests and CI. `AnthropicJudge`
calls the real Anthropic API with exponential-backoff retries, rate
limiting, and token-usage accounting. `LLMJudge` orchestrates the triple-
judgement anonymised pairwise comparison on top of any `JudgeClient`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from lsob_contracts import DiffOp, Trigger

from lsob_evaluator_l6.llm_judge.prompt_loader import (
    load_prompt_template,
    prompt_hash,
)
from lsob_evaluator_l6.llm_judge.rate_limit import TokenBucket

# The build plan pins Layer 6 to "Claude Sonnet 4.6". Keep the exact model id
# here so changes require an intentional commit.
ANTHROPIC_MODEL_ID = "claude-sonnet-4-6"

# Default pricing (USD per 1M tokens) for Sonnet-4.6. Callers can override via
# JudgeConfig for updated rate cards.
DEFAULT_INPUT_PRICE_PER_MTOK = 3.0
DEFAULT_OUTPUT_PRICE_PER_MTOK = 15.0


@dataclass
class JudgeRunCost:
    """Aggregate Anthropic usage + estimated cost across a run."""

    input_tokens: int = 0
    output_tokens: int = 0
    estimated_usd: float = 0.0
    n_calls: int = 0

    def add(self, other: "JudgeRunCost") -> "JudgeRunCost":
        return JudgeRunCost(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            estimated_usd=self.estimated_usd + other.estimated_usd,
            n_calls=self.n_calls + other.n_calls,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "estimated_usd": round(self.estimated_usd, 6),
            "n_calls": self.n_calls,
        }


@dataclass
class JudgeConfig:
    """Runtime config for `AnthropicJudge` / `LLMJudge`."""

    model: str = ANTHROPIC_MODEL_ID
    temperature: float = 0.0
    max_tokens: int = 1024
    max_retries: int = 6
    rate_per_minute: float = 50.0
    input_price_per_mtok: float = DEFAULT_INPUT_PRICE_PER_MTOK
    output_price_per_mtok: float = DEFAULT_OUTPUT_PRICE_PER_MTOK
    human_review_queue_path: Path | None = None
    api_key_env: str = "ANTHROPIC_API_KEY"


@dataclass
class JudgeResult:
    """Single pairwise comparison result surfaced to callers.

    `winner` is in {reference, sut, tie}. `low_confidence` is True when the
    three judgments did not reach a strict majority.
    """

    winner: str
    raw_votes: list[str]
    ordering: str  # "ref_first" | "sut_first"
    scores_reference: dict[str, int]
    scores_sut: dict[str, int]
    prompt_hash: str
    model: str
    low_confidence: bool = False
    cost: JudgeRunCost = field(default_factory=JudgeRunCost)
    rationale: str | None = None


# Backwards-compat alias for the older name used by the evaluator.
PairwiseOutcome = JudgeResult


class JudgeClient(Protocol):
    """Minimal Protocol any judge backend must implement."""

    name: str

    async def judge(self, prompt: str) -> dict[str, Any]:
        ...


def _render_prompt(
    template: str,
    trigger: Trigger,
    diff_a: DiffOp,
    diff_b: DiffOp,
) -> str:
    return (
        template.replace("{trigger_json}", trigger.model_dump_json())
        .replace("{diff_a_json}", diff_a.model_dump_json())
        .replace("{diff_b_json}", diff_b.model_dump_json())
    )


def parse_judge_json(raw: str) -> dict[str, Any]:
    """Best-effort JSON parse of a raw model response."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw)


class MockJudge:
    """Deterministic mock judge. Identical prompts yield identical outputs."""

    name: str = "mock-judge-v1"

    # Mock token accounting: scale with prompt length so cost tests produce
    # realistic totals. Callers can override via `tokens_per_call` for tests.
    def __init__(
        self,
        tokens_per_call: tuple[int, int] | None = None,
    ) -> None:
        self._override_tokens = tokens_per_call

    async def judge(self, prompt: str) -> dict[str, Any]:
        h = hashlib.sha256(prompt.encode("utf-8")).digest()
        score_a = {
            "scope": h[0] % 6,
            "reasoning": h[1] % 6,
            "completeness": h[2] % 6,
            "fabrication": h[3] % 6,
        }
        score_b = {
            "scope": h[4] % 6,
            "reasoning": h[5] % 6,
            "completeness": h[6] % 6,
            "fabrication": h[7] % 6,
        }
        total_a = sum(score_a.values())
        total_b = sum(score_b.values())
        if total_a > total_b:
            winner = "A"
        elif total_b > total_a:
            winner = "B"
        else:
            winner = "tie"
        if self._override_tokens is not None:
            in_tok, out_tok = self._override_tokens
        else:
            # Approximate: 1 token ~= 4 bytes of prompt + fixed 64 tok response.
            in_tok = max(1, len(prompt) // 4)
            out_tok = 64
        return {
            "scores_a": score_a,
            "scores_b": score_b,
            "winner": winner,
            "rationale": "mock-deterministic",
            "usage": {"input_tokens": in_tok, "output_tokens": out_tok},
        }


class _ScriptedJudge:
    """Test helper: returns a predetermined sequence of judge outputs."""

    name: str = "scripted-judge"

    def __init__(self, outputs: list[dict[str, Any]]) -> None:
        self._outputs = list(outputs)
        self._i = 0

    async def judge(self, prompt: str) -> dict[str, Any]:  # noqa: ARG002
        out = self._outputs[self._i % len(self._outputs)]
        self._i += 1
        return dict(out)


class AnthropicJudge:
    """Real Anthropic-backed judge with retries, rate-limit, cost tracking."""

    name: str = "anthropic-judge-v1"

    def __init__(
        self,
        config: JudgeConfig | None = None,
        client: Any | None = None,
        rate_limiter: TokenBucket | None = None,
        sleep_fn: Any | None = None,
    ) -> None:
        self.config = config or JudgeConfig()
        self.name = f"anthropic-judge:{self.config.model}"
        self._client = client
        self._rate_limiter = rate_limiter or TokenBucket(
            rate_per_minute=self.config.rate_per_minute
        )
        self._sleep = sleep_fn or asyncio.sleep
        self._last_cost = JudgeRunCost()

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        api_key = os.environ.get(self.config.api_key_env)
        if not api_key:
            raise RuntimeError(
                f"{self.config.api_key_env} is not set; cannot call Anthropic"
            )
        # Import lazily so environments without the SDK configured can still
        # import this module (e.g. CI running MockJudge).
        from anthropic import AsyncAnthropic  # type: ignore

        self._client = AsyncAnthropic(api_key=api_key)
        return self._client

    def _is_retryable(self, exc: BaseException) -> bool:
        status = getattr(exc, "status_code", None)
        if status == 429 or (status is not None and 500 <= status < 600):
            return True
        name = type(exc).__name__
        return name in {
            "RateLimitError",
            "APITimeoutError",
            "APIConnectionError",
            "InternalServerError",
            "APIError",
        }

    async def judge(self, prompt: str) -> dict[str, Any]:
        attempt = 0
        while True:
            await self._rate_limiter.acquire()
            try:
                client = self._ensure_client()
                response = await client.messages.create(
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    messages=[{"role": "user", "content": prompt}],
                )
            except Exception as exc:  # noqa: BLE001
                if attempt >= self.config.max_retries or not self._is_retryable(exc):
                    raise
                # Exponential backoff with full jitter: base 0.5s, cap 30s.
                base = min(30.0, 0.5 * (2**attempt))
                delay = random.uniform(0, base)
                await self._sleep(delay)
                attempt += 1
                continue

            text = _extract_text(response)
            parsed = parse_judge_json(text)
            usage = _extract_usage(response)
            parsed["usage"] = usage
            return parsed


def _extract_text(response: Any) -> str:
    """Read the first text block out of an Anthropic `Message` response."""
    blocks = getattr(response, "content", None) or []
    parts: list[str] = []
    for block in blocks:
        text = getattr(block, "text", None)
        if text is None and isinstance(block, dict):
            text = block.get("text")
        if text:
            parts.append(text)
    return "".join(parts)


def _extract_usage(response: Any) -> dict[str, int]:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    if usage is None:
        return {"input_tokens": 0, "output_tokens": 0}
    return {
        "input_tokens": int(
            getattr(usage, "input_tokens", None)
            if not isinstance(usage, dict)
            else usage.get("input_tokens", 0)
        ),
        "output_tokens": int(
            getattr(usage, "output_tokens", None)
            if not isinstance(usage, dict)
            else usage.get("output_tokens", 0)
        ),
    }


def _majority(votes: list[str]) -> tuple[str, bool]:
    """Return `(winner, low_confidence)` for the triple vote.

    low_confidence is True when no single outcome has a strict majority
    (i.e. all three judgments disagree, or a 2-1 tie would still be majority;
    only all-different triples trigger low-confidence here).
    """
    counts = Counter(votes)
    top = counts.most_common()
    if not top:
        return "tie", True
    if top[0][1] >= 2:
        return top[0][0], False
    # No value has >=2 occurrences -> all three distinct -> no majority.
    return "tie", True


def _mean_scores(pool: list[dict[str, int]]) -> dict[str, int]:
    if not pool:
        return {}
    keys = set().union(*(s.keys() for s in pool))
    out: dict[str, int] = {}
    for k in keys:
        vals = [int(s.get(k, 0)) for s in pool]
        out[k] = int(round(sum(vals) / len(vals)))
    return out


def _estimate_cost(
    input_tokens: int,
    output_tokens: int,
    config: JudgeConfig,
) -> float:
    return (
        input_tokens * config.input_price_per_mtok / 1_000_000.0
        + output_tokens * config.output_price_per_mtok / 1_000_000.0
    )


class LLMJudge:
    """Triple-judged pairwise comparison with anonymised ordering."""

    def __init__(
        self,
        judge_client: JudgeClient | None = None,
        template: str | None = None,
        seed: int = 0,
        config: JudgeConfig | None = None,
    ) -> None:
        self.client: JudgeClient = judge_client or MockJudge()
        self.template: str = template if template is not None else load_prompt_template()
        self._rng = random.Random(seed)
        self.config = config or JudgeConfig()
        self.cost = JudgeRunCost()

    @property
    def prompt_hash(self) -> str:
        return prompt_hash(self.template)

    @property
    def model(self) -> str:
        model = getattr(self.client, "name", None) or self.config.model
        return str(model)

    def _choose_ordering(self) -> bool:
        return self._rng.random() < 0.5

    def _append_review_queue(self, record: dict[str, Any]) -> None:
        path = self.config.human_review_queue_path
        if path is None:
            return
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True, default=str) + "\n")

    async def compare(
        self,
        trigger: Trigger,
        reference: DiffOp,
        sut: DiffOp,
    ) -> JudgeResult:
        ref_first = self._choose_ordering()
        diff_a, diff_b = (
            (reference, sut) if ref_first else (sut, reference)
        )
        prompt = _render_prompt(self.template, trigger, diff_a, diff_b)

        votes: list[str] = []
        scores_ref_pool: list[dict[str, int]] = []
        scores_sut_pool: list[dict[str, int]] = []
        rationales: list[str] = []
        call_cost = JudgeRunCost()

        for _ in range(3):
            result = await self.client.judge(prompt)
            raw_winner = result.get("winner", "tie")
            if raw_winner == "A":
                mapped = "reference" if ref_first else "sut"
            elif raw_winner == "B":
                mapped = "sut" if ref_first else "reference"
            else:
                mapped = "tie"
            votes.append(mapped)

            scores_a = result.get("scores_a", {})
            scores_b = result.get("scores_b", {})
            if ref_first:
                scores_ref_pool.append(scores_a)
                scores_sut_pool.append(scores_b)
            else:
                scores_ref_pool.append(scores_b)
                scores_sut_pool.append(scores_a)

            rationale = result.get("rationale")
            if rationale:
                rationales.append(str(rationale))

            usage = result.get("usage") or {}
            in_tok = int(usage.get("input_tokens", 0))
            out_tok = int(usage.get("output_tokens", 0))
            call_cost = call_cost.add(
                JudgeRunCost(
                    input_tokens=in_tok,
                    output_tokens=out_tok,
                    estimated_usd=_estimate_cost(in_tok, out_tok, self.config),
                    n_calls=1,
                )
            )

        winner, low_confidence = _majority(votes)
        self.cost = self.cost.add(call_cost)

        result = JudgeResult(
            winner=winner,
            raw_votes=votes,
            ordering="ref_first" if ref_first else "sut_first",
            scores_reference=_mean_scores(scores_ref_pool),
            scores_sut=_mean_scores(scores_sut_pool),
            prompt_hash=self.prompt_hash,
            model=self.model,
            low_confidence=low_confidence,
            cost=call_cost,
            rationale=" | ".join(rationales) if rationales else None,
        )

        if low_confidence:
            self._append_review_queue(
                {
                    "trigger_id": trigger.trigger_id,
                    "reference_diff_id": reference.diff_id,
                    "sut_diff_id": sut.diff_id,
                    "raw_votes": votes,
                    "ordering": result.ordering,
                    "scores_reference": result.scores_reference,
                    "scores_sut": result.scores_sut,
                    "prompt_hash": self.prompt_hash,
                    "model": self.model,
                    "rationale": result.rationale,
                }
            )

        return result


__all__ = [
    "ANTHROPIC_MODEL_ID",
    "AnthropicJudge",
    "JudgeClient",
    "JudgeConfig",
    "JudgeResult",
    "JudgeRunCost",
    "LLMJudge",
    "MockJudge",
    "PairwiseOutcome",
    "parse_judge_json",
]
