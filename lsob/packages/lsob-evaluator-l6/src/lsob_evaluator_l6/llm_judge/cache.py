"""Per-judgment disk cache.

`CachedJudge` wraps another `LLMJudge` (or anything with a `compare` method)
and persists `JudgeResult`s on disk keyed by
`sha256(reference_diff_json || sut_diff_json || prompt_hash || model)`. This
makes reruns hermetic and avoids re-billing identical comparisons across
runs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from lsob_contracts import DiffOp, Trigger

from lsob_evaluator_l6.llm_judge.client import (
    JudgeResult,
    JudgeRunCost,
    LLMJudge,
)

DEFAULT_CACHE_DIR = Path.home() / ".cache" / "lsob-l6-judge"


def _canonical_dump(diff: DiffOp) -> str:
    return json.dumps(
        json.loads(diff.model_dump_json()), sort_keys=True, separators=(",", ":")
    )


def cache_key(
    reference: DiffOp,
    sut: DiffOp,
    prompt_hash: str,
    model: str,
) -> str:
    payload = "||".join(
        [_canonical_dump(reference), _canonical_dump(sut), prompt_hash, model]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _result_to_json(result: JudgeResult) -> dict[str, Any]:
    d = asdict(result)
    d["cost"] = result.cost.to_dict() | {"estimated_usd": result.cost.estimated_usd}
    return d


def _result_from_json(d: dict[str, Any]) -> JudgeResult:
    cost_d = d.get("cost") or {}
    cost = JudgeRunCost(
        input_tokens=int(cost_d.get("input_tokens", 0)),
        output_tokens=int(cost_d.get("output_tokens", 0)),
        estimated_usd=float(cost_d.get("estimated_usd", 0.0)),
        n_calls=int(cost_d.get("n_calls", 0)),
    )
    return JudgeResult(
        winner=d["winner"],
        raw_votes=list(d.get("raw_votes", [])),
        ordering=d.get("ordering", "ref_first"),
        scores_reference=dict(d.get("scores_reference", {})),
        scores_sut=dict(d.get("scores_sut", {})),
        prompt_hash=d.get("prompt_hash", ""),
        model=d.get("model", ""),
        low_confidence=bool(d.get("low_confidence", False)),
        cost=cost,
        rationale=d.get("rationale"),
    )


class CachedJudge:
    """Wrap an inner judge with a keyed on-disk cache.

    The inner object must expose `compare(trigger, reference, sut) -> JudgeResult`
    and a `prompt_hash` property. Cache misses delegate to the inner judge;
    hits replay the persisted `JudgeResult` without any network or cost.
    """

    def __init__(
        self,
        inner: LLMJudge,
        cache_dir: Path | str | None = None,
    ) -> None:
        self.inner = inner
        self.cache_dir = Path(cache_dir) if cache_dir is not None else DEFAULT_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.hits = 0
        self.misses = 0

    @property
    def prompt_hash(self) -> str:
        return self.inner.prompt_hash

    @property
    def model(self) -> str:
        return self.inner.model

    def _path(self, key: str) -> Path:
        # Shard by the first 2 hex chars to keep directories reasonably small.
        return self.cache_dir / key[:2] / f"{key}.json"

    def get(self, key: str) -> JudgeResult | None:
        path = self._path(key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return _result_from_json(data)

    def put(self, key: str, result: JudgeResult) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(_result_to_json(result), sort_keys=True, default=str),
            encoding="utf-8",
        )

    async def compare(
        self,
        trigger: Trigger,
        reference: DiffOp,
        sut: DiffOp,
    ) -> JudgeResult:
        key = cache_key(reference, sut, self.inner.prompt_hash, self.inner.model)
        cached = self.get(key)
        if cached is not None:
            self.hits += 1
            return cached
        self.misses += 1
        result = await self.inner.compare(trigger, reference, sut)
        self.put(key, result)
        return result


__all__ = ["CachedJudge", "DEFAULT_CACHE_DIR", "cache_key"]
