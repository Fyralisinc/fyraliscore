"""Scripted LLMProvider for deterministic rendering-service tests.

Replays a list of canned text outputs or exceptions. Matches the
pattern used in `lib/llm/tests/test_provider.py`.
"""
from __future__ import annotations

from lib.llm.provider import LLMConfig, LLMProvider, compute_cost_usd


class ScriptedProvider(LLMProvider):
    """Returns canned responses in order. Records each call's args.

    Also emits usage (100 input / 50 output tokens by default) so the
    aggregator/cost path is exercised.
    """

    def __init__(
        self,
        responses: list[str | BaseException],
        *,
        cfg: LLMConfig | None = None,
        input_tokens_per_call: int = 100,
        output_tokens_per_call: int = 50,
    ) -> None:
        super().__init__(
            cfg or LLMConfig(provider="deepseek", api_key="test", model="deepseek-chat")
        )
        self._responses = list(responses)
        self.calls: list[dict] = []
        self._in = input_tokens_per_call
        self._out = output_tokens_per_call

    async def _raw_call(
        self, *, system: str, user: str, temperature: float, max_tokens: int, schema_hint: str,
    ) -> str:
        self.calls.append({
            "system": system, "user": user,
            "temperature": temperature, "max_tokens": max_tokens,
            "schema_hint": schema_hint,
        })
        if not self._responses:
            raise RuntimeError("ScriptedProvider exhausted")
        nxt = self._responses.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        # Simulate usage bookkeeping.
        self._record_usage(self._in, self._out)
        return nxt


__all__ = ["ScriptedProvider"]
