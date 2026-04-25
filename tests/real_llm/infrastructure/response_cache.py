"""
tests/real_llm/infrastructure/response_cache.py — keyed LLM response cache.

Caches the raw JSON string a provider returns, so iteration on test
assertions does not re-spend on the model. Cache key is a sha256 of
(system_prompt, user_prompt, model_name, temperature, max_tokens,
schema_name). The schema name is included so that changing the
output schema invalidates relevant entries automatically.

Cache layout:
    tests/real_llm/cache/<epoch_hash>/<key>.json

Where <epoch_hash> is the first 12 chars of sha256 over the contents
of services/think/{reason,prompt,llm_reason}.py — so editing any of
those files rotates the epoch and invalidates all cached entries.

Env knobs:
    LLM_CACHE_BYPASS=1  — skip cache reads, still write
    LLM_CACHE_DISABLE=1 — skip cache entirely (no read, no write)
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable


# Repo root is four levels above this file:
# tests/real_llm/infrastructure/response_cache.py -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_CACHE_DIR = _REPO_ROOT / "tests" / "real_llm" / "cache"

_EPOCH_SOURCES = (
    _REPO_ROOT / "services" / "think" / "reason.py",
    _REPO_ROOT / "services" / "think" / "prompt.py",
    _REPO_ROOT / "services" / "think" / "llm_reason.py",
)


class LLMResponseCache:
    """Disk-backed cache of raw LLM JSON responses, scoped per epoch."""

    def __init__(
        self,
        cache_dir: Path | None = None,
        current_epoch: str | None = None,
    ) -> None:
        base = Path(cache_dir) if cache_dir is not None else _DEFAULT_CACHE_DIR
        epoch = current_epoch if current_epoch is not None else self.current_epoch()
        self.epoch = epoch
        self.cache_dir = base / epoch
        if not self._disabled():
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def current_epoch() -> str:
        """Hash of Think prompt/reason source files; first 12 hex chars of sha256."""
        h = hashlib.sha256()
        for path in _EPOCH_SOURCES:
            h.update(path.as_posix().encode("utf-8"))
            h.update(b"\0")
            try:
                h.update(path.read_bytes())
            except FileNotFoundError:
                h.update(b"<missing>")
            h.update(b"\0")
        return h.hexdigest()[:12]

    async def get_or_fetch(
        self,
        *,
        system: str,
        user: str,
        model: str,
        temperature: float,
        max_tokens: int,
        schema_name: str,
        fetch_fn: Callable[[], Any],
    ) -> dict[str, Any]:
        """
        Return cached entry for the given inputs, or invoke `fetch_fn`
        (async) to produce a JSON-serializable dict and persist it.
        """
        if self._disabled():
            result = await fetch_fn()
            return result

        key = self._hash_key(system, user, model, temperature, max_tokens, schema_name)
        cache_path = self.cache_dir / f"{key}.json"

        if cache_path.exists() and not os.environ.get("LLM_CACHE_BYPASS"):
            return json.loads(cache_path.read_text())

        result = await fetch_fn()
        tmp_path = cache_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(result))
        tmp_path.replace(cache_path)
        return result

    @staticmethod
    def _disabled() -> bool:
        return bool(os.environ.get("LLM_CACHE_DISABLE"))

    @staticmethod
    def _hash_key(
        system: str,
        user: str,
        model: str,
        temperature: float,
        max_tokens: int,
        schema_name: str,
    ) -> str:
        payload = json.dumps(
            {
                "system": system,
                "user": user,
                "model": model,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema": schema_name,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__ = ["LLMResponseCache"]
