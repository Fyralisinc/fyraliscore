"""Provider identity preflight for active epistemic-repair proof lanes."""

from __future__ import annotations

import os
from collections.abc import Mapping


def require_codex_cli_environment(
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Fail closed unless the proof explicitly selects Codex CLI."""

    values = os.environ if environ is None else environ
    observed = {
        "LLM_PROVIDER": values.get("LLM_PROVIDER", ""),
        "CODEX_TRANSPORT": values.get("CODEX_TRANSPORT", ""),
    }
    expected = {"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "cli"}
    if observed != expected:
        raise RuntimeError(
            "epistemic-repair provider proof requires exact environment "
            f"{expected}; got {observed}"
        )
    return expected


__all__ = ["require_codex_cli_environment"]
