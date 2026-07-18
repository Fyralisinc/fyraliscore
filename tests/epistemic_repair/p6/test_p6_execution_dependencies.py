from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.evaluation.epistemic_repair import p6_think_runner


pytestmark = pytest.mark.asyncio


async def test_injected_execution_avoids_production_provider_construction(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    provider = SimpleNamespace(
        config=SimpleNamespace(provider="scripted", model="cf2-script-v1")
    )
    embedder = object()
    dependencies = p6_think_runner.P6ThinkExecutionDependencies(
        llm_provider=provider,
        mention_candidate_adapter=None,
        embedder=embedder,
        run_provenance={
            "runtime_identity": "cf2-provider-free-v1",
            "gold_visible_during_execution": False,
        },
        transport="in_process_scripted",
    )
    captured = {}

    def forbidden(*_args, **_kwargs):
        raise AssertionError("injected execution constructed production dependencies")

    async def fake_execute(**kwargs):
        captured.update(kwargs)
        return {"complete": False, "proof_boundary": "mechanics_only"}

    monkeypatch.setattr(p6_think_runner, "require_codex_cli_environment", forbidden)
    monkeypatch.setattr(p6_think_runner, "build_provider", forbidden)
    monkeypatch.setattr(p6_think_runner, "_run_provenance", forbidden)
    monkeypatch.setattr(p6_think_runner, "_execute_p6_think", fake_execute)

    result = await p6_think_runner.run_p6_think_with_dependencies(
        database_url="postgresql://unused",
        population=build_p6_population(),
        checkpoint_path=tmp_path / "checkpoint.json",
        dependencies=dependencies,
        max_batches=4,
    )

    assert result == {"complete": False, "proof_boundary": "mechanics_only"}
    assert captured["dependencies"] is dependencies
    assert captured["max_batches"] == 4
    assert captured["population"].population_digest == (
        build_p6_population().population_digest
    )


async def test_production_wrapper_fails_before_provider_build_when_preflight_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    calls: list[str] = []

    def reject_environment() -> None:
        calls.append("preflight")
        raise RuntimeError("Codex CLI unavailable")

    def forbidden_build():
        calls.append("build")
        raise AssertionError("provider build must follow strict preflight")

    monkeypatch.setattr(
        p6_think_runner, "require_codex_cli_environment", reject_environment
    )
    monkeypatch.setattr(p6_think_runner, "build_provider", forbidden_build)

    with pytest.raises(RuntimeError, match="Codex CLI unavailable"):
        await p6_think_runner.run_p6_production_think(
            database_url="postgresql://unused",
            population=build_p6_population(),
            checkpoint_path=tmp_path / "checkpoint.json",
            max_batches=1,
        )

    assert calls == ["preflight"]


async def test_production_wrapper_rejects_non_codex_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    provider = SimpleNamespace(
        config=SimpleNamespace(provider="scripted", model="not-codex")
    )

    class Embedder:
        async def close(self) -> None:
            return None

    async def close_provider() -> None:
        return None

    monkeypatch.setattr(p6_think_runner, "require_codex_cli_environment", lambda: None)
    monkeypatch.setattr(
        p6_think_runner, "_run_provenance",
        lambda: {"git_commit": "a" * 40, "worktree_clean": True},
    )
    monkeypatch.setattr(p6_think_runner, "build_provider", lambda: provider)
    monkeypatch.setattr(p6_think_runner, "_codex_transport", lambda: "cli")
    monkeypatch.setattr(p6_think_runner, "OllamaClient", lambda _config: Embedder())
    monkeypatch.setattr(p6_think_runner.OllamaConfig, "from_env", lambda: object())
    monkeypatch.setattr(
        p6_think_runner, "close_codex_app_server_client", close_provider
    )

    with pytest.raises(RuntimeError, match="provider=codex"):
        await p6_think_runner.run_p6_production_think(
            database_url="postgresql://unused",
            population=build_p6_population(),
            checkpoint_path=tmp_path / "checkpoint.json",
            max_batches=1,
        )
