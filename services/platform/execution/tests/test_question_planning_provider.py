from __future__ import annotations

from types import SimpleNamespace

from services.platform.execution import question_planning_provider


def setup_function() -> None:
    question_planning_provider.reset_question_planning_provider_health()


def teardown_function() -> None:
    question_planning_provider.reset_question_planning_provider_health()


def test_codex_question_planning_fallback_defaults_to_light_model(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_cached_fallback_provider(source_config, model, timeout_s, max_retries):
        captured.update(
            {
                "source_config": source_config,
                "model": model,
                "timeout_s": timeout_s,
                "max_retries": max_retries,
            }
        )
        return SimpleNamespace(config=SimpleNamespace(provider="codex", model=model))

    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_FALLBACK_MODEL", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_FALLBACK_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_FALLBACK_MAX_RETRIES", raising=False)
    monkeypatch.setattr(
        question_planning_provider,
        "_cached_fallback_provider",
        fake_cached_fallback_provider,
    )

    source = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.5",
            timeout_s=120,
            max_retries=2,
        )
    )
    failed = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )
    )

    provider = question_planning_provider.select_question_planning_fallback_provider(
        source,
        failed,
    )

    assert provider is not None
    assert captured["model"] == "gpt-5.4-mini"
    assert captured["timeout_s"] == 36.0
    assert captured["max_retries"] == 0


def test_codex_question_planning_fallback_honors_model_override(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_cached_fallback_provider(source_config, model, timeout_s, max_retries):
        captured.update(
            {
                "source_config": source_config,
                "model": model,
                "timeout_s": timeout_s,
                "max_retries": max_retries,
            }
        )
        return SimpleNamespace(config=SimpleNamespace(provider="codex", model=model))

    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_FALLBACK_MODEL", "gpt-5.5")
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_FALLBACK_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_FALLBACK_MAX_RETRIES", raising=False)
    monkeypatch.setattr(
        question_planning_provider,
        "_cached_fallback_provider",
        fake_cached_fallback_provider,
    )

    source = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.5",
            timeout_s=90,
            max_retries=2,
        )
    )
    failed = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )
    )

    provider = question_planning_provider.select_question_planning_fallback_provider(
        source,
        failed,
    )

    assert provider is not None
    assert captured["model"] == "gpt-5.5"
    assert captured["timeout_s"] == 60.0
    assert captured["max_retries"] == 0


def test_question_planning_provider_failure_backoff_records_and_expires(
    monkeypatch,
) -> None:
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS", "10")
    provider = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )
    )

    note = question_planning_provider.record_question_planning_provider_failure(
        provider, RuntimeError("codex app-server turn ended with status 'failed'"), now=100.0
    )
    active = question_planning_provider.question_planning_provider_backoff_note(
        provider,
        now=105.0,
    )
    expired = question_planning_provider.question_planning_provider_backoff_note(
        provider,
        now=111.0,
    )

    assert note is not None
    assert note["llm_model"] == "gpt-5.3-codex-spark"
    assert note["reason"] == "RuntimeError"
    assert note["failure_count"] == 1
    assert note["backoff_kind"] == "generic"
    assert note["backoff_seconds"] == 10.0
    assert note["backoff_remaining_ms"] == 10000
    assert active is not None
    assert active["backoff_remaining_ms"] == 5000
    assert expired is None


def test_question_planning_provider_quota_failure_uses_long_backoff(
    monkeypatch,
) -> None:
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS", "10")
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_QUOTA_BACKOFF_SECONDS", "7200")
    provider = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )
    )

    note = question_planning_provider.record_question_planning_provider_failure(
        provider,
        RuntimeError("You've hit your usage limit for GPT-5.3-Codex-Spark"),
        now=100.0,
    )
    active = question_planning_provider.question_planning_provider_backoff_note(
        provider,
        now=1000.0,
    )

    assert note is not None
    assert note["backoff_kind"] == "quota"
    assert note["backoff_seconds"] == 7200.0
    assert note["backoff_remaining_ms"] == 7200000
    assert active is not None
    assert active["backoff_remaining_ms"] == 6300000


def test_question_planning_provider_timeout_failure_uses_timeout_backoff(
    monkeypatch,
) -> None:
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS", "10")
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_TIMEOUT_BACKOFF_SECONDS", "600")
    provider = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.4-mini",
            reasoning_effort="low",
            timeout_s=36,
            max_retries=0,
        )
    )

    note = question_planning_provider.record_question_planning_provider_failure(
        provider,
        TimeoutError("fallback planner exceeded 60s"),
        now=100.0,
    )
    active = question_planning_provider.question_planning_provider_backoff_note(
        provider,
        now=200.0,
    )

    assert note is not None
    assert note["backoff_kind"] == "timeout"
    assert note["backoff_seconds"] == 600.0
    assert note["backoff_remaining_ms"] == 600000
    assert active is not None
    assert active["backoff_remaining_ms"] == 500000


def test_question_planning_provider_success_clears_backoff(monkeypatch) -> None:
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS", "10")
    provider = SimpleNamespace(
        config=SimpleNamespace(
            provider="codex",
            api_key="test-key",
            model="gpt-5.3-codex-spark",
            reasoning_effort="low",
            timeout_s=24,
            max_retries=0,
        )
    )

    question_planning_provider.record_question_planning_provider_failure(
        provider,
        TimeoutError("planner timed out"),
        now=100.0,
    )
    question_planning_provider.record_question_planning_provider_success(provider)

    assert (
        question_planning_provider.question_planning_provider_backoff_note(
            provider,
            now=101.0,
        )
        is None
    )
