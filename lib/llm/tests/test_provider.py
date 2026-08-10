"""Tests for lib/llm/provider.py."""
from __future__ import annotations

import json
import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import BaseModel, Field

from lib.llm.provider import (
    AnthropicProvider,
    CodexProvider,
    DeepSeekProvider,
    LLMConfig,
    LLMConfigError,
    LLMParseError,
    LLMProvider,
    LLMUsageAggregator,
    OpenAIProvider,
    build_provider,
    _codex_transport,
    _codex_should_use_cli_transport,
)
from lib.llm import provider as provider_mod


# ---------------------------------------------------------------------
# A simple Pydantic schema the LLM is expected to produce.
# ---------------------------------------------------------------------

class Claim(BaseModel):
    """A single claim with confidence in [0.05, 0.95]."""
    claim: str
    confidence: float = Field(ge=0.05, le=0.95)
    kind: Literal["state", "prediction"]


# ---------------------------------------------------------------------
# Test double: a Provider whose _raw_call is scripted.
# ---------------------------------------------------------------------

class ScriptedProvider(LLMProvider):
    """
    Replays a list of canned responses (or exceptions) in order.
    Each call to `_raw_call` pops the next item from `responses`.
    """

    def __init__(self, responses: list[str | Exception], cfg: LLMConfig | None = None):
        super().__init__(cfg or LLMConfig(provider="anthropic", api_key="test", model="m"))
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        self.calls.append({
            "system": system, "user": user,
            "temperature": temperature, "max_tokens": max_tokens,
            "schema_hint": schema_hint,
        })
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _valid_payload() -> str:
    return json.dumps({"claim": "Alice ships fast", "confidence": 0.7, "kind": "state"})


def test_provider_records_actual_usage_when_present() -> None:
    provider = ScriptedProvider(
        [],
        LLMConfig(provider="openai", api_key="test", model="gpt-4o"),
    )
    usage = LLMUsageAggregator()
    provider.set_usage_aggregator(usage)

    provider._record_provider_usage_or_estimate(
        provider_transport="openai_chat",
        input_tokens=42,
        output_tokens=9,
        cache_read_tokens=5,
        cache_creation_tokens=0,
        system="system",
        user="user",
        schema_hint="{}",
        content=_valid_payload(),
    )

    assert usage.call_count == 1
    assert usage.total_input_tokens == 42
    assert usage.total_output_tokens == 9
    assert usage.total_cache_read_tokens == 5
    assert usage.total_cost_usd > 0


def test_provider_estimates_usage_when_sdk_usage_is_missing() -> None:
    provider = ScriptedProvider(
        [],
        LLMConfig(provider="openai", api_key="test", model="gpt-4o"),
    )
    usage = LLMUsageAggregator()
    provider.set_usage_aggregator(usage)

    provider._record_provider_usage_or_estimate(
        provider_transport="openai_chat",
        input_tokens=0,
        output_tokens=0,
        cache_read_tokens=0,
        cache_creation_tokens=0,
        system="system prompt",
        user="user prompt",
        schema_hint='{"type":"object"}',
        content=_valid_payload(),
    )

    assert usage.call_count == 1
    assert usage.total_input_tokens > 0
    assert usage.total_output_tokens > 0
    assert usage.total_cost_usd > 0


# =====================================================================
# Config
# =====================================================================

def test_config_from_env_defaults(monkeypatch, tmp_path):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    monkeypatch.delenv("CODEX_AUTH_FILE", raising=False)
    monkeypatch.delenv("CODEX_MODEL", raising=False)
    monkeypatch.delenv("CODEX_TRANSPORT", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    monkeypatch.setattr(provider_mod, "_codex_config_model", lambda: None)
    monkeypatch.setenv("LLM_API_KEY", "k")
    cfg = LLMConfig.from_env()
    assert cfg.provider == "codex"
    assert cfg.api_key == "k"
    assert cfg.model == "gpt-5.5"
    assert cfg.timeout_s == 180.0


def test_config_from_env_openai(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.provider == "openai"
    assert cfg.model == "gpt-4o"


def test_config_from_env_openai_prefers_provider_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "provider-key")
    monkeypatch.setenv("LLM_API_KEY", "generic-key")
    cfg = LLMConfig.from_env()
    assert cfg.api_key == "provider-key"


def test_config_from_env_reads_max_retries(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("LLM_MAX_RETRIES", "0")
    monkeypatch.delenv("LLM_MODEL", raising=False)

    cfg = LLMConfig.from_env()

    assert cfg.max_retries == 0


def test_config_from_env_codex_api_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_API_KEY", "codex-key")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.provider == "codex"
    assert cfg.api_key == "codex-key"
    assert cfg.model


def test_config_from_env_codex_reasoning_effort(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_API_KEY", "codex-key")
    monkeypatch.setenv("CODEX_REASONING_EFFORT", "low")
    monkeypatch.setenv("LLM_MODEL", "gpt-5.3-codex")
    monkeypatch.delenv("CODEX_MODEL", raising=False)

    cfg = LLMConfig.from_env()

    assert cfg.reasoning_effort == "low"


def test_config_from_env_codex_reads_auth_json(monkeypatch, tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps({
            "OPENAI_API_KEY": None,
            "tokens": {
                "access_token": "oauth-token",
                "account_id": "acct_123",
            },
        }),
        encoding="utf-8",
    )

    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_AUTH_FILE", str(auth_file))
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)

    cfg = LLMConfig.from_env()

    assert cfg.api_key == "oauth-token"


def test_config_from_env_codex_local_uses_codex_config_model(
    monkeypatch, tmp_path,
):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    (codex_home / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "oauth-token"}}),
        encoding="utf-8",
    )
    (codex_home / "config.toml").write_text('model = "gpt-5.5"\n')

    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("LLM_MODEL", "gpt-5.3-codex")
    monkeypatch.delenv("CODEX_MODEL", raising=False)
    monkeypatch.delenv("CODEX_TRANSPORT", raising=False)
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    cfg = LLMConfig.from_env()

    assert cfg.model == "gpt-5.5"


def test_config_from_env_codex_model_explicit_override_wins(
    monkeypatch, tmp_path,
):
    codex_home = tmp_path / "codex"
    codex_home.mkdir()
    (codex_home / "auth.json").write_text(
        json.dumps({"tokens": {"access_token": "oauth-token"}}),
        encoding="utf-8",
    )
    (codex_home / "config.toml").write_text('model = "gpt-5.5"\n')

    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("LLM_MODEL", "gpt-5.3-codex")
    monkeypatch.setenv("CODEX_MODEL", "gpt-5.3-codex-spark")
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    cfg = LLMConfig.from_env()

    assert cfg.model == "gpt-5.3-codex-spark"


def test_config_from_env_codex_rejects_deepseek_model(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_API_KEY", "codex-key")
    monkeypatch.setenv("CODEX_TRANSPORT", "responses")
    monkeypatch.setenv("LLM_MODEL", "deepseek-chat")
    monkeypatch.delenv("CODEX_MODEL", raising=False)

    with pytest.raises(LLMConfigError, match="LLM_PROVIDER=codex"):
        LLMConfig.from_env()


def test_codex_transport_auto_uses_cli_for_oauth_auth_json(monkeypatch, tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps({"tokens": {"access_token": "oauth-token"}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("CODEX_AUTH_FILE", str(auth_file))
    monkeypatch.delenv("CODEX_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    monkeypatch.delenv("CODEX_TRANSPORT", raising=False)

    assert _codex_transport() == "app-server"
    assert _codex_should_use_cli_transport() is False


def test_codex_transport_auto_uses_responses_for_api_key(monkeypatch, tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps({"tokens": {"access_token": "oauth-token"}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("CODEX_AUTH_FILE", str(auth_file))
    monkeypatch.setenv("CODEX_API_KEY", "platform-key")
    monkeypatch.delenv("CODEX_TRANSPORT", raising=False)

    assert _codex_should_use_cli_transport() is False


def test_codex_transport_override(monkeypatch):
    monkeypatch.setenv("CODEX_TRANSPORT", "cli")
    assert _codex_should_use_cli_transport() is True

    monkeypatch.setenv("CODEX_TRANSPORT", "responses")
    assert _codex_transport() == "responses"
    assert _codex_should_use_cli_transport() is False

    monkeypatch.setenv("CODEX_TRANSPORT", "app-server")
    assert _codex_transport() == "app-server"
    assert _codex_should_use_cli_transport() is False


def test_config_from_env_unknown_provider(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "my-llm")
    monkeypatch.setenv("LLM_API_KEY", "k")
    with pytest.raises(LLMConfigError):
        LLMConfig.from_env()


def test_build_provider_anthropic(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_API_KEY", "k")
    provider = build_provider()
    assert isinstance(provider, AnthropicProvider)


def test_build_provider_openai(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "k")
    provider = build_provider()
    assert isinstance(provider, OpenAIProvider)


def test_build_provider_codex(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "codex")
    monkeypatch.setenv("CODEX_API_KEY", "k")
    monkeypatch.setenv("LLM_MODEL", "gpt-5.3-codex")
    provider = build_provider()
    assert isinstance(provider, CodexProvider)


async def test_codex_provider_uses_responses_api(monkeypatch):
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")
    monkeypatch.setenv("CODEX_TRANSPORT", "responses")
    monkeypatch.setenv("CODEX_ACCOUNT_ID", "acct_123")

    captured: dict = {}

    class FakeResponses:
        async def create(self, **kwargs):
            captured["request"] = kwargs
            return SimpleNamespace(
                output_text=_valid_payload(),
                usage=SimpleNamespace(input_tokens=13, output_tokens=5),
            )

    class FakeOpenAIClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs
            self.responses = FakeResponses()

    import openai
    from lib.llm.provider import _schema_hint

    monkeypatch.setattr(openai, "AsyncOpenAI", FakeOpenAIClient)
    provider = CodexProvider(LLMConfig(
        provider="codex",
        api_key="codex-key",
        model="gpt-5.3-codex",
    ))

    raw = await provider._raw_call(
        system="s",
        user="u",
        temperature=0.1,
        max_tokens=128,
        schema_hint=_schema_hint(Claim),
    )

    assert json.loads(raw)["claim"] == "Alice ships fast"
    assert captured["client"]["api_key"] == "codex-key"
    assert captured["client"]["default_headers"] == {
        "ChatGPT-Account-Id": "acct_123",
    }
    assert captured["request"]["model"] == "gpt-5.3-codex"
    assert captured["request"]["text"]["format"]["type"] == "json_schema"
    assert captured["request"]["text"]["format"]["strict"] is False


async def test_codex_provider_uses_cli_transport(monkeypatch):
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")
    monkeypatch.setenv("CODEX_TRANSPORT", "cli")
    captured: dict = {}

    class FakeProc:
        returncode = 0

        async def communicate(self, input=None):
            captured["stdin"] = input.decode("utf-8")
            args = captured["args"]
            output_path = Path(args[args.index("--output-last-message") + 1])
            output_path.write_text(_valid_payload(), encoding="utf-8")
            return b"codex\n", b""

        def kill(self):
            captured["killed"] = True

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["args"] = list(args)
        captured["kwargs"] = kwargs
        return FakeProc()

    from lib.llm.provider import _schema_hint

    monkeypatch.setattr(
        "lib.llm.provider.asyncio.create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    provider = CodexProvider(LLMConfig(
        provider="codex",
        api_key="oauth-token",
        model="gpt-5.3-codex",
    ))
    usage = LLMUsageAggregator()
    provider.set_usage_aggregator(usage)

    raw = await provider._raw_call(
        system="system prompt",
        user="user prompt",
        temperature=0.1,
        max_tokens=128,
        schema_hint=_schema_hint(Claim),
    )

    assert json.loads(raw)["claim"] == "Alice ships fast"
    assert captured["args"][:4] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
    ]
    assert "--output-schema" not in captured["args"]
    assert "system prompt" in captured["stdin"]
    assert "user prompt" in captured["stdin"]
    assert "Alice ships fast" not in captured["stdin"]
    assert '"properties"' in captured["stdin"]
    assert usage.call_count == 1
    assert usage.total_input_tokens > 0
    assert usage.total_output_tokens > 0


async def test_codex_provider_reuses_app_server_transport(monkeypatch):
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")
    monkeypatch.setenv("CODEX_TRANSPORT", "app-server")
    monkeypatch.setenv("CODEX_REASONING_EFFORT", "low")

    import lib.llm.provider as provider_module

    provider_module._CODEX_APP_SERVER_CLIENT = None
    provider_module._CODEX_APP_SERVER_LOOP = None
    captured: dict = {"spawns": 0, "turns": []}

    class FakeStdout:
        def __init__(self):
            self.queue: asyncio.Queue[bytes] = asyncio.Queue()

        async def readline(self):
            return await self.queue.get()

        def push(self, obj):
            self.queue.put_nowait((json.dumps(obj) + "\n").encode("utf-8"))

    class FakeStderr:
        async def readline(self):
            return b""

    class FakeStdin:
        def __init__(self, stdout: FakeStdout):
            self.stdout = stdout

        def write(self, data):
            msg = json.loads(data.decode("utf-8"))
            method = msg["method"]
            request_id = msg.get("id")
            if method == "initialize":
                self.stdout.push({"id": request_id, "result": {"protocolVersion": "0.1"}})
            elif method == "thread/start":
                self.stdout.push({
                    "id": request_id,
                    "result": {"thread": {"id": f"thread-{request_id}"}},
                })
            elif method == "turn/start":
                captured["turns"].append(msg["params"])
                self.stdout.push({
                    "id": request_id,
                    "result": {"turn": {"id": f"turn-{request_id}"}},
                })
                self.stdout.push({
                    "method": "item/completed",
                    "params": {
                        "item": {
                            "type": "agentMessage",
                            "text": _valid_payload(),
                        }
                    },
                })
                self.stdout.push({
                    "method": "turn/completed",
                    "params": {
                        "turn": {
                            "id": f"turn-{request_id}",
                            "status": "completed",
                        }
                    },
                })

        async def drain(self):
            return None

    class FakeAppServerProc:
        returncode = None

        def __init__(self):
            self.stdout = FakeStdout()
            self.stdin = FakeStdin(self.stdout)
            self.stderr = FakeStderr()

        def kill(self):
            self.returncode = -9

        async def communicate(self):
            return b"", b""

    async def fake_create_subprocess_exec(*args, **_kwargs):
        captured["spawns"] += 1
        captured["args"] = list(args)
        return FakeAppServerProc()

    monkeypatch.setattr(
        "lib.llm.provider.asyncio.create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    codex = CodexProvider(LLMConfig(
        provider="codex",
        api_key="oauth-token",
        model="gpt-5.3-codex",
    ))
    usage = LLMUsageAggregator()
    codex.set_usage_aggregator(usage)

    first = await codex._raw_call(
        system="system prompt",
        user="user prompt",
        temperature=0.1,
        max_tokens=128,
        schema_hint="",
    )
    second = await codex._raw_call(
        system="system prompt",
        user="user prompt 2",
        temperature=0.1,
        max_tokens=128,
        schema_hint="",
    )

    assert json.loads(first)["claim"] == "Alice ships fast"
    assert json.loads(second)["claim"] == "Alice ships fast"
    assert captured["spawns"] == 1
    assert captured["args"] == ["codex", "app-server", "--listen", "stdio://"]
    assert captured["turns"][0]["effort"] == "low"
    assert captured["turns"][0]["sandboxPolicy"] == {"type": "readOnly"}
    assert usage.call_count == 2
    assert usage.total_input_tokens > 0
    assert usage.total_output_tokens > 0

    if provider_module._CODEX_APP_SERVER_CLIENT is not None:
        await provider_module._CODEX_APP_SERVER_CLIENT._restart()
        provider_module._CODEX_APP_SERVER_CLIENT = None
        provider_module._CODEX_APP_SERVER_LOOP = None


async def test_codex_provider_restarts_app_server_after_failed_turn(monkeypatch):
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")
    monkeypatch.setenv("CODEX_TRANSPORT", "app-server")

    import lib.llm.provider as provider_module

    provider_module._CODEX_APP_SERVER_CLIENT = None
    provider_module._CODEX_APP_SERVER_LOOP = None
    captured: dict = {"spawns": 0, "kills": 0}

    class FakeStdout:
        def __init__(self):
            self.queue: asyncio.Queue[bytes] = asyncio.Queue()

        async def readline(self):
            return await self.queue.get()

        def push(self, obj):
            self.queue.put_nowait((json.dumps(obj) + "\n").encode("utf-8"))

    class FakeStderr:
        async def readline(self):
            return b""

    class FakeStdin:
        def __init__(self, stdout: FakeStdout, *, fail_turn: bool):
            self.stdout = stdout
            self.fail_turn = fail_turn

        def write(self, data):
            msg = json.loads(data.decode("utf-8"))
            method = msg["method"]
            request_id = msg.get("id")
            if method == "initialize":
                self.stdout.push({"id": request_id, "result": {"protocolVersion": "0.1"}})
            elif method == "thread/start":
                self.stdout.push({
                    "id": request_id,
                    "result": {"thread": {"id": f"thread-{request_id}"}},
                })
            elif method == "turn/start":
                self.stdout.push({
                    "id": request_id,
                    "result": {"turn": {"id": f"turn-{request_id}"}},
                })
                if self.fail_turn:
                    self.stdout.push({
                        "method": "turn/completed",
                        "params": {
                            "turn": {
                                "id": f"turn-{request_id}",
                                "status": "failed",
                            }
                        },
                    })
                else:
                    self.stdout.push({
                        "method": "item/completed",
                        "params": {
                            "item": {
                                "type": "agentMessage",
                                "text": _valid_payload(),
                            }
                        },
                    })
                    self.stdout.push({
                        "method": "turn/completed",
                        "params": {
                            "turn": {
                                "id": f"turn-{request_id}",
                                "status": "completed",
                            }
                        },
                    })

        async def drain(self):
            return None

    class FakeAppServerProc:
        returncode = None

        def __init__(self, *, fail_turn: bool):
            self.stdout = FakeStdout()
            self.stdin = FakeStdin(self.stdout, fail_turn=fail_turn)
            self.stderr = FakeStderr()

        def kill(self):
            captured["kills"] += 1
            self.returncode = -9

        async def communicate(self):
            return b"", b""

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        captured["spawns"] += 1
        return FakeAppServerProc(fail_turn=captured["spawns"] == 1)

    monkeypatch.setattr(
        "lib.llm.provider.asyncio.create_subprocess_exec",
        fake_create_subprocess_exec,
    )
    codex = CodexProvider(LLMConfig(
        provider="codex",
        api_key="oauth-token",
        model="gpt-5.3-codex",
    ))

    with pytest.raises(provider_module.LLMError, match="turn ended with status"):
        await codex._raw_call(
            system="system prompt",
            user="user prompt",
            temperature=0.1,
            max_tokens=128,
            schema_hint="",
        )

    raw = await codex._raw_call(
        system="system prompt",
        user="user prompt",
        temperature=0.1,
        max_tokens=128,
        schema_hint="",
    )

    assert json.loads(raw)["claim"] == "Alice ships fast"
    assert captured["spawns"] == 2
    assert captured["kills"] == 1

    if provider_module._CODEX_APP_SERVER_CLIENT is not None:
        await provider_module._CODEX_APP_SERVER_CLIENT._restart()
        provider_module._CODEX_APP_SERVER_CLIENT = None
        provider_module._CODEX_APP_SERVER_LOOP = None


async def test_codex_provider_does_not_infer_account_header_for_api_key(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")
    monkeypatch.setenv("CODEX_TRANSPORT", "responses")
    monkeypatch.setenv("CODEX_API_KEY", "codex-key")
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps({"tokens": {"account_id": "acct_from_local_login"}}),
        encoding="utf-8",
    )
    monkeypatch.setenv("CODEX_AUTH_FILE", str(auth_file))

    captured: dict = {}

    class FakeResponses:
        async def create(self, **kwargs):
            captured["request"] = kwargs
            return SimpleNamespace(output_text=_valid_payload())

    class FakeOpenAIClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs
            self.responses = FakeResponses()

    import openai

    monkeypatch.setattr(openai, "AsyncOpenAI", FakeOpenAIClient)
    provider = CodexProvider(LLMConfig(
        provider="codex",
        api_key="codex-key",
        model="gpt-5.3-codex",
    ))

    raw = await provider._raw_call(
        system="s",
        user="u",
        temperature=0.1,
        max_tokens=128,
        schema_hint="",
    )

    assert json.loads(raw)["claim"] == "Alice ships fast"
    assert captured["client"]["api_key"] == "codex-key"
    assert "default_headers" not in captured["client"]


def test_deepseek_reasoner_uses_json_mode_not_strict_tools(monkeypatch):
    """DeepSeek reasoner rejects tool_choice; chat models keep strict tools."""
    from lib.llm.provider import _deepseek_supports_strict_tool_calling

    assert not _deepseek_supports_strict_tool_calling("deepseek-reasoner")
    assert not _deepseek_supports_strict_tool_calling("deepseek-reasoner-v2")
    assert _deepseek_supports_strict_tool_calling("deepseek-chat")


def test_deepseek_strict_repair_does_not_corrupt_valid_colon_strings():
    from lib.llm.provider import _repair_deepseek_strict_json

    raw = '{"note": "owner: alice", "ok": true}'
    assert _repair_deepseek_strict_json(raw) == raw


def test_deepseek_strict_repair_only_repairs_object_keys():
    from lib.llm.provider import _repair_deepseek_strict_json

    repaired = _repair_deepseek_strict_json(
        '{"trigger_ref: "x", "note": "owner: alice"}'
    )
    assert repaired == '{"trigger_ref": "x", "note": "owner: alice"}'


async def test_deepseek_strict_parse_failure_falls_back_to_json_mode(
    monkeypatch,
):
    """Malformed strict tool args should not be the final failure mode."""
    from services.reasoning.think.diff_schema import RawDiff

    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")

    strict_calls = 0
    fallback_call: dict = {}

    class FakeCompletions:
        async def create(self, **_kwargs):
            nonlocal strict_calls
            strict_calls += 1
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        arguments='{"trigger_ref": "x" "tenant_id": "y"}'
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7),
            )

    class FakeOpenAIClient:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(
                completions=FakeCompletions()
            )

    async def fake_json_mode(self, **kwargs):
        fallback_call.update(kwargs)
        return '{"fallback": true}'

    import openai

    monkeypatch.setattr(openai, "AsyncOpenAI", FakeOpenAIClient)
    monkeypatch.setattr(OpenAIProvider, "_structured_raw", fake_json_mode)

    provider = DeepSeekProvider(LLMConfig(
        provider="deepseek",
        api_key="k",
        model="deepseek-chat",
        max_retries=1,
    ))

    raw = await provider._structured_raw(
        system="s",
        user="u",
        schema=RawDiff,
        temperature=0.0,
        max_tokens=128,
    )

    assert raw == '{"fallback": true}'
    assert strict_calls == 2
    assert "Prior strict tool-call output failed validation" in fallback_call["user"]


# =====================================================================
# Happy-path structured()
# =====================================================================

async def test_structured_happy_path():
    p = ScriptedProvider([_valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert isinstance(out, Claim)
    assert out.claim == "Alice ships fast"
    assert out.confidence == 0.7


async def test_structured_strips_code_fences():
    raw = "```json\n" + _valid_payload() + "\n```"
    p = ScriptedProvider([raw])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.claim == "Alice ships fast"


async def test_structured_schema_hint_included_in_first_call():
    p = ScriptedProvider([_valid_payload()])
    await p.structured(system="s", user="u", schema=Claim)
    assert "confidence" in p.calls[0]["schema_hint"]
    assert "claim" in p.calls[0]["schema_hint"]


# =====================================================================
# Retry-on-parse-failure
# =====================================================================

async def test_structured_retries_on_bad_json_then_succeeds():
    p = ScriptedProvider([
        "not json at all",
        _valid_payload(),
    ])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2
    # Second call includes a repair note.
    assert "Prior attempt failed validation" in p.calls[1]["user"]


async def test_structured_retries_on_schema_validation_failure():
    bad = json.dumps({"claim": "x", "confidence": 2.0, "kind": "state"})  # out of range
    p = ScriptedProvider([bad, _valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2


async def test_structured_exhausts_max_retries():
    # TK-5: default max_retries=1 → 2 total attempts (simplified from
    # the legacy 3 now that strict-mode makes parse errors rare).
    p = ScriptedProvider(["junk"] * 2)
    with pytest.raises(LLMParseError) as exc:
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2
    assert exc.value.context["schema"] == "Claim"


async def test_structured_respects_custom_max_retries():
    cfg = LLMConfig(provider="anthropic", api_key="k", model="m", max_retries=1)
    p = ScriptedProvider(["bad", "bad"], cfg=cfg)
    with pytest.raises(LLMParseError):
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2


async def test_structured_accepts_prose_prefixed_json_only_when_fenced():
    """
    The repair-aware parser tolerates code fences but NOT arbitrary
    prose prefixes. Prose-before-JSON should fail parse and trigger
    a retry.
    """
    bad = "Here is my answer:\n" + _valid_payload()
    p = ScriptedProvider([bad, _valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2


async def test_structured_passes_temperature_and_max_tokens():
    p = ScriptedProvider([_valid_payload()])
    await p.structured(
        system="s", user="u", schema=Claim,
        temperature=0.35, max_tokens=128,
    )
    call = p.calls[0]
    assert call["temperature"] == 0.35
    assert call["max_tokens"] == 128


async def test_structured_propagates_raw_call_errors():
    class Boom(Exception):
        pass

    p = ScriptedProvider([Boom("server down")])
    with pytest.raises(Boom):
        await p.structured(system="s", user="u", schema=Claim)


async def test_structured_error_rejects_invalid_literal_field():
    # TK-5: default retry budget is now 1 (2 total attempts).
    bad_kind = json.dumps({"claim": "x", "confidence": 0.5, "kind": "not_a_kind"})
    p = ScriptedProvider([bad_kind, bad_kind])
    with pytest.raises(LLMParseError):
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2


async def test_anthropic_requires_api_key(monkeypatch):
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    cfg = LLMConfig(provider="anthropic", api_key="", model="m")
    p = AnthropicProvider(cfg)
    with pytest.raises(LLMConfigError):
        await p._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=10, schema_hint="{}",
        )


async def test_openai_requires_api_key():
    cfg = LLMConfig(provider="openai", api_key="", model="m")
    p = OpenAIProvider(cfg)
    with pytest.raises(LLMConfigError):
        await p._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=10, schema_hint="{}",
        )


def test_schema_hint_is_json_valid():
    """The inlined schema hint must itself be valid JSON."""
    from lib.llm.provider import _schema_hint
    hint = _schema_hint(Claim)
    parsed = json.loads(hint)
    assert "properties" in parsed
    assert "claim" in parsed["properties"]


def test_strip_code_fences():
    from lib.llm.provider import _strip_code_fences
    assert _strip_code_fences("```json\n{\"a\": 1}\n```") == '{"a": 1}'
    assert _strip_code_fences("```\n{\"a\": 1}\n```") == '{"a": 1}'
    assert _strip_code_fences('{"a": 1}') == '{"a": 1}'


def test_try_parse_handles_plain_json():
    from lib.llm.provider import _try_parse
    parsed, err = _try_parse(_valid_payload(), Claim)
    assert err is None
    assert isinstance(parsed, Claim)


def test_try_parse_returns_error_on_bad_json():
    from lib.llm.provider import _try_parse
    parsed, err = _try_parse("not json", Claim)
    assert parsed is None
    assert err is not None


async def test_structured_different_schema_per_call():
    class Other(BaseModel):
        topic: str

    raw = json.dumps({"topic": "ship"})
    p = ScriptedProvider([raw])
    out = await p.structured(system="s", user="u", schema=Other)
    assert out.topic == "ship"
