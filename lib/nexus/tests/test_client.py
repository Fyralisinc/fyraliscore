"""Tests for lib/nexus/client.py — stub attestation client."""
from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from lib.nexus.client import (
    AgentSignal,
    AttestationResult,
    NexusClient,
    NexusConfig,
    NexusError,
)


def _valid_signal(**overrides) -> dict:
    base = {
        "agent_id": "support-bot-v3",
        "action": "create_commitment",
        "payload": {"title": "Fix the billing webhook"},
        "signature": "deadbeef",
        "signed_at": "2026-04-20T00:00:00Z",
        "specification_id": "spec-1",
    }
    base.update(overrides)
    return base


def test_agent_signal_happy_path():
    s = AgentSignal(**_valid_signal())
    assert s.agent_id == "support-bot-v3"


def test_agent_signal_rejects_extra_field():
    with pytest.raises(PydanticValidationError):
        AgentSignal(**_valid_signal(extra="not allowed"))


@pytest.mark.parametrize("missing", ["agent_id", "action", "signature", "signed_at"])
def test_agent_signal_requires_fields(missing: str):
    payload = _valid_signal()
    del payload[missing]
    with pytest.raises(PydanticValidationError):
        AgentSignal(**payload)


def test_agent_signal_empty_fields_rejected():
    with pytest.raises(PydanticValidationError):
        AgentSignal(**_valid_signal(agent_id=""))
    with pytest.raises(PydanticValidationError):
        AgentSignal(**_valid_signal(action=""))
    with pytest.raises(PydanticValidationError):
        AgentSignal(**_valid_signal(signature=""))


def test_attestation_result_rejects_unknown_field():
    with pytest.raises(PydanticValidationError):
        AttestationResult(
            attested=True, agent_id="a", action="b",
            trust_tier="attested_agent", extra="nope",
        )


async def test_attest_structurally_valid_signal():
    client = NexusClient()
    result = await client.attest(_valid_signal())
    assert result.attested is True
    assert result.trust_tier == "attested_agent"
    assert result.agent_id == "support-bot-v3"
    assert result.action == "create_commitment"
    assert result.reason is None


async def test_attest_accepts_agent_signal_object():
    client = NexusClient()
    s = AgentSignal(**_valid_signal())
    result = await client.attest(s)
    assert result.attested is True


async def test_attest_rejects_invalid_shape():
    client = NexusClient()
    with pytest.raises(NexusError):
        await client.attest({"malformed": True})


async def test_attest_rejects_whitespace_signature():
    client = NexusClient()
    # Valid shape but signature is whitespace only.
    with pytest.raises(NexusError):
        await client.attest(_valid_signal(signature="   "))


async def test_reject_returns_negative_attestation():
    client = NexusClient()
    result = await client.reject(_valid_signal(), reason="policy_denied:budget")
    assert result.attested is False
    assert result.trust_tier == "inferential"
    assert result.reason == "policy_denied:budget"


async def test_reject_agent_signal_object():
    client = NexusClient()
    s = AgentSignal(**_valid_signal())
    r = await client.reject(s, reason="scope_exceeded")
    assert r.attested is False


def test_config_from_env_defaults(monkeypatch):
    monkeypatch.delenv("NEXUS_URL", raising=False)
    monkeypatch.delenv("NEXUS_API_KEY", raising=False)
    monkeypatch.delenv("NEXUS_TIMEOUT_S", raising=False)
    cfg = NexusConfig.from_env()
    assert cfg.base_url == "http://localhost:8090"
    assert cfg.api_key == ""
    assert cfg.timeout_s == 5.0


def test_config_from_env_overrides(monkeypatch):
    monkeypatch.setenv("NEXUS_URL", "http://nexus.prod")
    monkeypatch.setenv("NEXUS_API_KEY", "sk_live")
    monkeypatch.setenv("NEXUS_TIMEOUT_S", "12")
    cfg = NexusConfig.from_env()
    assert cfg.base_url == "http://nexus.prod"
    assert cfg.api_key == "sk_live"
    assert cfg.timeout_s == 12.0


async def test_attest_is_stateless():
    """Two calls on the same client return independent results."""
    client = NexusClient()
    a = await client.attest(_valid_signal(agent_id="a1"))
    b = await client.attest(_valid_signal(agent_id="b2", action="close_commitment"))
    assert a.agent_id == "a1"
    assert b.agent_id == "b2"
    assert b.action == "close_commitment"


async def test_attest_default_empty_payload_allowed():
    client = NexusClient()
    payload = _valid_signal()
    del payload["payload"]
    result = await client.attest(payload)
    assert result.attested is True


async def test_specification_id_optional():
    client = NexusClient()
    s = _valid_signal()
    del s["specification_id"]
    result = await client.attest(s)
    assert result.attested is True


async def test_many_concurrent_attestations_independent():
    import asyncio
    client = NexusClient()
    results = await asyncio.gather(*[
        client.attest(_valid_signal(agent_id=f"a-{i}"))
        for i in range(25)
    ])
    assert all(r.attested for r in results)
    assert {r.agent_id for r in results} == {f"a-{i}" for i in range(25)}
