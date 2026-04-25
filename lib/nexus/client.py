"""
lib/nexus/client.py — Nexus attestation client (stub for Phases 0-3).

Per spec §25 and BUILD-PLAN Prompt 0.2 #8: Nexus is the external
policy engine that attests AI agent identity. Phase 4 swaps in the
real Nexus RPC; Phases 0-3 use this stub that validates the signal
has the structural shape an attested agent signal would have.

The stub:
- rejects signals with missing / malformed fields
- returns an AttestationResult where `attested = True` whenever the
  structural check passes
- is structured so swapping in real Nexus is a one-line change
  (`NexusClient` -> `RealNexusClient`) at the call site

Real Nexus integration lives outside this repo. See Rachin's Nexus
project for the protocol. This stub does NOT attempt cryptographic
verification.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from lib.shared.errors import CompanyOSError


class NexusError(CompanyOSError):
    default_code = "nexus_error"


class AgentSignal(BaseModel):
    """
    Structural shape of an AI agent signal. `signature` is the
    cryptographic field that the real Nexus will verify; in the stub,
    we simply check that it is present and non-empty.
    """
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=1)
    action: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    signature: str = Field(min_length=1)
    signed_at: str = Field(min_length=1)      # ISO-8601, validated at the real Nexus
    specification_id: str | None = None


class AttestationResult(BaseModel):
    """Return value of NexusClient.attest."""
    model_config = ConfigDict(extra="forbid")

    attested: bool
    agent_id: str
    action: str
    trust_tier: str                           # maps to observations.trust_tier
    reason: str | None = None


@dataclass(frozen=True)
class NexusConfig:
    base_url: str = "http://localhost:8090"
    api_key: str = ""
    timeout_s: float = 5.0

    @classmethod
    def from_env(cls) -> "NexusConfig":
        return cls(
            base_url=os.environ.get("NEXUS_URL", cls.base_url),
            api_key=os.environ.get("NEXUS_API_KEY", cls.api_key),
            timeout_s=float(os.environ.get("NEXUS_TIMEOUT_S", cls.timeout_s)),
        )


class NexusClient:
    """
    Phase 0-3 stub. Every signal that passes structural validation
    is marked `attested=True`, `trust_tier='attested_agent'`. Missing
    or malformed fields raise NexusError.

    Phase 4: real Nexus via HTTP RPC. Swap this class for
    RealNexusClient at the call site; the signature of `attest`
    stays identical.
    """

    def __init__(self, config: NexusConfig | None = None) -> None:
        self.config = config or NexusConfig.from_env()

    async def attest(self, signal: AgentSignal | dict[str, Any]) -> AttestationResult:
        """
        Validate the signal's structural shape and return an
        AttestationResult. In the stub, every structurally-valid
        signal is attested.
        """
        validated = self._coerce(signal)
        if not validated.signature.strip():
            raise NexusError("signature is empty", agent_id=validated.agent_id)
        # Real Nexus would verify the signature against the
        # agent's registered public key here.
        return AttestationResult(
            attested=True,
            agent_id=validated.agent_id,
            action=validated.action,
            trust_tier="attested_agent",
            reason=None,
        )

    async def reject(self, signal: AgentSignal | dict[str, Any], reason: str) -> AttestationResult:
        """
        Explicit rejection helper — used by tests and by future
        policy hooks that want to record a negative attestation.
        """
        validated = self._coerce(signal)
        return AttestationResult(
            attested=False,
            agent_id=validated.agent_id,
            action=validated.action,
            trust_tier="inferential",
            reason=reason,
        )

    @staticmethod
    def _coerce(signal: AgentSignal | dict[str, Any]) -> AgentSignal:
        if isinstance(signal, AgentSignal):
            return signal
        try:
            return AgentSignal.model_validate(signal)
        except Exception as e:
            raise NexusError(f"invalid agent signal shape: {e}") from e


__all__ = [
    "AgentSignal",
    "AttestationResult",
    "NexusConfig",
    "NexusClient",
    "NexusError",
]
