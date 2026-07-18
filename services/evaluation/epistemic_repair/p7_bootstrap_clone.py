"""Deterministic provider replay and equality receipts for P7 bootstrap clones."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import re
from typing import Any, AsyncIterator

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.llm.provider import LLMProvider, _CURRENT_USAGE_AGG
from lib.shared.errors import InvariantViolation


_UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)


@dataclass(frozen=True, slots=True)
class BootstrapCall:
    normalized_request_digest: str
    source_uuids: tuple[str, ...]
    raw_response: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int


class P7BootstrapCloneReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_tenant_id: str
    target_tenant_id: str
    through_batch: int = Field(default=3, frozen=True)
    canonical_checkpoint_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_checkpoint_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    replay_call_count: int = Field(ge=0)
    replay_transcript_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    equality_proven: bool
    receipt_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_receipt(self) -> "P7BootstrapCloneReceipt":
        body = self.model_dump(mode="json", exclude={"receipt_digest"})
        if self.receipt_digest != canonical_sha256(body):
            raise ValueError("bootstrap clone receipt digest mismatch")
        if self.equality_proven != (
            self.source_checkpoint_digest == self.canonical_checkpoint_digest
        ):
            raise ValueError("bootstrap clone equality claim contradicts its digests")
        return self


def _request_material(kwargs: dict[str, Any]) -> tuple[str, tuple[str, ...]]:
    text = json.dumps(kwargs, sort_keys=True, separators=(",", ":"), default=str)
    uuids: list[str] = []

    def replace(match: re.Match[str]) -> str:
        value = match.group(0).lower()
        try:
            index = uuids.index(value)
        except ValueError:
            uuids.append(value)
            index = len(uuids) - 1
        return f"<uuid:{index}>"

    return _UUID.sub(replace, text), tuple(uuids)


def _remap_response(raw: str, source: tuple[str, ...], target: tuple[str, ...]) -> str:
    mapping = dict(zip(source, target, strict=True))
    return _UUID.sub(lambda match: mapping.get(match.group(0).lower(), match.group(0)), raw)


class BootstrapCassette:
    """Records one real bootstrap and fail-closed replays matching requests."""

    def __init__(self) -> None:
        self.calls: list[BootstrapCall] = []

    @asynccontextmanager
    async def record(self, provider: LLMProvider) -> AsyncIterator[None]:
        original = provider._raw_call

        async def recording(**kwargs: Any) -> str:
            normalized, uuids = _request_material(kwargs)
            aggregator = _CURRENT_USAGE_AGG.get() or provider._usage_aggregator
            offset = len(aggregator.calls) if aggregator is not None else 0
            raw = await original(**kwargs)
            usages = aggregator.calls[offset:] if aggregator is not None else ()
            self.calls.append(BootstrapCall(
                normalized_request_digest=canonical_sha256(normalized),
                source_uuids=uuids,
                raw_response=raw,
                input_tokens=sum(item.input_tokens for item in usages),
                output_tokens=sum(item.output_tokens for item in usages),
                cache_read_tokens=sum(item.cache_read_tokens for item in usages),
                cache_creation_tokens=sum(item.cache_creation_tokens for item in usages),
            ))
            return raw

        provider._raw_call = recording  # type: ignore[method-assign]
        try:
            yield
        finally:
            provider._raw_call = original  # type: ignore[method-assign]

    @asynccontextmanager
    async def replay(self, provider: LLMProvider) -> AsyncIterator[None]:
        original = provider._raw_call
        cursor = 0

        async def replaying(**kwargs: Any) -> str:
            nonlocal cursor
            if cursor >= len(self.calls):
                raise InvariantViolation(
                    "P7_BOOTSTRAP_REPLAY_EXHAUSTED",
                    "bootstrap replay requested more provider calls than the source",
                )
            call = self.calls[cursor]
            cursor += 1
            normalized, target_uuids = _request_material(kwargs)
            if canonical_sha256(normalized) != call.normalized_request_digest:
                raise InvariantViolation(
                    "P7_BOOTSTRAP_REPLAY_REQUEST_MISMATCH",
                    "bootstrap replay request diverged from the source transcript",
                    call_index=cursor,
                )
            if len(target_uuids) != len(call.source_uuids):
                raise InvariantViolation(
                    "P7_BOOTSTRAP_REPLAY_UUID_SHAPE_MISMATCH",
                    "bootstrap replay request has a different identity shape",
                    call_index=cursor,
                )
            provider._record_usage(
                call.input_tokens,
                call.output_tokens,
                cache_read_tokens=call.cache_read_tokens,
                cache_creation_tokens=call.cache_creation_tokens,
                usage_exactness="reported",
            )
            return _remap_response(call.raw_response, call.source_uuids, target_uuids)

        provider._raw_call = replaying  # type: ignore[method-assign]
        try:
            yield
            if cursor != len(self.calls):
                raise InvariantViolation(
                    "P7_BOOTSTRAP_REPLAY_INCOMPLETE",
                    "bootstrap replay consumed fewer provider calls than the source",
                    expected=len(self.calls), observed=cursor,
                )
        finally:
            provider._raw_call = original  # type: ignore[method-assign]

    @property
    def transcript_digest(self) -> str:
        return canonical_sha256([
            {
                "request": call.normalized_request_digest,
                "response": canonical_sha256(call.raw_response),
                "input_tokens": call.input_tokens,
                "output_tokens": call.output_tokens,
            }
            for call in self.calls
        ])


def checkpoint_digest(stage_snapshot: dict[str, Any]) -> str:
    """Digest canonical semantic state while excluding tenant-local identities."""

    models = sorted((
        {
            "proposition": row.get("proposition"),
            "natural_text": row.get("natural_text"),
            "truth_semantic_digest": row.get("truth_semantic_digest"),
            "confidence": row.get("confidence"),
            "scope_entities": row.get("scope_entities"),
            "truth_lifecycle": row.get("truth_lifecycle"),
            "evidence_count": len(row.get("evidence_observation_ids") or ()),
        }
        for row in stage_snapshot.get("accepted_models") or ()
    ), key=canonical_sha256)
    relations = sorted((
        {
            "kind": row.get("truth_relation_kind"),
            "truth_semantic_digest": row.get("truth_semantic_digest"),
            "rationale": row.get("truth_rationale"),
            "participant_roles": sorted(
                item.get("participant_role")
                for item in (row.get("participants") or ())
            ),
            "evidence_count": len(row.get("evidence_ids") or ()),
        }
        for row in stage_snapshot.get("accepted_relations") or ()
    ), key=canonical_sha256)
    return canonical_sha256({"models": models, "relations": relations})


def clone_receipt(
    *, source_tenant_id: str, target_tenant_id: str,
    source_digest: str, target_digest: str, cassette: BootstrapCassette,
) -> P7BootstrapCloneReceipt:
    body = {
        "source_tenant_id": source_tenant_id,
        "target_tenant_id": target_tenant_id,
        "through_batch": 3,
        "canonical_checkpoint_digest": target_digest,
        "source_checkpoint_digest": source_digest,
        "replay_call_count": len(cassette.calls),
        "replay_transcript_digest": cassette.transcript_digest,
        "equality_proven": source_digest == target_digest,
    }
    return P7BootstrapCloneReceipt(**body, receipt_digest=canonical_sha256(body))


__all__ = [
    "BootstrapCassette", "P7BootstrapCloneReceipt", "checkpoint_digest",
    "clone_receipt",
]
