from __future__ import annotations

from uuid import uuid4

import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.errors import InvariantViolation
from services.evaluation.epistemic_repair.p7_bootstrap_clone import (
    BootstrapCassette,
    checkpoint_digest,
    clone_receipt,
)
from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import (
    exact_bootstrap_clone_receipts,
)
from lib.evaluation.epistemic_repair.p7_runner import P7_ARMS


class _Provider(LLMProvider):
    def __init__(self, response: str) -> None:
        super().__init__(LLMConfig(provider="codex", api_key="test", model="gpt-5.4"))
        self.response = response

    async def _raw_call(self, **_: object) -> str:
        return self.response


@pytest.mark.asyncio
async def test_bootstrap_cassette_remaps_identities_and_fails_closed() -> None:
    source_id, target_id = uuid4(), uuid4()
    provider = _Provider(f'{{"evidence_id":"{source_id}"}}')
    cassette = BootstrapCassette()
    request = dict(
        system=f"tenant={source_id}", user="same evidence", temperature=0.0,
        max_tokens=10, schema_hint="{}",
    )
    async with cassette.record(provider):
        assert str(source_id) in await provider._raw_call(**request)
    async with cassette.replay(provider):
        replayed = await provider._raw_call(**{
            **request, "system": f"tenant={target_id}",
        })
        assert str(target_id) in replayed
        assert str(source_id) not in replayed
    with pytest.raises(InvariantViolation, match="diverged"):
        async with cassette.replay(provider):
            await provider._raw_call(**{**request, "user": "different"})


def test_checkpoint_digest_ignores_local_ids_but_not_semantics() -> None:
    def snapshot(model_id: str, proposition: str) -> dict[str, object]:
        return {
            "accepted_models": [{
                "id": model_id, "truth_version_id": str(uuid4()),
                "proposition": {"predicate": proposition},
                "natural_text": proposition, "confidence": 0.8,
                "scope_entities": [], "truth_lifecycle": "active",
                "evidence_observation_ids": [str(uuid4())],
            }],
            "accepted_relations": [],
        }

    first = checkpoint_digest(snapshot(str(uuid4()), "ships"))
    second = checkpoint_digest(snapshot(str(uuid4()), "ships"))
    assert first == second
    assert first != checkpoint_digest(snapshot(str(uuid4()), "blocked"))


def test_clone_receipt_is_immutable_and_truthful() -> None:
    cassette = BootstrapCassette()
    digest = "a" * 64
    receipt = clone_receipt(
        source_tenant_id=str(uuid4()), target_tenant_id=str(uuid4()),
        source_digest=digest, target_digest=digest, cassette=cassette,
    )
    assert receipt.equality_proven
    assert receipt.canonical_checkpoint_digest == digest
    with pytest.raises(Exception):
        receipt.equality_proven = False  # type: ignore[misc]
    with pytest.raises(ValueError, match="digest mismatch"):
        receipt.model_copy(update={"receipt_digest": "b" * 64}).__class__(
            **{
                **receipt.model_dump(mode="json"),
                "receipt_digest": "b" * 64,
            }
        )


def test_oracle_rejects_population_membership_without_equality_receipts() -> None:
    superficial = {
        "population_digest": "present",
        "arm_results": [{"arm": arm} for arm in P7_ARMS],
    }
    assert not exact_bootstrap_clone_receipts(superficial)
    proven = {
        **superficial,
        "arm_results": [{
            "arm": arm,
            "bootstrap_clone_receipt": {
                "canonical_checkpoint_digest": "a" * 64,
                "equality_proven": True,
            },
        } for arm in P7_ARMS],
    }
    assert exact_bootstrap_clone_receipts(proven)
    proven["arm_results"][-1]["bootstrap_clone_receipt"][
        "canonical_checkpoint_digest"
    ] = "b" * 64
    assert not exact_bootstrap_clone_receipts(proven)
