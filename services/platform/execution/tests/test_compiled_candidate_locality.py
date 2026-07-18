"""Provider-blind contract test for workstream-local compiled candidates."""

from __future__ import annotations

from dataclasses import asdict
import json
import random
import re
from uuid import NAMESPACE_URL, UUID, uuid5

from services.platform.execution.context_packet import memory_decision_candidates
from services.platform.execution.types import SufficiencyVerdict
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    build_compiled_batch_memory_decision_request,
)


_WORKSTREAMS = {
    "Atlas Workstream": "certificate ownership blocks release readiness",
    "Beacon Workstream": "access review blocks migration readiness",
    "Cobalt Workstream": "schema approval blocks analytics cutover",
    "Delta Workstream": "security review blocks customer rollout",
}


def _id(label: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"fyralis:candidate-locality:{label}")


def _fragments() -> tuple[list[dict[str, str]], dict[str, set[str]]]:
    fragments: list[dict[str, str]] = []
    expected: dict[str, set[str]] = {}
    for scope, fact in _WORKSTREAMS.items():
        expected[scope] = set()
        for ordinal in range(1, 6):
            observation_id = str(_id(f"{scope}:{ordinal}"))
            expected[scope].add(observation_id)
            fragments.append(
                {
                    "observation_id": observation_id,
                    "source_channel": "slack",
                    "text": f"{scope}, update {ordinal}: {fact}; marker {ordinal}",
                }
            )
    for ordinal in range(1, 6):
        fragments.append(
            {
                "observation_id": str(_id(f"distractor:{ordinal}")),
                "source_channel": "slack",
                "text": (
                    f"Noise marker {ordinal}: generic launch owner review chatter "
                    "is background only"
                ),
            }
        )
    return fragments, expected


def _trigger(
    fragments: list[dict[str, str]],
    *,
    wrapper_text: str,
) -> TriggerContext:
    wrapper_id = _id("delivery-envelope")
    return TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=_id("tenant"),
        observation_id=wrapper_id,
        observation_ids=[UUID(row["observation_id"]) for row in fragments],
        seed_natural_text=wrapper_text,
        seed_signature={
            "batch": True,
            "signal_type": "event_batch",
            "batch_signal_fragments": fragments,
        },
    )


def _compile(trigger: TriggerContext):
    return memory_decision_candidates(
        trigger,
        (),
        [],
        [],
        [],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "four workstream-local groups have repeated evidence",
            20,
            0,
            (),
        ),
    )


def _by_scope(candidates):
    grouped = {}
    for candidate in candidates:
        grouped.setdefault(candidate.semantic_scope[0], []).append(candidate)
    return grouped


def _candidate_blocks(prompt: str) -> list[str]:
    return re.findall(r"<candidate>\n(.*?)\n  </candidate>", prompt, re.DOTALL)


def test_compiled_candidates_and_prompt_remain_workstream_local() -> None:
    fragments, expected = _fragments()
    distractor_ids = {
        row["observation_id"]
        for row in fragments
        if row["text"].startswith("Noise marker")
    }
    wrapper_text = (
        "about=batch transport wrapper combining Atlas Beacon Cobalt Delta; "
        "not a business claim"
    )
    trigger = _trigger(fragments, wrapper_text=wrapper_text)
    candidates = _compile(trigger)
    by_scope = _by_scope(candidates)

    assert set(by_scope) == set(_WORKSTREAMS)
    assert len(candidates) == 20
    member_sets: list[set[str]] = []
    for scope, scoped_candidates in by_scope.items():
        member_ids = {
            observation_id
            for candidate in scoped_candidates
            for observation_id in candidate.member_observation_ids
        }
        member_sets.append(member_ids)
        assert len(scoped_candidates) == 5
        assert all(
            candidate.semantic_scope == (scope,)
            for candidate in scoped_candidates
        )
        assert member_ids == expected[scope]
        assert all(
            candidate.source_observation_ids == candidate.member_observation_ids
            for candidate in scoped_candidates
        )
        assert all(
            len(candidate.observation_evidence) == 1
            for candidate in scoped_candidates
        )
        assert {
            item["observation_id"]
            for candidate in scoped_candidates
            for item in candidate.observation_evidence
        } == expected[scope]
        assert not member_ids & distractor_ids
        assert str(trigger.observation_id) not in member_ids
        rendered = json.dumps(
            [asdict(candidate) for candidate in scoped_candidates],
            sort_keys=True,
        ).casefold()
        assert "about=batch" not in rendered
        assert "transport wrapper" not in rendered
        assert "evidence window" not in rendered
        assert "batch-level" not in rendered

    assert set().union(*member_sets) == set().union(*expected.values())
    assert all(
        left.isdisjoint(right)
        for index, left in enumerate(member_sets)
        for right in member_sets[index + 1 :]
    )

    packet = {
        "signal_summary": "Four independent workstream-local candidate groups.",
        "sufficiency_verdict": {"status": "sufficient_for_reasoning"},
        "memory_decision_candidates": [asdict(candidate) for candidate in candidates],
        "important_unknowns": [],
        "tiers": {},
    }
    request = build_compiled_batch_memory_decision_request(
        trigger,
        ContextBundle(notes={"inquiry_context_packet": packet}),
    )
    assert request is not None
    blocks = _candidate_blocks(request.user)
    assert len(blocks) == 20
    for scope, expected_ids in expected.items():
        scoped_blocks = [part for part in blocks if scope in part]
        assert len(scoped_blocks) == 5
        for observation_id in expected_ids:
            body = next(
                row["text"]
                for row in fragments
                if row["observation_id"] == observation_id
            )
            matching = [part for part in scoped_blocks if observation_id in part]
            assert len(matching) == 1
            assert body in matching[0]
        foreign_ids = set().union(
            *(ids for other_scope, ids in expected.items() if other_scope != scope)
        )
        assert all(
            observation_id not in block
            for block in scoped_blocks
            for observation_id in foreign_ids
        )
        assert all(
            observation_id not in block
            for block in scoped_blocks
            for observation_id in distractor_ids
        )
        assert all(str(trigger.observation_id) not in block for block in scoped_blocks)
        assert all(wrapper_text not in block for block in scoped_blocks)


def test_wrapper_changes_and_member_permutation_do_not_change_locality() -> None:
    fragments, expected = _fragments()
    baseline = _by_scope(
        _compile(_trigger(fragments, wrapper_text="about=batch wrapper version one"))
    )
    shuffled = list(fragments)
    random.Random(20260718).shuffle(shuffled)
    adversarial = _by_scope(
        _compile(
            _trigger(
                shuffled,
                wrapper_text=(
                    "about=batch Atlas Workstream update 1: certificate ownership "
                    "blocks release readiness; wrapper repeats all business names"
                ),
            )
        )
    )

    assert set(baseline) == set(adversarial) == set(_WORKSTREAMS)
    for scope in _WORKSTREAMS:
        baseline_projection = {
            (candidate.candidate_id, candidate.member_observation_ids)
            for candidate in baseline[scope]
        }
        adversarial_projection = {
            (candidate.candidate_id, candidate.member_observation_ids)
            for candidate in adversarial[scope]
        }
        assert {
            observation_id
            for candidate in baseline[scope]
            for observation_id in candidate.member_observation_ids
        } == expected[scope]
        assert baseline_projection == adversarial_projection
