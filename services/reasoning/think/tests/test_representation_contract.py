from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import ClaimOp, RawDiff
from services.reasoning.think.representation_contract import (
    contextual_frames_compatible,
    enrich_raw_diff_representation,
)


def _obs(
    *,
    oid=None,
    source_channel="github:webhook",
    text="",
    actor_id=None,
    content=None,
    entities_mentioned=None,
    occurred_at=None,
):
    return SimpleNamespace(
        id=oid or uuid4(),
        source_channel=source_channel,
        source_actor_ref="source:actor",
        actor_id=actor_id,
        content_text=text,
        content=content or {},
        entities_mentioned=entities_mentioned or [],
        occurred_at=occurred_at or datetime(2026, 6, 17, tzinfo=timezone.utc),
    )


def test_representation_enrichment_binds_repeated_wording_to_context() -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    obs = _obs(
        text="Actor A raised PR #123 for ENG-42 in alpenlabs/strata-bridge",
        actor_id=actor_id,
        content={"thread_id": "slack-thread-1"},
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs.id,
        scope_actors=[actor_id],
    )
    raw = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": obs.id,
                    "proposition": {
                        "kind": "belief",
                        "subject": "PR #123",
                        "assertion": "A PR was raised.",
                        "claim_role": "fact",
                    },
                    "natural": "Actor A raised PR #123 for ENG-42 in alpenlabs/strata-bridge.",
                    "confidence": 0.74,
                    "scope_actors": [actor_id],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": "2026-06-17T00:00:00+00:00",
                        "valid_until": None,
                    },
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "PR #123 does not exist or is not tied to ENG-42.",
                        "within_window": "P1D",
                    },
                },
            )
        ],
    )

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=[obs]))

    prop = raw.claim_ops[0].entry["proposition"]
    assert "workstream" in prop["coverage_roles"]
    assert "source" in prop["coverage_roles"]
    assert "progress_signal" in prop["retrieval_tags"]
    assert "object_bound" in prop["retrieval_tags"]
    assert "work_item_bound" in prop["retrieval_tags"]
    assert "repo_bound" in prop["retrieval_tags"]
    assert prop["contextual_frame"]["action"] == "raise_pr"
    assert "pr_123" in prop["contextual_frame"]["object_refs"]
    assert "work_item_eng_42" in prop["contextual_frame"]["work_item_refs"]
    assert "repo_alpenlabs_strata_bridge" in prop["contextual_frame"]["repo_refs"]
    assert "progress_signal" in raw.claim_ops[0].entry["domain_tags"]


def test_representation_enrichment_adds_source_bound_default_falsifier() -> None:
    tenant_id = uuid4()
    obs = _obs(
        source_channel="grafana:alert",
        text="Grafana reported API latency above the paging threshold.",
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs.id,
    )
    raw = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": obs.id,
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "subject": "API latency",
                        "assertion": "API latency exceeded the paging threshold.",
                    },
                    "natural": "Grafana reported API latency above the paging threshold.",
                    "confidence": 0.82,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": "2026-06-17T00:00:00+00:00",
                        "valid_until": None,
                    },
                },
            )
        ],
    )

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=[obs]))

    falsifier = raw.claim_ops[0].entry["falsifier"]
    assert falsifier["kind"] == "observation_pattern"
    assert falsifier["within_window"] == "P30D"
    assert "grafana_alert" in falsifier["pattern"]


def test_source_digest_fallback_turns_repetitive_batch_into_pattern_model() -> None:
    tenant_id = uuid4()
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="aws:event",
            text="[aws] iam:CreateAccessKey",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(10)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=observations))

    assert len(raw.claim_ops) == 1
    entry = raw.claim_ops[0].entry
    prop = entry["proposition"]
    assert prop["claim_role"] == "pattern"
    assert "source_digest" in prop["retrieval_tags"]
    assert "contextual_recurrence" in prop["retrieval_tags"]
    assert "source" in prop["coverage_roles"]
    assert "discovered_pattern" in prop["coverage_roles"]
    assert "source_observability" in entry["domain_tags"]
    assert "source_digest synthesized" in raw.reasoning_trace


def test_repetitive_batch_gets_pattern_digest_even_with_ordinary_claim() -> None:
    tenant_id = uuid4()
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="aws:event",
            text="[aws] lambda error code 503",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(10)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    raw = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": observations[0].id,
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "assertion": "A lambda error was observed.",
                    },
                    "natural": "A lambda error was observed.",
                    "confidence": 0.7,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": "2026-06-17T00:00:00+00:00",
                        "valid_until": None,
                    },
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "No lambda error appears in the source event.",
                        "within_window": "P1D",
                    },
                },
            )
        ],
    )

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=observations))

    assert len(raw.claim_ops) == 2
    pattern_props = [
        op.entry["proposition"]
        for op in raw.claim_ops
        if op.entry["proposition"].get("claim_role") == "pattern"
    ]
    assert len(pattern_props) == 1
    assert "source_digest" in pattern_props[0]["retrieval_tags"]
    assert "discovered_pattern" in pattern_props[0]["coverage_roles"]


def test_existing_recurrence_claim_only_suppresses_its_own_source() -> None:
    tenant_id = uuid4()
    aws_rows = [
        _obs(
            oid=uuid4(),
            source_channel="aws:event",
            text=f"[aws] lambda event {i}",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(8)
    ]
    github_rows = [
        _obs(
            oid=uuid4(),
            source_channel="github:webhook",
            text=f"github pull request event {i}",
            occurred_at=datetime(2026, 6, 17, 1, i, tzinfo=timezone.utc),
        )
        for i in range(8)
    ]
    observations = [*aws_rows, *github_rows]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    raw = RawDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": aws_rows[0].id,
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "pattern",
                        "abstraction_level": "pattern",
                        "signature": "aws:event recurring source pattern",
                        "observed_tendency": "The aws:event source is recurring.",
                        "domain_tags": ["source_digest"],
                    },
                    "natural": "The aws:event source is showing a recurring pattern.",
                    "confidence": 0.7,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": "2026-06-17T00:00:00+00:00",
                        "valid_until": None,
                    },
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "aws:event no longer recurs",
                        "within_window": "P7D",
                    },
                },
            )
        ],
    )

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=observations))

    naturals = [op.entry["natural"] for op in raw.claim_ops]
    assert len(raw.claim_ops) == 2
    assert any("aws:event source" in natural for natural in naturals)
    assert any("github:webhook source" in natural for natural in naturals)
    assert sum("source_digest" in op.entry["domain_tags"] for op in raw.claim_ops) == 2


def test_batch_fragments_fill_source_digest_when_bundle_is_pruned() -> None:
    tenant_id = uuid4()
    aws_rows = [
        _obs(
            oid=uuid4(),
            source_channel="aws:event",
            text=f"[aws] lambda event {i}",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(8)
    ]
    github_rows = [
        _obs(
            oid=uuid4(),
            source_channel="github:webhook",
            text=f"github pull request event {i}",
            occurred_at=datetime(2026, 6, 17, 1, i, tzinfo=timezone.utc),
        )
        for i in range(8)
    ]
    all_rows = [*aws_rows, *github_rows]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=all_rows[0].id,
        observation_ids=[obs.id for obs in all_rows],
        seed_signature={
            "batch": True,
            "signal_type": "event_batch",
            "batch_signal_fragments": [
                {
                    "observation_id": str(obs.id),
                    "occurred_at": obs.occurred_at.isoformat(),
                    "source_channel": obs.source_channel,
                    "kind": "signal",
                    "text": obs.content_text,
                }
                for obs in all_rows
            ],
        },
    )
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=aws_rows))

    naturals = [op.entry["natural"] for op in raw.claim_ops]
    assert len(raw.claim_ops) == 2
    assert any("aws:event source" in natural for natural in naturals)
    assert any("github:webhook source" in natural for natural in naturals)


def test_major_source_window_gets_digest_even_when_text_is_diverse() -> None:
    tenant_id = uuid4()
    texts = [
        "[aws] lambda cold start exceeded baseline",
        "[aws] iam permission boundary changed",
        "[aws] s3 bucket lifecycle policy updated",
        "[aws] cloudwatch alarm moved to ok",
        "[aws] rds backup completed",
        "[aws] vpc route table modified",
        "[aws] kms key rotation enabled",
        "[aws] ecs service deployment steady",
        "[aws] ecr image scan completed",
        "[aws] secrets manager value rotated",
    ]
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="aws:event",
            text=text,
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i, text in enumerate(texts)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=observations))

    assert len(raw.claim_ops) == 1
    prop = raw.claim_ops[0].entry["proposition"]
    assert prop["claim_role"] == "pattern"
    assert prop["repetition_mode"] == "source_cadence"
    assert "source_digest" in prop["retrieval_tags"]
    assert "major_source_window" in prop["retrieval_tags"]
    assert "discovered_pattern" in prop["coverage_roles"]
    assert "source" in prop["coverage_roles"]
    assert "source_observability" in raw.claim_ops[0].entry["domain_tags"]
    assert "compute_activity" in raw.claim_ops[0].entry["domain_tags"]


def test_event_batch_subkind_is_enough_to_enable_digest_fallback() -> None:
    tenant_id = uuid4()
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="github:webhook",
            text=f"github pull request review event {i}",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(8)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
    )
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(raw, trigger, SimpleNamespace(observations=observations))

    assert len(raw.claim_ops) == 1
    prop = raw.claim_ops[0].entry["proposition"]
    assert prop["claim_role"] == "pattern"
    assert "source_digest" in prop["retrieval_tags"]
    assert "repo_activity" in raw.claim_ops[0].entry["domain_tags"]


def test_contextual_frame_conflicts_block_false_duplicate_compression() -> None:
    entry = {
        "natural": "Actor A raised PR #123 for ENG-42.",
        "proposition": {
            "kind": "belief",
            "claim_role": "fact",
            "assertion": "A PR was raised.",
            "contextual_frame": {
                "action": "raise_pr",
                "object_refs": ["pr_123"],
                "work_item_refs": ["work_item_eng_42"],
            },
        },
    }
    row = {
        "natural": "Actor B raised PR #456 for ENG-77.",
        "proposition": {
            "kind": "belief",
            "claim_role": "fact",
            "assertion": "A PR was raised.",
            "contextual_frame": {
                "action": "raise_pr",
                "object_refs": ["pr_456"],
                "work_item_refs": ["work_item_eng_77"],
            },
        },
    }

    compatible, detail = contextual_frames_compatible(entry, row)

    assert compatible is False
    assert detail["incompatible_key"] == "object_refs"


def test_contextual_frame_overlap_allows_true_duplicate_absorption() -> None:
    entry = {
        "proposition": {
            "kind": "belief",
            "claim_role": "fact",
            "assertion": "A PR was raised.",
            "contextual_frame": {
                "action": "raise_pr",
                "object_refs": ["pr_123"],
                "work_item_refs": ["work_item_eng_42"],
            },
        },
    }
    row = {
        "proposition": {
            "kind": "belief",
            "claim_role": "fact",
            "assertion": "A PR was raised.",
            "contextual_frame": {
                "action": "raise_pr",
                "object_refs": ["pr_123"],
                "work_item_refs": ["work_item_eng_42"],
            },
        },
    }

    compatible, detail = contextual_frames_compatible(entry, row)

    assert compatible is True
    assert detail["compared"] is True


def test_inquiry_unknowns_become_durable_curiosity_hypothesis() -> None:
    tenant_id = uuid4()
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="slack:message",
            text=f"Atlas launch blocker discussion {i}",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(12)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    packet = {
        "signal_summary": "Atlas launch has repeated blocker discussion.",
        "important_unknowns": [
            "affected commitment",
            "responsible owner",
            "whether the blocker is on the critical path",
        ],
        "question_path": [
            {
                "question": "Who owns the next action for Atlas launch?",
                "primitive": "OWNERSHIP",
                "score": 0.91,
            },
            {
                "question": "Is the blocker on the critical path?",
                "primitive": "DEPENDENCY",
                "score": 0.88,
            },
        ],
        "answer_obligations": {
            "missing_slots": ["affected goal"],
        },
        "sufficiency_verdict": {
            "remaining_unknowns": ["counterevidence"],
        },
    }
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(
        raw,
        trigger,
        SimpleNamespace(observations=observations, notes={"inquiry_context_packet": packet}),
    )

    curiosity = [
        op.entry["proposition"]
        for op in raw.claim_ops
        if op.entry["proposition"].get("claim_role") == "hypothesis"
    ]
    assert len(curiosity) == 1
    prop = curiosity[0]
    assert "curiosity" in prop["coverage_roles"]
    assert "epistemic" in prop["coverage_roles"]
    assert "open_question" in prop["retrieval_tags"]
    assert "success_driver" in prop["retrieval_tags"]
    assert "executive_question" in prop["retrieval_tags"]
    assert "manager_question" in prop["retrieval_tags"]
    assert "operator_question" in prop["retrieval_tags"]
    assert "question_ownership" in prop["retrieval_tags"]
    assert "unknown_responsible_owner" in prop["retrieval_tags"]
    assert "Who owns the next action" in prop["open_questions"][0]
    assert "curiosity synthesized" in raw.reasoning_trace


def test_curiosity_hypothesis_binds_to_provisional_substrate_candidates() -> None:
    tenant_id = uuid4()
    candidate_actor = uuid4()
    candidate_commitment = uuid4()
    observations = [
        _obs(
            oid=uuid4(),
            source_channel="signal:message",
            text=f"Signal discussion says someone owns Atlas blocker {i}",
            occurred_at=datetime(2026, 6, 17, 0, i, tzinfo=timezone.utc),
        )
        for i in range(12)
    ]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    packet = {
        "signal_summary": "Atlas blocker ownership is unclear.",
        "important_unknowns": ["responsible owner", "affected commitment"],
        "question_path": [
            {
                "question": "Who owns the Atlas blocker?",
                "primitive": "OWNERSHIP",
            }
        ],
    }
    raw = RawDiff(trigger_ref=uuid4(), tenant_id=tenant_id, claim_ops=[])

    enrich_raw_diff_representation(
        raw,
        trigger,
        SimpleNamespace(
            observations=observations,
            notes={
                "inquiry_context_packet": packet,
                "substrate_candidates": [
                    {
                        "id": str(candidate_actor),
                        "kind": "actor",
                        "label": "Signal user U123",
                        "confidence": 0.68,
                        "status": "proposed",
                        "scope_ref": {
                            "type": "candidate_actor",
                            "id": str(candidate_actor),
                        },
                    },
                    {
                        "id": str(candidate_commitment),
                        "kind": "commitment",
                        "label": "Atlas blocker work item",
                        "confidence": 0.74,
                        "status": "proposed",
                        "scope_ref": {
                            "type": "candidate_commitment",
                            "id": str(candidate_commitment),
                        },
                    },
                ],
            },
        ),
    )

    curiosity = [
        op.entry
        for op in raw.claim_ops
        if op.entry["proposition"].get("claim_role") == "hypothesis"
    ][0]
    prop = curiosity["proposition"]
    scope_entities = curiosity["scope_entities"]
    assert {"type": "candidate_actor", "id": str(candidate_actor)} in scope_entities
    assert {
        "type": "candidate_commitment",
        "id": str(candidate_commitment),
    } in scope_entities
    assert "entity" in prop["coverage_roles"]
    assert "candidate_bound_curiosity" in prop["retrieval_tags"]
    assert "candidate_actor_question" in prop["retrieval_tags"]
    assert prop["candidate_bindings"][0]["scope_ref"]["type"] == "candidate_commitment"
