from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
import services.reasoning.think.reason as reason_mod
from services.reasoning.think.diff_schema import ClaimOp, RawDiff, ValidatedDiff
from services.reasoning.think.reason import ThinkRunOutcome
from services.reasoning.think.run_pipeline import _drop_event_batch_wrapper_claims


def test_drop_event_batch_wrapper_claims_drops_batch_subject_insert():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "The batch combines unrelated GitHub activity.",
                    },
                    "natural": "The batch combines unrelated GitHub activity.",
                    "confidence": 0.55,
                },
            ),
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Checkpoint explorer incident response has an active owner.",
                    },
                    "natural": "Checkpoint explorer incident response has an active owner.",
                    "confidence": 0.66,
                },
            ),
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert len(out.claim_ops) == 1
    assert out.claim_ops[0].entry["natural"].startswith("Checkpoint explorer")
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"


@pytest.mark.asyncio
@pytest.mark.skip(reason="P1 removed the benchmark-aligned noise fast path")
async def test_noise_only_run_once_skips_retrieval_and_llm(monkeypatch):
    tenant_id = uuid7()
    trigger_id = uuid7()
    obs_id = uuid7()
    content_text = (
        "General operational chatter: lunch logistics, duplicated dashboard "
        "links, and a non-actionable reminder. This should not dominate memory."
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        subkind="event_batch",
        observation_id=obs_id,
        observation_ids=[obs_id],
        seed_natural_text=f"Evidence window containing 1 source signal:\n- {content_text}",
        seed_signature={
            "trigger_id": str(trigger_id),
            "source_channels": ["slack:storyline-noise"],
            "batch_signal_fragments": [{"text": content_text}],
        },
    )
    record = reason_mod.ThinkRunRecord(
        id=uuid7(),
        tenant_id=tenant_id,
        trigger_id=trigger_id,
        trigger_kind="T1:event_batch",
    )

    class FakeTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return False

    class FakeConn:
        def is_in_transaction(self):
            return False

        def transaction(self):
            return FakeTransaction()

    async def fail_prepare_reasoning_run_state(**_kwargs):
        raise AssertionError("noise-only fast path should skip retrieval")

    async def fail_build_raw_reasoning_output(**_kwargs):
        raise AssertionError("noise-only fast path should skip llm reasoning")

    updates: list[dict] = []
    inserted_runs: list[object] = []
    validated_seen: list[ValidatedDiff] = []

    async def fake_insert_think_run(_conn, inserted_record, **_kwargs):
        inserted_runs.append(inserted_record)

    async def fake_update_think_run(_conn, _run_id, **kwargs):
        updates.append(kwargs)

    async def fake_acquire_region_lock(_conn, tenant, entity_ids):
        assert tenant == tenant_id
        assert entity_ids == []
        return SimpleNamespace(
            tenant_hash=1,
            entity_hash=2,
            entity_ids=[],
            wait_duration_ms=0,
        )

    async def fake_debug_capture(*_args, **_kwargs):
        return None

    async def fake_validate_raw_reasoning_output(**kwargs):
        raw = kwargs["raw"]
        assert raw.llm_latency_ms == 0
        assert raw.raw_diff.claim_ops == []
        assert raw.raw_diff.edge_ops == []
        assert "discard_as_noise" in (raw.raw_diff.reasoning_trace or "")
        assert kwargs["bundle"].models == []
        assert kwargs["retrieval_result"].models == []
        validated = ValidatedDiff(
            trigger_ref=raw.raw_diff.trigger_ref,
            tenant_id=raw.raw_diff.tenant_id,
            reasoning_trace=raw.raw_diff.reasoning_trace,
        )
        validated_seen.append(validated)
        return validated, {"context_use_grade": "no_selected_context"}

    async def fake_apply_validated_diff(**kwargs):
        assert kwargs["llm_latency_ms"] == 0
        assert kwargs["validated"] is validated_seen[0]
        return {
            "applied_model_ids": [],
            "state_changes_emitted": 0,
            "negative_memory_inserts": 1,
        }, None

    async def fake_record_representation_audit(**_kwargs):
        return None

    async def fake_record_apply_observability(**_kwargs):
        return None

    async def fake_publish_anomalies_and_enqueue_post_commit(**_kwargs):
        return []

    async def fake_finalize_successful_run(**kwargs):
        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=trigger_id,
            trigger_kind=record.trigger_kind,
            status="success",
            llm_latency_ms=kwargs["llm_latency_ms"],
            ops_applied_count=0,
        )

    monkeypatch.setattr(
        reason_mod,
        "prepare_reasoning_run_state",
        fail_prepare_reasoning_run_state,
    )
    monkeypatch.setattr(
        reason_mod,
        "build_raw_reasoning_output",
        fail_build_raw_reasoning_output,
    )
    monkeypatch.setattr(reason_mod, "insert_think_run", fake_insert_think_run)
    monkeypatch.setattr(reason_mod, "update_think_run", fake_update_think_run)
    monkeypatch.setattr(reason_mod, "acquire_region_lock", fake_acquire_region_lock)
    monkeypatch.setattr(reason_mod, "debug_capture", fake_debug_capture)
    monkeypatch.setattr(
        reason_mod,
        "validate_raw_reasoning_output",
        fake_validate_raw_reasoning_output,
    )
    monkeypatch.setattr(reason_mod, "_apply_validated_diff", fake_apply_validated_diff)
    monkeypatch.setattr(
        reason_mod,
        "_record_representation_audit",
        fake_record_representation_audit,
    )
    monkeypatch.setattr(
        reason_mod,
        "_record_apply_observability",
        fake_record_apply_observability,
    )
    monkeypatch.setattr(
        reason_mod,
        "_publish_anomalies_and_enqueue_post_commit",
        fake_publish_anomalies_and_enqueue_post_commit,
    )
    monkeypatch.setattr(
        reason_mod,
        "_finalize_successful_run",
        fake_finalize_successful_run,
    )

    outcome = await reason_mod._run_once(
        conn=FakeConn(),
        trigger=trigger,
        llm_provider=None,
        access_context=None,
        triggering_content=content_text,
        reason_for_trigger="noise batch",
        record=record,
        expanded_region=None,
    )

    assert outcome.status == "success"
    assert outcome.llm_latency_ms == 0
    assert inserted_runs == [record]
    assert {"retrieval_model_count": 0, "retrieval_observation_count": 0} in updates
    assert {"llm_latency_ms": 0} in updates


def test_drop_event_batch_wrapper_claims_drops_batch_level_insert():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Batch-level planner hypothesis H1 with support.",
                    },
                    "natural": "Batch-level planner hypothesis H1 with support.",
                    "confidence": 0.55,
                },
            )
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert out.claim_ops == []
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"


def test_drop_event_batch_wrapper_claims_drops_mid_sentence_batch_wrapper():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "summary": "Checkpoint response is moving, but the batch also preserves ambiguity.",
                    },
                    "natural": "Checkpoint response is moving, but the batch also preserves ambiguity.",
                    "confidence": 0.55,
                },
            )
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert out.claim_ops == []
    assert out.reasoning_trace == "dropped 1 T1:event_batch wrapper claim(s)"


def test_drop_event_batch_wrapper_claims_keeps_control_plane_out_of_models():
    tenant_id = uuid7()
    obs_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=obs_id,
        member_trigger_ids=[uuid7()],
    )
    diff = RawDiff(
        trigger_ref=obs_id,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "capability",
                        "assessment": "Clarification could improve write precision.",
                    },
                    "natural": (
                        "Question-policy learning: clarification could improve "
                        "write precision."
                    ),
                    "domain_tags": [
                        "question_policy",
                        "lifecycle_obligation",
                    ],
                    "confidence": 0.6,
                },
            ),
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "assertion": "Atlas renewal is blocked by legal.",
                    },
                    "natural": "Atlas renewal is blocked by legal.",
                    "domain_tags": ["customer_risk"],
                    "confidence": 0.72,
                },
            ),
        ],
    )

    out = _drop_event_batch_wrapper_claims(diff, trigger)

    assert [op.entry["natural"] for op in out.claim_ops] == [
        "Atlas renewal is blocked by legal."
    ]
