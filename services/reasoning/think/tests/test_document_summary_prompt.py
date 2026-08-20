"""Phase 1 (Layer 2, Think-mediated) — document-structured-summary evidence.

The summarization worker enriches the post-summary T1 trigger with the structured
document extraction + re-resolved scope (carried on `trigger.seed_signature`).
These tests pin the Think-prompt contract: the evidence block renders for a
document-memory T1, the commitment carries its due date so Think can set
evaluate_at + a deadline falsifier, noise is gated, scope is surfaced, and a
non-document trigger is unchanged.

See docs/plans/document-memory-substrate.md §4.2 and §8.
"""
from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.prompt import build_prompt


_DOC_STRUCTURED = {
    "summary": "Acme weekly sync: SOW pending, SOC2 risk to renewal.",
    "key_points": ["call lasted 45 minutes"],
    "decisions": ["ship billing revamp before Sept 30"],
    "action_items": [
        {"who": "Priya", "what": "send Acme the revised SOW", "due": "2026-06-17"},
        "follow up on pricing",  # back-compat bare string
    ],
    "risks": ["SOC2 slip endangers the Acme renewal"],
}


def _doc_trigger(**extra) -> TriggerContext:
    signature = {
        "source_channel": "fireflies",
        "trust_tier": "verified",
        "doc_structured_summary": _DOC_STRUCTURED,
        **extra,
    }
    return TriggerContext(
        kind="T1",
        subkind="event_arrival",
        tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_signature=signature,
    )


def test_document_summary_block_renders_for_doc_trigger():
    prompt = build_prompt(_doc_trigger(), ContextBundle(), triggering_content="x")
    user = prompt.user
    assert "<document_structured_summary>" in user
    assert "</document_structured_summary>" in user
    # The sharp scope-bearing fields are present.
    assert "ship billing revamp before Sept 30" in user
    assert "send Acme the revised SOW" in user
    assert "SOC2 slip endangers the Acme renewal" in user


def test_commitment_carries_owner_and_due_for_deadline_minting():
    # The commitment must surface owner + due so Think can set
    # evaluate_at = due and a prediction_deadline falsifier (§4.2).
    user = build_prompt(_doc_trigger(), ContextBundle(), triggering_content="x").user
    assert "owner: Priya" in user
    assert "due: 2026-06-17" in user


def test_bare_string_action_item_still_renders():
    user = build_prompt(_doc_trigger(), ContextBundle(), triggering_content="x").user
    assert "follow up on pricing" in user


def test_key_points_are_noise_gated_out_of_block():
    # key_points are recap noise; they must NOT appear under the commitments /
    # risks / decisions of the evidence block (§8 noise control). They are not
    # rendered as a structured sub-list (only decisions/commitments/risks +
    # key_points-as-label are), but the prose key_point must not masquerade as a
    # decision/commitment/risk item.
    user = build_prompt(_doc_trigger(), ContextBundle(), triggering_content="x").user
    block = user.split("<document_structured_summary>", 1)[1].split(
        "</document_structured_summary>", 1
    )[0]
    # key_points renders under its own label, never under decisions/risks.
    decisions_seg = block.split("<decisions>", 1)[1].split("</decisions>", 1)[0]
    assert "call lasted 45 minutes" not in decisions_seg


def test_resolved_scope_is_surfaced_actors_uuids_only():
    actor_uuid = str(uuid7())
    cust_id = str(uuid7())
    trigger = _doc_trigger(
        doc_scope_entities=[{"type": "customer", "id": cust_id}],
        doc_scope_actors=[actor_uuid],
        doc_unresolved_actor_refs=["Priya"],
    )
    user = build_prompt(trigger, ContextBundle(), triggering_content="x").user
    assert "resolved_scope_entities" in user
    assert cust_id in user
    assert "resolved_scope_actors" in user
    assert actor_uuid in user
    # Unresolved owner names are explicitly labeled as text-only, NOT scope.
    assert "unresolved_owner_names" in user
    assert "do NOT put in scope_actors" in user


def test_contract_instructions_present_in_system_prompt():
    prompt = build_prompt(_doc_trigger(), ContextBundle(), triggering_content="x")
    system = prompt.system
    assert "Document structured summaries:" in system
    # The claim_role mapping + deadline contract is spelled out.
    assert 'claim_role="situation"' in system
    assert "evaluate_at = the due date" in system
    assert "prediction_deadline" in system
    assert 'polarity="negative"' in system


def test_non_document_trigger_renders_no_block():
    trigger = TriggerContext(
        kind="T1",
        subkind="event_arrival",
        tenant_id=uuid7(),
        observation_id=uuid7(),
        seed_signature={"source_channel": "slack", "trust_tier": "verified"},
    )
    user = build_prompt(trigger, ContextBundle(), triggering_content="x").user
    assert "<document_structured_summary>" not in user


def test_empty_structured_summary_renders_no_block():
    trigger = _doc_trigger(doc_structured_summary={})
    # An empty dict overwrites the default; no block should appear.
    trigger.seed_signature["doc_structured_summary"] = {}
    user = build_prompt(trigger, ContextBundle(), triggering_content="x").user
    assert "<document_structured_summary>" not in user
