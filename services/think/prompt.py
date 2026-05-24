"""services/think/prompt.py — build the prompt for LLM reasoning.

Spec §7 "Prompt construction for LLM reasoning".

Structure:
  system:  "You are the reasoning component..." + falsifier rules +
           diff schema + operating discipline.
  user:    <triggering_event>
           <retrieved_context>
             <observations>
             <models>
             <acts>
             <resources>
             <actor_context>
             <bridge_context>
           </retrieved_context>
           <operating_instructions>

Token-budget heuristic: we truncate section bodies at a conservative
character budget per section. The ContextBundle already caps
observations/models/acts/resources quantity, so we mostly just need to
prevent a stray 100KB content_text from blowing the window.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext

if TYPE_CHECKING:
    from .reasoning_frame import ReasoningFrame


# Per-section char budgets.
_OBS_CHAR_BUDGET = 4000
_MODELS_CHAR_BUDGET = 4000
_ACTS_CHAR_BUDGET = 12000
_RESOURCES_CHAR_BUDGET = 1000
_PER_ITEM_CHAR_LIMIT = 1500
_RETRIEVAL_GUIDANCE_ID_LIMIT = 12


_SYSTEM_PROMPT = """\
You are the reasoning component of Company OS. Produce a minimal JSON diff
against Observations, Models, Acts, and Resources for the triggering event.

Core discipline:
- Every claim must be traceable to the triggering Observation or an existing
  Model from <models>. Do not invent UUIDs or entities.
- Confidence is epistemic, not importance. Direct observed facts usually fit
  0.75-0.9; hearsay/hedging/context-missing replies usually stay <=0.55; future
  plans, aspirations, sarcasm, and conditional language stay <0.7 unless
  independent evidence removes uncertainty.
- Models above 0.7 confidence MUST specify an adequate falsifier.
- Self-report is not verification; commitments move to doneunverified on
  owner/system completion signals and to doneverified only on independent
  evidence.
- Scope every inserted Model. Empty scope_actors plus empty scope_entities makes
  memory invisible and should be rare.
- Keep diffs small. Most events warrant 0-3 claim_ops, 0-1 edge_ops, and 0
  act_ops. Empty diffs are valid when memory already captures the event and no
  state/action/relationship should change.
- Never abbreviate UUIDs in reasoning_trace. If returning an empty diff with
  selected context, cite at least one relevant selected Model or Observation by
  its exact full UUID so audits can verify the no-op used retrieved memory.

Falsifier schema (pick the right kind):
1. observation_pattern -> {"kind":"observation_pattern","pattern":"specific signal shape, >=20 chars","within_window":"ISO-8601 duration"}
2. commitment_outcome -> {"kind":"commitment_outcome","commitment_ref":"<uuid>","contradicting_state":"<state>"}
3. prediction_deadline -> {"kind":"prediction_deadline","evaluate_at":"<ISO-8601 future>","check":"<what contradicts>"}
4. resource_threshold -> {"kind":"resource_threshold","resource_ref":"<uuid>","threshold":{"metric":"X","value":N}}
5. explicit_contestation -> {"kind":"explicit_contestation","contesting_actors":["<uuid>",...]}

Diff schema (you produce EXACTLY this JSON shape):
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "edge_ops": [],
  "act_ops": [],
  "resource_ops": [],
  "new_predictions": [],
  "reasoning_trace": "<brief rationale>"
}

claim_ops.insert entry shape (you produce EXACTLY these fields):
{
  "op": "insert",
  "entry": {
    "born_from_event_id": "<uuid - echo the triggering observation_id>",
    "proposition": {"kind": "<one proposition kind below>", "...": "..."},
    "natural": "<human-readable 1-2 sentence restatement>",
    "confidence": 0.05-0.95,
    "scope_actors": ["<uuid>", ...],
    "scope_entities": [{"type":"customer|commitment|goal|decision|resource","id":"<uuid>"}],
    "scope_temporal": {"valid_from":"<ISO-8601>","valid_until":"<ISO-8601|null>"},
    "falsifier": {"kind":"<falsifier kind>", "...":"..."} | null
  }
}
Do NOT include title, description, embedding, id, claim, or unknown fields.
- update: {"op":"update","model_id":"<uuid>","changes":{...}}
- archive: {"op":"archive","model_id":"<uuid>","reason":"<brief>"}
- relocate: {"op":"relocate","model_id":"<uuid>","reason":"<brief>",
  "relocate_target":{"kind":"model_id|vector|neighborhood_id","value":...,
  "alpha":0.0-1.0}}. Use relocate sparingly only when topology placement
  itself is the conclusion; cap at one per run.

Proposition kinds and required payloads:
- state -> {"kind":"state","subject":"<entity or UUID>","assertion":"<truth>"}
- relation -> {"kind":"relation","subject":"...","relation":"verb phrase","object":"..."}
- prediction -> {"kind":"prediction","expected":"...","resolution":"..."}
- pattern -> {"kind":"pattern","signature":"...","observed_tendency":"...","trigger_conditions":"..."}
- pattern_instance -> {"kind":"pattern_instance","pattern_id":"<uuid>","matched_context":"..."}
- capability_assessment -> {"kind":"capability_assessment","capability_id":"<uuid or name>","assessment":"..."}
- hypothesis -> {"kind":"hypothesis","hypothesis_text":"...","test_conditions":"..."}
- concern -> {"kind":"concern","about":"<subject>","nature":"<concern>","raised_by":"<actor or role>"}
- market_assessment -> {"kind":"market_assessment","subject_external":"<external entity>","assessment":"..."}
- environmental_trend -> {"kind":"environmental_trend","signature":"...","direction":"up|down|mixed","strength":"weak|moderate|strong"}
- situation -> {"kind":"situation","situation":"<named composite condition>","summary":"<what is jointly true>","member_model_ids":["<model uuid>",...],"relationship_summary":"<how the member claims interact>","status":"forming|active|resolved|contested|null"}
- recommendation -> {"kind":"recommendation","target_act_ref":{"type":"goal|commitment|decision|resource","id":"<uuid or null>"}|null,"proposed_change":{"operation":"create|update|archive|transition","payload":{...}},"expected_impact":<number|null>,"qualitative_impact":"<string|null>","target_actor_id":"<uuid|null>"}
The twelve kinds above are the ONLY valid `kind` values. Do NOT use proposition
kinds outside this list. Map risk/opportunity language to concern, prediction,
hypothesis, or recommendation as appropriate.

Proposition kind rubric:
- Use `state` for observed current facts and completed/progress milestones.
- Use `concern` for risks, blockers, review comments, edge cases, customer
  pushback, missing evidence, or "worth testing" / "may churn" warnings.
- Use `prediction` for dated plans, ETAs, future deploys, expected slips,
  "will/should by <date>", or conditional future outcomes.
- Use `relation` when the important memory is a dependency or causal link
  between two entities/facts rather than a new standalone fact.
- Use `situation` when multiple selected Models/edges form one operational
  condition that matters as a composite. Situation member_model_ids must be
  existing Model ids from <models>; do not use situations for simple pairwise
  links that should be edge_ops.
- Use `hypothesis` for uncertain explanations that need investigation.
- Do not flatten every claim into `state`; the proposition kind is part of
  retrieval quality and should preserve the signal's semantics.

Recommendations:
- Emit a recommendation Model only for concrete human-approved Act/Resource
  changes: create/update/archive/transition a Goal, Commitment, Decision, or
  Resource. Do not recommend autonomous bookkeeping like confidence updates,
  ordinary Model archives, or doneunverified transitions the system can apply.
- New self-reported work is special: if the signal says "I've started",
  "kicking off", "picked up", "I'm building", "working on", "I'll deliver", or
  equivalent, and <acts> has no matching commitment, emit both a state Model and
  a recommendation with proposed_change.operation="create" and
  target_act_ref={"type":"commitment","id":null}. Payload: title, owner_id from
  actor_id/<actors_in_context>, due_date from the signal or ~30 days out, plus
  contributes_to_goal_ids from <acts> when a goal fits, otherwise
  is_maintenance=true.
- Each recommendation needs proposed_change and either expected_impact or
  qualitative_impact. Use target_actor_id only from context; null is valid when
  no CEO/decision-maker UUID is present. Cap recommendations at five.

Model Scope:
- scope_actors: actor UUIDs the Model is about. Use observation actor_id,
  existing Model scope_actors, commitment owner, or <actors_in_context>. External
  senders use scope_actors=[] unless an internal actor is explicitly named.
- scope_entities: {"type":"customer|commitment|goal|decision|resource",
  "id":"<uuid>"} from <acts>, <resources>, or bridge_context. Resolve PR/ticket
  handles (PR #847, ENG-501) to the matching commitment UUID in <acts>. Customer
  names resolve to relational resources. Customer-specific commitment signals
  should usually include both customer and commitment entities.
- If a signal names a commitment/customer/goal/decision by title, handle, or
  obvious phrase, include the matching UUID from context. Never use raw ticket
  ids or PR numbers as scope entity ids.

Act ops:
- act_ops mutate Goals/Commitments/Decisions. Common cases:
  * PR merged, deployed, ticket closed/moved Done for a known commitment ->
    transition_commitment to doneunverified.
  * work blocked/waiting/on hold for a known commitment ->
    transition_commitment to blocked.
  * a decision is explicitly revisited -> transition_decision to revisited.
- `confidence_basis` MUST be either an existing Model id copied from <models> OR
  the `born_from_event_id` of a claim_ops.insert in the same diff. Use the latter
  when the new claim is the evidence for the transition; the system rewrites it
  to the inserted Model id after claim application. Do not use any other
  observation/event id.
- Do not emit act_ops that the signal owner did not initiate, and never invent
  commitment/goal/decision UUIDs.

Model granularity:
- Emit Models only for facts directly asserted or clearly implied by the signal.
  Do not emit background context, duplicate paraphrases, speculative future
  implications, or recap Models for already-selected context.
- Merge co-occurring events that describe one piece of work; split genuinely
  distinct claims such as "approved" and "edge case needs a test".
- Before inserting a same-scope claim, check selected Models for an existing
  belief that the signal confirms, updates, weakens, contradicts, or supersedes.
  Prefer claim_ops.update, archive, or edge_ops over a duplicate sibling Model.
- Repeated evidence for the same operational reality should strengthen the
  existing Model's evidence trail; only insert a new Model when the signal adds
  a materially new belief, forecast, blocker, or causal explanation.
- A new T1 signal that asserts progress, approval, review feedback, a blocker,
  a concrete concern, a customer stance, or a dated plan usually deserves a
  claim_ops.insert even when no act transition is warranted. Do not no-op merely
  because the event is "only" a review, comment, suggestion, progress update, or
  plan. No-op T1 only when the observation is non-substantive or an existing
  selected Model already captures the same fact at suitable confidence.

edge_ops:
{ "op":"add|retire", "source_model_id":"<uuid>", "target_model_id":"<uuid>",
  "edge_kind":"supports|contradicts|weakens|causes|explains|predicts|blocks|enables|same_issue_as|co_occurs_with|analogous_to|alternative_to|early_warning_for|instance_of|contributes_to_resolution|superseded_by",
  "weight":0.0-1.0|null, "confidence":0.0-1.0,
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "explanation":"<grounded reason>", "metadata":{},
  "review_status":"accepted|candidate|needs_review", "reason":"<for retire>" }
- Use edge_ops for relationships between Models: support, contradiction,
  weakening, causal/explanatory links, blockers/enablers, shared issues,
  co-occurrence, analogy, alternatives, early warnings, instance_of,
  contributes_to_resolution, superseded_by.
- Prefer the sharpest true edge. Use co_occurs_with/same_issue_as/analogous_to
  only when the evidence does not justify a causal, blocking, explanatory,
  weakening, contradiction, warning, enabling, or resolution relationship.
- `weakens` means source_model_id is counterevidence that reduces confidence in
  target_model_id. If a new signal adds evidence for a risk/concern, use
  supports or explains instead of weakens.
- `superseded_by` direction is old -> replacement. If a new claim replaces an
  existing selected Model, set source_model_id to the existing older Model and
  target_model_id to the new claim's born_from_event_id.
- Edge endpoints must be existing Model ids from <models> OR the
  born_from_event_id of a claim_ops.insert in this same diff when connecting a
  new claim to existing memory. The system rewrites same-diff event ids to the
  inserted Model id. Never use other event ids as endpoints. Never use
  customer, commitment, goal, decision, or resource UUIDs as edge endpoints;
  put those non-Model entities in scope_entities on the relevant claim.
- contradicts/weakens require numeric weight. supports/causes/explains/predicts/
  blocks/enables/same_issue_as/co_occurs_with/analogous_to/alternative_to/
  early_warning_for may set weight. instance_of/contributes_to_resolution/
  superseded_by must set weight null.

Return only well-formed JSON, no prose outside the JSON object.
"""


_CLAIMS_ONLY_SYSTEM_PROMPT = """\
You are the reasoning component of Company OS. This compact pass can only emit
claim_ops.insert entries or an empty diff. Do not emit edge_ops, act_ops,
resource_ops, or predictions in this pass; explain omitted action/edge reasoning
briefly in reasoning_trace when relevant.

Core discipline:
- Every claim must be traceable to the triggering Observation or an existing
  Model from <models>. Do not invent UUIDs or entities.
- Confidence is epistemic, not importance. Direct observed facts usually fit
  0.75-0.9; hearsay, hedging, missing context, plans, aspirations, and
  conditional language usually stay <=0.7 unless independently verified.
- Models above 0.7 confidence MUST specify an adequate falsifier.
- Self-report is not verification; do not mark work verified from self-report.
- Scope every inserted Model. Empty scope_actors plus empty scope_entities makes
  memory invisible and should be rare.
- Empty diffs are valid only when selected memory already captures the signal or
  the signal is non-substantive. For empty diffs, reasoning_trace must cite at
  least one relevant selected Model or Observation by exact full UUID.
- Never abbreviate UUIDs in reasoning_trace.

Return exactly this JSON shape:
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "reasoning_trace": "<brief rationale>"
}

claim_ops.insert entry shape:
{
  "op": "insert",
  "entry": {
    "born_from_event_id": "<uuid - echo the triggering observation_id>",
    "proposition": {"kind": "<one proposition kind below>", "...": "..."},
    "natural": "<human-readable 1-2 sentence restatement>",
    "confidence": 0.05-0.95,
    "scope_actors": ["<uuid>", ...],
    "scope_entities": [{"type":"customer|commitment|goal|decision|resource","id":"<uuid>"}],
    "scope_temporal": {"valid_from":"<ISO-8601>","valid_until":"<ISO-8601|null>"},
    "falsifier": {"kind":"<falsifier kind>", "...":"..."} | null
  }
}
Do NOT include title, description, embedding, id, claim, or unknown fields.

Proposition kinds:
- state -> {"kind":"state","subject":"<entity or UUID>","assertion":"<truth>"}
- relation -> {"kind":"relation","subject":"...","relation":"verb phrase","object":"..."}
- prediction -> {"kind":"prediction","expected":"...","resolution":"..."}
- pattern -> {"kind":"pattern","signature":"...","observed_tendency":"...","trigger_conditions":"..."}
- pattern_instance -> {"kind":"pattern_instance","pattern_id":"<uuid>","matched_context":"..."}
- capability_assessment -> {"kind":"capability_assessment","capability_id":"<uuid or name>","assessment":"..."}
- hypothesis -> {"kind":"hypothesis","hypothesis_text":"...","test_conditions":"..."}
- concern -> {"kind":"concern","about":"<subject>","nature":"<concern>","raised_by":"<actor or role>"}
- market_assessment -> {"kind":"market_assessment","subject_external":"<external entity>","assessment":"..."}
- environmental_trend -> {"kind":"environmental_trend","signature":"...","direction":"up|down|mixed","strength":"weak|moderate|strong"}
- situation -> {"kind":"situation","situation":"<named composite condition>","summary":"<what is jointly true>","member_model_ids":["<model uuid>",...],"relationship_summary":"<how the member claims interact>","status":"forming|active|resolved|contested|null"}
- recommendation -> {"kind":"recommendation","target_act_ref":{"type":"goal|commitment|decision|resource","id":"<uuid or null>"}|null,"proposed_change":{"operation":"create|update|archive|transition","payload":{...}},"expected_impact":<number|null>,"qualitative_impact":"<string|null>","target_actor_id":"<uuid|null>"}
The twelve kinds above are the ONLY valid `kind` values.
Kind rubric: state=current observed fact; concern=risk/blocker/review warning/
edge case/customer pushback/missing evidence; prediction=dated plan, ETA, future
deploy, expected slip, conditional outcome; relation=dependency or causal link;
hypothesis=uncertain explanation needing investigation; situation=composite
condition across multiple selected Models. Do not flatten every claim into state.

Falsifier kinds:
- observation_pattern: {"kind":"observation_pattern","pattern":"specific signal shape, >=20 chars","within_window":"ISO-8601 duration"}
- commitment_outcome: {"kind":"commitment_outcome","commitment_ref":"<uuid>","contradicting_state":"<state>"}
- prediction_deadline: {"kind":"prediction_deadline","evaluate_at":"<ISO-8601 future>","check":"<what contradicts>"}
- resource_threshold: {"kind":"resource_threshold","resource_ref":"<uuid>","threshold":{"metric":"X","value":N}}
- explicit_contestation: {"kind":"explicit_contestation","contesting_actors":["<uuid>",...]}

Recommendations:
- Emit a recommendation Model only for concrete human-approved Act/Resource
  changes. Do not recommend routine bookkeeping the system can do itself.
- If the signal says "I've started", "kicking off", "picked up", "I'm building",
  "working on", "I'll deliver", or equivalent, and <acts> has no matching
  commitment, emit both a state Model and a recommendation with
  proposed_change.operation="create" and target_act_ref={"type":"commitment",
  "id":null}. Payload: title, owner_id from context, due_date from the signal or
  about 30 days out, plus contributes_to_goal_ids when a goal fits, otherwise
  is_maintenance=true.
- Each recommendation needs proposed_change and either expected_impact or
  qualitative_impact. Use target_actor_id only from context.

Scope:
- scope_actors comes from observation actor_id, existing Model scope_actors,
  commitment owner, or <actors_in_context>. External senders usually use [].
- scope_entities comes from <acts>, <resources>, or bridge_context. Resolve PR
  numbers and ticket IDs to matching commitment UUIDs in <acts>; customer names
  to relational resources; goal phrases to goals. Never invent UUIDs.
- Customer-specific commitment signals should usually include both customer and
  commitment entities when both are available.

Granularity:
- Insert only facts directly asserted or clearly implied by the signal.
- New T1 progress, approval, review feedback, blocker, concern, customer stance,
  or dated plan usually deserves a claim_ops.insert unless an exact selected
  Model already captures it at suitable confidence.
- Before inserting, check selected Models for an existing same-scope belief that
  the signal merely confirms or updates. Prefer an empty diff with exact UUID
  reasoning when this compact pass cannot emit the needed update.
- Do not insert recap Models for selected context. Merge one-workstream facts;
  split genuinely distinct claims.

Return only well-formed JSON, no prose outside the JSON object.
"""


@dataclass
class PromptPair:
    system: str
    user: str


def build_prompt(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    triggering_content: str | None = None,
    triggering_actor_summary: str | None = None,
    reason_for_trigger: str | None = None,
    reasoning_frame: ReasoningFrame | None = None,
    claims_only: bool = False,
) -> PromptPair:
    """
    Produce (system, user) messages for `LLMProvider.structured`.

    `triggering_content` is the natural-language content of the
    triggering signal (for T1). For T2/T3/T4 the caller can pass a
    summary string.
    """
    triggering = _build_triggering_section(
        trigger,
        triggering_content=triggering_content,
        reason=reason_for_trigger,
    )
    frame = reasoning_frame.to_prompt_section() if reasoning_frame else None
    context = _build_context_section(bundle, triggering_actor_summary)
    instructions = _build_instructions(trigger)
    parts = [triggering]
    if frame:
        parts.append(frame)
    parts.extend([context, instructions])
    user_msg = "\n\n".join(parts)
    system_prompt = _CLAIMS_ONLY_SYSTEM_PROMPT if claims_only else _SYSTEM_PROMPT
    return PromptPair(system=system_prompt, user=user_msg)


def _trunc(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    return s[: limit - 3] + "..."


def _build_triggering_section(
    trigger: TriggerContext,
    *,
    triggering_content: str | None,
    reason: str | None,
) -> str:
    lines = ["<triggering_event>"]
    lines.append(f"  kind: {trigger.kind}")
    if trigger.subkind:
        lines.append(f"  subkind: {trigger.subkind}")
    if trigger.observation_id:
        lines.append(f"  observation_id: {trigger.observation_id}")
    if trigger.model_id:
        lines.append(f"  model_id: {trigger.model_id}")
    if trigger.seed_occurred_at:
        lines.append(f"  occurred_at: {trigger.seed_occurred_at.isoformat()}")
    if triggering_content:
        lines.append(f"  content: {_trunc(triggering_content, _PER_ITEM_CHAR_LIMIT)}")
    if trigger.seed_natural_text:
        lines.append(
            f"  seed_natural_text: "
            f"{_trunc(trigger.seed_natural_text, _PER_ITEM_CHAR_LIMIT)}"
        )
    if reason:
        lines.append(f"  reason: {reason}")
    lines.append("</triggering_event>")
    return "\n".join(lines)


def _build_context_section(
    bundle: ContextBundle,
    actor_summary: str | None,
) -> str:
    lines = ["<retrieved_context>"]

    # Track every UUID we surface as a valid scope target so we can
    # emit a de-duped <actors_in_context> section below. The LLM sees
    # the per-observation actor_id inline and can also draw from this
    # list when the specific observation it wants to scope has been
    # truncated.
    actor_mentions: dict[str, int] = {}  # actor_id (str) -> obs count
    selected_model_ids, graph_model_ids = _selected_model_sets(bundle)

    retrieval_guidance = _build_retrieval_guidance_section(
        bundle,
        selected_model_ids=selected_model_ids,
        graph_model_ids=graph_model_ids,
    )
    if retrieval_guidance:
        lines.extend(retrieval_guidance)

    # Observations
    obs_parts = ["  <observations>"]
    used = 0
    for o in bundle.observations:
        actor_repr = str(o.actor_id) if o.actor_id is not None else "external"
        if o.actor_id is not None:
            actor_mentions[str(o.actor_id)] = (
                actor_mentions.get(str(o.actor_id), 0) + 1
            )
        piece = (
            f"    - id={o.id} trust={o.trust_tier} channel={o.source_channel} "
            f"actor_id={actor_repr} "
            f"at={o.occurred_at.isoformat()}: "
            f"{_trunc(o.content_text, _PER_ITEM_CHAR_LIMIT)}"
        )
        if used + len(piece) > _OBS_CHAR_BUDGET:
            obs_parts.append("    - [truncated — more observations omitted]")
            break
        obs_parts.append(piece)
        used += len(piece)
    obs_parts.append("  </observations>")
    lines.extend(obs_parts)

    # Models — include existing scope so the LLM sees how peers are
    # scoped and can reuse the same actor/entity UUIDs for new Models.
    mod_parts = ["  <models>"]
    used = 0
    for m in bundle.models:
        falsifier = (
            m.falsifier.get("kind") if isinstance(m.falsifier, dict) else None
        )
        for a in m.scope_actors:
            actor_mentions[str(a)] = actor_mentions.get(str(a), 0) + 1
        scope_actors_repr = (
            "[" + ",".join(str(a) for a in m.scope_actors) + "]"
            if m.scope_actors else "[]"
        )
        scope_entities_repr = json.dumps(
            [
                {"type": e.get("type"), "id": str(e.get("id"))}
                for e in m.scope_entities
                if isinstance(e, dict)
            ],
            default=str,
        )
        piece = (
            f"    - id={m.id} kind={m.proposition_kind} "
            f"retrieval={_retrieval_tags(m.id, selected_model_ids, graph_model_ids)} "
            f"conf={m.confidence:.2f} act={m.activation:.2f} "
            f"falsifier={falsifier} status={m.status} "
            f"scope_actors={scope_actors_repr} "
            f"scope_entities={_trunc(scope_entities_repr, 400)} "
            f"natural={_trunc(m.natural, _PER_ITEM_CHAR_LIMIT)}"
        )
        if used + len(piece) > _MODELS_CHAR_BUDGET:
            mod_parts.append("    - [truncated — more models omitted]")
            break
        mod_parts.append(piece)
        used += len(piece)
    mod_parts.append("  </models>")
    lines.extend(mod_parts)

    # Acts (goals/commitments/decisions)
    act_parts = ["  <acts>"]
    used = 0
    for g in bundle.acts_summary.get("goals", []):
        piece = (
            f"    - goal id={g.id} state={g.state} altitude={g.altitude} "
            f"health={g.cached_health} title={_trunc(g.title, 200)}"
        )
        if used + len(piece) > _ACTS_CHAR_BUDGET:
            break
        act_parts.append(piece); used += len(piece)
    for c in bundle.acts_summary.get("commitments", []):
        if c.owner_id is not None:
            actor_mentions[str(c.owner_id)] = (
                actor_mentions.get(str(c.owner_id), 0) + 1
            )
        piece = (
            f"    - commitment id={c.id} state={c.state} "
            f"owner={c.owner_id} due={c.due_date} "
            f"title={_trunc(c.title, 200)}"
        )
        if used + len(piece) > _ACTS_CHAR_BUDGET:
            break
        act_parts.append(piece); used += len(piece)
    for d in bundle.acts_summary.get("decisions", []):
        piece = (
            f"    - decision id={d.id} state={d.state} "
            f"title={_trunc(d.title, 200)}"
        )
        if used + len(piece) > _ACTS_CHAR_BUDGET:
            break
        act_parts.append(piece); used += len(piece)
    act_parts.append("  </acts>")
    lines.extend(act_parts)

    # Resources
    res_parts = ["  <resources>"]
    used = 0
    for r in bundle.resources_summary:
        cv = r.current_value or {}
        piece = (
            f"    - resource id={r.id} kind={r.kind} "
            f"identity={_trunc(r.identity, 120)} "
            f"description={_trunc(r.description or '', 180)} "
            f"util={r.utilization_state} "
            f"current_value={_trunc(json.dumps(cv, default=str), 400)}"
        )
        if used + len(piece) > _RESOURCES_CHAR_BUDGET:
            break
        res_parts.append(piece); used += len(piece)
    res_parts.append("  </resources>")
    lines.extend(res_parts)

    # Actors in context — distinct actor UUIDs drawn from observations,
    # existing Models' scope, and commitment owners. This is the
    # explicit list the system prompt tells the LLM to draw from when
    # populating scope_actors on new Models. Every UUID here is safe to
    # reference (it exists in the tenant); the LLM is still expected to
    # pick the RIGHT one for each Model.
    lines.append("  <actors_in_context>")
    if actor_mentions:
        sorted_actors = sorted(
            actor_mentions.items(), key=lambda kv: (-kv[1], kv[0])
        )
        for actor_id, count in sorted_actors:
            lines.append(
                f"    - {actor_id}  (referenced {count}x in retrieved "
                f"observations / models / commitments)"
            )
    else:
        lines.append(
            "    [no internal actors in context — any Model scoped to "
            "an internal actor would need to cite a UUID not present here, "
            "which is NOT allowed; leave scope_actors=[] and scope to an "
            "entity instead]"
        )
    lines.append("  </actors_in_context>")

    # Actor context (stub — the retrieval assembler doesn't pack this
    # yet; we pass a string through for flexibility).
    lines.append("  <actor_context>")
    if actor_summary:
        lines.append(f"    {_trunc(actor_summary, 500)}")
    else:
        lines.append("    [no actor context provided]")
    lines.append("  </actor_context>")

    # Bridge context
    lines.append("  <bridge_context>")
    if bundle.bridge_context:
        lines.append(
            f"    {_trunc(json.dumps(bundle.bridge_context, default=str), 1000)}"
        )
    else:
        lines.append("    [no customer counterparty touched]")
    lines.append("  </bridge_context>")

    # Topology context (S3) — surfaces the active neighborhoods the
    # retrieved Models cluster into, plus recent phase events on the
    # seed neighborhood for T6 triggers. Only rendered when the
    # bundle has `topology_context`.
    lines.append("  <topology_context>")
    topo = bundle.topology_context
    if topo and (topo.get("neighborhoods") or topo.get("recent_phase_events")):
        seed_id = topo.get("seed_neighborhood_id")
        if seed_id is not None:
            lines.append(f"    seed_neighborhood_id: {seed_id}")
        for n in topo.get("neighborhoods", []) or []:
            density = n.get("density")
            density_repr = (
                f"{density:.2f}" if isinstance(density, float) else "n/a"
            )
            sig = n.get("named_signature") or "[unnamed]"
            seed_marker = " (SEED)" if n.get("is_seed") else ""
            lines.append(
                f"    - neighborhood id={n.get('id')}{seed_marker} "
                f"name={_trunc(str(sig), 100)} "
                f"members={n.get('member_count')} "
                f"matched_in_bundle={n.get('matched_in_bundle')} "
                f"density={density_repr}"
            )
        evs = topo.get("recent_phase_events") or []
        if evs:
            lines.append("    recent_phase_events:")
            for e in evs:
                mag = e.get("magnitude")
                mag_repr = f"{mag:.2f}" if isinstance(mag, float) else "n/a"
                lines.append(
                    f"      - kind={e.get('kind')} "
                    f"at={e.get('occurred_at')} "
                    f"name={_trunc(str(e.get('named_signature') or '[unnamed]'), 80)} "
                    f"magnitude={mag_repr}"
                )
    else:
        lines.append("    [no neighborhood context for this trigger]")
    lines.append("  </topology_context>")

    lines.append("</retrieved_context>")
    return "\n".join(lines)


def _selected_model_sets(bundle: ContextBundle) -> tuple[set[str], set[str]]:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    selection = notes.get("model_selection")
    if not isinstance(selection, dict):
        return set(), set()

    selected = {
        str(mid)
        for mid in (selection.get("selected_model_ids") or [])
        if mid
    }
    pathway_survival = selection.get("pathway_survival")
    graph_selected: set[str] = set()
    if isinstance(pathway_survival, dict):
        graph = pathway_survival.get("G")
        if isinstance(graph, dict):
            graph_selected = {
                str(mid)
                for mid in (graph.get("selected_model_ids") or [])
                if mid
            }
    return selected, graph_selected


def _retrieval_tags(
    model_id: Any,
    selected_model_ids: set[str],
    graph_model_ids: set[str],
) -> str:
    tags: list[str] = []
    mid = str(model_id)
    if mid in selected_model_ids:
        tags.append("selected_context")
    if mid in graph_model_ids:
        tags.append("graph_anchor")
    return ",".join(tags) if tags else "context"


def _build_retrieval_guidance_section(
    bundle: ContextBundle,
    *,
    selected_model_ids: set[str],
    graph_model_ids: set[str],
) -> list[str]:
    if not selected_model_ids and not graph_model_ids:
        return []

    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    selection = notes.get("model_selection")
    selected_count = len(selected_model_ids)
    graph_count = len(graph_model_ids)
    if isinstance(selection, dict):
        selected_count = int(selection.get("selected_count") or selected_count)

    def _ids(values: set[str]) -> str:
        ordered = sorted(values)
        shown = ordered[:_RETRIEVAL_GUIDANCE_ID_LIMIT]
        suffix = (
            f", ... +{len(ordered) - len(shown)} more"
            if len(ordered) > len(shown)
            else ""
        )
        return ", ".join(shown) + suffix

    lines = ["  <retrieval_priority>"]
    lines.append(
        "    These are not extra facts; they explain why retrieved Models "
        "survived into the prompt."
    )
    if selected_model_ids:
        lines.append(
            f"    selected_model_ids ({selected_count}): {_ids(selected_model_ids)}"
        )
    if graph_model_ids:
        lines.append(
            f"    graph_anchor_model_ids ({graph_count}): {_ids(graph_model_ids)}"
        )
        lines.append(
            "    Graph anchors are the memory layer's strongest candidate "
            "connections. Before returning an empty diff or an observation-only "
            "claim, test whether the trigger confirms, weakens, contradicts, "
            "blocks, enables, explains, or warns about any graph_anchor Models. "
            "If it does, use existing Model UUIDs in claim_ops.update, "
            "edge_ops, act confidence_basis, or evidence_model_ids."
        )
        lines.append(
            "    Co-selection is NOT itself a stored graph connection. If the "
            "trigger explicitly says one graph-anchor Model blocks, causes, "
            "enables, explains, warns about, or contradicts another, add an "
            "edge_ops entry. For 'blocked by', missing prerequisite, or 'at "
            "risk because' language, prefer edge_kind='blocks' or 'causes' "
            "over generic supports/explains."
        )
        lines.append(
            "    Use weakens only for counterevidence that lowers confidence "
            "in the target Model. Evidence that makes a risk more believable "
            "usually supports or explains that risk; it does not weaken it."
        )
        lines.append(
            "    For superseded_by, direction is old Model -> replacement "
            "Model. If this trigger creates the replacement, source is the "
            "existing graph-anchor Model and target is the new claim's "
            "born_from_event_id."
        )
        lines.append(
            "    Do not create new claim_ops merely to mention graph anchors. "
            "If the selected Model already captures the fact, prefer an update, "
            "an edge, or an empty diff with a clear reasoning_trace."
        )
        lines.append(
            "    Edge endpoints must be existing Model ids from <models>, "
            "or the born_from_event_id of a claim_ops.insert in this same "
            "diff when connecting a new claim to existing memory."
        )
        lines.append(
            "    If you insert a new claim that advances, confirms, completes, "
            "or is a milestone within the same workstream as a graph-anchor "
            "Model, add an edge from the new claim's born_from_event_id to "
            "the most relevant graph-anchor Model (usually supports, enables, "
            "contributes_to_resolution, or co_occurs_with). Do not treat an "
            "act confidence_basis as a substitute for the graph edge."
        )
    lines.append(
        "    If selected context is irrelevant, an empty diff is valid, but "
        "reasoning_trace must name at least one relevant selected/graph Model "
        "by its full UUID and briefly say why that context did not warrant a "
        "state change, edge, or action."
    )
    lines.append("  </retrieval_priority>")
    return lines


def _build_instructions(trigger: TriggerContext) -> str:
    """
    Trigger-kind-specific instructions. Same core operating discipline
    but the T-kind suggests what the model should focus on.
    """
    body = [
        "<operating_instructions>",
        "Produce the minimal diff that correctly represents:",
        "  (1) what this event reveals about reality (claim_ops)",
        "  (2) what performative changes this event warrants (act_ops)",
        "  (3) what resource/holding changes this event causes (resource_ops)",
        "",
    ]
    if trigger.kind == "T1":
        body.append(
            "This is a T1 trigger — a new signal. Focus on what this "
            "event reveals (claim_ops) and any state transitions it "
            "warrants (act_ops).\n"
            "\n"
            "The triggering observation is itself selected context. If it "
            "contains a concrete new fact, progress update, review result, "
            "approval, blocker, concern, customer stance, or dated plan that "
            "is not already captured by a selected Model, emit a "
            "claim_ops.insert grounded in that observation. Do not return an "
            "empty diff merely because no Act transition is warranted. If the "
            "new claim is part of the same workstream as a graph-anchor "
            "Model, also emit the strongest useful edge from the new claim to "
            "that graph anchor.\n"
            "\n"
            "MANDATORY: if the signal contains 'I've started', "
            "'kicking off', 'picked up', 'I'm building', 'working on', "
            "'I'll deliver', or any equivalent self-report of new "
            "in-flight work, AND <acts> contains NO commitment whose "
            "title matches that work, you MUST emit a recommendation "
            "claim_op with `proposition.proposed_change.operation = "
            "\"create\"` and `target_act_ref = {\"type\":\"commitment\","
            "\"id\":null}`. Do not skip this with reasoning like "
            "'purely informational' or 'no human approval needed' — the "
            "approval here is the CEO ratifying the new scope into the "
            "ledger. Co-emit the state Model AND the recommendation; "
            "they are not redundant. Use the create-commitment payload "
            "shape in the system prompt."
        )
    elif trigger.kind == "T2" and trigger.subkind == "belief_updated":
        body.append(
            "This is a T2:belief_updated trigger — a new state or concern "
            "model was just inserted by a T1 run. Decide whether the CEO "
            "needs to act on this belief.\n"
            "\n"
            "  • If a team member is blocked, waiting on a decision, or "
            "the CEO needs to unblock someone: emit ONE claim_op with "
            "proposition_kind='recommendation'. Use only actor UUIDs that "
            "appear in <actors_in_context> for scope_actors. Write the "
            "natural field as a clear, actionable sentence for the CEO.\n"
            "\n"
            "  • If the new state Model encodes a self-report of new "
            "in-flight work ('started X', 'building Y', 'picked up Z') "
            "AND <acts> has no matching commitment, you MUST emit a "
            "recommendation with proposed_change.operation='create' and "
            "target_act_ref={\"type\":\"commitment\",\"id\":null}. Use "
            "the create-commitment payload shape in the system prompt. "
            "'Purely informational progress update' is NOT an acceptable "
            "reason to skip — the ledger needs a "
            "commitment for the work to be tracked.\n"
            "\n"
            "  • If purely informational and no CEO action is needed: "
            "return an empty diff (zero claim_ops). The selected Model that "
            "caused this T2 trigger already records the underlying fact; do "
            "not create recap, elaboration, or bookkeeping state Models.\n"
            "\n"
            "CRITICAL CONSTRAINTS for the recommendation claim_op:\n"
            "  - Do NOT set scope_entities unless a UUID appears in <acts> "
            "or <retrieved_context>. Leave scope_entities as [] if unsure.\n"
            "  - Set target_act_ref to null unless you have an exact UUID "
            "from <acts>. Never invent a UUID.\n"
            "  - Do NOT invent UUIDs. If no CEO UUID is in the context, "
            "leave scope_actors as [].\n"
            "  - Do NOT emit a duplicate if a similar recommendation already "
            "exists with status 'active' in <acts>."
        )
    elif trigger.kind == "T2":
        body.append(
            "This is a T2 trigger — a prediction Model's evaluate_at "
            "has passed. Resolve the prediction: update confidence, "
            "set resolved_at / resolution_outcome, adjust contributors."
        )
    elif trigger.kind == "T3":
        body.append(
            "This is a T3 trigger — an anomaly region. Reflect on the "
            "full situation. Consider whether any Model should be "
            "marked contested_false or archived. Update signal_readings "
            "where appropriate."
        )
    elif trigger.kind == "T4":
        body.append(
            "This is a T4 trigger — background / maintenance / dependent "
            "re-evaluation. If the trigger carries a cause_model_id and "
            "cause_kind, update the dependent Model's confidence or "
            "archive it as appropriate."
        )
    elif trigger.kind == "T6":
        # T6 is the topology phase-event trigger. The triggering "event"
        # is structural (a neighborhood emerged / dissolved / split /
        # merged / drifted). The LLM's job is to:
        #   - Optionally NAME the neighborhood (overwrite the heuristic).
        #   - Decide whether the structural shift warrants a CEO-facing
        #     `recommendation` claim_op.
        #   - Update confidence / status on Models that no longer fit
        #     their (former) neighborhood, when warranted.
        # See the <topology_context> section above for what changed.
        body.append(
            "This is a T6 trigger — a TOPOLOGY phase event. The "
            "substrate's emergent neighborhood structure just shifted; "
            "see <topology_context> above for details (kind, magnitude, "
            "members, neighborhood lineage). The seed neighborhood, "
            "predecessor neighborhoods, and member Model ids are all "
            "in the trigger payload.\n"
            "\n"
            "Decide whether this structural shift warrants any of:\n"
            "  • Naming the neighborhood — emit a `claim_op.update` on "
            "    one of the member Models recording a `state` "
            "    proposition that captures the cluster's theme. The "
            "    heuristic name is in <topology_context>; if a more "
            "    accurate human-readable description fits, write a "
            "    state Model whose subject is the neighborhood theme.\n"
            "  • Surfacing the shift to the CEO — when the event kind "
            "    is `emergence`, `merge`, or a high-magnitude `split` "
            "    (>= 0.5), the structural transition often deserves a "
            "    CEO-facing `recommendation` Model. Use it to flag "
            "    \"these Models are now clustering together, here's "
            "    what that probably means for the org.\"\n"
            "  • No-op — when the phase event is small or routine "
            "    (low-magnitude drift, expected dissolution of stale "
            "    Models), return an empty diff. Topology shifts that "
            "    do not warrant human or epistemic action are valid.\n"
            "\n"
            "STRICT CONSTRAINTS:\n"
            "  - Do NOT invent member Model ids — use only ids in "
            "    <models> or in the trigger payload.\n"
            "  - Do NOT emit cascade-y act_ops just because the "
            "    cluster shape changed; act_ops require a signal "
            "    asserting a state transition on a specific Act, not "
            "    a topology shift.\n"
            "  - Cap the diff at 2 claim_ops for T6 unless the event "
            "    explicitly demands more (rarely)."
        )
    body.append("")
    body.append(
        "Reminder before you emit each claim_ops.insert entry: populate "
        "scope_actors and scope_entities by pulling UUIDs from the context "
        "sections above (observations' actor_id, acts, resources, "
        "bridge_context, actors_in_context). If the signal names a PR or "
        "ticket (e.g., 'PR #847', 'ENG-501'), resolve the handle to the "
        "commitment in <acts> and include that commitment's UUID in "
        "scope_entities. Do NOT invent UUIDs."
    )
    body.append(
        "Retrieval discipline: when <retrieval_priority> names selected or "
        "graph-anchor Models, do not ignore them by default. Prefer updating "
        "an existing selected Model, adding an edge between existing Models, "
        "or citing selected Models in evidence_model_ids when the trigger "
        "changes their meaning. Do NOT satisfy this rule by inserting a new "
        "Model that merely restates selected context. An observation-only "
        "insert is appropriate only when none of the selected Models is "
        "actually affected; in that case, say so in reasoning_trace. If the "
        "trigger only confirms an existing selected Model and confidence is "
        "already appropriate, prefer an empty diff with a specific trace over "
        "a duplicate insert."
    )
    body.append(
        "Return ONLY a single JSON object conforming to the Diff schema. "
        "Use trigger_ref and tenant_id exactly as given in the triggering "
        "event metadata."
    )
    body.append("</operating_instructions>")
    return "\n".join(body)


__all__ = ["build_prompt", "PromptPair"]
