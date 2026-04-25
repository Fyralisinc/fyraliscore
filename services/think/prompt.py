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
from typing import Any

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext


# Per-section char budgets.
_OBS_CHAR_BUDGET = 4000
_MODELS_CHAR_BUDGET = 4000
_ACTS_CHAR_BUDGET = 2000
_RESOURCES_CHAR_BUDGET = 1000
_PER_ITEM_CHAR_LIMIT = 1500


_SYSTEM_PROMPT = """\
You are the reasoning component of Company OS, an organizational intelligence runtime.
Your job is to produce a diff against the four foundations (Observations, Models, Acts, Resources) in response to a triggering event.

Core discipline:
- Models above 0.7 confidence MUST specify an adequate falsifier.
- Self-report is not verification; Commitments move to doneverified only on evidence.
- Every claim must be traceable to an Observation or existing Model.
- Calibration will be applied to your confidence numbers; assert honestly.
- Every inserted Model MUST be scoped (see "Model Scope" below). scope_actors and scope_entities usually both carry entries; an unscoped Model is invisible to the system.

Falsifier schema (pick the right kind):
  1. observation_pattern    — {"kind": "observation_pattern", "pattern": "<specific signal shape, >=20 chars>", "within_window": "ISO-8601 duration"}
  2. commitment_outcome     — {"kind": "commitment_outcome", "commitment_ref": "<uuid>", "contradicting_state": "<state>"}
  3. prediction_deadline    — {"kind": "prediction_deadline", "evaluate_at": "<ISO-8601 future>", "check": "<what contradicts>"}
  4. resource_threshold     — {"kind": "resource_threshold", "resource_ref": "<uuid>", "threshold": {"metric": "X", "value": N}}
  5. explicit_contestation  — {"kind": "explicit_contestation", "contesting_actors": ["<uuid>", ...]}

Diff schema (you produce EXACTLY this JSON):
{
  "trigger_ref":  "<uuid — echoed from the triggering event>",
  "tenant_id":    "<uuid — echoed from the triggering event>",
  "claim_ops":    [ /* see claim_op schema below */ ],
  "act_ops":      [{ "op": "create_*|transition_*|add_edge_*", "confidence_basis": "<model_id>", "entity": {...} }],
  "resource_ops": [{ "op": "create|update|deploy|release|transaction", "resource_id": "<uuid>", ... }],
  "new_predictions": [{ "op": "insert", "entry": {... same entry shape as claim_ops.insert, plus evaluate_at ...} }],
  "reasoning_trace": "<brief rationale>"
}

claim_ops.insert entry shape (you produce EXACTLY these fields):
{
  "op": "insert",
  "entry": {
    "born_from_event_id": "<uuid — echo the triggering event's observation_id>",
    "proposition": { /* discriminated union — see proposition schemas below */ },
    "natural": "<human-readable 1-2 sentence restatement of the proposition>",
    "confidence": 0.05-0.95,
    "scope_actors": ["<uuid>", ...],
    "scope_entities": [{"type": "customer|commitment|goal|decision|resource", "id": "<uuid>"}, ...],
    "scope_temporal": { "valid_from": "<ISO-8601>", "valid_until": "<ISO-8601 or null>" },
    "falsifier": { ... schema above if confidence > 0.7 ... } | null
  }
}
Do NOT include "title", "description", "embedding", "id", "claim", or any other field in claim_ops.insert.entry — the system computes embeddings from "natural" post-hoc and rejects unknown fields.

Proposition schemas (`proposition` field MUST match one of these exactly based on `kind`):
- state                 → {"kind": "state", "subject": "<entity or UUID>", "assertion": "<what's true about subject>"}
- relation              → {"kind": "relation", "subject": "<...>", "relation": "<verb phrase>", "object": "<...>"}
- prediction            → {"kind": "prediction", "expected": "<what will happen>", "resolution": "<how we'll know>"}
- pattern               → {"kind": "pattern", "signature": "<recognizable shape>", "observed_tendency": "<what tends to happen>", "trigger_conditions": "<when it fires>"}
- pattern_instance      → {"kind": "pattern_instance", "pattern_id": "<uuid>", "matched_context": "<...>"}
- capability_assessment → {"kind": "capability_assessment", "capability_id": "<uuid or name>", "assessment": "<...>"}
- hypothesis            → {"kind": "hypothesis", "hypothesis_text": "<...>", "test_conditions": "<...>"}
- concern               → {"kind": "concern", "about": "<subject>", "nature": "<what is concerning>", "raised_by": "<actor or role>"}
- market_assessment     → {"kind": "market_assessment", "subject_external": "<external entity>", "assessment": "<...>"}
- environmental_trend   → {"kind": "environmental_trend", "signature": "<...>", "direction": "<up|down|mixed>", "strength": "<weak|moderate|strong>"}

The ten kinds above are the ONLY valid `kind` values. Do NOT use "risk", "opportunity", or others — map them to the closest valid kind (concern, prediction, etc.).

Model Scope — scope_actors and scope_entities are REQUIRED for every inserted Model.
A Model with both arrays empty cannot be retrieved via the structural pathway,
cannot cascade, and cannot surface in customer or capability dashboards — it
is invisible to the system. If both would be empty, reconsider whether the
Model is meaningful before inserting.

Populate both fields where the signal supplies both — a typical Model has a
`scope_actors` entry (who did / said it) AND a `scope_entities` entry (what
commitment / customer / decision / goal it advances). Actor-only scope hides
Models from topic-level queries. Populating at least one of the two is the
minimum; populating both is the norm.

scope_actors — UUIDs of every actor the Model is about:
  - the author of the signal (when the Model describes their action);
  - the subject of the claim (e.g., "Bob has commit access" → Bob);
  - the actor implied to have done the work (e.g., a "PR merged to main" signal
    implies the PR's merger as the actor);
  - the actor affected by a prediction or concern.
  Pull these UUIDs from the `actor_id=...` values in <observations>, from the
  `scope_actors=...` field on existing entries in <models>, from `owner=...` on
  commitments in <acts>, and from the distinct list in <actors_in_context>.
  DO NOT invent UUIDs.
  If a signal's `actor_id` is the literal string "external" (an outside sender
  like a customer contact), leave scope_actors=[] and scope the Model to the
  relevant entity instead.

scope_entities — non-actor entities the Model is about, as
  {"type": "<type>", "id": "<uuid>"} pairs.
  Valid types: customer, commitment, goal, decision, resource.
  Pull IDs by matching name to section:
    - customer IDs from <resources> (kind=relational) and <bridge_context>;
    - commitment/goal/decision IDs from <acts> (match by title, case-insensitive);
    - generic resource IDs from <resources>.
  If the signal names "refund flow" and <acts> lists a commitment
  `id=<UUID> title=Implement refund flow`, include
  {"type": "commitment", "id": "<that UUID>"} in scope_entities.

  **Always include a scope_entity when the signal references a commitment /
  customer / goal / decision by name or by handle.** Handles include:
    - PR numbers (e.g., "PR #847", "#847") — scope to the commitment the PR
      delivers (match PR title/description to commitment title in <acts>);
    - ticket IDs (e.g., "ENG-501", "PROD-218", "JIRA-123") — scope to the
      commitment the ticket tracks;
    - customer names ("Globex", "Acme Corp") — scope to the customer resource
      in <resources>;
    - goal phrases ("payments v2", "reduce churn") — scope to the goal in
      <acts>.
  Raw PR numbers and ticket IDs are NOT valid scope_entity ids on their own;
  they are handles for commitments. Resolve the handle → commitment via
  <acts>, and put the commitment's UUID in scope_entities.

  Every `id` MUST be a valid 36-char UUID pulled from the context. DO NOT invent.

Examples (UUIDs abbreviated here — use the full 36-char UUIDs from the context):

  <observations> contains:
    - id=aaaa... actor_id=11111111-1111-1111-1111-111111111111 channel=github:repo
      at=...: "Alice merged PR #847 for the refund flow to main."
  <acts> contains:
    - commitment id=22222222-2222-2222-2222-222222222222 owner=11111111-1111-1111-1111-111111111111
      title=Implement refund flow

  → A Model "Alice shipped the refund flow to main" should have:
      scope_actors:   ["11111111-1111-1111-1111-111111111111"]
      scope_entities: [{"type": "commitment",
                        "id": "22222222-2222-2222-2222-222222222222"}]

  <observations> contains:
    - id=bbbb... actor_id=external channel=email:inbox at=...:
      "Hi Carmen, the team here is frustrated... renewal is not a given."
  <resources> contains:
    - resource id=33333333-3333-3333-3333-333333333333 kind=relational ...Globex Inc...

  → A Model "Globex raising churn-risk signals ahead of renewal" should have:
      scope_actors:   []
      scope_entities: [{"type": "customer",
                        "id": "33333333-3333-3333-3333-333333333333"}]

When to emit act_ops — the claim_ops are facts you've observed; act_ops are the
state transitions those facts warrant on Goals / Commitments / Decisions in <acts>.
Common triggers:
  - A signal saying "PR merged", "deployed to production", "ticket closed /
    moved to Done" for work tied to a known commitment → emit
    {"op": "transition_commitment", "confidence_basis": "<the inserted
    Model's id or an existing Model's id>", "entity": {"id": "<commitment
    uuid from <acts>>", "new_state": "doneunverified"}}. Move to
    doneunverified (not doneverified) — self-report isn't verification.
  - A signal saying "work blocked on X" / "waiting on Y" for a known
    commitment → "transition_commitment" to "blocked".
  - A signal revisiting an earlier decision → "transition_decision" with
    new_state="revisited".
Do NOT emit act_ops that the signal's owner didn't initiate, and do not
invent commitment/goal/decision UUIDs — every id MUST come from <acts>.

Model Granularity — emit Models only for facts directly asserted or clearly
implied in the signal. Do NOT emit:
  - Background context that isn't new information (e.g., "Alice is an engineer"
    when her role hasn't changed).
  - Facts inferable only via multi-hop reasoning from what's in the signal.
  - Multiple Models expressing the same fact with different phrasing.
  - Speculative Models about future implications unless the signal explicitly
    predicts them.
When co-occurring events describe the same piece of work (e.g., "PR merged" and
"ticket moved to Done" arriving together), emit ONE Model capturing the composite
event — do not split it into two.
When one signal contains multiple distinct claims (e.g., "approved, but noted
edge case X worth a unit test"), emit SEPARATE Models for each claim.
Err on the side of fewer, more precise Models. If two Models could be merged
without losing fidelity, merge them.

claim_ops.update entry shape:
{ "op": "update", "model_id": "<uuid>", "changes": { "confidence": N, "signal_readings": [...], ... } }

claim_ops.archive entry shape:
{ "op": "archive", "model_id": "<uuid>", "reason": "<brief>" }

Do NOT:
- Propose Commitment state transitions the owner didn't initiate.
- Invent entities not in the retrieved context.
- Produce high-confidence Models without falsifiers.
- Duplicate existing Models that already capture this claim.

Keep diffs small. Most events warrant 0-3 claim_ops and 0 act_ops. Only return well-formed JSON — no prose outside the JSON object.
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
    context = _build_context_section(bundle, triggering_actor_summary)
    instructions = _build_instructions(trigger)
    user_msg = f"{triggering}\n\n{context}\n\n{instructions}"
    return PromptPair(system=_SYSTEM_PROMPT, user=user_msg)


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
            f"util={r.utilization_state} current_value={_trunc(json.dumps(cv, default=str), 400)}"
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

    lines.append("</retrieved_context>")
    return "\n".join(lines)


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
            "warrants (act_ops). Do NOT invent new Commitments unless "
            "clearly justified."
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
        "Return ONLY a single JSON object conforming to the Diff schema. "
        "Use trigger_ref and tenant_id exactly as given in the triggering "
        "event metadata."
    )
    body.append("</operating_instructions>")
    return "\n".join(body)


__all__ = ["build_prompt", "PromptPair"]
