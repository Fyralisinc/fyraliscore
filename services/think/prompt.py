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
             <customer_context>
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
_MODELS_INQUIRY_CHAR_BUDGET = 2400
_ACTS_CHAR_BUDGET = 12000
_RESOURCES_CHAR_BUDGET = 1000
_PER_ITEM_CHAR_LIMIT = 1500
_MODEL_DETAIL_CHAR_LIMIT = 700
_MODEL_MANIFEST_CHAR_LIMIT = 220
_MODEL_DETAIL_ROW_LIMIT = 8
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
  "ontology_gap_ops": [],
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
    "proposition": {"kind": "<observation|belief|prediction|norm>", "...": "..."},
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

Proposition stance and grammar:
- `kind` has only four valid values: observation, belief, prediction, norm.
- Use `kind` for epistemic stance only:
  - observation = something that happened and should not be revised in place.
  - belief = what we currently hold to be true from observations.
  - prediction = a falsifiable future claim.
  - norm = what the organization wants to happen.
- Put subject semantics in grammar fields, not in `kind`:
  - `claim_role`: fact | concern | hypothesis | prediction | pattern | situation | capability | relation | recommendation
  - `abstraction_level`: atomic | relationship | composite | pattern
  - `time_mode`: past | current | future | recurring | unspecified
  - `modality`: observed | inferred | expected | normative
  - `polarity`: positive | negative | mixed | neutral
  - `domain_tags`: short lowercase tags like ["market"], ["customers"], ["execution"].

Payload examples:
- observation -> {"kind":"observation","event":"<what happened>","claim_role":"fact","time_mode":"past","modality":"observed"}
- belief/fact -> {"kind":"belief","subject":"<entity or UUID>","assertion":"<current truth>","claim_role":"fact"}
- belief/relation -> {"kind":"belief","subject":"...","relation":"verb phrase","object":"...","claim_role":"relation","abstraction_level":"relationship"}
- prediction -> {"kind":"prediction","expected":"...","resolution":"...","claim_role":"prediction","time_mode":"future","modality":"expected"}
- belief/pattern -> {"kind":"belief","signature":"...","observed_tendency":"...","trigger_conditions":"...","claim_role":"pattern","abstraction_level":"pattern","time_mode":"recurring"}
- belief/concern -> {"kind":"belief","about":"<subject>","nature":"<concern>","raised_by":"<actor or role>","claim_role":"concern","polarity":"negative"}
- belief/market pattern -> {"kind":"belief","subject_external":"<external entity>","assessment":"...","claim_role":"pattern","domain_tags":["market"]}
- belief/situation -> {"kind":"belief","claim_role":"situation","abstraction_level":"composite","situation":"<named composite condition>","summary":"<what is jointly true>","member_model_ids":["<model uuid>",...],"relationship_summary":"<how the member claims interact>","status":"forming|active|resolved|contested|null","pressure_type":"capacity|trust|revenue|compliance|decision|execution|market|resource","shared_mechanism":"<one sentence: why these members belong together>","judgment_change":"<one sentence: what becomes clear only when seen together>","affected_decisions":["<string>",...],"affected_customers":["<entity name or actor id>",...],"affected_teams":["<string>",...],"evidence_event_ids":["<observation uuid from bundle>",...],"open_falsifier":"<under what observation this composite would be invalid>"}
- norm/recommendation -> {"kind":"norm","claim_role":"recommendation","target_act_ref":{"type":"goal|commitment|decision|resource","id":"<uuid or null>"}|null,"proposed_change":{"operation":"create|update|archive|transition","payload":{...}},"expected_impact":<number|null>,"qualitative_impact":"<string|null>","target_actor_id":"<uuid|null>"}
Do NOT invent new proposition kinds. Preserve nuance with `claim_role`,
`domain_tags`, and the other grammar axes.

Situation compositional fields (mandatory when emitting `kind="belief", claim_role="situation"`):
- `pressure_type` MUST be one of capacity, trust, revenue, compliance,
  decision, execution, market, resource. Pick the dominant pressure the
  composite expresses; do not invent new categories.
- `shared_mechanism` is one sentence naming the operational/causal
  thread that ties the member Models together (not a restatement of
  `summary`).
- `judgment_change` is one sentence stating what becomes clear ONLY when
  these members are read as one composite — the marginal insight beyond
  the individual member Models.
- `affected_decisions`, `affected_customers`, `affected_teams` should
  list known downstream surfaces drawn from the bundle (decision titles,
  customer names, team labels). Omit a field rather than guessing; empty
  lists are fine when the surface genuinely is not touched.
- `evidence_event_ids` MUST cite Observation UUIDs that already appear
  in <observations> or in member Models' provenance. Do not invent UUIDs.
- `open_falsifier` is ALWAYS required — describe the concrete signal that
  would invalidate the composite (e.g., "Globex renews on schedule and
  capacity load drops below 70% for two consecutive weeks").

Stance/grammar rubric:
- Use `belief` + `claim_role="fact"` for current facts and progress milestones.
- Use `belief` + `claim_role="concern"` for risks, blockers, review comments,
  edge cases, customer pushback, missing evidence, or "worth testing" warnings.
- Use `prediction` for dated plans, ETAs, future deploys, expected slips,
  "will/should by <date>", or conditional future outcomes.
- Use `belief` + `claim_role="relation"` when the important memory is a
  dependency or causal link between two entities/facts.
- Use `belief` + `claim_role="situation"` when multiple selected Models/edges
  form one operational condition that matters as a composite. Situation
  member_model_ids must be existing Model ids from <models>; do not use
  situations for simple pairwise links that should be edge_ops.
- Use `belief` + `claim_role="capability"` for evidenced skill/capacity/trust,
  `claim_role="pattern"` for repeated behavior, and `claim_role="hypothesis"`
  for uncertain motive or constraint explanations.
- Do not flatten every claim into plain fact; the grammar fields are part of
  retrieval quality and should preserve the signal's semantics.

Recommendations:
- Emit a recommendation Model only for concrete human-approved Act/Resource
  changes: create/update/archive/transition a Goal, Commitment, Decision, or
  Resource. Do not recommend autonomous bookkeeping like confidence updates,
  ordinary Model archives, or doneunverified transitions the system can apply.
- New self-reported work is special: if the signal says "I've started",
  "kicking off", "picked up", "I'm building", "working on", "I'll deliver", or
  equivalent, and <acts> has no matching commitment, emit both a fact Model and
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
- Actor claims must be evidenced. Do not psychologize from a single signal:
  write capability/constraint/support claims only when the signal directly says
  so or the retrieved actor context shows a repeated pattern.
- scope_entities: {"type":"customer|commitment|goal|decision|resource",
  "id":"<uuid>"} from <acts>, <resources>, or customer_context. Resolve PR/ticket
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
  * work waiting/on hold/stalled for a known commitment ->
    transition_commitment to paused, but only if that commitment is already
    active, blocked, paused, or doneunverified. Proposed commitments cannot be
    paused or blocked; use a scoped concern/state claim instead.
  * transition_commitment to blocked ONLY when the context explicitly shows
    an unsatisfied dependency or a revisited constraining decision for that
    exact commitment; otherwise use paused plus a state/concern claim.
  * an active decision is explicitly revisited -> transition_decision to
    revisited. If the matching decision is still drafted, write a scoped
    concern/state claim instead; drafted decisions can only transition to
    active.
- `confidence_basis` MUST be either an existing Model id copied from <models> OR
  the `born_from_event_id` of a claim_ops.insert in the same diff. Use the latter
  when the new claim is the evidence for the transition; the system rewrites it
  to the inserted Model id after claim application. Do not use any other
  observation/event id.
- Do not emit act_ops that the signal owner did not initiate, and never invent
  commitment/goal/decision UUIDs.

Model granularity:
- Atomicity rule: each `model` entry expresses ONE claim about ONE subject. If
  the world-state has multiple linked claims (e.g. "X is happening AND Y is at
  risk AND Z needs to happen"), emit them as SEPARATE `model` entries plus ONE
  `situation` entry whose `member_model_ids` references the atomic Models after
  creation. Do NOT pack multi-clause compound claims into a single model entry —
  they collapse under dedupe and prevent meaningful adjudication.
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
- Causal edges (`causes`, `explains`, `blocks`, `enables`) require a concrete
  mechanism in `explanation`. If the mechanism is plausible but unconfirmed,
  set `review_status` to `candidate` or `needs_review` and put causal metadata
  under `metadata.causal` when known: mechanism_summary, intervention_surface,
  expected_delay, confounders. Do not turn mere co-occurrence into causality.
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
- DAG-scoped edges (`supports`, `instance_of`, `contributes_to_resolution`,
  `superseded_by`) must not create reciprocal or transitive loops. If the
  selected graph already implies target reaches source, omit the edge; if
  direction is uncertain, choose no edge rather than a cyclic support chain.
- contradicts/weakens require numeric weight. supports/causes/explains/predicts/
  blocks/enables/same_issue_as/co_occurs_with/analogous_to/alternative_to/
  early_warning_for may set weight. instance_of/contributes_to_resolution/
  superseded_by must set weight null.

ontology_gap_ops:
{ "op":"propose_edge_type", "source_model_id":"<uuid>",
  "target_model_id":"<uuid>", "proposed_edge_kind":"snake_case_new_kind",
  "description":"<what this relation means>",
  "relationship_summary":"<why this pair has that relation>",
  "parent_kind":"<nearest registered edge_kind or null>",
  "nearest_existing_kind":"<nearest registered edge_kind or null>",
  "directionality":"directed|symmetric|unknown",
  "inverse_label":"<optional inverse label or null>",
  "dropped_dimensions":["<semantic loss if coerced>", "..."],
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "confidence":0.0-1.0, "impact":0.0-1.0, "actionability":0.0-1.0,
  "urgency":0.0-1.0, "uncertainty":0.0-1.0,
  "authority_required":0.0-1.0, "novelty":0.0-1.0 }
- Use ontology_gap_ops when the relationship between two Models is valuable
  and grounded, but no registered edge_kind preserves its decision-relevant
  semantics. This writes a pre-truth edge-type candidate, not an accepted edge.
- Do NOT use ontology_gap_ops for registered edge kinds. If `supports`,
  `blocks`, `contradicts`, etc. is precise enough, use edge_ops.
- Good examples: `gated_by_decision`, `depends_on_assumption`,
  `trades_off_with`, `competes_for_priority_with`, `transfers_risk_to`,
  `obscures`, `proxy_for`, `lags`, `accountable_for`, `reinforces`.
- `parent_kind` / `nearest_existing_kind` must be a registered edge_kind when
  provided. It is the retrieval fallback; `dropped_dimensions` must explain
  what that fallback would lose.

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

Proposition stance:
- `kind` has only four valid values: observation, belief, prediction, norm.
- Use grammar fields for semantics: `claim_role`, `abstraction_level`,
  `time_mode`, `modality`, `polarity`, `domain_tags`.
- fact -> {"kind":"belief","subject":"<entity or UUID>","assertion":"<truth>","claim_role":"fact"}
- relation -> {"kind":"belief","subject":"...","relation":"verb phrase","object":"...","claim_role":"relation","abstraction_level":"relationship"}
- prediction -> {"kind":"prediction","expected":"...","resolution":"...","claim_role":"prediction"}
- pattern -> {"kind":"belief","signature":"...","observed_tendency":"...","trigger_conditions":"...","claim_role":"pattern","abstraction_level":"pattern","time_mode":"recurring"}
- concern -> {"kind":"belief","about":"<subject>","nature":"<concern>","raised_by":"<actor or role>","claim_role":"concern","polarity":"negative"}
- market assessment -> {"kind":"belief","subject_external":"<external entity>","assessment":"...","claim_role":"pattern","domain_tags":["market"]}
- situation -> {"kind":"belief","claim_role":"situation","abstraction_level":"composite","situation":"<named composite condition>","summary":"<what is jointly true>","member_model_ids":["<model uuid>",...],"relationship_summary":"<how the member claims interact>","status":"forming|active|resolved|contested|null","pressure_type":"capacity|trust|revenue|compliance|decision|execution|market|resource","shared_mechanism":"<one sentence: why these members belong together>","judgment_change":"<one sentence: what becomes clear only when seen together>","affected_decisions":["<string>",...],"affected_customers":["<entity name or actor id>",...],"affected_teams":["<string>",...],"evidence_event_ids":["<observation uuid>",...],"open_falsifier":"<sentence: under what observation this composite is invalid>"}
- recommendation -> {"kind":"norm","claim_role":"recommendation","target_act_ref":{"type":"goal|commitment|decision|resource","id":"<uuid or null>"}|null,"proposed_change":{"operation":"create|update|archive|transition","payload":{...}},"expected_impact":<number|null>,"qualitative_impact":"<string|null>","target_actor_id":"<uuid|null>"}
Do NOT invent new proposition kinds.
When emitting a situation, populate `pressure_type` (one of the eight
categories), `shared_mechanism` (one sentence), `judgment_change` (one
sentence), and `open_falsifier`. List `affected_decisions`,
`affected_customers`, `affected_teams`, and cite `evidence_event_ids`
from the retrieval bundle whenever they are known. Omit a field rather
than guess.
Grammar rubric: fact=current observed truth; concern=risk/blocker/review warning/
edge case/customer pushback/missing evidence; prediction=dated plan, ETA, future
deploy, expected slip, conditional outcome; relation=dependency or causal link;
hypothesis=uncertain explanation needing investigation; situation=composite
condition across multiple selected Models. Do not flatten every claim into fact.

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
  commitment, emit both a fact Model and a recommendation with
  proposed_change.operation="create" and target_act_ref={"type":"commitment",
  "id":null}. Payload: title, owner_id from context, due_date from the signal or
  about 30 days out, plus contributes_to_goal_ids when a goal fits, otherwise
  is_maintenance=true.
- Each recommendation needs proposed_change and either expected_impact or
  qualitative_impact. Use target_actor_id only from context.

Scope:
- scope_actors comes from observation actor_id, existing Model scope_actors,
  commitment owner, or <actors_in_context>. External senders usually use [].
- Actor claims must be directly evidenced or supported by repeated actor
  context; do not infer motives or hidden psychology from one message.
- scope_entities comes from <acts>, <resources>, or customer_context. Resolve PR
  numbers and ticket IDs to matching commitment UUIDs in <acts>; customer names
  to relational resources; goal phrases to goals. Never invent UUIDs.
- Customer-specific commitment signals should usually include both customer and
  commitment entities when both are available.

Granularity:
- Atomicity rule: each `model` entry expresses ONE claim about ONE subject. If
  the world-state has multiple linked claims (e.g. "X is happening AND Y is at
  risk AND Z needs to happen"), emit them as SEPARATE `model` entries plus ONE
  `situation` entry whose `member_model_ids` references the atomic Models after
  creation. Do NOT pack multi-clause compound claims into a single model entry —
  they collapse under dedupe and prevent meaningful adjudication.
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


def _profiled_system_prompt(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    claims_only: bool,
) -> str:
    base = _CLAIMS_ONLY_SYSTEM_PROMPT if claims_only else _SYSTEM_PROMPT
    profile = _build_reasoning_profile(
        trigger,
        bundle,
        claims_only=claims_only,
    )
    return f"{profile}\n\n{base}"


def _trigger_metadata(trigger: TriggerContext) -> dict[str, str]:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )

    def _get(*names: str) -> str | None:
        for name in names:
            value = signature.get(name)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    source_channel = _get("source_channel")
    observation_kind = _get("observation_kind")
    if not observation_kind and trigger.kind == "T1":
        # Older T1 queue payloads used `kind` for the Observation kind.
        observation_kind = _get("kind")
    trust_tier = _get("trust_tier")
    signal_type = _get("signal_type")

    if not signal_type and trigger.kind == "T6" and trigger.topology_event_kind:
        signal_type = f"topology/{trigger.topology_event_kind}"
    if not signal_type:
        signal_type = trigger.subkind or trigger.kind

    out: dict[str, str] = {"signal_type": signal_type}
    if source_channel:
        out["source_channel"] = source_channel
    if observation_kind:
        out["observation_kind"] = observation_kind
    if trust_tier:
        out["trust_tier"] = trust_tier
    return out


def _build_reasoning_profile(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    claims_only: bool,
) -> str:
    meta = _trigger_metadata(trigger)
    source = meta.get("source_channel", "unknown-source")
    signal_type = meta.get("signal_type", trigger.subkind or trigger.kind)
    trust = meta.get("trust_tier", "unknown-trust")
    observation_kind = meta.get("observation_kind", trigger.kind)

    body = [
        "Reasoning profile for this call:",
        f"- Signal: source={source}; type={signal_type}; "
        f"observation_kind={observation_kind}; trust={trust}.",
        f"- Working personality: {_source_personality(trigger, meta)}",
        f"- Model surface: {_surface_personality(trigger, bundle, claims_only)}",
        "- Abstraction level: "
        f"{_abstraction_guidance(trigger, meta, bundle, claims_only)}",
        "Use this profile to choose what to preserve versus compress. It does "
        "not override the diff schema, scoping rules, confidence calibration, "
        "or JSON-only output requirement.",
    ]
    return "\n".join(body)


def _source_personality(
    trigger: TriggerContext,
    meta: dict[str, str],
) -> str:
    source = meta.get("source_channel", "")
    trust = meta.get("trust_tier", "")
    observation_kind = meta.get("observation_kind", "")

    if trigger.kind == "T6":
        return (
            "systems cartographer; reason about structural movement among "
            "Models before naming or escalating it."
        )
    if trigger.kind == "T3":
        return (
            "forensic investigator; inspect anomaly evidence for contestation, "
            "drift, or missing causal links."
        )
    if trigger.kind == "T2" and trigger.subkind == "belief_updated":
        return (
            "decision reviewer; decide whether the new Model requires CEO "
            "action instead of recapping the belief."
        )

    if trust in {
        "reputable",
        "inferential_external",
        "unvetted",
    } or source.startswith(("news:", "social:", "market:", "regulatory:", "analyst:")):
        return (
            "outside analyst; separate external facts from internal implications "
            "and keep confidence bounded by provenance."
        )

    authoritative = trust == "authoritative"
    if authoritative or observation_kind in {
        "state_change",
        "prediction_resolution",
    }:
        return (
            "ledger clerk; preserve exact system-of-record state transitions "
            "and avoid adding motivation or strategy that the signal did not say."
        )
    if (
        source.startswith(("slack:", "email:", "discord:"))
        or trust == "attested_agent"
    ):
        return (
            "contextual listener; extract commitments, blockers, concerns, and "
            "stances while preserving hedging and social nuance."
        )
    return (
        "conservative memory editor; turn only durable, evidenced meaning into "
        "Models and leave weak implications out."
    )


def _surface_personality(
    trigger: TriggerContext,
    bundle: ContextBundle,
    claims_only: bool,
) -> str:
    _selected, graph_models = _selected_model_sets(bundle)
    has_acts = _has_acts(bundle)
    has_resources = bool(bundle.resources_summary)

    if claims_only:
        return (
            "claim triage; emit scoped claim inserts or a justified empty diff, "
            "and mention omitted action/edge reasoning only in reasoning_trace."
        )
    if trigger.kind == "T4" and trigger.subkind == "latent_relationship_candidate":
        return (
            "topology candidate interpreter; decide whether latent "
            "consequence signals deserve an edge, situation, situation update, "
            "or no-op."
        )
    if trigger.kind == "T6":
        return (
            "legacy graph-transition review; prefer edge-aware interpretation "
            "or a no-op over unrelated Act mutations."
        )
    if graph_models:
        return (
            "graph cartographer; test selected graph anchors for confirmation, "
            "weakening, contradiction, blocking, enabling, or explanation before "
            "creating sibling Models."
        )
    if has_acts or has_resources:
        return (
            "operator; map concrete handles to Acts/Resources and emit mutations "
            "only when the signal itself warrants them."
        )
    if bundle.models:
        return (
            "memory reconciler; update, archive, or carefully distinguish existing "
            "Models before inserting new ones."
        )
    return "first-pass extractor; create only the durable belief the signal supports."


def _abstraction_guidance(
    trigger: TriggerContext,
    meta: dict[str, str],
    bundle: ContextBundle,
    claims_only: bool,
) -> str:
    observation_kind = meta.get("observation_kind", "")
    trust = meta.get("trust_tier", "")
    _selected, graph_models = _selected_model_sets(bundle)

    if trigger.kind in {"T3", "T6"}:
        return (
            "high, but still evidence-bound: describe the pattern, anomaly, or "
            "neighborhood shift only as far as retrieved context supports it."
        )
    if observation_kind in {"state_change", "prediction_resolution"} or trust in {
        "authoritative",
        "authoritative_external",
    }:
        return (
            "low and exact: one source event should become the narrowest useful "
            "state, transition, or resolution."
        )
    if graph_models:
        return (
            "relationship level: explain how this signal changes existing Models "
            "or edges before abstracting into a new situation."
        )
    if claims_only:
        return (
            "atomic claim level: one scoped Model per materially new fact, with "
            "no pattern/situation unless the signal itself directly asserts it."
        )
    return (
        "middle level: compress chatty wording into durable business meaning "
        "without inventing a broader pattern from one signal."
    )


def _has_acts(bundle: ContextBundle) -> bool:
    for rows in (bundle.acts_summary or {}).values():
        if rows:
            return True
    return False


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
    context = _build_context_section(trigger, bundle, triggering_actor_summary)
    instructions = _build_instructions(trigger)
    parts = [triggering]
    if frame:
        parts.append(frame)
    parts.extend([context, instructions])
    user_msg = "\n\n".join(parts)
    system_prompt = _profiled_system_prompt(
        trigger,
        bundle,
        claims_only=claims_only,
    )
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
    meta = _trigger_metadata(trigger)
    if "source_channel" in meta:
        lines.append(f"  source_channel: {meta['source_channel']}")
    if "signal_type" in meta:
        lines.append(f"  signal_type: {meta['signal_type']}")
    if "observation_kind" in meta:
        lines.append(f"  observation_kind: {meta['observation_kind']}")
    if "trust_tier" in meta:
        lines.append(f"  trust_tier: {meta['trust_tier']}")
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
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    candidate = signature.get("relationship_candidate")
    if isinstance(candidate, dict):
        lines.extend(_relationship_candidate_lines(candidate))
    if reason:
        lines.append(f"  reason: {reason}")
    lines.append("</triggering_event>")
    return "\n".join(lines)


def _relationship_candidate_lines(candidate: dict[str, Any]) -> list[str]:
    lines = ["  <relationship_candidate>"]
    for key in (
        "id",
        "candidate_kind",
        "basis",
        "edge_kind",
        "source_model_id",
        "target_model_id",
        "member_model_ids",
        "evidence_model_ids",
        "judgment_leverage_score",
    ):
        value = candidate.get(key)
        if value not in (None, [], {}):
            lines.append(f"    {key}: {_trunc(str(value), _PER_ITEM_CHAR_LIMIT)}")
    explanation = candidate.get("explanation")
    if explanation:
        lines.append(
            f"    explanation: {_trunc(str(explanation), _PER_ITEM_CHAR_LIMIT)}"
        )
    proposed = candidate.get("proposed_proposition")
    if proposed:
        lines.append(
            "    proposed_proposition: "
            f"{_trunc(json.dumps(proposed, sort_keys=True, default=str), 1200)}"
        )
    metadata = candidate.get("metadata")
    topology = metadata.get("topology") if isinstance(metadata, dict) else None
    if isinstance(topology, dict):
        compact = {
            key: topology.get(key)
            for key in (
                "kind",
                "object_type",
                "score_components",
                "impact_signatures",
            )
            if topology.get(key) not in (None, [], {})
        }
        lines.append(
            "    topology_evidence: "
            f"{_trunc(json.dumps(compact, sort_keys=True, default=str), 2200)}"
        )
    lines.append("  </relationship_candidate>")
    return lines


def _build_context_section(
    trigger: TriggerContext,
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

    inquiry_packet = _build_inquiry_context_packet_section(bundle)
    if inquiry_packet:
        lines.extend(inquiry_packet)

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

    lines.extend(
        _build_models_section(
            trigger,
            bundle,
            selected_model_ids=selected_model_ids,
            graph_model_ids=graph_model_ids,
            actor_mentions=actor_mentions,
        )
    )

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

    # Customer context
    lines.append("  <customer_context>")
    if bundle.customer_context:
        lines.append(
            f"    {_trunc(json.dumps(bundle.customer_context, default=str), 1000)}"
        )
    else:
        lines.append("    [no customer counterparty touched]")
    lines.append("  </customer_context>")

    # Legacy accepted-memory topology context. Active topology now
    # reaches this prompt through relationship_candidates carried in
    # trigger.seed_signature / member_model_ids.
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
        lines.append("    [no legacy accepted-memory topology context]")
    lines.append("  </topology_context>")

    lines.append("</retrieved_context>")
    return "\n".join(lines)


def _build_models_section(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    selected_model_ids: set[str],
    graph_model_ids: set[str],
    actor_mentions: dict[str, int],
) -> list[str]:
    inquiry_packet = _inquiry_context_packet(bundle)
    inquiry_mode = inquiry_packet is not None
    actionable_ids = _actionable_model_ids(
        trigger,
        bundle,
        graph_model_ids=graph_model_ids,
    )
    budget = _MODELS_INQUIRY_CHAR_BUDGET if inquiry_mode else _MODELS_CHAR_BUDGET
    lines = ["  <models>"]
    if inquiry_mode:
        lines.append(
            "    manifest_mode: compact; use <inquiry_context_packet> as the "
            "primary evidence summary. Full rows are limited to actionable "
            "anchors."
        )

    used = 0
    detailed_rows = 0
    for idx, model in enumerate(bundle.models):
        for actor_id in _model_scope_actors(model):
            actor_mentions[str(actor_id)] = actor_mentions.get(str(actor_id), 0) + 1

        model_id = _model_id(model)
        wants_detail = (
            not inquiry_mode
            or model_id in actionable_ids
            or (not actionable_ids and idx < 2)
        )
        include_detail = wants_detail and detailed_rows < _MODEL_DETAIL_ROW_LIMIT
        piece = _format_model_row(
            model,
            selected_model_ids=selected_model_ids,
            graph_model_ids=graph_model_ids,
            detail=include_detail,
        )
        if used + len(piece) > budget:
            remaining = max(0, len(bundle.models) - idx)
            lines.append(
                f"    - [truncated — {remaining} more model manifest rows omitted]"
            )
            break
        lines.append(piece)
        used += len(piece)
        if include_detail:
            detailed_rows += 1

    lines.append("  </models>")
    return lines


def _format_model_row(
    model: Any,
    *,
    selected_model_ids: set[str],
    graph_model_ids: set[str],
    detail: bool,
) -> str:
    model_id = _model_id(model)
    natural = _model_natural(model)
    scope_entities_repr = _model_scope_entities_repr(
        model,
        limit=400 if detail else 180,
    )
    scope_actors = _model_scope_actors(model)
    scope_actors_repr = (
        "[" + ",".join(str(actor_id) for actor_id in scope_actors) + "]"
        if scope_actors else "[]"
    )
    falsifier = getattr(model, "falsifier", None)
    falsifier_kind = falsifier.get("kind") if isinstance(falsifier, dict) else None
    detail_label = "full" if detail else "manifest"
    text_label = "natural" if detail else "summary"
    text_limit = _MODEL_DETAIL_CHAR_LIMIT if detail else _MODEL_MANIFEST_CHAR_LIMIT
    return (
        f"    - id={model_id} detail={detail_label} "
        f"kind={getattr(model, 'proposition_kind', 'unknown')} "
        f"retrieval={_retrieval_tags(model_id, selected_model_ids, graph_model_ids)} "
        f"conf={_score(getattr(model, 'confidence', None))} "
        f"act={_score(getattr(model, 'activation', None))} "
        f"falsifier={falsifier_kind} "
        f"status={getattr(model, 'status', 'unknown')} "
        f"scope_actors={scope_actors_repr} "
        f"scope_entities={scope_entities_repr} "
        f"{text_label}={_trunc(natural, text_limit)}"
    )


def _model_id(model: Any) -> str:
    return str(getattr(model, "id", "unknown"))


def _model_natural(model: Any) -> str:
    natural = getattr(model, "natural", None)
    if natural:
        return str(natural)
    proposition = getattr(model, "proposition", None)
    if proposition is not None:
        return json.dumps(proposition, sort_keys=True, default=str)
    return ""


def _model_scope_actors(model: Any) -> list[Any]:
    actors = getattr(model, "scope_actors", None) or []
    return list(actors)


def _model_scope_entities_repr(model: Any, *, limit: int) -> str:
    entities = getattr(model, "scope_entities", None) or []
    compact = [
        {"type": e.get("type"), "id": str(e.get("id"))}
        for e in entities
        if isinstance(e, dict)
    ]
    return _trunc(json.dumps(compact, default=str), limit)


def _score(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "n/a"


def _inquiry_context_packet(bundle: ContextBundle) -> dict[str, Any] | None:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    packet = notes.get("inquiry_context_packet")
    return packet if isinstance(packet, dict) else None


def _actionable_model_ids(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    graph_model_ids: set[str],
) -> set[str]:
    ids = set(graph_model_ids)
    if trigger.model_id:
        ids.add(str(trigger.model_id))
    ids.update(str(mid) for mid in (trigger.member_model_ids or []))
    ids.update(_model_ids_from_inquiry_packet(bundle))
    return ids


def _model_ids_from_inquiry_packet(bundle: ContextBundle) -> set[str]:
    packet = _inquiry_context_packet(bundle)
    if packet is None:
        return set()
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    evidence = tiers.get("decisive_evidence") or []
    ids: set[str] = set()
    for item in evidence:
        if not isinstance(item, dict):
            continue
        model_id = _model_id_from_evidence_ref(item)
        if model_id:
            ids.add(model_id)
    for group in tiers.get("supporting_evidence_groups") or []:
        if not isinstance(group, dict):
            continue
        claim_supported = str(group.get("claim_supported") or "")
        if claim_supported in {"", "model"}:
            continue
        for ref in group.get("source_refs") or []:
            model_id = _model_id_from_source_ref(ref)
            if model_id:
                ids.add(model_id)
    return ids


def _model_id_from_evidence_ref(item: dict[str, Any]) -> str | None:
    if item.get("source_type") != "model":
        return None
    for key in ("source_ref", "raw_content_ref", "source_ref_id"):
        value = item.get(key)
        if not value:
            continue
        model_id = _model_id_from_source_ref(value)
        if model_id:
            return model_id
        if key == "source_ref_id":
            return str(value)
    return None


def _model_id_from_source_ref(value: Any) -> str | None:
    text = str(value)
    if text.startswith("model:"):
        return text.split(":", 1)[1]
    return None


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
            "    Do not emit reciprocal or transitive loops for DAG-scoped "
            "edges: supports, instance_of, contributes_to_resolution, and "
            "superseded_by. If a selected graph path already runs from the "
            "target back to the source, omit the edge."
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


def _build_inquiry_context_packet_section(bundle: ContextBundle) -> list[str]:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    packet = notes.get("inquiry_context_packet")
    if not isinstance(packet, dict):
        return []
    verdict = packet.get("sufficiency_verdict")
    hypotheses = packet.get("hypotheses") or []
    questions = packet.get("question_path") or []
    tiers = packet.get("tiers") or {}
    decisive = tiers.get("decisive_evidence") or []
    supporting = tiers.get("supporting_evidence_groups") or []
    omissions = tiers.get("omission_ledger") or []

    lines = ["  <inquiry_context_packet>"]
    lines.append(
        "    This packet is the adaptive inquiry summary. Treat it as "
        "retrieval guidance and provenance, not as permission to invent ids."
    )
    lines.append(
        "    signal_summary: "
        + _trunc(str(packet.get("signal_summary") or ""), 700)
    )
    if isinstance(verdict, dict):
        lines.append(
            "    sufficiency: "
            + _trunc(json.dumps(verdict, sort_keys=True, default=str), 900)
        )
    if hypotheses:
        lines.append("    hypotheses:")
        for h in hypotheses[:5]:
            lines.append(
                "      - "
                + _trunc(json.dumps(h, sort_keys=True, default=str), 500)
            )
    if questions:
        lines.append("    question_path:")
        for q in questions[:8]:
            qid = q.get("question_id") if isinstance(q, dict) else None
            question = q.get("question") if isinstance(q, dict) else q
            primitive = q.get("primitive") if isinstance(q, dict) else None
            lines.append(
                f"      - {qid or 'Q'} [{primitive or 'question'}]: "
                + _trunc(str(question), 260)
            )
    if decisive:
        lines.append("    decisive_evidence:")
        for e in decisive[:12]:
            lines.append(
                "      - "
                + _trunc(json.dumps(e, sort_keys=True, default=str), 700)
            )
    if supporting:
        lines.append("    supporting_evidence_groups:")
        for group in supporting[:6]:
            lines.append(
                "      - "
                + _trunc(json.dumps(group, sort_keys=True, default=str), 500)
            )
    if omissions:
        lines.append("    omission_ledger:")
        for item in omissions[:6]:
            lines.append(
                "      - "
                + _trunc(json.dumps(item, sort_keys=True, default=str), 400)
            )
    lines.append("  </inquiry_context_packet>")
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
            "ledger. Co-emit the fact Model AND the recommendation; "
            "they are not redundant. Use the create-commitment payload "
            "shape in the system prompt."
        )
    elif trigger.kind == "T2" and trigger.subkind == "belief_updated":
        body.append(
            "This is a T2:belief_updated trigger — a new fact or concern "
            "model was just inserted by a T1 run. Decide whether the CEO "
            "needs to act on this belief.\n"
            "\n"
            "  • If a team member is blocked, waiting on a decision, or "
            "the CEO needs to unblock someone: emit ONE claim_op with "
            "`kind='norm'` and `claim_role='recommendation'`. Use only actor UUIDs that "
            "appear in <actors_in_context> for scope_actors. Write the "
            "natural field as a clear, actionable sentence for the CEO.\n"
            "\n"
            "  • If the new fact Model encodes a self-report of new "
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
            "not create recap, elaboration, or bookkeeping fact Models.\n"
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
        if trigger.subkind == "latent_relationship_candidate":
            body.append(
                "This is a T4 trigger from the topology layer — a latent "
                "relationship candidate. Topology here means a "
                "consequence-sensitive discovery field, not accepted graph "
                "layout. Treat it as evidence, not truth.\n"
                "\n"
                "Your job is to inspect the candidate member Models and "
                "decide whether the topology signal should become durable "
                "knowledge:\n"
                "  - emit an edge_op or edge candidate when a precise "
                "pairwise relation is real;\n"
                "  - emit an ontology_gap_op when the pairwise relation is "
                "real and useful but no registered edge_kind captures it;\n"
                "  - emit a `situation` Model when the members are symptoms "
                "of one operational condition;\n"
                "  - update/archive only when an existing Model is clearly "
                "changed by this candidate;\n"
                "  - return an empty diff when the relationship is merely "
                "surface similarity or shared noise.\n"
                "\n"
                "STRICT CONSTRAINTS:\n"
                "  - Use only member Model ids from <models>, "
                "<reasoning_frame>, or the trigger payload.\n"
                "  - Do not create action mutations from topology alone.\n"
                "  - Explain the consequence: which flow/pressure/customer/"
                "actor/commitment changes meaning if this candidate is true."
            )
        else:
            body.append(
                "This is a T4 trigger — background / maintenance / dependent "
                "re-evaluation. If the trigger carries a cause_model_id and "
                "cause_kind, update the dependent Model's confidence or "
                "archive it as appropriate."
            )
    elif trigger.kind == "T6":
        # T6 is retained for legacy accepted-memory graph phase events.
        # New topology candidates arrive as T4 latent_relationship_candidate.
        #   - Optionally NAME the neighborhood (overwrite the heuristic).
        #   - Decide whether the structural shift warrants a CEO-facing
        #     `recommendation` claim_op.
        #   - Update confidence / status on Models that no longer fit
        #     their (former) neighborhood, when warranted.
        # See the <topology_context> section above for what changed.
        body.append(
            "This is a T6 trigger — a legacy accepted-memory graph phase "
            "event. A stored neighborhood structure just shifted; "
            "see <topology_context> above for details (kind, magnitude, "
            "members, neighborhood lineage). The seed neighborhood, "
            "predecessor neighborhoods, and member Model ids are all "
            "in the trigger payload.\n"
            "\n"
            "Decide whether this structural shift warrants any of:\n"
            "  • Naming the neighborhood — emit a `claim_op.update` on "
            "    one of the member Models recording a `belief` "
            "    proposition that captures the cluster's theme. The "
            "    heuristic name is in <topology_context>; if a more "
            "    accurate human-readable description fits, write a "
            "    fact Model whose subject is the neighborhood theme.\n"
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
        "customer_context, actors_in_context). If the signal names a PR or "
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
