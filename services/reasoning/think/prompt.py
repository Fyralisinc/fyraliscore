"""services/reasoning/think/prompt.py — build the prompt for LLM reasoning.

Spec §7 "Prompt construction for LLM reasoning".

Structure:
  system:  "You are the reasoning component..." + falsifier rules +
           diff schema + operating discipline +
           <operating_instructions>.
  user:    Reasoning profile for this call
           <triggering_event>
           <retrieved_context>
             <observations>
             <models>
             <acts>
             <resources>
             <actor_context>
             <customer_context>
           </retrieved_context>

Token-budget heuristic: we truncate section bodies at a conservative
character budget per section. The ContextBundle already caps
observations/models/acts/resources quantity, so we mostly just need to
prevent a stray 100KB content_text from blowing the window.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.domain.models.formation import build_model_formation_candidates

from .reasoning_frame import ReasoningFrame, reasoning_job_from_trigger


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    """Cost-plan §1.3: env-overridable prompt budgets. Read once at import;
    operators tune via env before the worker starts (the deployment model).
    Falls back to `default` on missing/blank/non-int, never below `minimum`."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


# Per-section char budgets. Cost-plan §1.3 makes these env-overridable
# (THINK_*_CHAR_BUDGET) so a prompt section can be tightened without a deploy.
# Defaults are the historical hardcoded values, so unset env = no behavior
# change.
_OBS_CHAR_BUDGET = _env_int("THINK_OBS_CHAR_BUDGET", 4000)
_MODELS_CHAR_BUDGET = _env_int("THINK_MODELS_CHAR_BUDGET", 4000)
_MODELS_INQUIRY_CHAR_BUDGET = _env_int("THINK_MODELS_INQUIRY_CHAR_BUDGET", 2400)
_MODELS_COMPILED_DECISION_CHAR_BUDGET = _env_int(
    "THINK_MODELS_COMPILED_DECISION_CHAR_BUDGET",
    1600,
)
_ACTS_CHAR_BUDGET = _env_int("THINK_ACTS_CHAR_BUDGET", 12000)
_ACTS_COMPILED_DECISION_CHAR_BUDGET = _env_int(
    "THINK_ACTS_COMPILED_DECISION_CHAR_BUDGET",
    3000,
)
_RESOURCES_CHAR_BUDGET = _env_int("THINK_RESOURCES_CHAR_BUDGET", 1000)
_SUBSTRATE_CHAR_BUDGET = _env_int("THINK_SUBSTRATE_CHAR_BUDGET", 3500)
# Previously-unbudgeted sections (cost-plan §1.3 tail protection). The
# relationship-candidates cap layers a char budget on top of the existing
# count cap; both fold into the same "omitted" marker.
_CANDIDATES_CHAR_BUDGET = _env_int("THINK_CANDIDATES_CHAR_BUDGET", 12000)
_PER_ITEM_CHAR_LIMIT = 1500
_MODEL_DETAIL_CHAR_LIMIT = 700
_MODEL_MANIFEST_CHAR_LIMIT = 220
_MODEL_COMPILED_DETAIL_CHAR_LIMIT = 420
_MODEL_COMPILED_MANIFEST_CHAR_LIMIT = 150
_MODEL_DETAIL_ROW_LIMIT = 8
_MODEL_COMPILED_DETAIL_ROW_LIMIT = 5
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
- <candidate_substrate> contains evidence-backed provisional company objects.
  You may use its exact `scope_ref` values in scope_entities when canonical
  actors/customers/workstreams/commitments/systems/vendors/patterns are not yet
  available. Candidate refs are not canonical truth: never put candidate UUIDs
  in scope_actors, and never rewrite a candidate UUID into another type.
- Keep diffs small. Most single events warrant 0-3 claim_ops, 0-1
  relation_claim_ops/edge_ops, 0-1 relation_frame_ops, and 0 act_ops. Batch
  or graph-anchor calls may legitimately need 2-4 relation claims when several
  selected Model relationships changed. Empty diffs are
  valid when memory already captures the event and no state/action/relationship
  should change.
- For T1 event batches, read the whole batch as evidence but do not emit an op
  per signal. Preserve background/duplicate/noisy signals in reasoning_trace;
  only promote the batch-level claims, situations, edges, predictions, or acts
  whose absence would make the durable world model materially less useful.
- If <inquiry_context_packet> includes memory_decision_candidates, treat them
  as the primary advisory decision surface, not as writes. For each material
  candidate, accept it through the right op, update/merge/reject it, or no-op it
  in reasoning_trace. Add a missing op only when the packet missed a durable
  memory change.
- If <model_formation_candidates> appears, each listed candidate is a required
  belief-formation decision. For every candidate, emit exactly one
  formation_resolutions item with resolution formed, updated, deferred,
  rejected, or already_covered. Use claim_ops/memory_lifecycle_ops for any
  durable Model change; formation_resolutions is accountability, not a write.
- If <inquiry_context_packet> says mode=compiled_memory_decision_boundary, do
  not reconstruct the hidden planner path. The packet has already compressed the
  batch; adjudicate the listed candidates, use the minimal cited evidence/model
  context, and emit only the durable diff that remains.
- For topology / relationship-candidate T4 calls, the candidate is pre-truth
  evidence, not a mandate to promote. Prefer an empty diff with exact UUIDs in
  reasoning_trace unless the candidate adds decision-relevant structure: a
  sharp grounded edge, a composite situation with marginal insight beyond its
  members, or a targeted update/archive to existing memory.
- Never silently ignore selected context. If any selected Model or Observation
  changes the diff, cite its full UUID in the relevant op. If selected context
  is irrelevant, reasoning_trace must cite at least one selected full UUID and
  say why it did not warrant a state, edge, action, or resource change.

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
  "memory_lifecycle_ops": [],
  "relation_claim_ops": [],
  "relation_frame_ops": [],
  "edge_ops": [],
  "ontology_gap_ops": [],
  "open_question_ops": [],
  "formation_resolutions": [],
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
    "scope_entities": [{"type":"customer|commitment|goal|decision|resource|candidate_actor|candidate_actor_alias|candidate_customer|candidate_workstream|candidate_system|candidate_vendor|candidate_commitment|candidate_pattern","id":"<uuid>"}],
    "scope_temporal": {"valid_from":"<ISO-8601>","valid_until":"<ISO-8601|null>"},
    "semantic_terms": ["<specific lexical phrase>", ...],
    "falsifier": {"kind":"<falsifier kind>", "...":"..."} | null
  }
}
Do NOT include title, description, embedding, id, claim, or unknown fields.
- update: {"op":"update","model_id":"<uuid>","changes":{...}}
- archive: {"op":"archive","model_id":"<uuid>","reason":"decay|falsifier_triggered|contested_incorrect|contested_reading_incorrect|superseded|manual|resolved_confirmed|resolved_violated|severe_drift|deprecated|acted_upon|dismissed_by_user"}

formation_resolutions item shape:
{"op":"resolve","candidate_id":"<candidate_id from model_formation_candidates>","resolution":"formed|updated|deferred|rejected|already_covered","rationale":"<why>","output_model_ids":["<existing model uuid>",...],"follow_up_question":"<question|null>"}
For newly inserted Models, do not invent output_model_ids; leave it empty and
cite the created claim in rationale.

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

Semantic terms:
- `semantic_terms` are top-level Model fields, not proposition fields.
- Add 6-16 compact lexical handles that make this exact belief findable by
  surface-language retrieval, e.g. "partial refund edge case",
  "idempotency key collision", "founder review bandwidth", "enterprise
  renewal delay".
- Prefer specific 2-4 word phrases over broad categories.
- Do NOT include actor names, customer/company/system/vendor names, UUIDs,
  PR/ticket handles, source channels, dates/times, exact `domain_tags`,
  `claim_role`, or anything already represented in `scope_actors`,
  `scope_entities`, or grammar axes.
- Every term must be grounded in the claim's natural text, proposition,
  falsifier, or resolution criteria. Do not invent SEO-like keywords.

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
  situations for simple pairwise links that should be relation_claim_ops.
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
- Employee formation lens: when selected observations or <actor_context> show
  repeated evidence about the same internal actor, form actor-scoped Models that
  make the operating profile useful. Use `capability` for demonstrated skills
  or capacity, `relation` for stable work-style/preferences/collaboration
  contexts, `concern` for support needs or load risks, and `pattern` for
  recurring behavior. Prefer specific operational claims ("needs uninterrupted
  design blocks before architecture review") over personality labels. If new
  evidence weakens an older employee belief, use lifecycle/supersession instead
  of adding a conflicting sibling.
- scope_entities: {"type":"customer|commitment|goal|decision|resource",
  "id":"<uuid>"} from <acts>, <resources>, or customer_context. Resolve PR/ticket
  handles (PR #847, ENG-501) to the matching commitment UUID in <acts>. Customer
  names resolve to relational resources. Customer-specific commitment signals
  should usually include both customer and commitment entities.
- When no canonical UUID exists, use an exact candidate `scope_ref` from
  <candidate_substrate> in scope_entities. This is preferred over leaving scope
  empty for meaningful actor/customer/workstream/system/vendor/commitment/pattern
  claims, but it remains provisional and should not be treated as canonical.
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
  Prefer memory_lifecycle_ops, claim_ops.update, archive, or relation_claim_ops
  over a duplicate sibling Model.
  Archive only with a registered lifecycle reason such as `decay`, `superseded`,
  `contested_incorrect`, `resolved_confirmed`, or `severe_drift`.
- Repeated evidence for the same operational reality should strengthen the
  existing Model's evidence trail; only insert a new Model when the signal adds
  a materially new belief, forecast, blocker, or causal explanation.
- Repeated wording is not enough to call two facts duplicates. First bind the
  observation to actor/action/object/work-item/repo/source/thread/time context.
  "raised a PR" for different actors, issues, repos, threads, or workstreams is
  different company meaning and should become distinct evidence, distinct
  models, or a recurrence/pattern model rather than a silent no-op.
- When repetition itself is meaningful, prefer a pattern/source-digest claim
  with `claim_role="pattern"`, `time_mode="recurring"`, and domain tags such as
  contextual_recurrence, source_digest, review_loop, delivery_risk,
  coordination_debt, finance_flow, or operational_churn.
- A new T1 signal that asserts progress, approval, review feedback, a blocker,
  a concrete concern, a customer stance, or a dated plan usually deserves a
  claim_ops.insert even when no act transition is warranted. Do not no-op merely
  because the event is "only" a review, comment, suggestion, progress update, or
  plan. No-op T1 only when the observation is non-substantive or an existing
  selected Model already captures the same fact at suitable confidence.

Document structured summaries:
- When <retrieved_context> contains a <document_structured_summary> block, a
  large document (meeting transcript, doc, page) was ingested and distilled to
  decisions / commitments / risks. Treat it as the primary evidence and turn it
  into durable Models:
  * Emit ONE situation anchor: kind="belief", claim_role="situation",
    abstraction_level="composite", confidence <= 0.7 (so no falsifier is
    required), summary = the document's gist, member_model_ids = the per-item
    Models you create. The anchor is recallable even with empty scope.
  * Each DECISION -> a claim_ops.insert with claim_role="recommendation" when it
    proposes a concrete Act/Resource change, otherwise claim_role="fact"
    (time_mode="current").
  * Each RISK -> claim_role="concern", polarity="negative".
  * Each COMMITMENT / action_item with a due date -> kind="prediction",
    claim_role="prediction", scope_temporal.valid_from = the document's
    occurred_at, AND set both evaluate_at = the due date and a
    {"kind":"prediction_deadline","evaluate_at":"<due ISO-8601>","check":"<what
    would show the commitment was missed>"} falsifier, plus a one-line
    resolution restating the completion criterion. A commitment without a due
    date is a recommendation/fact, not a prediction.
- Provenance is free: set born_from_event_id = the triggering observation_id on
  every document Model; do NOT invent provenance edges.
- Scope from the block's resolved_scope_entities / resolved_scope_actors ONLY
  (they are already validated). Never put unresolved_owner_names in scope_actors
  — keep them as text in `natural`. An empty scope is acceptable; the anchor is
  still semantically recallable.
- Link the Models with EXISTING edge kinds only: anchor->member via
  `instance_of`/`supports`, risk->decision via `explains`/`relates`. Do not
  propose new edge kinds for document provenance.
- DEDUPE: before inserting, check the retrieved Models in THIS context. A second
  document restating the same commitment/risk should reconcile (confirm/revise)
  the existing Model via memory_lifecycle_ops, not insert a duplicate.

memory_lifecycle_ops:
{ "op":"reconcile",
  "model_id":"<existing model uuid>",
  "action":"confirm|falsify|revise|unchanged|archive|supersede",
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "confidence_delta":-1.0-1.0|null,
  "confidence":0.0-1.0|null,
  "resolution_outcome":true|false|null,
  "rationale":"<why this existing memory changed or was checked>",
  "reason":"<archive reason|null>",
  "superseded_by_model_id":"<replacement model uuid|null>",
  "metadata":{} }
- Use memory_lifecycle_ops when a selected existing prediction, situation,
  pattern, concern, recommendation, or compressed memory is confirmed,
  contradicted, resolved, revised, unchanged after review, archived, or
  superseded by the triggering evidence.
- Use action="confirm" or "falsify" for predictions whose outcome is now known.
  This compiles to confidence/count updates plus resolution_outcome for
  prediction Models.
- Use action="revise" when new evidence materially changes confidence or
  evidence support without requiring a new sibling Model. Use action="unchanged"
  when a selected memory was explicitly evaluated and remains valid.
- Use action="archive" only with a registered lifecycle reason. Use
  action="supersede" with superseded_by_model_id when a replacement Model exists.

open_question_ops:
{ "op":"insert|resolve|archive",
  "id":"<optional question uuid for insert|null>",
  "question_id":"<existing question uuid for resolve/archive|null>",
  "model_id":"<target model uuid or same-diff born_from_event_id>",
  "question":"<specific unresolved question|null>",
  "question_type":"evidence_gap|temporal_status|causal_mechanism|constraint_boundary|owner_or_decision|impact_scope|contradiction_check|projection_gap|other",
  "rationale":"<why answering this materially improves the belief|null>",
  "priority":0.0-1.0,
  "expected_resolution_signal":{"signal_shape":"<what kind of evidence would answer it>"},
  "search_signature":{"semantic_terms":["<specific phrase>",...],"hints":["<search hint>",...]},
  "source_model_ids":["<model uuid>",...],
  "resolution_model_id":"<model uuid that answers it|null>",
  "resolution_note":"<why it is resolved/archived|null>",
  "status":"resolved|stale|superseded|duplicate|archived|null" }
- Use open_question_ops only when an unresolved question would materially change
  confidence, scope, falsifiability, projection, or actionability of a Model.
  Do not write generic "need more data" questions.
- Do not duplicate information already represented by scope_actors,
  scope_entities, domain_tags, semantic_terms, or grammar axes. The question is
  the missing evidence boundary, not another label for the Model.
- `search_signature` should contain specific lexical handles and search hints
  for retrieval. Avoid actor/customer/company/system names, UUIDs, source
  channels, broad categories, and fields already represented elsewhere.
- Prefer question_type="constraint_boundary" for missing resource/runway/owner
  limits, "temporal_status" for stale current-state uncertainty, and
  "contradiction_check" when the next useful search is for counterevidence.

relation_claim_ops:
{ "op":"upsert",
  "source_model_id":"<model uuid|null>",
  "target_model_id":"<model uuid|null>",
  "subject_ref":{"kind":"model|text","model_id":"<uuid optional>","text":"<span optional>"},
  "object_ref":{"kind":"model|text","model_id":"<uuid optional>","text":"<span optional>"},
  "predicate":"<relation verb, usually same as edge_kind>",
  "edge_kind":"supports|contradicts|weakens|causes|explains|predicts|blocks|enables|same_issue_as|co_occurs_with|analogous_to|alternative_to|early_warning_for|instance_of|contributes_to_resolution|superseded_by",
  "direction":"source_to_target|target_to_source|symmetric|unknown",
  "endpoint_binding_status":"bound|partially_bound|unbound|ambiguous",
  "write_policy":"accepted_edge|candidate|needs_review|no_edge",
  "status":"active|accepted|candidate|needs_review|rejected|retired",
  "confidence":0.0-1.0, "binding_confidence":0.0-1.0,
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "evidence_text":"<grounded evidence span>",
  "explanation":"<why this relation is true>", "metadata":{} }
- Prefer relation_claim_ops for relationship-bearing facts. A bound,
  high-confidence relation claim with write_policy="accepted_edge" writes a
  durable model_edges row in the same transaction, while preserving the
  relation claim lifecycle for future learning.
- Use write_policy="candidate" or "needs_review" when the predicate is valuable
  but endpoint binding, direction, or mechanism is not decisive enough.
- Use write_policy="no_edge" only when explicitly recording why a tempting
  relation should not become graph truth.

relation_frame_ops:
{ "op":"upsert",
  "relation_kind":"blocked_workstream",
  "participants":[
    {"model_id":"<uuid>","role":"blocker","binding_confidence":0.0-1.0},
    {"model_id":"<uuid>","role":"blocked_work","binding_confidence":0.0-1.0},
    {"model_id":"<uuid>","role":"owner","binding_confidence":0.0-1.0},
    {"model_id":"<uuid>","role":"downstream_risk","binding_confidence":0.0-1.0},
    {"model_id":"<uuid>","role":"possible_resolution","binding_confidence":0.0-1.0}
  ],
  "participant_binding_status":"bound|partially_bound|unbound|ambiguous",
  "write_policy":"project_edges|candidate|needs_review|no_projection",
  "status":"active|candidate|accepted|needs_review|disputed|rejected|retired",
  "confidence":0.0-1.0,
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "evidence_text":"<grounded evidence span>",
  "explanation":"<why these Models participate in one relation frame>" }
- Use relation_frame_ops when one relation needs 3+ typed participant roles,
  not just one source and one target. Example: a DPA blocker, a HubSpot import,
  the owner, a launch-date risk, and a legal approval path are one
  blocked_workstream frame.
- Use relation_claim_ops for simple pairwise relationships. Do not wrap a
  two-model `blocks`, `contradicts`, or `supports` relation in a frame.
- Use relation_kind="blocked_workstream" only when the roles are clear. With
  write_policy="project_edges" and status="accepted", the system stores the
  frame and deterministically projects only precise binary edges:
  blocker->blocked_work as blocks, blocked_work->downstream_risk as
  early_warning_for, and possible_resolution->blocker as
  contributes_to_resolution. Owner/accountable roles remain frame participants
  unless an explicit registered edge kind is also warranted.
- Use write_policy="candidate" or "needs_review" when the group relation is
  meaningful but one participant role, mechanism, or projection is uncertain.

edge_ops:
{ "op":"add|retire", "source_model_id":"<uuid>", "target_model_id":"<uuid>",
  "edge_kind":"supports|contradicts|weakens|causes|explains|predicts|blocks|enables|same_issue_as|co_occurs_with|analogous_to|alternative_to|early_warning_for|instance_of|contributes_to_resolution|superseded_by",
  "weight":0.0-1.0|null, "confidence":0.0-1.0,
  "evidence_event_ids":["<observation uuid>",...],
  "evidence_model_ids":["<model uuid>",...],
  "explanation":"<grounded reason>", "metadata":{},
  "review_status":"accepted|candidate|needs_review|disputed", "reason":"<for retire>" }
- Direct edge_ops are the compatibility surface for relationships between Models:
  support, contradiction,
  weakening, causal/explanatory links, blockers/enablers, shared issues,
  co-occurrence, analogy, alternatives, early warnings, instance_of,
  contributes_to_resolution, superseded_by.
- Treat operational edge kinds as the durable graph backbone, preferably via
  relation_claim_ops:
  blocks, explains, weakens, contradicts, early_warning_for,
  contributes_to_resolution, enables, and supports. Similarity kinds
  (same_issue_as, analogous_to, co_occurs_with) are weak/review-only and
  should not substitute for a real blocker, warning, contradiction,
  explanation, or resolution relation.
- Relationship decision contract: when graph-selected Models or candidate
  member Models are relevant, explicitly choose one of:
  (a) emit the sharpest registered edge_op;
  (b) emit an ontology_gap_op because every registered edge loses important
      decision semantics;
  (c) update/archive a Model because that is the stronger representation; or
  (d) write `no edge:` plus the relevant full UUIDs and reason in
      reasoning_trace. Do not leave relational graph context only in prose.
- Prefer the sharpest true edge. Use co_occurs_with/same_issue_as/analogous_to
  only when the evidence does not justify a causal, blocking, explanatory,
  weakening, contradiction, warning, enabling, or resolution relationship.
- If the explanation would say "similar but not a direct dependency/causal
  relation", prefer `no edge:` unless the similarity itself is
  decision-relevant enough to keep as a candidate.
- Quick edge-kind guide: `blocks` = source prevents target progress;
  `explains` = source is the mechanism/reason for target; `weakens` =
  source is counterevidence against target; `contradicts` = both cannot be
  true; `enables` = source makes target possible; `contributes_to_resolution`
  = source helps settle or resolve target; `supports` = source adds evidence
  without a sharper relationship.
- Use `review_status="disputed"` when the relation itself is actively contested
  and both endpoints should remain visible for uncertainty/review.
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
  `blocks`, `contradicts`, etc. is precise enough, use relation_claim_ops.
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
resource_ops, memory_lifecycle_ops, or predictions in this pass; explain omitted
action/edge/lifecycle reasoning briefly in reasoning_trace when relevant.

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
- <candidate_substrate> contains evidence-backed provisional company objects.
  Use its exact `scope_ref` values in scope_entities when no canonical UUID is
  available; never place candidate UUIDs in scope_actors.
- Empty diffs are valid only when selected memory already captures the signal or
  the signal is non-substantive. If selected context is irrelevant to an empty
  or non-empty diff, reasoning_trace must cite at least one selected full UUID
  and say why it did not warrant a claim, edge, action, or resource change.
- Never abbreviate UUIDs in reasoning_trace.
- For T1 event batches, read all batch observations as evidence but do not emit
  one claim per signal. Promote only durable batch-level facts, concerns,
  predictions, situations, or recommendations; cite background, duplicate, or
  noisy signals in reasoning_trace when they do not warrant a claim.
- If <inquiry_context_packet> includes memory_decision_candidates, use claim or
  no-op candidates as the primary advisory decision surface. Do not emit edge,
  act, resource, or prediction ops from this compact schema; mention omitted
  non-claim candidates in reasoning_trace when relevant.
- If a selected prediction, situation, pattern, recommendation, or compressed
  memory should be confirmed, falsified, revised, archived, or superseded, do
  not create a duplicate claim just because this compact schema cannot emit
  memory_lifecycle_ops. Cite the exact Model UUID and the omitted lifecycle
  action in reasoning_trace.

Return exactly this JSON shape:
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "formation_resolutions": [],
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
    "scope_entities": [{"type":"customer|commitment|goal|decision|resource|candidate_actor|candidate_actor_alias|candidate_customer|candidate_workstream|candidate_system|candidate_vendor|candidate_commitment|candidate_pattern","id":"<uuid>"}],
    "scope_temporal": {"valid_from":"<ISO-8601>","valid_until":"<ISO-8601|null>"},
    "semantic_terms": ["<specific lexical phrase>", ...],
    "falsifier": {"kind":"<falsifier kind>", "...":"..."} | null
  }
}
Do NOT include title, description, embedding, id, claim, or unknown fields.

formation_resolutions item shape:
{"op":"resolve","candidate_id":"<candidate_id from model_formation_candidates>","resolution":"formed|updated|deferred|rejected|already_covered","rationale":"<why>","output_model_ids":["<existing model uuid>",...],"follow_up_question":"<question|null>"}
If <model_formation_candidates> appears, every candidate requires exactly one
formation_resolutions item. Use claim_ops.insert for formed beliefs; do not
invent Model UUIDs for new inserts.

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
Semantic terms: add 6-16 top-level `semantic_terms` per inserted claim. They
must be specific belief phrases, not actor/entity names, UUIDs, PR/ticket
handles, source channels, dates, exact `domain_tags`, `claim_role`, or anything
already captured by scope/grammar. Prefer phrases like "partial refund edge
case", "idempotency key collision", "founder review bandwidth".

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
- Employee formation lens: when there is repeated evidence about one internal
  actor, create specific actor-scoped Models for demonstrated capability,
  work-style/preference, support need, load risk, relationship/collaboration
  pattern, or recurring behavior. Avoid generic personality labels. If the new
  signal changes an older employee belief, prefer lifecycle/supersession over a
  duplicate conflicting sibling.
- scope_entities comes from <acts>, <resources>, customer_context, or exact
  candidate refs in <candidate_substrate>. Resolve PR numbers and ticket IDs to
  matching commitment UUIDs in <acts>; customer names to relational resources;
  goal phrases to goals. Never invent UUIDs.
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

Document structured summaries:
- When <retrieved_context> has a <document_structured_summary> block, a large
  document was distilled to decisions/commitments/risks. Mint Models from it:
  one situation anchor (claim_role="situation", abstraction_level="composite",
  confidence<=0.7), each decision -> recommendation/fact, each risk ->
  claim_role="concern" with polarity="negative", each commitment/action_item
  with a due date -> kind="prediction" with claim_role="prediction", evaluate_at
  set to the due date, AND a {"kind":"prediction_deadline","evaluate_at":"<due
  ISO-8601>","check":"..."} falsifier plus a resolution criterion.
- born_from_event_id = the triggering observation_id on every document Model.
  Scope only from the block's resolved_scope_entities / resolved_scope_actors;
  never invent UUIDs and never put unresolved owner names in scope_actors. Dedupe
  against retrieved Models in this context (reconcile, don't duplicate).

Return only well-formed JSON, no prose outside the JSON object.
"""


@dataclass
class PromptPair:
    system: str
    user: str


@dataclass(frozen=True)
class PromptSurface:
    """Static prompt bucket selected for one Think call."""

    packs: tuple[str, ...]
    claims_only: bool = False
    lean_output_contract: bool = False

    def includes(self, pack: str) -> bool:
        return pack in self.packs

    def to_prompt_section(self) -> str:
        lines = [
            "<prompt_surface>",
            "  version: surface_aware_v1",
            f"  schema: {'claims_only' if self.claims_only else 'full'}",
            "  packs: " + ", ".join(self.packs),
        ]
        if self.lean_output_contract:
            lines.append("  lean_output_contract: true")
        lines.append("</prompt_surface>")
        return "\n".join(lines)


_SURFACE_PROMPT_FLAG = "THINK_SURFACE_AWARE_PROMPT"

_SURFACE_CORE_PROMPT = """\
You are the reasoning component of Company OS. Produce the smallest useful JSON
diff for the triggering event. This surface-aware prompt is assembled from a
small invariant core plus only the operation packs that this call can use.

Core contract:
- Return only JSON matching the selected schema. No prose outside the object.
- The LLM proposes; validators constrain; appliers mutate durable state. Do not
  rely on prompt text as the final authority for schema or lifecycle safety.
- Observations are immutable evidence. Models are the semantic memory backbone.
  Acts and Resources are operational sidecars that change only through their
  explicit packs.
- Every emitted operation must be grounded in the triggering event, selected
  Observations, selected Models, Acts, Resources, candidate_substrate, or the
  reasoning_frame. Do not invent UUIDs, entities, edge kinds, states, or hidden
  facts.
- Respect <reasoning_frame> allowed_ops and budgets when present. If a pack is
  absent, mention any tempting but unavailable operation only in reasoning_trace.
- Keep diffs small. Empty diffs are valid when selected memory already captures
  the signal, the signal is non-substantive, or the available context does not
  support a durable write.
- Never silently ignore selected context. If selected context matters, cite full
  UUIDs in the relevant operation. If it does not matter, cite at least one full
  selected UUID in reasoning_trace and say why no state, edge, action, or
  resource change is warranted.
- Confidence is epistemic, not importance. Direct observed facts usually fit
  0.75-0.9; hearsay, hedging, missing context, future plans, aspirations, and
  conditional language usually stay <=0.7 unless independent evidence removes
  uncertainty.
- Models above 0.7 confidence require an adequate falsifier unless the schema
  or validator rejects falsifiers for that operation.

Falsifier kinds:
- observation_pattern: pattern plus within_window
- commitment_outcome: commitment_ref plus contradicting_state
- prediction_deadline: evaluate_at plus check
- resource_threshold: resource_ref plus threshold
- explicit_contestation: contesting_actors
"""

_SURFACE_FULL_SCHEMA_PROMPT = """\
Output schema for the full RawDiff pass:
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "memory_lifecycle_ops": [],
  "relation_claim_ops": [],
  "relation_frame_ops": [],
  "edge_ops": [],
  "ontology_gap_ops": [],
  "open_question_ops": [],
  "formation_resolutions": [],
  "act_ops": [],
  "resource_ops": [],
  "new_predictions": [],
  "reasoning_trace": "<brief rationale>"
}
Use exactly these top-level fields. Leave unavailable operation arrays empty.
"""

_SURFACE_CLAIMS_ONLY_SCHEMA_PROMPT = """\
Output schema for the compact claims-only pass:
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "formation_resolutions": [],
  "reasoning_trace": "<brief rationale>"
}
Do not emit edge_ops, act_ops, resource_ops, memory_lifecycle_ops,
open_question_ops, or predictions in this pass. Explain omitted non-claim work
briefly in reasoning_trace when it matters.
"""

_SURFACE_MODEL_MEMORY_PACK = """\
Surface pack: Model memory and claim formation.
- claim_ops.insert creates one scoped Model for one durable belief, observation,
  prediction, norm, relation, concern, capability, pattern, or situation.
- Insert entry fields: born_from_event_id, proposition, natural, confidence,
  scope_actors, scope_entities, scope_temporal, semantic_terms, falsifier.
  Do not include title, description, embedding, id, claim, or unknown fields.
- Proposition kind is only observation, belief, prediction, or norm. Put meaning
  into grammar fields such as claim_role, abstraction_level, time_mode,
  modality, polarity, and domain_tags.
- Use top-level semantic_terms: 6-16 specific lexical phrases grounded in the
  claim. Avoid names, UUIDs, dates, source channels, exact domain_tags,
  claim_role, and data already represented by scope fields or grammar axes.
- Scope every inserted Model using actor_id, existing Model scopes, Acts,
  Resources, customer_context, actors_in_context, or exact candidate_substrate
  scope_ref values. Never invent UUIDs.
- Prefer updating, reconciling, or no-oping selected existing Models over
  duplicate siblings. Insert only what materially changes retrieval, judgment,
  prediction, or action.
- If <model_formation_candidates> appears, emit exactly one formation_resolutions
  item for each candidate: formed, updated, deferred, rejected, or
  already_covered. Formation resolutions are accountability, not writes.
"""

_SURFACE_LIFECYCLE_PACK = """\
Surface pack: Model lifecycle, uncertainty, and questions.
- memory_lifecycle_ops reconcile selected Models when triggering evidence
  confirms, falsifies, revises, leaves unchanged, archives, or supersedes them.
- Use lifecycle over duplicate inserts when selected memory already captures the
  subject but its confidence, status, support, or resolution changed.
- Archive only with registered reasons such as decay, superseded,
  contested_incorrect, resolved_confirmed, resolved_violated, severe_drift,
  deprecated, acted_upon, or dismissed_by_user.
- open_question_ops are for missing evidence boundaries that would materially
  change confidence, scope, falsifiability, projection, or actionability. Do not
  write generic "need more data" questions.
- Predictions whose outcome is now known should use lifecycle confirm/falsify
  when possible; otherwise use a targeted update and explain the remaining
  uncertainty.
"""

_SURFACE_GRAPH_PACK = """\
Surface pack: Model graph and relationship topology.
- Relationship writes connect Models to Models. Edge endpoints must be existing
  Model ids from <models> or the born_from_event_id of a claim_ops.insert in the
  same diff. Never use observation, customer, commitment, goal, decision, or
  resource UUIDs as edge endpoints.
- Prefer relation_claim_ops for relationship-bearing facts. A precise accepted
  relation can project a durable edge while keeping relation evidence available
  for future review.
- Use relation_frame_ops when one relation needs 3+ typed participant roles. Use
  relation_claim_ops for simple pairwise relationships.
- Registered edge kinds include supports, contradicts, weakens, causes,
  explains, predicts, blocks, enables, same_issue_as, co_occurs_with,
  analogous_to, alternative_to, early_warning_for, instance_of,
  contributes_to_resolution, and superseded_by.
- Prefer sharp operational semantics over weak similarity. If the evidence only
  shows surface similarity, write no edge or a candidate with a clear reason.
- ontology_gap_ops propose pre-truth edge types only when a real useful relation
  is grounded but no registered edge kind preserves its decision semantics.
- Relationship decision contract: for important graph anchors, emit the sharpest
  relation/edge/frame, propose an ontology gap, update/archive a Model if that
  is stronger, or write `no edge:` with full UUIDs in reasoning_trace.
"""

_SURFACE_ACTS_PACK = """\
Surface pack: Acts and recommendations.
- act_ops mutate Goals, Commitments, and Decisions only when the signal itself
  warrants the state change. Do not create operational mutations from topology
  or selected context alone.
- Common commitment transitions: merged/deployed/closed work can move to
  doneunverified; independent evidence moves doneunverified to doneverified;
  explicit waiting/hold signals can pause active work; explicit unsatisfied
  dependencies can block active work.
- confidence_basis must be an existing Model id from <models> or the
  born_from_event_id of a same-diff claim_ops.insert. The applier rewrites
  same-diff event ids to inserted Model ids. Do not use arbitrary observation ids.
- Recommendations are norm Models for concrete human-approved Act or Resource
  changes. Do not recommend routine bookkeeping that validators/appliers can do.
- New self-reported in-flight work with no matching commitment should usually
  emit both a fact Model and a recommendation whose proposed_change creates a
  commitment with target_act_ref {"type":"commitment","id":null}.
"""

_SURFACE_RESOURCES_PACK = """\
Surface pack: Resources.
- resource_ops mutate durable resources, holdings, allocations, deployments,
  releases, valuations, or transactions only from explicit resource evidence.
- Scope claims to resource UUIDs from <resources> when a signal mentions budget,
  runway, capacity, vendor, license, tool, account, dataset, asset, contract, or
  infrastructure already present in context.
- For uncertain resource implications, prefer a scoped claim or open question
  over inventing a resource identity, transaction, valuation, or deployment.
- Resource falsifiers should use resource_threshold when a metric boundary would
  contradict the claim.
"""

_SURFACE_BATCH_PACK = """\
Surface pack: Batch compression.
- Read the whole batch as evidence, but do not emit one operation per signal.
  Promote only batch-level facts, concerns, predictions, situations, edges,
  recommendations, or lifecycle changes whose absence would make durable memory
  materially less useful.
- Preserve background, duplicate, and noisy signals in reasoning_trace when they
  explain why no write was emitted.
- If <inquiry_context_packet> includes memory_decision_candidates, treat them as
  advisory decisions. Accept, update, reject, merge, or no-op each material
  candidate through the available operations; only add missing ops for durable
  changes the packet missed.
- In compiled memory decision mode, do not reconstruct hidden planner paths. Use
  the compressed signal summary, candidate evidence, and listed source ids.
"""

_SURFACE_TOPOLOGY_CANDIDATE_PACK = """\
Surface pack: Topology and pattern candidates.
- Topology, precipitation, and SAGE candidates are pre-truth evidence, not
  mandates. Promote only when the candidate adds decision-relevant structure:
  a grounded edge, an explanatory situation, a useful pattern, a targeted
  lifecycle update, or a concrete ontology gap.
- For latent relationship candidates, inspect member Models and explain what
  changes if the candidate is true: flow, pressure, customer, actor,
  commitment, resource, or decision meaning.
- For pattern_review, promote a Pattern Model only when evidence is stable,
  useful, explainable, falsifiable, and action-shaping. If evidence is thin,
  counterexamples are unresolved, or the candidate is surface similarity,
  return an empty diff or a targeted open question.
"""

_SURFACE_PACKS: dict[str, str] = {
    "model_memory": _SURFACE_MODEL_MEMORY_PACK,
    "lifecycle": _SURFACE_LIFECYCLE_PACK,
    "graph": _SURFACE_GRAPH_PACK,
    "acts": _SURFACE_ACTS_PACK,
    "resources": _SURFACE_RESOURCES_PACK,
    "batch": _SURFACE_BATCH_PACK,
    "topology_candidate": _SURFACE_TOPOLOGY_CANDIDATE_PACK,
}


def _surface_aware_prompt_enabled() -> bool:
    return os.environ.get(_SURFACE_PROMPT_FLAG, "0").strip().lower() in {
        "1", "on", "true", "yes",
    }


def _strict_lean_prompt_enabled() -> bool:
    """Cost-plan §1.2 flag `THINK_STRICT_LEAN_PROMPT`. Default off. Only takes
    effect when the provider also *enforces* the output schema (DeepSeek strict
    tool-calling); on hint-only providers (Codex/OpenAI/Anthropic) the prose
    contract is load-bearing and is always kept."""
    return os.environ.get("THINK_STRICT_LEAN_PROMPT", "0").strip().lower() in {
        "1", "on", "true", "yes",
    }


def _compiled_memory_decision_prompt_enabled() -> bool:
    raw = os.environ.get("THINK_COMPILED_MEMORY_DECISION_PROMPT")
    if raw is None or raw.strip() == "":
        return False
    return raw.strip().lower() in {"1", "on", "true", "yes"}


def _compiled_memory_decision_mode(
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> bool:
    if not _compiled_memory_decision_prompt_enabled():
        return False
    if not trigger.is_batch:
        return False
    packet = _inquiry_context_packet(bundle)
    if packet is None:
        return False
    candidates = packet.get("memory_decision_candidates")
    return any(isinstance(candidate, dict) for candidate in (candidates or []))


def _packet_evidence_policy(packet: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(packet, dict):
        return {}
    budget = packet.get("budget")
    if not isinstance(budget, dict):
        return {}
    policy = budget.get("evidence_policy")
    return policy if isinstance(policy, dict) else {}


def _packet_suppresses_t1_raw_observations(
    packet: dict[str, Any] | None,
) -> bool:
    if not isinstance(packet, dict):
        return False
    policy = _packet_evidence_policy(packet)
    if policy.get("mode") != "models_only":
        return False
    if policy.get("fallback_reason") == "no_model_evidence":
        return False
    source_metadata = packet.get("source_metadata")
    if not isinstance(source_metadata, dict):
        return False
    return source_metadata.get("trigger_kind") == "T1"


def _suppress_raw_trigger_text(trigger: TriggerContext, bundle: ContextBundle) -> bool:
    if not trigger.is_batch:
        return False
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    selection = notes.get("observation_selection")
    if (
        isinstance(selection, dict)
        and selection.get("floor_reason")
        == "explicit_t1_event_batch_raw_evidence_floor"
    ):
        return False
    return _packet_suppresses_t1_raw_observations(_inquiry_context_packet(bundle))


# Cost-plan §1.2: the ONLY prose safely droppable when the strict tool schema
# is server-enforced is the top-level JSON-shape skeleton (the schema's
# `required` + field set enforce exactly this). Every *semantic* rule stays —
# edge-kind vocabulary, falsifier kinds, payload examples, scoping/confidence
# discipline — because the schema validates shape, not meaning.
_DIFF_SHAPE_SKELETON = """Diff schema (you produce EXACTLY this JSON shape):
{
  "trigger_ref": "<uuid echoed from triggering_event>",
  "tenant_id": "<uuid echoed from triggering_event>",
  "claim_ops": [],
  "memory_lifecycle_ops": [],
  "relation_claim_ops": [],
  "relation_frame_ops": [],
  "edge_ops": [],
  "ontology_gap_ops": [],
  "open_question_ops": [],
  "formation_resolutions": [],
  "act_ops": [],
  "resource_ops": [],
  "new_predictions": [],
  "reasoning_trace": "<brief rationale>"
}"""

_DIFF_SHAPE_POINTER = (
    "Diff schema: the strict tool schema enforces the exact top-level shape "
    "(trigger_ref, tenant_id, claim_ops, memory_lifecycle_ops, relation_claim_ops, "
    "relation_frame_ops, edge_ops, "
    "ontology_gap_ops, open_question_ops, formation_resolutions, resource_ops, "
    "new_predictions, reasoning_trace). Act ops are available in the full "
    "RawDiff schema but omitted from this strict tool surface."
)


def _lean_strict_base(base: str) -> str:
    """Return `base` with schema-enforced shape prose collapsed to a pointer.
    Guarded: if the target block is absent (prompt was edited), `base` is
    returned unchanged — never silently drops more than the known block."""
    if _DIFF_SHAPE_SKELETON in base:
        return base.replace(_DIFF_SHAPE_SKELETON, _DIFF_SHAPE_POINTER)
    return base


def select_prompt_surface(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    reasoning_frame: ReasoningFrame | None = None,
    claims_only: bool = False,
    lean_output_contract: bool = False,
    compiled_decision_mode: bool = False,
) -> PromptSurface:
    """Choose the static prompt packs for a Think call.

    The selector is intentionally conservative: it removes large operation
    contracts from simple claim-only calls, while keeping the relevant pack when
    the trigger, retrieval notes, or reasoning job makes that operation likely.
    """

    frame = reasoning_frame or ReasoningFrame.from_trigger(trigger)
    allowed_ops = set(frame.allowed_ops)
    packs: list[str] = ["model_memory"]
    text = _trigger_surface_text(trigger)
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    job = reasoning_job_from_trigger(trigger)

    if trigger.is_batch or compiled_decision_mode or _has_batch_signature(trigger):
        packs.append("batch")

    if not claims_only:
        if bundle.models or job.family == "internal_reflection" or trigger.kind in {"T2", "T3", "T4", "T6"}:
            packs.append("lifecycle")

        if "relation_claim_ops" in allowed_ops and _has_graph_surface(trigger, bundle, notes):
            packs.append("graph")

        if "act_ops" in allowed_ops and _has_act_surface(trigger, bundle, text):
            packs.append("acts")

        if "resource_ops" in allowed_ops and _has_resource_surface(bundle, text):
            packs.append("resources")

        if _has_topology_candidate_surface(trigger, notes):
            if "graph" not in packs:
                packs.append("graph")
            packs.append("topology_candidate")
    elif _has_topology_candidate_surface(trigger, notes):
        packs.append("topology_candidate")

    return PromptSurface(
        packs=tuple(dict.fromkeys(packs)),
        claims_only=claims_only,
        lean_output_contract=lean_output_contract,
    )


def _build_surface_aware_system_prompt(surface: PromptSurface) -> str:
    schema_prompt = (
        _SURFACE_CLAIMS_ONLY_SCHEMA_PROMPT
        if surface.claims_only
        else _SURFACE_FULL_SCHEMA_PROMPT
    )
    parts = [_SURFACE_CORE_PROMPT, schema_prompt]
    for pack in surface.packs:
        pack_prompt = _SURFACE_PACKS.get(pack)
        if pack_prompt:
            parts.append(pack_prompt)
    if surface.lean_output_contract and _strict_lean_prompt_enabled():
        parts.append(
            "Strict schema mode: the provider enforces the JSON shape server-side; "
            "the semantic grounding, scoping, and operation-pack rules above remain "
            "load-bearing."
        )
    return "\n\n".join(parts)


def prompt_static_size_report() -> dict[str, Any]:
    """Return static prompt size estimates for regression tests and reports."""

    baseline_full = _SYSTEM_PROMPT
    baseline_claims = _CLAIMS_ONLY_SYSTEM_PROMPT
    canonical_surfaces = {
        "surface_claims_only": PromptSurface(("model_memory",), claims_only=True),
        "surface_full_model_graph": PromptSurface(
            ("model_memory", "lifecycle", "graph"),
            claims_only=False,
        ),
        "surface_full_all_packs": PromptSurface(
            (
                "model_memory",
                "lifecycle",
                "graph",
                "acts",
                "resources",
                "batch",
                "topology_candidate",
            ),
            claims_only=False,
        ),
    }

    def _stats(text: str) -> dict[str, float]:
        return {
            "chars": len(text),
            "estimated_tokens_char4": round(len(text) / 4, 2),
        }

    report: dict[str, Any] = {
        "baseline_full": _stats(baseline_full),
        "baseline_claims_only": _stats(baseline_claims),
    }
    for name, surface in canonical_surfaces.items():
        report[name] = _stats(_build_surface_aware_system_prompt(surface))
    return report


def _trigger_surface_text(trigger: TriggerContext) -> str:
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    pieces = [
        trigger.kind,
        trigger.subkind or "",
        trigger.seed_natural_text or "",
        trigger.topology_event_kind or "",
    ]
    for key in (
        "source_channel",
        "signal_type",
        "observation_kind",
        "relationship_candidate",
        "relationship_candidates",
        "pattern_candidate_id",
        "proposed_signature",
        "observed_tendency",
        "assessment",
    ):
        value = signature.get(key)
        if isinstance(value, str):
            pieces.append(value)
        elif isinstance(value, (dict, list)):
            pieces.append(json.dumps(value, default=str)[:2000])
    return " ".join(pieces).lower()


def _has_batch_signature(trigger: TriggerContext) -> bool:
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    return bool(
        signature.get("batch_observation_ids")
        or signature.get("member_trigger_ids")
        or signature.get("batch_size")
    )


def _has_graph_surface(
    trigger: TriggerContext,
    bundle: ContextBundle,
    notes: dict[str, Any],
) -> bool:
    _selected, graph_models = _selected_model_sets(bundle)
    if graph_models:
        return True
    if trigger.kind == "T6" or trigger.topology_event_kind:
        return True
    if trigger.kind == "T4" and trigger.subkind in {
        "latent_relationship_candidate",
        "pattern_review",
        "representation_repair",
    }:
        return True
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    if signature.get("relationship_candidate") or signature.get("relationship_candidates"):
        return True
    if trigger.member_model_ids and len(trigger.member_model_ids) >= 2:
        return True
    topology = bundle.topology_context or {}
    if topology.get("neighborhoods") or topology.get("recent_phase_events"):
        return True
    selection = notes.get("model_selection")
    if isinstance(selection, dict):
        selected = selection.get("selected_model_ids") or []
        return len(selected) >= 2 and len(bundle.models) >= 2
    return len(bundle.models) >= 2 and trigger.kind in {"T2", "T3", "T4", "T6"}


def _has_act_surface(
    trigger: TriggerContext,
    bundle: ContextBundle,
    surface_text: str,
) -> bool:
    if _has_acts(bundle):
        return True
    if trigger.kind == "T2" and trigger.subkind == "belief_updated":
        return True
    if trigger.kind != "T1":
        return False
    return _surface_mentions(
        surface_text,
        (
            "commitment",
            "goal",
            "decision",
            "started",
            "kicking off",
            "picked up",
            "working on",
            "building",
            "deliver",
            "blocked",
            "waiting on",
            "paused",
            "done",
            "merged",
            "deployed",
            "approved",
        ),
    )


def _has_resource_surface(bundle: ContextBundle, surface_text: str) -> bool:
    if bundle.resources_summary:
        return True
    return _surface_mentions(
        surface_text,
        (
            "resource",
            "runway",
            "budget",
            "capacity",
            "vendor",
            "license",
            "quota",
            "tooling",
            "infrastructure",
            "contract",
            "asset",
            "dataset",
            "allocation",
        ),
    )


def _has_topology_candidate_surface(
    trigger: TriggerContext,
    notes: dict[str, Any],
) -> bool:
    if trigger.kind == "T4" and trigger.subkind in {
        "latent_relationship_candidate",
        "pattern_review",
    }:
        return True
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    if signature.get("relationship_candidate") or signature.get("relationship_candidates"):
        return True
    if signature.get("pattern_candidate_id"):
        return True
    packet = notes.get("inquiry_context_packet")
    if isinstance(packet, dict):
        return bool(packet.get("memory_decision_candidates"))
    return False


def _surface_mentions(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


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
    job = reasoning_job_from_trigger(trigger)
    if job.family == "internal_reflection":
        return (
            "internal memory reviewer; decide whether the existing model "
            "layer needs propagation, explanation, reorganization, or no-op."
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
    job = reasoning_job_from_trigger(trigger)
    if (
        job.family == "internal_reflection"
        and job.intent == "adjudicate_candidate"
    ):
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

    job = reasoning_job_from_trigger(trigger)
    if job.family == "internal_reflection":
        return (
            "model-layer level: inspect existing beliefs, edges, and actions "
            "as one internal reflection pass; revise only what this trigger's "
            "intent and evidence support."
        )
    if trigger.kind == "T6":
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
    lean_output_contract: bool = False,
) -> PromptPair:
    """
    Produce (system, user) messages for `LLMProvider.structured`.

    `triggering_content` is the natural-language content of the
    triggering signal (for T1). For T2/T3/T4 the caller can pass a
    summary string.
    """
    compiled_decision_mode = _compiled_memory_decision_mode(trigger, bundle)
    suppress_raw_trigger_text = _suppress_raw_trigger_text(trigger, bundle)
    triggering = _build_triggering_section(
        trigger,
        triggering_content=triggering_content,
        reason=reason_for_trigger,
        compiled_decision_mode=compiled_decision_mode,
        suppress_raw_trigger_text=suppress_raw_trigger_text,
    )
    frame = reasoning_frame.to_prompt_section() if reasoning_frame else None
    context = _build_context_section(
        trigger,
        bundle,
        triggering_actor_summary,
        compiled_decision_mode=compiled_decision_mode,
        suppress_raw_observations=suppress_raw_trigger_text,
    )
    prompt_surface: PromptSurface | None = None
    if _surface_aware_prompt_enabled():
        prompt_surface = select_prompt_surface(
            trigger,
            bundle,
            reasoning_frame=reasoning_frame,
            claims_only=claims_only,
            lean_output_contract=(
                lean_output_contract and _strict_lean_prompt_enabled()
            ),
            compiled_decision_mode=compiled_decision_mode,
        )
        base = _build_surface_aware_system_prompt(prompt_surface)
        instructions = _build_surface_operating_instructions(trigger, prompt_surface)
    else:
        instructions = _build_instructions(trigger)
        base = _CLAIMS_ONLY_SYSTEM_PROMPT if claims_only else _SYSTEM_PROMPT
        # Cost-plan §1.2: lean only when the caller says the provider enforces the
        # schema AND the flag is on. Hint-only providers keep the full prose.
        if lean_output_contract and _strict_lean_prompt_enabled():
            base = _lean_strict_base(base)
    profile = _build_reasoning_profile(trigger, bundle, claims_only=claims_only)

    # Stable system prefix: static base + per-trigger-kind operating
    # instructions. Dynamic call-specific profile/context live in the user
    # message so provider prefix caches can reuse the system bucket.
    system_prompt = f"{base}\n\n{instructions}"
    parts = [profile, triggering]
    if prompt_surface:
        parts.insert(1, prompt_surface.to_prompt_section())
    if frame:
        parts.append(frame)
    parts.append(context)
    user_msg = "\n\n".join(parts)
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
    compiled_decision_mode: bool = False,
    suppress_raw_trigger_text: bool = False,
) -> str:
    lines = ["<triggering_event>"]
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
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
    if triggering_content and suppress_raw_trigger_text:
        lines.append(
            "  content: [raw batch text suppressed by model-only Think evidence policy]"
        )
    elif triggering_content and compiled_decision_mode:
        lines.append(
            "  content: [batch text compiled into "
            "<inquiry_context_packet>.signal_summary and candidates]"
        )
        lines.append(
            f"  content_digest: {_trunc(triggering_content, 500)}"
        )
    elif triggering_content:
        lines.append(f"  content: {_trunc(triggering_content, _PER_ITEM_CHAR_LIMIT)}")
    if trigger.seed_natural_text and suppress_raw_trigger_text:
        observation_count = len(signature.get("batch_observation_ids") or [])
        lines.append(
            "  seed_natural_text: "
            "[raw batch text suppressed by model-only Think evidence policy]"
        )
        if observation_count:
            lines.append(f"  batch_observation_count: {observation_count}")
    elif trigger.seed_natural_text and compiled_decision_mode:
        lines.append(
            f"  seed_natural_text_digest: "
            f"{_trunc(trigger.seed_natural_text, 500)}"
        )
    elif trigger.seed_natural_text:
        lines.append(
            f"  seed_natural_text: "
            f"{_trunc(trigger.seed_natural_text, _PER_ITEM_CHAR_LIMIT)}"
        )
    candidates = signature.get("relationship_candidates")
    if isinstance(candidates, list):
        # Cost-plan §1.3: cap by count (8) AND by char budget; the tail folds
        # into the omitted-count marker. At least one candidate always emits.
        emitted = 0
        used = 0
        for candidate in candidates[:8]:
            if not isinstance(candidate, dict):
                continue
            cand_lines = _relationship_candidate_lines(candidate)
            cand_len = sum(len(line) + 1 for line in cand_lines)
            if emitted > 0 and used + cand_len > _CANDIDATES_CHAR_BUDGET:
                break
            lines.extend(cand_lines)
            used += cand_len
            emitted += 1
        omitted = len(candidates) - emitted
        if omitted > 0:
            lines.append(
                f"  relationship_candidate_omitted_count: {omitted}"
            )
    else:
        candidate = signature.get("relationship_candidate")
        if isinstance(candidate, dict):
            lines.extend(_relationship_candidate_lines(candidate))
    if trigger.kind == "T4" and trigger.subkind == "pattern_review":
        lines.extend(_pattern_review_candidate_lines(signature))
    if reason:
        lines.append(f"  reason: {reason}")
    lines.append("</triggering_event>")
    return "\n".join(lines)


def _pattern_review_candidate_lines(signature: dict[str, Any]) -> list[str]:
    candidate_id = signature.get("pattern_candidate_id")
    if not candidate_id:
        return []
    lines = ["  <pattern_review_candidate>"]
    lines.append(f"    id: {candidate_id}")
    lines.append("    source: precipitation_or_sage_latent_pattern")
    lines.append("    status: weak_evidence_requires_semantic_review")
    for key in (
        "cluster_size",
        "density",
        "promotion_readiness_score",
        "surface_domain_count",
        "counterexample_count",
    ):
        if key in signature:
            lines.append(f"    {key}: {signature[key]}")
    for key in (
        "constituent_model_ids",
        "support_source_refs",
        "shared_facets",
    ):
        value = signature.get(key)
        if isinstance(value, list) and value:
            lines.append(f"    {key}: {_trunc(json.dumps(value, default=str), 700)}")
    for key in (
        "proposed_signature",
        "observed_tendency",
        "assessment",
        "rubric",
    ):
        value = signature.get(key)
        if isinstance(value, dict) and value:
            lines.append(
                f"    {key}: {_trunc(json.dumps(value, sort_keys=True, default=str), 900)}"
            )
    lines.append("  </pattern_review_candidate>")
    return lines


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


_DOC_SUMMARY_ITEM_LIMIT = 12
_DOC_SUMMARY_ITEM_CHARS = 400


def _doc_summary_item_text(item: Any) -> str:
    """Render a structured item (str or {who?, what, due?}) for the prompt.

    Commitments keep owner/due inline so Think can set evaluate_at = due and a
    deadline falsifier (§4.2). Bare strings render as-is.
    """
    if isinstance(item, str):
        return item.strip()
    if isinstance(item, dict):
        what = item.get("what")
        if not isinstance(what, str) or not what.strip():
            return ""
        parts = [what.strip()]
        who = item.get("who")
        if isinstance(who, str) and who.strip():
            parts.append(f"(owner: {who.strip()})")
        due = item.get("due")
        if isinstance(due, str) and due.strip():
            parts.append(f"(due: {due.strip()})")
        return " ".join(parts)
    return ""


def _build_document_summary_section(trigger: TriggerContext) -> list[str]:
    """Render the enriched-T1 structured document summary as evidence.

    Reads `doc_structured_summary` from the trigger payload (carried on
    `trigger.seed_signature`). Emits nothing when the trigger is not a
    document-memory T1, so non-document triggers are byte-for-byte unchanged.
    """
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    structured = signature.get("doc_structured_summary")
    if not isinstance(structured, dict) or not structured:
        return []

    lines = ["  <document_structured_summary>"]
    lines.append(
        "    A large document was ingested and summarized; the structured "
        "extraction below is the durable evidence. Distill it into Models: one "
        "situation anchor (claim_role=situation) plus per-item claims — "
        "decisions -> recommendation/fact, risks -> concern (polarity=negative), "
        "commitments/action_items -> prediction with evaluate_at=due + a "
        "prediction_deadline falsifier. born_from_event_id MUST be the "
        "triggering observation_id; cite it in evidence/supporting ids. "
        "Dedupe against retrieved Models in this context."
    )
    summary_text = structured.get("summary")
    if isinstance(summary_text, str) and summary_text.strip():
        lines.append(f"    summary: {_trunc(summary_text.strip(), 600)}")

    for field_key, label in (
        ("decisions", "decisions"),
        ("action_items", "commitments"),
        ("risks", "risks"),
        ("key_points", "key_points"),
    ):
        values = structured.get(field_key)
        if not isinstance(values, list) or not values:
            continue
        rendered: list[str] = []
        for item in values[:_DOC_SUMMARY_ITEM_LIMIT]:
            text = _doc_summary_item_text(item)
            if text:
                rendered.append(_trunc(text, _DOC_SUMMARY_ITEM_CHARS))
        if not rendered:
            continue
        lines.append(f"    <{label}>")
        for text in rendered:
            lines.append(f"      - {text}")
        omitted = len(values) - len(rendered)
        if omitted > 0:
            lines.append(f"      - [{omitted} more {label} omitted]")
        lines.append(f"    </{label}>")

    # Re-resolved scope (resolved refs/UUIDs only; never invented). Think uses
    # these directly for scope_entities/scope_actors on the document Models.
    scope_entities = signature.get("doc_scope_entities")
    if isinstance(scope_entities, list) and scope_entities:
        lines.append(
            "    resolved_scope_entities: "
            + _trunc(json.dumps(scope_entities, default=str), 800)
        )
    scope_actors = signature.get("doc_scope_actors")
    if isinstance(scope_actors, list) and scope_actors:
        lines.append(
            "    resolved_scope_actors: "
            + _trunc(json.dumps([str(a) for a in scope_actors], default=str), 800)
        )
    unresolved = signature.get("doc_unresolved_actor_refs")
    if isinstance(unresolved, list) and unresolved:
        lines.append(
            "    unresolved_owner_names (text only; do NOT put in scope_actors): "
            + _trunc(json.dumps([str(u) for u in unresolved], default=str), 400)
        )
    lines.append("  </document_structured_summary>")
    return lines


def _build_context_section(
    trigger: TriggerContext,
    bundle: ContextBundle,
    actor_summary: str | None,
    *,
    compiled_decision_mode: bool = False,
    suppress_raw_observations: bool = False,
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
        compiled_decision_mode=compiled_decision_mode,
    )
    if retrieval_guidance:
        lines.extend(retrieval_guidance)

    inquiry_packet = _build_inquiry_context_packet_section(
        bundle,
        compiled_decision_mode=compiled_decision_mode,
    )
    if inquiry_packet:
        lines.extend(inquiry_packet)

    formation_candidates = _build_model_formation_candidates_section(trigger, bundle)
    if formation_candidates:
        lines.extend(formation_candidates)

    # Observations
    if compiled_decision_mode:
        lines.extend(_build_compiled_observation_manifest(bundle, actor_mentions))
    elif suppress_raw_observations:
        lines.extend(_build_redacted_observation_manifest(bundle))
    else:
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

    # Document-memory Layer 2 (Phase 1): when the summarization worker enriched
    # the T1 with a structured document summary, surface it as a dedicated
    # evidence block so Think can distill the document into Models. Gated by
    # INGEST_DOC_MEMORY_ENABLED on the ingest side; here it is simply rendered
    # when present (docs/plans/document-memory-substrate.md §4.2).
    lines.extend(_build_document_summary_section(trigger))

    lines.extend(
        _build_models_section(
            trigger,
            bundle,
            selected_model_ids=selected_model_ids,
            graph_model_ids=graph_model_ids,
            actor_mentions=actor_mentions,
            compiled_decision_mode=compiled_decision_mode,
        )
    )

    # Acts (goals/commitments/decisions)
    act_parts = ["  <acts>"]
    used = 0
    acts_budget = (
        _ACTS_COMPILED_DECISION_CHAR_BUDGET
        if compiled_decision_mode
        else _ACTS_CHAR_BUDGET
    )
    for g in bundle.acts_summary.get("goals", []):
        piece = (
            f"    - goal id={g.id} state={g.state} altitude={g.altitude} "
            f"health={g.cached_health} title={_trunc(g.title, 200)}"
        )
        if used + len(piece) > acts_budget:
            break
        act_parts.append(piece)
        used += len(piece)
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
        if used + len(piece) > acts_budget:
            break
        act_parts.append(piece)
        used += len(piece)
    for d in bundle.acts_summary.get("decisions", []):
        piece = (
            f"    - decision id={d.id} state={d.state} "
            f"title={_trunc(d.title, 200)}"
        )
        if used + len(piece) > acts_budget:
            break
        act_parts.append(piece)
        used += len(piece)
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
        res_parts.append(piece)
        used += len(piece)
    res_parts.append("  </resources>")
    lines.extend(res_parts)

    lines.extend(_build_candidate_substrate_section(bundle))

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


def _build_compiled_observation_manifest(
    bundle: ContextBundle,
    actor_mentions: dict[str, int],
) -> list[str]:
    packet = _inquiry_context_packet(bundle) or {}
    candidates = _decision_candidates_from_packet(packet)
    source_ids = _candidate_source_observation_ids(candidates)
    lines = ["  <observations>"]
    lines.append(
        "    compiled_mode: full retrieved observation bodies omitted; use "
        "<inquiry_context_packet>.signal_summary, candidate_evidence, and "
        "source_observation_ids for batch evidence."
    )
    if source_ids:
        lines.append(
            "    candidate_source_observation_ids: "
            + _trunc(json.dumps(sorted(source_ids), default=str), 900)
        )

    shown = 0
    for observation in bundle.observations:
        actor_id = getattr(observation, "actor_id", None)
        if actor_id is not None:
            actor_mentions[str(actor_id)] = actor_mentions.get(str(actor_id), 0) + 1
        observation_id = str(getattr(observation, "id", ""))
        if observation_id not in source_ids or shown >= 8:
            continue
        lines.append(
            "    - id="
            + observation_id
            + f" trust={getattr(observation, 'trust_tier', None)}"
            + f" channel={getattr(observation, 'source_channel', None)}"
            + f" actor_id={actor_id if actor_id is not None else 'external'}"
            + f" at={getattr(observation, 'occurred_at', '')}"
        )
        shown += 1
    if bundle.observations and shown == 0:
        lines.append(
            f"    retrieved_observation_count: {len(bundle.observations)} "
            "(bodies omitted by compiled decision prompt)"
        )
    lines.append("  </observations>")
    return lines


def _build_candidate_substrate_section(bundle: ContextBundle) -> list[str]:
    lines = ["  <candidate_substrate>"]
    candidates = []
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    raw_candidates = notes.get("substrate_candidates") or []
    if isinstance(raw_candidates, list):
        candidates = [item for item in raw_candidates if isinstance(item, dict)]
    if not candidates:
        lines.append(
            "    [no provisional substrate candidates available from this "
            "context]"
        )
        lines.append("  </candidate_substrate>")
        return lines

    used = 0
    for candidate in candidates:
        scope_ref = candidate.get("scope_ref")
        kind = str(candidate.get("kind") or "").strip()
        candidate_id = str(candidate.get("id") or "").strip()
        if not isinstance(scope_ref, dict) and kind and candidate_id:
            scope_ref = {"type": f"candidate_{kind}", "id": candidate_id}
        if not isinstance(scope_ref, dict):
            continue
        aliases = candidate.get("aliases") or []
        evidence_ids = candidate.get("evidence_observation_ids") or []
        metadata = candidate.get("metadata") or {}
        compact_metadata = {
            key: metadata.get(key)
            for key in (
                "basis",
                "source_root",
                "action",
                "object_key",
                "count_in_context",
                "actor_fingerprints",
            )
            if isinstance(metadata, dict) and metadata.get(key) not in (None, [], {})
        }
        piece = (
            "    - "
            f"scope_ref={json.dumps(scope_ref, sort_keys=True)} "
            f"label={_trunc(str(candidate.get('label') or ''), 120)} "
            f"confidence={candidate.get('confidence')} "
            f"status={candidate.get('status') or 'proposed'} "
            f"aliases={_trunc(json.dumps(aliases[:3], default=str), 360)} "
            f"evidence_observation_ids={evidence_ids[:6]} "
            f"metadata={_trunc(json.dumps(compact_metadata, default=str), 360)}"
        )
        if used + len(piece) > _SUBSTRATE_CHAR_BUDGET:
            lines.append("    - [truncated — more candidate substrate omitted]")
            break
        lines.append(piece)
        used += len(piece)
    lines.append("  </candidate_substrate>")
    return lines


def _build_model_formation_candidates_section(
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> list[str]:
    candidates = build_model_formation_candidates(trigger, bundle)
    if not candidates:
        return []
    lines = ["  <model_formation_candidates>"]
    lines.append(f"    required_decision_count: {len(candidates)}")
    used = 0
    for candidate in candidates:
        payload = candidate.to_prompt_dict()
        rendered = json.dumps(payload, sort_keys=True, default=str)
        if used + len(rendered) > _CANDIDATES_CHAR_BUDGET and used > 0:
            remaining = len(candidates) - len(lines) + 2
            if remaining > 0:
                lines.append(
                    f"    [truncated - {remaining} formation candidates omitted]"
                )
            break
        lines.append(f"    - {rendered}")
        used += len(rendered)
    lines.append("  </model_formation_candidates>")
    return lines


def _build_redacted_observation_manifest(bundle: ContextBundle) -> list[str]:
    lines = ["  <observations>"]
    lines.append(
        "    redaction_policy: raw observation bodies omitted by model-only "
        "Think evidence policy; use Models, trigger observation IDs, and Model "
        "provenance for evidence_event_ids."
    )
    if bundle.observations:
        lines.append(f"    retrieved_observation_count: {len(bundle.observations)}")
    lines.append("  </observations>")
    return lines


def _build_models_section(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    selected_model_ids: set[str],
    graph_model_ids: set[str],
    actor_mentions: dict[str, int],
    compiled_decision_mode: bool = False,
) -> list[str]:
    inquiry_packet = _inquiry_context_packet(bundle)
    inquiry_mode = inquiry_packet is not None
    actionable_ids = _actionable_model_ids(
        trigger,
        bundle,
        graph_model_ids=graph_model_ids,
    )
    if compiled_decision_mode:
        budget = _MODELS_COMPILED_DECISION_CHAR_BUDGET
        detail_row_limit = _MODEL_COMPILED_DETAIL_ROW_LIMIT
        detail_char_limit = _MODEL_COMPILED_DETAIL_CHAR_LIMIT
        manifest_char_limit = _MODEL_COMPILED_MANIFEST_CHAR_LIMIT
    else:
        budget = _MODELS_INQUIRY_CHAR_BUDGET if inquiry_mode else _MODELS_CHAR_BUDGET
        detail_row_limit = _MODEL_DETAIL_ROW_LIMIT
        detail_char_limit = _MODEL_DETAIL_CHAR_LIMIT
        manifest_char_limit = _MODEL_MANIFEST_CHAR_LIMIT
    lines = ["  <models>"]
    if compiled_decision_mode:
        lines.append(
            "    manifest_mode: compiled_memory_decision; only candidate "
            "target/evidence/graph Models receive detail. Use candidate ids "
            "and exact Model ids from this section."
        )
    elif inquiry_mode:
        lines.append(
            "    manifest_mode: compact; use <inquiry_context_packet> as the "
            "primary evidence summary. Full rows are limited to actionable "
            "anchors."
        )

    used = 0
    detailed_rows = 0
    models = list(bundle.models)
    if compiled_decision_mode:
        models.sort(
            key=lambda model: (
                0 if _model_id(model) in actionable_ids else 1,
                0 if _model_id(model) in graph_model_ids else 1,
                _model_id(model),
            )
        )
    for idx, model in enumerate(models):
        for actor_id in _model_scope_actors(model):
            actor_mentions[str(actor_id)] = actor_mentions.get(str(actor_id), 0) + 1

        model_id = _model_id(model)
        if compiled_decision_mode:
            wants_detail = model_id in actionable_ids or (
                not actionable_ids and idx < 1
            )
        else:
            wants_detail = (
                not inquiry_mode
                or model_id in actionable_ids
                or (not actionable_ids and idx < 2)
            )
        include_detail = wants_detail and detailed_rows < detail_row_limit
        piece = _format_model_row(
            model,
            selected_model_ids=selected_model_ids,
            graph_model_ids=graph_model_ids,
            detail=include_detail,
            detail_char_limit=detail_char_limit,
            manifest_char_limit=manifest_char_limit,
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
    detail_char_limit: int = _MODEL_DETAIL_CHAR_LIMIT,
    manifest_char_limit: int = _MODEL_MANIFEST_CHAR_LIMIT,
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
    text_limit = detail_char_limit if detail else manifest_char_limit
    return (
        f"    - id={model_id} detail={detail_label} "
        f"kind={getattr(model, 'proposition_kind', 'unknown')} "
        f"role={getattr(model, 'claim_role', None) or 'unknown'} "
        f"retrieval={_retrieval_tags(model_id, selected_model_ids, graph_model_ids)} "
        f"conf={_score(getattr(model, 'confidence', None))} "
        f"act={_score(getattr(model, 'activation', None))} "
        f"falsifier={falsifier_kind} "
        f"status={getattr(model, 'status', 'unknown')} "
        f"lifecycle={_model_lifecycle_repr(model)} "
        f"scope_actors={scope_actors_repr} "
        f"scope_entities={scope_entities_repr} "
        f"{text_label}={_trunc(natural, text_limit)}"
    )


def _model_lifecycle_repr(model: Any) -> str:
    parts: list[str] = []
    for key in (
        "evaluate_at",
        "resolved_at",
        "resolution_outcome",
        "last_confirmed_at",
        "confirmed_count",
        "contested_count",
    ):
        value = getattr(model, key, None)
        if value is None:
            continue
        parts.append(f"{key}={value}")
    return "{" + ",".join(parts) + "}" if parts else "{}"


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


def _decision_candidates_from_packet(packet: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(packet, dict):
        return []
    candidates = packet.get("memory_decision_candidates") or []
    return [candidate for candidate in candidates if isinstance(candidate, dict)]


def _candidate_values(
    candidates: list[dict[str, Any]],
    *keys: str,
    limit: int | None = None,
) -> set[str]:
    values: set[str] = set()
    for candidate in candidates:
        for key in keys:
            raw = candidate.get(key) or []
            if isinstance(raw, str):
                raw_values = [raw]
            elif isinstance(raw, list | tuple | set):
                raw_values = list(raw)
            else:
                continue
            for value in raw_values:
                if value:
                    values.add(str(value))
                    if limit is not None and len(values) >= limit:
                        return values
    return values


def _candidate_source_observation_ids(
    candidates: list[dict[str, Any]],
    *,
    limit: int | None = 20,
) -> set[str]:
    return _candidate_values(candidates, "source_observation_ids", limit=limit)


def _candidate_model_ids(
    candidates: list[dict[str, Any]],
    *,
    limit: int | None = 20,
) -> set[str]:
    return _candidate_values(
        candidates,
        "target_model_ids",
        "evidence_model_ids",
        limit=limit,
    )


def _candidate_evidence_ids(
    candidates: list[dict[str, Any]],
    *,
    limit: int | None = 24,
) -> set[str]:
    return _candidate_values(
        candidates,
        "supporting_evidence_ids",
        "counterevidence_ids",
        limit=limit,
    )


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
    ids = set(_candidate_model_ids(_decision_candidates_from_packet(packet)))
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    evidence = tiers.get("decisive_evidence") or []
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
    compiled_decision_mode: bool = False,
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

    if compiled_decision_mode:
        lines = ["  <retrieval_priority>"]
        lines.append(
            "    compiled_mode: retrieval was already compressed into "
            "memory_decision_candidates; use this as accountability metadata, "
            "not as an invitation to re-plan retrieval."
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
                "    For candidate target/evidence Models, emit the sharpest "
                "needed relation claim/frame/update or write no-op rationale "
                "by candidate id."
            )
        lines.append("  </retrieval_priority>")
        return lines

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
            "If it does, use existing Model UUIDs in memory_lifecycle_ops, "
            "claim_ops.update, relation_claim_ops, relation_frame_ops, act "
            "confidence_basis, or evidence_model_ids."
        )
        lines.append(
            "    Co-selection is NOT itself a stored graph connection. If the "
            "trigger explicitly says one graph-anchor Model blocks, causes, "
            "enables, explains, warns about, or contradicts another, add a "
            "relation_claim_ops entry. If the relation has 3+ typed roles "
            "that matter together, use relation_frame_ops instead. For "
            "'blocked by', missing prerequisite, or 'at risk because' "
            "language, prefer edge_kind='blocks' or 'causes' over generic "
            "supports/explains."
        )
        lines.append(
            "    Preserve explicit relationship semantics. Use 'blocks' for "
            "blocking dependencies, 'weakens' only for counterevidence, "
            "'explains' for causal/accounting explanations, and "
            "'contributes_to_resolution' when a new claim advances or closes "
            "a known issue."
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
            "    Relationship decision contract: for each important graph "
            "anchor pair or new-claim-to-anchor link, emit the sharpest "
            "relation_claim_op or relation_frame_op, emit an ontology_gap_op, "
            "update/archive the existing Model if that is stronger, or write "
            "`no edge:` with the full UUIDs and reason in reasoning_trace. A "
            "relational insight should not remain only as prose."
        )
    lines.append(
        "    Context accountability: for every non-empty diff, selected "
        "context should either appear in memory_lifecycle_ops, claim_ops.update, "
        "relation_claim_ops, relation_frame_ops, evidence_model_ids, "
        "evidence_event_ids, act confidence_basis, or resource reasoning. "
        "If selected context is "
        "irrelevant, "
        "reasoning_trace must name at least one selected/graph Model or "
        "selected Observation by full UUID and briefly say why that context "
        "did not warrant a state change, edge, action, or resource change."
    )
    lines.append("  </retrieval_priority>")
    return lines


def _build_inquiry_context_packet_section(
    bundle: ContextBundle,
    *,
    compiled_decision_mode: bool = False,
) -> list[str]:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    packet = notes.get("inquiry_context_packet")
    if not isinstance(packet, dict):
        return []
    verdict = packet.get("sufficiency_verdict")
    hypotheses = packet.get("hypotheses") or []
    decision_candidates = _decision_candidates_from_packet(packet)
    residual_spine = packet.get("model_residual_spine") or []
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
    if decision_candidates:
        lines.append(
            "    memory_decision_candidates are advisory: accept, update, "
            "reject, merge, or no-op them; only add missing ops for material "
            "durable changes the packet missed."
        )
    if compiled_decision_mode:
        lines.append("    mode: compiled_memory_decision_boundary")
        lines.append(
            "    planner_artifacts: omitted from prompt; candidates and "
            "candidate_evidence replace hypotheses/question_path/full tiers."
        )
    if _packet_suppresses_t1_raw_observations(packet):
        lines.append(
            "    signal_summary: "
            "[raw T1 signal summary suppressed by model-only Think evidence policy]"
        )
    else:
        lines.append(
            "    signal_summary: "
            + _trunc(str(packet.get("signal_summary") or ""), 700)
        )
    if isinstance(verdict, dict):
        verdict_limit = 420 if compiled_decision_mode else 900
        lines.append(
            "    sufficiency: "
            + _trunc(
                json.dumps(verdict, sort_keys=True, default=str),
                verdict_limit,
            )
        )
    if compiled_decision_mode:
        unknowns = packet.get("important_unknowns") or []
        if unknowns:
            lines.append(
                "    unresolved_slots: "
                + _trunc(json.dumps(unknowns[:8], default=str), 420)
            )
        obligations = packet.get("answer_obligations")
        if isinstance(obligations, dict):
            missing = obligations.get("missing_slots") or []
            premise = obligations.get("premise_status")
            if missing or premise:
                lines.append(
                    "    answer_obligations: "
                    + _trunc(
                        json.dumps(
                            {
                                "missing_slots": missing[:8],
                                "premise_status": premise,
                            },
                            sort_keys=True,
                            default=str,
                        ),
                        420,
                    )
                )
    elif hypotheses:
        lines.append("    hypotheses:")
        for h in hypotheses[:5]:
            lines.append(
                "      - "
                + _trunc(json.dumps(h, sort_keys=True, default=str), 500)
            )
    if decision_candidates:
        lines.append("    memory_decision_candidates:")
        for candidate in decision_candidates[:5]:
            lines.extend(_format_memory_decision_candidate(candidate))
    if isinstance(residual_spine, list) and residual_spine:
        lines.append(
            "    model_residual_spine: non-canonical compact compression debt; "
            "repair or absorb only with ordinary model-layer evidence."
        )
        for residual in residual_spine[:5]:
            if not isinstance(residual, dict):
                continue
            lines.append(
                "      - "
                + _trunc(json.dumps(residual, sort_keys=True, default=str), 520)
            )
    if compiled_decision_mode:
        evidence_lines = _format_candidate_evidence(packet, decision_candidates)
        if evidence_lines:
            lines.append("    candidate_evidence:")
            lines.extend(evidence_lines)
        budget = packet.get("budget")
        if isinstance(budget, dict):
            summary = {
                "reservoir_evidence_count": budget.get("reservoir_evidence_count"),
                "packet_evidence_count": budget.get("packet_evidence_count"),
                "evidence_policy": budget.get("evidence_policy"),
            }
            lines.append(
                "    hidden_packet_budget: "
                + _trunc(json.dumps(summary, sort_keys=True, default=str), 520)
            )
    elif questions:
        lines.append("    question_path:")
        for q in questions[:8]:
            qid = q.get("question_id") if isinstance(q, dict) else None
            question = q.get("question") if isinstance(q, dict) else q
            primitive = q.get("primitive") if isinstance(q, dict) else None
            lines.append(
                f"      - {qid or 'Q'} [{primitive or 'question'}]: "
                + _trunc(str(question), 260)
            )
    if not compiled_decision_mode and decisive:
        lines.append("    decisive_evidence:")
        for e in decisive[:12]:
            lines.append(
                "      - "
                + _trunc(json.dumps(e, sort_keys=True, default=str), 700)
            )
    if not compiled_decision_mode and supporting:
        lines.append("    supporting_evidence_groups:")
        for group in supporting[:6]:
            lines.append(
                "      - "
                + _trunc(json.dumps(group, sort_keys=True, default=str), 500)
            )
    if not compiled_decision_mode and omissions:
        lines.append("    omission_ledger:")
        for item in omissions[:6]:
            lines.append(
                "      - "
                + _trunc(json.dumps(item, sort_keys=True, default=str), 400)
            )
    lines.append("  </inquiry_context_packet>")
    return lines


def _format_memory_decision_candidate(candidate: dict[str, Any]) -> list[str]:
    cid = candidate.get("candidate_id") or "candidate"
    op_family = candidate.get("op_family") or "unknown"
    confidence = candidate.get("confidence")
    proposed = _trunc(str(candidate.get("proposed_text") or ""), 300)
    lines = [
        f"      - id={cid} op={op_family} confidence={confidence}: {proposed}"
    ]
    targets = {
        "target_model_ids": candidate.get("target_model_ids") or [],
        "target_act_ids": candidate.get("target_act_ids") or [],
        "evidence_model_ids": candidate.get("evidence_model_ids") or [],
        "source_observation_ids": candidate.get("source_observation_ids") or [],
    }
    non_empty_targets = {
        key: value[:6] if isinstance(value, list) else value
        for key, value in targets.items()
        if value
    }
    if non_empty_targets:
        lines.append(
            "        targets: "
            + _trunc(json.dumps(non_empty_targets, sort_keys=True, default=str), 450)
        )
    uncertainty = candidate.get("uncertainty_slots") or []
    if uncertainty:
        lines.append(
            "        uncertainty: "
            + _trunc(json.dumps(uncertainty[:6], default=str), 320)
        )
    evidence = {
        "supporting_evidence_ids": candidate.get("supporting_evidence_ids") or [],
        "counterevidence_ids": candidate.get("counterevidence_ids") or [],
    }
    evidence = {
        key: value[:6] if isinstance(value, list) else value
        for key, value in evidence.items()
        if value
    }
    if evidence:
        lines.append(
            "        evidence: "
            + _trunc(json.dumps(evidence, sort_keys=True, default=str), 420)
        )
    edge_hints = candidate.get("suggested_edge_kinds") or []
    if edge_hints:
        lines.append(
            "        suggested_edge_kinds: "
            + _trunc(json.dumps(edge_hints[:6], default=str), 220)
        )
    preconditions = candidate.get("write_preconditions") or []
    if preconditions:
        lines.append(
            "        write_preconditions: "
            + _trunc(json.dumps(preconditions[:4], default=str), 520)
        )
    answer_summary = candidate.get("answer_summary")
    if answer_summary:
        lines.append("        answer_summary: " + _trunc(str(answer_summary), 520))
    reason = candidate.get("reason")
    if reason:
        lines.append("        reason: " + _trunc(str(reason), 220))
    return lines


def _format_candidate_evidence(
    packet: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> list[str]:
    wanted_ids = _candidate_evidence_ids(candidates)
    tiers = packet.get("tiers") if isinstance(packet.get("tiers"), dict) else {}
    decisive = tiers.get("decisive_evidence") or []
    supporting = tiers.get("supporting_evidence_groups") or []
    lines: list[str] = []
    seen: set[str] = set()

    def add_decisive(item: dict[str, Any]) -> None:
        evidence_id = str(item.get("evidence_id") or "")
        if evidence_id and evidence_id in seen:
            return
        if evidence_id:
            seen.add(evidence_id)
        compact_item = {
            "evidence_id": evidence_id or None,
            "source_type": item.get("source_type"),
            "source_ref": item.get("source_ref"),
            "summary": item.get("summary"),
            "supports": item.get("supports_hypotheses"),
            "weakens": item.get("weakens_hypotheses"),
            "contradicts": item.get("contradicts_hypotheses"),
        }
        lines.append(
            "      - "
            + _trunc(json.dumps(compact_item, sort_keys=True, default=str), 520)
        )

    for item in decisive:
        if not isinstance(item, dict):
            continue
        evidence_id = str(item.get("evidence_id") or "")
        if wanted_ids and evidence_id not in wanted_ids:
            continue
        add_decisive(item)
        if len(lines) >= 8:
            return lines

    for group in supporting:
        if not isinstance(group, dict):
            continue
        evidence_ids = {str(eid) for eid in (group.get("evidence_ids") or [])}
        if wanted_ids and not (evidence_ids & wanted_ids):
            continue
        group_key = "group:" + ",".join(sorted(evidence_ids))
        if group_key in seen:
            continue
        seen.add(group_key)
        compact_group = {
            "claim_supported": group.get("claim_supported"),
            "evidence_count": group.get("evidence_count"),
            "sources": group.get("sources"),
            "evidence_ids": sorted(evidence_ids)[:8],
            "source_refs": (group.get("source_refs") or [])[:6],
            "summary": group.get("summary"),
        }
        lines.append(
            "      - "
            + _trunc(json.dumps(compact_group, sort_keys=True, default=str), 520)
        )
        if len(lines) >= 8:
            return lines

    if not lines and not wanted_ids:
        for item in decisive[:3]:
            if isinstance(item, dict):
                add_decisive(item)
    return lines


def _internal_reflection_instructions(trigger: TriggerContext) -> str:
    job = reasoning_job_from_trigger(trigger)
    header = (
        "This is an internal-reflection job. The legacy wake-up label is "
        f"{trigger.kind}"
        + (f":{trigger.subkind}" if trigger.subkind else "")
        + f"; source={job.source}; intent={job.intent}.\n"
        "\n"
        "Treat T2/T3/T4 as one belief-maintenance family: inspect the "
        "current model layer, then revise, connect, downgrade, promote, "
        "explain, act, or no-op according to the intent below."
    )
    if job.intent == "propagate_consequence":
        return (
            header
            + "\n\n"
            "Intent: propagate consequence from a newly inserted fact or "
            "concern Model. Decide whether the CEO needs to act on this "
            "belief.\n"
            "\n"
            "  - If a team member is blocked, waiting on a decision, or "
            "the CEO needs to unblock someone: emit ONE claim_op with "
            "`kind='norm'` and `claim_role='recommendation'`. Use only "
            "actor UUIDs that appear in <actors_in_context> for "
            "scope_actors. Write the natural field as a clear, actionable "
            "sentence for the CEO.\n"
            "\n"
            "  - If the new fact Model encodes a self-report of new "
            "in-flight work ('started X', 'building Y', 'picked up Z') "
            "AND <acts> has no matching commitment, you MUST emit a "
            "recommendation with proposed_change.operation='create' and "
            "target_act_ref={\"type\":\"commitment\",\"id\":null}. Use "
            "the create-commitment payload shape in the system prompt. "
            "'Purely informational progress update' is NOT an acceptable "
            "reason to skip; the ledger needs a commitment for the work to "
            "be tracked.\n"
            "\n"
            "  - If purely informational and no CEO action is needed: "
            "return an empty diff. The selected Model that caused this "
            "internal job already records the underlying fact; do not "
            "create recap, elaboration, or bookkeeping fact Models.\n"
            "\n"
            "CRITICAL CONSTRAINTS for the recommendation claim_op:\n"
            "  - Do NOT set scope_entities unless a UUID appears in <acts> "
            "or <retrieved_context>. Leave scope_entities as [] if unsure.\n"
            "  - Set target_act_ref to null unless you have an exact UUID "
            "from <acts>. Never invent a UUID.\n"
            "  - Do NOT invent UUIDs. If no CEO UUID is in the context, "
            "leave scope_actors as [].\n"
            "  - Do NOT emit a duplicate if a similar recommendation "
            "already exists with status 'active' in <acts>."
        )
    if job.intent == "evaluate_existing_belief":
        return (
            header
            + "\n\n"
            "Intent: evaluate an existing prediction Model whose "
            "evaluate_at has passed. Resolve the prediction with "
            "memory_lifecycle_ops action='confirm' or action='falsify' when "
            "possible; otherwise use claim_ops.update to set confidence, "
            "resolved_at, resolution_outcome, contributors, and propagate only "
            "materially supported dependent updates."
        )
    if job.intent == "explain_inconsistency":
        return (
            header
            + "\n\n"
            "Intent: explain an anomaly region. Reflect on the full "
            "situation. Consider whether any Model should be marked "
            "contested_false or archived, whether a missing causal "
            "relationship explains the discontinuity, and whether "
            "signal_readings should update."
        )
    if job.intent == "adjudicate_candidate":
        return (
            header
            + "\n\n"
            "Intent: adjudicate a latent relationship candidate from the "
            "topology layer. Topology here means a consequence-sensitive "
            "discovery field, not accepted graph layout. Treat it as "
            "evidence, not truth.\n"
            "\n"
            "Inspect the candidate member Models and decide whether the "
            "topology signal should become durable knowledge:\n"
            "  - emit an edge_op or edge candidate when a precise pairwise "
            "relation is real;\n"
            "  - emit an ontology_gap_op when the pairwise relation is "
            "real and useful but no registered edge_kind captures it;\n"
            "  - emit a `situation` Model when the members are symptoms of "
            "one operational condition;\n"
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
    if job.intent == "repair_representation_gap":
        signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
        warning_code = str(signature.get("audit_warning_code") or "unknown")
        repair_intent = str(signature.get("repair_intent") or "repair")
        return (
            header
            + "\n\n"
            "Intent: repair a representation gap detected by a previous "
            "Think run's audit or by an authoritative outcome oracle. "
            "The trigger payload names "
            f"audit_warning_code={warning_code!r} and "
            f"repair_intent={repair_intent!r}.\n"
            "\n"
            "If the trigger payload contains repair_batch_items, treat each "
            "item as a separate repair obligation sharing one reasoning pass. "
            "Prefer one compact diff that resolves the supported obligations; "
            "leave unsupported items as no-op rationale in reasoning_trace.\n"
            "\n"
            "Use selected observations and Models to exercise the missing "
            "loop directly:\n"
            "  - human_correction_submitted / apply_human_correction: treat "
            "oracle_outcome_fact as authoritative user feedback. Update, "
            "archive, contest, split, or attach counterevidence to the matching "
            "Models instead of restating the submitted correction.\n"
            "  - prediction_lifecycle_not_exercised: emit "
            "memory_lifecycle_ops action='confirm', 'falsify', 'revise', "
            "'archive', or 'unchanged' for the selected prediction-like Model.\n"
            "  - truth_pressure_absent_for_contestable_memory: emit a "
            "counterevidence relation, negative lifecycle op, or contested "
            "claim update only when supported by evidence.\n"
            "  - missing_curiosity_coverage: emit a bounded curiosity/unknown "
            "claim tied to concrete entities or commitments.\n"
            "  - missing_source_coverage, selected_raw_evidence_too_low, or "
            "selected_model_support_runaway: attach evidence, split overloaded "
            "claims, absorb near-duplicates, or no-op if the audit was already "
            "satisfied.\n"
            "\n"
            "Do not create recap facts solely to appease the audit. If the "
            "retrieved context does not support a repair, return an empty diff "
            "and explain the missing evidence in reasoning_trace."
        )
    if job.intent == "reorganize_memory":
        return (
            header
            + "\n\n"
            "Intent: background / maintenance / dependent re-evaluation. "
            "If the trigger carries a cause_model_id and cause_kind, update "
            "the dependent Model's confidence or archive it as appropriate. "
            "Prefer merge, retirement, confidence adjustment, or no-op over "
            "creating a sibling Model."
        )
    return header


def _build_surface_operating_instructions(
    trigger: TriggerContext,
    surface: PromptSurface,
) -> str:
    body = [
        "<operating_instructions>",
        "Produce the minimal diff supported by this surface:",
        "  (1) claim_ops for durable reality and scoped recommendations;",
    ]
    if surface.includes("lifecycle"):
        body.append("  (2) memory_lifecycle_ops/open_question_ops for selected memory review;")
    if surface.includes("graph"):
        body.append("  (3) relation/edge/frame/ontology ops for grounded Model relationships;")
    if surface.includes("acts"):
        body.append("  (4) act_ops only for explicit Goal/Commitment/Decision changes;")
    if surface.includes("resources"):
        body.append("  (5) resource_ops only for explicit resource changes;")

    if trigger.kind == "T1":
        body.append(
            "This is a T1 new signal. If it contains a concrete new fact, "
            "progress update, review result, approval, blocker, concern, "
            "customer stance, or dated plan not already captured by selected "
            "memory, emit a grounded claim_ops.insert. Do not no-op merely "
            "because no Act transition is available."
        )
        if surface.claims_only:
            body.append(
                "In this compact pass, represent warranted recommendations as "
                "claim_ops.insert norm/recommendation Models; do not emit "
                "act_ops, resource_ops, edge_ops, or lifecycle ops."
            )
        if surface.includes("acts"):
            body.append(
                "If the signal reports new in-flight work and <acts> has no "
                "matching commitment, co-emit a fact Model and a recommendation "
                "to create a commitment with target_act_ref "
                '{"type":"commitment","id":null}.'
            )
        if surface.includes("graph"):
            body.append(
                "When graph-anchor Models are selected, test whether the new "
                "claim confirms, weakens, contradicts, blocks, enables, "
                "explains, or advances them before creating unrelated siblings."
            )
    elif trigger.kind == "T4" and trigger.subkind == "pattern_review":
        body.append(
            "This is a T4 pattern_review trigger. Treat the candidate as weak "
            "evidence. Promote only when it is stable, useful, explainable, "
            "falsifiable, and action-shaping; otherwise no-op or ask a targeted "
            "open question when that operation is available."
        )
    elif reasoning_job_from_trigger(trigger).family == "internal_reflection":
        job = reasoning_job_from_trigger(trigger)
        body.append(
            "This is internal reflection. The trigger is asking how existing "
            f"memory should change for intent={job.intent}; prefer revise, "
            "connect, downgrade, promote, explain, or no-op over duplicate recap "
            "facts."
        )
        if job.intent == "adjudicate_candidate":
            body.append(
                "For latent relationship candidates, topology is evidence, not "
                "truth. Promote only a real relation, situation, ontology gap, "
                "or targeted lifecycle change."
            )
    elif trigger.kind == "T6":
        body.append(
            "This is a legacy topology shift. Use only member Model ids from "
            "<models>, <reasoning_frame>, or the trigger payload; do not emit "
            "Act mutations from topology alone."
        )

    if surface.includes("batch"):
        body.append(
            "Batch discipline: compress the batch into the few durable updates "
            "that change memory or action; cite duplicate/background signals in "
            "reasoning_trace instead of writing one op per signal."
        )
    if surface.includes("resources"):
        body.append(
            "Resource discipline: mutate Resources only from explicit resource "
            "evidence; otherwise use a scoped claim or question."
        )

    body.append(
        "For every claim_ops.insert, populate scope_actors and scope_entities "
        "from context UUIDs only, and add 6-16 top-level semantic_terms grounded "
        "in the claim text. Do not include names, UUIDs, handles, source "
        "channels, dates, exact domain_tags, claim_role, or data already stored "
        "in scope or grammar fields."
    )
    body.append(
        "Return ONLY a single JSON object. Use trigger_ref and tenant_id exactly "
        "as given in the triggering event metadata."
    )
    body.append("</operating_instructions>")
    return "\n".join(body)


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
    elif trigger.kind == "T4" and trigger.subkind == "pattern_review":
        body.append(
            "This is a T4 pattern_review trigger. The candidate comes from "
            "SAGE or precipitation and is weak evidence, not accepted truth. "
            "Review it like a latent regularity asking to become explicit "
            "company memory.\n"
            "\n"
            "Apply this promotion rubric before emitting any Pattern Model:\n"
            "  - stable: repeated behavior is supported by selected Models or "
            "Observations, not merely embedding density;\n"
            "  - useful: accepting it would change retrieval, judgment, "
            "action, escalation, or interpretation;\n"
            "  - explainable: the shared mechanism can be stated in ordinary "
            "company language;\n"
            "  - falsifiable: a concrete future signal could weaken or break "
            "the pattern;\n"
            "  - action-shaping: the pattern changes what the system should "
            "ask, retrieve, prioritize, or recommend.\n"
            "\n"
            "If the candidate passes, emit a normal `claim_ops.insert` with "
            "`kind=\"belief\"`, `claim_role=\"pattern\"`, "
            "`abstraction_level=\"pattern\"`, and `time_mode=\"recurring\"`. "
            "Cite selected evidence UUIDs in the claim text, natural text, "
            "supporting context, or reasoning_trace. Include a falsifier. "
            "If the candidate is only an active composite condition, prefer "
            "`claim_role=\"situation\"` instead of pattern.\n"
            "\n"
            "If evidence is thin, counterexamples are unresolved, or the "
            "candidate is only surface similarity, return an empty diff or a "
            "targeted open_question_op. In reasoning_trace, name the missing "
            "evidence or counterexample. Do not promote solely from "
            "cluster_size, density, or candidate_id."
        )
    elif reasoning_job_from_trigger(trigger).family == "internal_reflection":
        body.append(_internal_reflection_instructions(trigger))
    elif trigger.kind == "T6":
        # T6 is retained for legacy accepted-memory graph phase events.
        # New topology-discovered proposals arrive as T4 latent_relationship_candidate.
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
        "customer_context, actors_in_context, and candidate_substrate). If "
        "the signal names a PR or ticket (e.g., 'PR #847', 'ENG-501'), "
        "resolve the handle to the commitment in <acts> when present; "
        "otherwise use the exact candidate_commitment/workstream scope_ref "
        "from <candidate_substrate>. Do NOT invent UUIDs."
    )
    body.append(
        "Also populate top-level semantic_terms for every claim_ops.insert: "
        "6-16 specific lexical phrases grounded in the claim text that improve "
        "surface-language retrieval. Do not include names/UUIDs/handles/source "
        "channels/dates, exact domain_tags, claim_role, or anything already "
        "stored in scope_actors, scope_entities, or grammar axes."
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


__all__ = [
    "PromptPair",
    "PromptSurface",
    "build_prompt",
    "prompt_static_size_report",
    "select_prompt_surface",
]
