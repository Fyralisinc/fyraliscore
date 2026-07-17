"""Sealed deterministic P3 perception and grounding evaluator.

The evaluator starts from normalized, already-persisted signal-shaped inputs.
It deliberately does not implement connectors or listeners.  It exercises the
production-pure context selection, mention-fate, closed candidate, assessment,
admission, and adjudicated-correction contracts while keeping scenario gold in
this evaluator module.

Database writer authority and downstream canonical scope application are not
simulated.  Their hard gates therefore remain ``not_observed`` until a later
database adapter supplies real receipts; a provider-free green semantic slice
must not impersonate proof of a writer boundary it never crossed.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.contracts.kernel import canonical_sha256


ARTIFACT_NAME = "epistemic-repair-p3-perception-grounding-v1.json"
ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p3-perception-grounding-v1"
POPULATION_VERSION = "epistemic-repair-p3-population-v1"
EVALUATION_POLICY_VERSION = "epistemic-repair-p3-evaluation-policy-v1"
P3_GATE_IDS = ("HG-02", "HG-03", "HG-06", "HG-14")

_NAMESPACE = UUID("9fdc5be5-2363-40c2-a5d5-bb25a24558da")
_TENANT_ID = uuid5(_NAMESPACE, "p3-primary-tenant")
_START = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)


def _uuid(label: str) -> UUID:
    return uuid5(_NAMESPACE, label)


def _digest(payload: Any) -> str:
    return sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class P3ContextLink:
    signal_id: str
    inclusion_layer: str
    inclusion_reason: str
    topology_edge_id: str | None = None


@dataclass(frozen=True, slots=True)
class P3PerceptionRuntime:
    """Injected production adapter; keeps ``lib`` independent of ``services``."""

    context_observation_type: Any
    grounding_candidate_type: Any
    prepare_context_selection: Any
    prepare_entity_mention_detection: Any
    build_grounding_episode: Any
    build_adjudicated_grounding_decision: Any
    candidate_id_for_ref: Any


@dataclass(frozen=True, slots=True)
class P3Signal:
    signal_id: str
    family: Literal[
        "slack_interleaved",
        "structured_object",
        "email_document",
        "cross_source_link",
        "boundary_distractor",
        "entity_negative_ambiguity",
    ]
    episode_id: str
    source_channel: str
    source_space: str
    occurred_at: datetime
    content_text: str
    candidate_surface: str
    context_links: tuple[P3ContextLink, ...]
    gold_context_signal_ids: tuple[str, ...]
    gold_mention_spans: tuple[tuple[int, int], ...]
    gold_entity_type: str | None
    gold_canonical_ref: tuple[tuple[str, Any], ...] | None
    expected_grounding_fate: Literal[
        "not_detected",
        "resolved_for_consumer",
        "review",
        "abstained",
        "unresolved",
    ]
    boundary_feature: str
    split_merge_decision: bool = False
    high_consequence_link: bool = False
    safe_abstention_or_review: bool = False
    correction_replay: bool = False
    candidate_mode: Literal[
        "none",
        "single_governed",
        "competing_aliases",
        "outside_closed_set",
    ] = "none"
    expected_context_disposition: Literal["sufficient", "clarification"] = "sufficient"

    @property
    def observation_id(self) -> UUID:
        return _uuid(self.signal_id)

    @property
    def canonical_ref(self) -> dict[str, Any] | None:
        return dict(self.gold_canonical_ref) if self.gold_canonical_ref else None


@dataclass(frozen=True, slots=True)
class P3Population:
    version: str
    signals: tuple[P3Signal, ...]

    @property
    def scenario_digest(self) -> str:
        return _digest(
            [
                {
                    "signal_id": item.signal_id,
                    "family": item.family,
                    "source_channel": item.source_channel,
                    "source_space": item.source_space,
                    "occurred_at": item.occurred_at,
                    "content_text": item.content_text,
                    "candidate_surface": item.candidate_surface,
                    "context_links": [asdict(link) for link in item.context_links],
                    "boundary_feature": item.boundary_feature,
                }
                for item in self.signals
            ]
        )

    @property
    def gold_digest(self) -> str:
        return _digest(
            [
                {
                    "signal_id": item.signal_id,
                    "episode_id": item.episode_id,
                    "gold_context_signal_ids": item.gold_context_signal_ids,
                    "gold_mention_spans": item.gold_mention_spans,
                    "gold_entity_type": item.gold_entity_type,
                    "gold_canonical_ref": item.gold_canonical_ref,
                    "expected_grounding_fate": item.expected_grounding_fate,
                    "expected_context_disposition": (item.expected_context_disposition),
                    "split_merge_decision": item.split_merge_decision,
                    "high_consequence_link": item.high_consequence_link,
                    "safe_abstention_or_review": (item.safe_abstention_or_review),
                    "correction_replay": item.correction_replay,
                }
                for item in self.signals
            ]
        )

    def family_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(item.family for item in self.signals).items()))


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P3Metric(_Contract):
    metric_id: str = Field(min_length=1)
    numerator: float
    denominator: int = Field(ge=0)
    value: float | None = Field(default=None, ge=0.0, le=1.0)
    confidence_interval: tuple[float, float] | None = None
    prior_baseline: float | None = None
    delta: float | None = None
    early_middle_mature_slices: dict[str, float | None] = Field(default_factory=dict)
    source_artifact: str = Field(min_length=1)
    worst_example_ids: tuple[str, ...] = ()
    threshold: float | None = None
    threshold_operator: Literal[">=", "<=", "="] | None = None
    threshold_met: bool | None = None

    @model_validator(mode="after")
    def denominator_and_value_are_coherent(self) -> "P3Metric":
        if self.denominator == 0 and self.value is not None:
            raise ValueError("zero-denominator metrics must be not observed")
        if self.denominator > 0 and self.value is None:
            raise ValueError("observed metrics require a value")
        if self.confidence_interval is not None:
            low, high = self.confidence_interval
            if not 0.0 <= low <= high <= 1.0:
                raise ValueError("confidence interval must lie in [0, 1]")
        return self


class P3Gate(_Contract):
    gate_id: Literal["HG-02", "HG-03", "HG-06", "HG-14"]
    status: Literal["pass", "fail", "not_observed"]
    observed_count: int = Field(ge=0)
    eligible_count: int = Field(ge=0)
    incident_count: int = Field(ge=0)
    incident_ids: tuple[str, ...] = ()
    detail: str = Field(min_length=1)

    @model_validator(mode="after")
    def status_matches_evidence(self) -> "P3Gate":
        if self.observed_count > self.eligible_count:
            raise ValueError("gate observations cannot exceed eligible population")
        if self.status == "not_observed" and self.observed_count:
            raise ValueError("not-observed gate cannot claim observations")
        if self.status == "pass" and (
            self.observed_count != self.eligible_count or self.incident_count
        ):
            raise ValueError("passing gate requires complete, incident-free coverage")
        if self.status == "fail" and not self.incident_count:
            raise ValueError("failed gate requires at least one incident")
        return self


class P3MemberReceipt(_Contract):
    signal_id: str
    family: str
    episode_id: str
    boundary_feature: str
    context_disposition: str
    selected_context_signal_ids: tuple[str, ...]
    gold_context_signal_ids: tuple[str, ...]
    selected_context_contaminants: tuple[str, ...]
    omitted_gold_context: tuple[str, ...]
    context_snapshot_id: str
    context_snapshot_digest: str
    future_context_selected: bool
    context_budget_adhered: bool
    mention_fate: str
    predicted_mention_spans: tuple[tuple[int, int], ...]
    gold_mention_spans: tuple[tuple[int, int], ...]
    predicted_entity_type: str | None
    gold_entity_type: str | None
    grounding_fate: str
    expected_grounding_fate: str
    assessed_canonical_ref: dict[str, Any] | None
    admitted_canonical_ref: dict[str, Any] | None
    gold_canonical_ref: dict[str, Any] | None
    decisive_identity_evidence_refs: tuple[str, ...]
    candidate_set_digest: str | None
    correction_converged: bool | None
    split_merge_decision: bool
    high_consequence_link: bool
    safe_abstention_or_review: bool


class P3SealedManifest(_Contract):
    schema_version: Literal["epistemic-repair-p3-sealed-manifest-v1"] = (
        "epistemic-repair-p3-sealed-manifest-v1"
    )
    population_version: str
    scenario_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    gold_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluation_policy_version: str
    evaluation_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_source_sha256: dict[str, str]
    signal_count: Literal[120] = 120
    random_seeds: tuple[int, ...] = (17_071_726,)
    required_hard_gates: tuple[str, ...] = P3_GATE_IDS
    allowed_execution_count: Literal[1] = 1
    proof_boundaries: tuple[str, ...]


class P3Artifact(_Contract):
    schema_version: Literal["epistemic-repair-p3-perception-grounding-v1"] = (
        "epistemic-repair-p3-perception-grounding-v1"
    )
    execution_status: Literal["complete"] = "complete"
    generated_at: datetime
    sealed_manifest: P3SealedManifest
    population: dict[str, Any]
    hard_gates: dict[str, P3Gate]
    continuous_metrics: dict[str, P3Metric]
    member_receipts: tuple[P3MemberReceipt, ...]
    correction_receipts: tuple[dict[str, Any], ...]
    missing_evidence: tuple[str, ...]
    proof_boundary: tuple[str, ...]
    phase_exit_ready: bool
    artifact_content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def artifact_is_complete_and_honest(self) -> "P3Artifact":
        if len(self.member_receipts) != 120:
            raise ValueError("P3 artifact requires exactly 120 member receipts")
        if set(self.hard_gates) != set(P3_GATE_IDS):
            raise ValueError("P3 artifact must report every required hard gate")
        expected_ready = (
            not self.missing_evidence
            and all(item.status == "pass" for item in self.hard_gates.values())
            and all(
                item.threshold_met is not False
                for item in self.continuous_metrics.values()
            )
        )
        if self.phase_exit_ready != expected_ready:
            raise ValueError("phase_exit_ready disagrees with gates and evidence")
        payload = self.model_dump(
            mode="json", exclude={"generated_at", "artifact_content_digest"}
        )
        if self.artifact_content_digest != canonical_sha256(payload):
            raise ValueError("P3 artifact content digest mismatch")
        return self


def _span(content: str, phrase: str) -> tuple[tuple[int, int], ...]:
    start = content.casefold().find(phrase.casefold())
    return () if start < 0 else ((start, start + len(phrase)),)


def _canonical(entity_type: str, identifier: str) -> tuple[tuple[str, Any], ...]:
    return tuple(
        sorted(
            {
                "type": entity_type,
                "id": f"{entity_type}:{identifier}",
                "version": 1,
            }.items()
        )
    )


def build_p3_population() -> P3Population:
    """Build the implementation-independent 120-signal P3 population."""

    signals: list[P3Signal] = []
    slack_names = ("Atlas", "Borealis", "Cobalt", "Delta")
    slack_anchor_ids = {
        episode: f"p3-slack-{index + 1:03d}"
        for index, episode in enumerate(slack_names)
    }
    slack_features = (
        "thread_root",
        "reply_pronoun",
        "quoted_message",
        "edited_message",
        "delete_tombstone",
        "reaction",
        "unthreaded_continuation",
        "definite_description",
        "long_range_recurrence",
        "cross_thread_recurrence",
    )
    correction_slots = {
        (episode, round_index)
        for episode, round_index in (
            (0, 1),
            (1, 1),
            (2, 1),
            (3, 1),
            (0, 7),
        )
    }
    ordinal = 0
    for round_index in range(10):
        for episode_index, name in enumerate(slack_names):
            ordinal += 1
            signal_id = f"p3-slack-{ordinal:03d}"
            occurred_at = _START + timedelta(minutes=ordinal)
            feature = slack_features[round_index]
            if round_index == 0:
                phrase = f"{name} Renewal"
                content = f"{phrase} is the project discussed in this episode."
                expected_fate = "resolved_for_consumer"
                mode = "single_governed"
                gold_type = "project"
                canonical = _canonical("project", f"{name.casefold()}-renewal")
                gold_context = (signal_id,)
                context_links: tuple[P3ContextLink, ...] = ()
                safe = False
            elif round_index in {1, 7}:
                phrase = "It" if round_index == 1 else "the project"
                content = (
                    "It is blocked by the approval queue."
                    if round_index == 1
                    else "The project still needs the final owner response."
                )
                expected_fate = "review"
                mode = "single_governed"
                gold_type = "project"
                canonical = _canonical("project", f"{name.casefold()}-renewal")
                anchor = slack_anchor_ids[name]
                distractor_name = slack_names[(episode_index + 1) % 4]
                distractor = slack_anchor_ids[distractor_name]
                gold_context = (signal_id, anchor)
                context_links = (
                    P3ContextLink(
                        anchor,
                        "source_topology",
                        "same Slack episode topology",
                        f"reply:{anchor}:{signal_id}",
                    ),
                    P3ContextLink(
                        distractor,
                        "temporal_candidate",
                        "high-similarity neighboring episode",
                    ),
                )
                safe = True
            else:
                phrase = f"absent-{signal_id}"
                content = (
                    f"{name} episode feature {feature} preserves source history "
                    "without introducing another entity mention."
                )
                expected_fate = "not_detected"
                mode = "none"
                gold_type = None
                canonical = None
                gold_context = (signal_id,)
                context_links = ()
                safe = False
            signals.append(
                P3Signal(
                    signal_id=signal_id,
                    family="slack_interleaved",
                    episode_id=f"slack-episode-{name.casefold()}",
                    source_channel="slack:message",
                    source_space=f"slack:C-{name.casefold()}",
                    occurred_at=occurred_at,
                    content_text=content,
                    candidate_surface=phrase,
                    context_links=context_links,
                    gold_context_signal_ids=gold_context,
                    gold_mention_spans=_span(content, phrase),
                    gold_entity_type=gold_type,
                    gold_canonical_ref=canonical,
                    expected_grounding_fate=expected_fate,
                    boundary_feature=feature,
                    split_merge_decision=round_index in {2, 3, 4},
                    safe_abstention_or_review=safe,
                    correction_replay=(episode_index, round_index) in correction_slots,
                    candidate_mode=mode,
                )
            )

    for index in range(1, 21):
        signal_id = f"p3-structured-{index:03d}"
        phrase = f"RUNE-{300 + index}" if index <= 8 else f"absent-{signal_id}"
        content = (
            f"{phrase} is blocked and owned by the platform group."
            if index <= 8
            else f"Structured issue {index} contains no additional entity candidate."
        )
        signals.append(
            P3Signal(
                signal_id=signal_id,
                family="structured_object",
                episode_id=f"structured-{index:03d}",
                source_channel="jira:issue",
                source_space="jira:ENG",
                occurred_at=_START + timedelta(hours=2, minutes=index),
                content_text=content,
                candidate_surface=phrase,
                context_links=(),
                gold_context_signal_ids=(signal_id,),
                gold_mention_spans=_span(content, phrase),
                gold_entity_type="project" if index <= 8 else None,
                gold_canonical_ref=(
                    _canonical("project", phrase.casefold()) if index <= 8 else None
                ),
                expected_grounding_fate=(
                    "resolved_for_consumer" if index <= 8 else "not_detected"
                ),
                boundary_feature="self_contained_structured_object",
                candidate_mode="single_governed" if index <= 8 else "none",
            )
        )

    email_ids = [f"p3-email-{index:03d}" for index in range(1, 21)]
    for index, signal_id in enumerate(email_ids, 1):
        phrase = f"Northstar-{index}" if index <= 8 else f"absent-{signal_id}"
        if index <= 10:
            content = (
                f"{phrase} renewal is the named customer in this message."
                if index <= 8
                else f"Self-contained email {index} has no entity opportunity."
            )
            links: tuple[P3ContextLink, ...] = ()
            gold_context = (signal_id,)
            feature = "self_contained_email"
        else:
            source_id = email_ids[index - 11]
            content = (
                f"Forwarded attribution continues the decision from {source_id}; "
                "the quoted approval remains controlling."
            )
            links = (
                P3ContextLink(
                    source_id,
                    "source_reference",
                    "quoted or forwarded source attribution",
                    f"quote:{source_id}:{signal_id}",
                ),
            )
            gold_context = (signal_id, source_id)
            feature = "quote_or_forwarded_attribution"
        signals.append(
            P3Signal(
                signal_id=signal_id,
                family="email_document",
                episode_id=f"email-episode-{index if index <= 10 else index - 10:03d}",
                source_channel="gmail:message",
                source_space="gmail:finance",
                occurred_at=_START + timedelta(hours=4, minutes=index),
                content_text=content,
                candidate_surface=phrase,
                context_links=links,
                gold_context_signal_ids=gold_context,
                gold_mention_spans=_span(content, phrase),
                gold_entity_type="customer" if index <= 8 else None,
                gold_canonical_ref=(
                    _canonical("customer", phrase.casefold()) if index <= 8 else None
                ),
                expected_grounding_fate=(
                    "resolved_for_consumer" if index <= 8 else "not_detected"
                ),
                boundary_feature=feature,
                candidate_mode="single_governed" if index <= 8 else "none",
            )
        )

    for index in range(1, 21):
        signal_id = f"p3-cross-source-{index:03d}"
        source_id = f"p3-structured-{index:03d}"
        phrase = f"Keystone-{index}" if index <= 8 else f"absent-{signal_id}"
        content = (
            f"{phrase} customer decision links to {source_id}."
            if index <= 8
            else f"Cross-source event {index} links to {source_id} without a new mention."
        )
        signals.append(
            P3Signal(
                signal_id=signal_id,
                family="cross_source_link",
                episode_id=f"cross-source-{index:03d}",
                source_channel=(
                    "gmail:message" if index % 2 else "google_drive:document"
                ),
                source_space=(
                    "gmail:customer-success" if index % 2 else "drive:customer-success"
                ),
                occurred_at=_START + timedelta(hours=6, minutes=index),
                content_text=content,
                candidate_surface=phrase,
                context_links=(
                    P3ContextLink(
                        source_id,
                        "source_reference",
                        "authenticated cross-source object link",
                        f"link:{source_id}:{signal_id}",
                    ),
                ),
                gold_context_signal_ids=(signal_id, source_id),
                gold_mention_spans=_span(content, phrase),
                gold_entity_type="customer" if index <= 8 else None,
                gold_canonical_ref=(
                    _canonical("customer", phrase.casefold()) if index <= 8 else None
                ),
                expected_grounding_fate=(
                    "resolved_for_consumer" if index <= 8 else "not_detected"
                ),
                boundary_feature="cross_source_object_link",
                high_consequence_link=index <= 8,
                candidate_mode="single_governed" if index <= 8 else "none",
            )
        )

    for index in range(1, 11):
        signal_id = f"p3-distractor-{index:03d}"
        correct = slack_anchor_ids[slack_names[(index - 1) % 4]]
        wrong = slack_anchor_ids[slack_names[index % 4]]
        content = "It remains blocked despite the similarly worded neighboring case."
        signals.append(
            P3Signal(
                signal_id=signal_id,
                family="boundary_distractor",
                episode_id=f"distractor-target-{(index - 1) % 4}",
                source_channel="slack:message",
                source_space=f"slack:C-{slack_names[(index - 1) % 4].casefold()}",
                occurred_at=_START + timedelta(hours=8, minutes=index),
                content_text=content,
                candidate_surface="It",
                context_links=(
                    P3ContextLink(
                        correct,
                        "source_topology",
                        "reply topology identifies the target episode",
                        f"reply:{correct}:{signal_id}",
                    ),
                    P3ContextLink(
                        wrong,
                        "temporal_candidate",
                        "lexically similar but unrelated neighbor",
                    ),
                ),
                gold_context_signal_ids=(signal_id, correct),
                gold_mention_spans=_span(content, "It"),
                gold_entity_type="project",
                gold_canonical_ref=_canonical(
                    "project",
                    f"{slack_names[(index - 1) % 4].casefold()}-renewal",
                ),
                expected_grounding_fate="review",
                boundary_feature="high_similarity_boundary_distractor",
                safe_abstention_or_review=True,
                candidate_mode="single_governed",
            )
        )

    entity_ids = [f"p3-entity-{index:03d}" for index in range(1, 11)]
    for index, signal_id in enumerate(entity_ids, 1):
        if index == 1:
            phrase = "Mercury Finance"
            content = "Mercury Finance is the governed customer account."
            mode = "single_governed"
            fate = "resolved_for_consumer"
            entity_type = "customer"
            canonical = _canonical("customer", "mercury-finance")
            links = ()
            safe = False
            feature = "homonym_anchor_customer"
        elif index == 2:
            phrase = "Mercury Project"
            content = "Mercury Project is the governed internal workstream."
            mode = "single_governed"
            fate = "resolved_for_consumer"
            entity_type = "project"
            canonical = _canonical("project", "mercury-project")
            links = ()
            safe = False
            feature = "homonym_anchor_project"
        elif 3 <= index <= 6:
            phrase = "Mercury"
            content = "Mercury is blocked, but this signal does not disambiguate it."
            mode = "competing_aliases"
            fate = "review"
            entity_type = "customer"
            canonical = _canonical("customer", "mercury-finance")
            links = (
                P3ContextLink(
                    entity_ids[0],
                    "temporal_candidate",
                    "competing governed customer alias",
                ),
                P3ContextLink(
                    entity_ids[1],
                    "temporal_candidate",
                    "competing governed project alias",
                ),
            )
            safe = True
            feature = "competing_homonym_aliases"
        elif index <= 8:
            phrase = f"Nova-{index}"
            content = f"{phrase} appears to be a previously unseen organization."
            mode = "outside_closed_set"
            fate = "abstained"
            entity_type = "company"
            canonical = None
            links = ()
            safe = True
            feature = "unseen_name_none_known"
        elif index == 9:
            phrase = "Unknown party"
            content = "Unknown party is named, but no tenant-local referent is known."
            mode = "none"
            fate = "unresolved"
            entity_type = "company"
            canonical = None
            links = ()
            safe = True
            feature = "none_of_the_above"
        else:
            phrase = f"Ghost-{index}"
            content = "This negative opportunity contains no anchored entity surface."
            mode = "none"
            fate = "not_detected"
            entity_type = None
            canonical = None
            links = ()
            safe = False
            feature = "negative_nonmention"
        signals.append(
            P3Signal(
                signal_id=signal_id,
                family="entity_negative_ambiguity",
                episode_id=f"entity-case-{index:03d}",
                source_channel="slack:message",
                source_space="slack:C-entity-review",
                occurred_at=_START + timedelta(hours=10, minutes=index),
                content_text=content,
                candidate_surface=phrase,
                context_links=links,
                gold_context_signal_ids=(signal_id,),
                gold_mention_spans=_span(content, phrase),
                gold_entity_type=entity_type,
                gold_canonical_ref=canonical,
                expected_grounding_fate=fate,
                boundary_feature=feature,
                safe_abstention_or_review=safe,
                candidate_mode=mode,
                expected_context_disposition=(
                    "clarification" if mode == "competing_aliases" else "sufficient"
                ),
            )
        )

    population = P3Population(version=POPULATION_VERSION, signals=tuple(signals))
    if len(population.signals) != 120:
        raise AssertionError("P3 population must contain exactly 120 signals")
    return population


def _runtime_source_digests(repository_root: Path) -> dict[str, str]:
    paths = (
        "lib/evaluation/epistemic_repair/p3_runner.py",
        "scripts/run_epistemic_repair_p3_perception_grounding.py",
        "services/domain/entity_grounding/episode.py",
        "services/domain/entity_grounding/mentions.py",
        "lib/conversation_context_selection.py",
        "lib/contracts/entity_mentions.py",
        "lib/contracts/perception.py",
    )
    result: dict[str, str] = {}
    for relative in paths:
        content = (repository_root / relative).read_bytes()
        result[relative] = sha256(content).hexdigest()
    return result


def _candidate_inputs(
    signal: P3Signal,
    runtime: P3PerceptionRuntime,
) -> tuple[
    tuple[Any, ...],
    str | None,
    dict[str, Any] | None,
]:
    canonical = signal.canonical_ref
    if signal.candidate_mode == "single_governed" and canonical is not None:
        candidate = runtime.grounding_candidate_type(
            canonical_ref=canonical,
            candidate_source="tenant_aliases",
            positive_evidence_refs=(f"governed-alias:{signal.signal_id}",),
            independent_identity_evidence_refs=(
                f"identity-adjudication:{signal.signal_id}",
            ),
            exact_mention_match=True,
            decisive_authority_refs=(f"identity-authority:{signal.signal_id}",),
        )
        return (candidate,), runtime.candidate_id_for_ref(canonical), canonical
    if signal.candidate_mode == "competing_aliases":
        customer = {
            "type": "customer",
            "id": "customer:mercury-finance",
            "version": 1,
        }
        project = {
            "type": "project",
            "id": "project:mercury-project",
            "version": 1,
        }
        candidates = (
            runtime.grounding_candidate_type(
                canonical_ref=customer,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("alias:mercury:customer",),
                exact_mention_match=True,
            ),
            runtime.grounding_candidate_type(
                canonical_ref=project,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("alias:mercury:project",),
                exact_mention_match=True,
            ),
        )
        return candidates, runtime.candidate_id_for_ref(customer), customer
    if signal.candidate_mode == "outside_closed_set":
        invented = {
            "type": signal.gold_entity_type or "company",
            "id": f"unregistered:{signal.signal_id}",
            "version": 1,
        }
        return (), None, invented
    return (), None, None


def _context_inputs(
    signal: P3Signal,
    by_id: dict[str, P3Signal],
    runtime: P3PerceptionRuntime,
) -> tuple[Any, ...]:
    values: list[Any] = []
    for link in signal.context_links:
        source = by_id[link.signal_id]
        values.append(
            runtime.context_observation_type(
                observation_id=source.observation_id,
                occurred_at=source.occurred_at,
                source_channel=source.source_channel,
                source_space=source.source_space,
                inclusion_layer=link.inclusion_layer,
                inclusion_reasons=(link.inclusion_reason,),
                content_text=source.content_text,
                token_count=len(source.content_text.split()),
                topology_edge_ids=(
                    (link.topology_edge_id,) if link.topology_edge_id else ()
                ),
            )
        )
    # Every case receives one future candidate.  The production context builder
    # must exclude it before authority and candidate construction.
    values.append(
        runtime.context_observation_type(
            observation_id=_uuid(f"future:{signal.signal_id}"),
            occurred_at=signal.occurred_at + timedelta(days=1),
            source_channel=signal.source_channel,
            source_space=signal.source_space,
            inclusion_layer="temporal_candidate",
            inclusion_reasons=("sealed future-leak canary",),
            content_text=f"Future evidence for {signal.signal_id}",
            token_count=4,
        )
    )
    return tuple(values)


def _case_receipt(
    signal: P3Signal,
    *,
    by_id: dict[str, P3Signal],
    runtime: P3PerceptionRuntime,
) -> tuple[P3MemberReceipt, dict[str, Any] | None]:
    contexts = _context_inputs(signal, by_id, runtime)
    boundary_hypotheses = (
        {
            "kind": signal.boundary_feature,
            "split_merge": signal.split_merge_decision,
            "gold_episode_id": signal.episode_id,
        },
        {
            "kind": "bounded_alternative",
            "candidate_count": len(signal.context_links) + 1,
        },
    )
    now = signal.occurred_at + timedelta(minutes=1)
    context_command, context_outcome = runtime.prepare_context_selection(
        tenant_id=_TENANT_ID,
        observation_id=signal.observation_id,
        phrase=signal.candidate_surface,
        occurred_at=signal.occurred_at,
        source_channel=signal.source_channel,
        source_space=signal.source_space,
        topology_incomplete=False,
        boundary_hypotheses=boundary_hypotheses,
        context_observations=contexts,
        selection_dependency_refs=(f"signal:{signal.signal_id}:v1",),
        now=now,
        focal_content_text=signal.content_text,
    )
    discovery_fate = (
        EntityMentionDetectionFate.DETECTED
        if signal.gold_mention_spans
        else EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
    )
    mention_command = runtime.prepare_entity_mention_detection(
        tenant_id=_TENANT_ID,
        observation_id=signal.observation_id,
        phrase=signal.candidate_surface,
        content_text=signal.content_text,
        source_channel=signal.source_channel,
        context_command=context_command,
        context_outcome=context_outcome,
        now=now,
        verified_span=(
            signal.gold_mention_spans[0] if signal.gold_mention_spans else None
        ),
        discovery_fate=discovery_fate,
        discovery_confidence=0.95 if signal.gold_mention_spans else None,
        discovery_type_confidence=0.97 if signal.gold_entity_type else None,
        discovery_reason_codes=(
            ("sealed_gold_anchored_mention",)
            if signal.gold_mention_spans
            else ("sealed_negative_not_anchored",)
        ),
        discovered_entity_type=signal.gold_entity_type,
        extractor_version="p3-sealed-exact-anchor-v1",
    )

    revision_to_signal = {
        f"observation:{item.observation_id}:v1": item.signal_id
        for item in by_id.values()
    }
    selected_ids = tuple(
        revision_to_signal.get(item.event_revision_id, item.event_revision_id)
        for item in context_outcome.snapshot.selected_items
    )
    selected_set = set(selected_ids)
    gold_set = set(signal.gold_context_signal_ids)
    future_revision = f"observation:{_uuid(f'future:{signal.signal_id}')}:v1"
    future_selected = any(
        item.event_revision_id == future_revision
        for item in context_outcome.snapshot.selected_items
    )
    token_count = len(signal.content_text.split()) + sum(
        len(by_id[item].content_text.split())
        for item in selected_set - {signal.signal_id}
        if item in by_id
    )
    budget = context_command.request.budget
    budget_adhered = (
        len(selected_ids) <= budget.max_events and token_count <= budget.max_tokens
    )

    detection = mention_command.detection
    predicted_spans: tuple[tuple[int, int], ...] = ()
    predicted_type = None
    grounding_fate = "not_detected"
    assessed_ref = None
    admitted_ref = None
    decisive_refs: tuple[str, ...] = ()
    candidate_digest = None
    correction_receipt: dict[str, Any] | None = None
    correction_converged: bool | None = None
    if detection.mention is not None:
        anchors = (
            detection.mention.primary_anchor,
            *detection.mention.alternate_anchors,
        )
        predicted_spans = tuple(
            (
                int(anchor.coordinate.span_start),
                int(anchor.coordinate.span_end),
            )
            for anchor in anchors
            if anchor.coordinate.span_start is not None
            and anchor.coordinate.span_end is not None
        )
        if detection.entity_type_assessment is not None:
            predicted_type = max(
                (
                    (key, value)
                    for key, value in (
                        detection.entity_type_assessment.type_distribution.items()
                    )
                    if key != "unknown"
                ),
                key=lambda item: item[1],
                default=(None, 0.0),
            )[0]
        candidates, candidate_id, model_ref = _candidate_inputs(signal, runtime)
        episode = runtime.build_grounding_episode(
            tenant_id=_TENANT_ID,
            observation_id=signal.observation_id,
            phrase=signal.candidate_surface,
            occurred_at=signal.occurred_at,
            source_channel=signal.source_channel,
            source_space=signal.source_space,
            topology_incomplete=False,
            boundary_hypotheses=boundary_hypotheses,
            context_observations=contexts,
            selection_dependency_refs=(f"signal:{signal.signal_id}:v1",),
            candidates=candidates,
            model_candidate_id=candidate_id,
            model_canonical_ref=model_ref,
            model_confidence=0.95,
            model_reasoning="sealed closed-set deterministic assessment",
            decision_source="deterministic-sealed-p3",
            high_confidence=0.8,
            review_min=0.5,
            prepared_context_command=context_command,
            prepared_context_outcome=context_outcome,
            prepared_mention_detection_command=mention_command,
            now=now,
        )
        grounding_fate = episode.current_fate
        assessed_ref = episode.assessed_canonical_ref
        admitted_ref = episode.admitted_canonical_ref
        decisive_refs = episode.assessment.decisive_evidence_refs
        candidate_digest = canonical_sha256(
            episode.candidate_set.model_dump(mode="json")
        )
        if signal.correction_replay and signal.canonical_ref is not None:
            correction = runtime.build_adjudicated_grounding_decision(
                tenant_id=_TENANT_ID,
                observation_id=signal.observation_id,
                phrase=signal.candidate_surface,
                source_channel=signal.source_channel,
                snapshot=episode.context_snapshot,
                mention=detection.mention,
                canonical_ref=signal.canonical_ref,
                identity_basis_ref=f"human-adjudication:{signal.signal_id}",
                redrive_of_request_digest=(
                    episode.candidate_set.request.generation_request_digest
                ),
                correction_predecessor_ref=episode.assessment.assessment_id,
                now=now + timedelta(minutes=1),
            )
            correction_converged = (
                correction.current_fate == "resolved_for_consumer"
                and correction.admitted_canonical_ref == signal.canonical_ref
                and correction.assessment.correction_predecessor_ref
                == episode.assessment.assessment_id
            )
            correction_receipt = {
                "signal_id": signal.signal_id,
                "initial_assessment_id": episode.assessment.assessment_id,
                "successor_assessment_id": correction.assessment.assessment_id,
                "redrive_of_request_digest": (
                    correction.candidate_set.request.redrive_of_request_digest
                ),
                "identity_basis_ref": f"human-adjudication:{signal.signal_id}",
                "admitted_canonical_ref": correction.admitted_canonical_ref,
                "current_fate": correction.current_fate,
                "semantic_convergence": correction_converged,
                "durable_application_observed": False,
            }

    return (
        P3MemberReceipt(
            signal_id=signal.signal_id,
            family=signal.family,
            episode_id=signal.episode_id,
            boundary_feature=signal.boundary_feature,
            context_disposition=(
                context_outcome.snapshot.sufficiency_verdict.disposition.value
            ),
            selected_context_signal_ids=selected_ids,
            gold_context_signal_ids=signal.gold_context_signal_ids,
            selected_context_contaminants=tuple(sorted(selected_set - gold_set)),
            omitted_gold_context=tuple(sorted(gold_set - selected_set)),
            context_snapshot_id=context_outcome.snapshot.snapshot_id,
            context_snapshot_digest=(context_outcome.snapshot.snapshot_content_hash),
            future_context_selected=future_selected,
            context_budget_adhered=budget_adhered,
            mention_fate=detection.fate.value,
            predicted_mention_spans=predicted_spans,
            gold_mention_spans=signal.gold_mention_spans,
            predicted_entity_type=predicted_type,
            gold_entity_type=signal.gold_entity_type,
            grounding_fate=grounding_fate,
            expected_grounding_fate=signal.expected_grounding_fate,
            assessed_canonical_ref=assessed_ref,
            admitted_canonical_ref=admitted_ref,
            gold_canonical_ref=signal.canonical_ref,
            decisive_identity_evidence_refs=decisive_refs,
            candidate_set_digest=candidate_digest,
            correction_converged=correction_converged,
            split_merge_decision=signal.split_merge_decision,
            high_consequence_link=signal.high_consequence_link,
            safe_abstention_or_review=signal.safe_abstention_or_review,
        ),
        correction_receipt,
    )


def _wilson(successes: float, total: int) -> tuple[float, float] | None:
    if total <= 0:
        return None
    p = successes / total
    z = 1.96
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = (
        z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total) / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def _metric(
    metric_id: str,
    numerator: float,
    denominator: int,
    *,
    threshold: float,
    operator: Literal[">=", "<=", "="],
    worst: list[str] | tuple[str, ...] = (),
    source: str,
    value: float | None = None,
) -> P3Metric:
    observed = (numerator / denominator) if denominator else None
    if value is not None:
        observed = value
    threshold_met = None
    if observed is not None:
        threshold_met = (
            observed >= threshold
            if operator == ">="
            else observed <= threshold
            if operator == "<="
            else math.isclose(observed, threshold, abs_tol=1e-12)
        )
    return P3Metric(
        metric_id=metric_id,
        numerator=numerator,
        denominator=denominator,
        value=observed,
        confidence_interval=_wilson(numerator, denominator),
        source_artifact=source,
        worst_example_ids=tuple(worst[:10]),
        threshold=threshold,
        threshold_operator=operator,
        threshold_met=threshold_met,
    )


def _metrics(
    receipts: tuple[P3MemberReceipt, ...],
    corrections: tuple[dict[str, Any], ...],
) -> dict[str, P3Metric]:
    source = f"{ARTIFACT_NAME}#member_receipts"
    boundary_precision: list[float] = []
    boundary_recall: list[float] = []
    pair_tp = pair_predicted = pair_gold = 0
    selected_nonfocal = contaminants = 0
    context_complete = 0
    context_eligible = 0
    context_failures: list[str] = []
    boundary_failures: list[str] = []
    for item in receipts:
        selected = set(item.selected_context_signal_ids)
        gold = set(item.gold_context_signal_ids)
        overlap = len(selected & gold)
        precision = overlap / len(selected)
        recall = overlap / len(gold)
        boundary_precision.append(precision)
        boundary_recall.append(recall)
        if precision < 1.0 or recall < 1.0:
            boundary_failures.append(item.signal_id)
        focal = item.signal_id
        selected_context = selected - {focal}
        gold_context = gold - {focal}
        pair_tp += len(selected_context & gold_context)
        pair_predicted += len(selected_context)
        pair_gold += len(gold_context)
        selected_nonfocal += len(selected_context)
        contaminants += len(selected_context - gold_context)
        if item.expected_grounding_fate != "review" or (
            item.boundary_feature not in {"competing_homonym_aliases"}
        ):
            context_eligible += 1
            complete = not item.omitted_gold_context
            context_complete += int(complete)
            if not complete:
                context_failures.append(item.signal_id)

    b_precision = mean(boundary_precision)
    b_recall = mean(boundary_recall)
    b_f1 = (
        2 * b_precision * b_recall / (b_precision + b_recall)
        if b_precision + b_recall
        else 0.0
    )
    exact_tp = exact_predicted = exact_gold = 0
    mention_failures: list[str] = []
    for item in receipts:
        predicted = set(item.predicted_mention_spans)
        gold = set(item.gold_mention_spans)
        exact_tp += len(predicted & gold)
        exact_predicted += len(predicted)
        exact_gold += len(gold)
        if predicted != gold:
            mention_failures.append(item.signal_id)
    mention_precision = exact_tp / exact_predicted if exact_predicted else 0.0
    mention_recall = exact_tp / exact_gold if exact_gold else 0.0
    mention_f1 = (
        2 * mention_precision * mention_recall / (mention_precision + mention_recall)
        if mention_precision + mention_recall
        else 0.0
    )
    typed = [item for item in receipts if item.gold_entity_type]
    type_correct = sum(
        item.predicted_entity_type == item.gold_entity_type for item in typed
    )
    links = [item for item in receipts if item.admitted_canonical_ref is not None]
    correct_links = sum(
        item.admitted_canonical_ref == item.gold_canonical_ref for item in links
    )
    link_gold = [
        item
        for item in receipts
        if item.expected_grounding_fate == "resolved_for_consumer"
    ]
    correctly_resolved = sum(
        item.admitted_canonical_ref == item.gold_canonical_ref
        and item.grounding_fate == "resolved_for_consumer"
        for item in link_gold
    )
    abstentions = [
        item
        for item in receipts
        if item.admitted_canonical_ref is None
        and item.grounding_fate in {"review", "abstained", "unresolved"}
    ]
    safe_correct = sum(
        item.safe_abstention_or_review
        and item.grounding_fate == item.expected_grounding_fate
        for item in abstentions
    )
    safe_predictions = sum(item.safe_abstention_or_review for item in abstentions)
    fate_correct = sum(
        item.grounding_fate == item.expected_grounding_fate for item in receipts
    )
    budget_correct = sum(item.context_budget_adhered for item in receipts)
    correction_correct = sum(bool(item["semantic_convergence"]) for item in corrections)
    future_excluded = sum(not item.future_context_selected for item in receipts)

    def metric_from_value(
        metric_id: str,
        value: float,
        total: int,
        *,
        threshold: float,
        operator: Literal[">=", "<=", "="],
        worst: list[str] | tuple[str, ...] = (),
    ) -> P3Metric:
        return _metric(
            metric_id,
            value * total,
            total,
            threshold=threshold,
            operator=operator,
            worst=worst,
            source=source,
            value=value,
        )

    return {
        "b_cubed_boundary_f1": metric_from_value(
            "b_cubed_boundary_f1",
            b_f1,
            len(receipts),
            threshold=0.90,
            operator=">=",
            worst=boundary_failures,
        ),
        "pairwise_boundary_precision": _metric(
            "pairwise_boundary_precision",
            pair_tp,
            pair_predicted,
            threshold=0.92,
            operator=">=",
            worst=boundary_failures,
            source=source,
        ),
        "pairwise_boundary_recall": _metric(
            "pairwise_boundary_recall",
            pair_tp,
            pair_gold,
            threshold=0.85,
            operator=">=",
            worst=boundary_failures,
            source=source,
        ),
        "selected_context_contamination": _metric(
            "selected_context_contamination",
            contaminants,
            selected_nonfocal,
            threshold=0.05,
            operator="<=",
            worst=[
                item.signal_id
                for item in receipts
                if item.selected_context_contaminants
            ],
            source=source,
        ),
        "sufficient_context_recall": _metric(
            "sufficient_context_recall",
            context_complete,
            context_eligible,
            threshold=0.95,
            operator=">=",
            worst=context_failures,
            source=source,
        ),
        "exact_mention_f1": metric_from_value(
            "exact_mention_f1",
            mention_f1,
            max(exact_gold, 1),
            threshold=0.92,
            operator=">=",
            worst=mention_failures,
        ),
        "type_accuracy": _metric(
            "type_accuracy",
            type_correct,
            len(typed),
            threshold=0.95,
            operator=">=",
            worst=[
                item.signal_id
                for item in typed
                if item.predicted_entity_type != item.gold_entity_type
            ],
            source=source,
        ),
        "canonical_link_precision": _metric(
            "canonical_link_precision",
            correct_links,
            len(links),
            threshold=0.98,
            operator=">=",
            worst=[
                item.signal_id
                for item in links
                if item.admitted_canonical_ref != item.gold_canonical_ref
            ],
            source=source,
        ),
        "canonical_link_recall": _metric(
            "canonical_link_recall",
            correctly_resolved,
            len(link_gold),
            threshold=0.90,
            operator=">=",
            worst=[
                item.signal_id
                for item in link_gold
                if item.admitted_canonical_ref != item.gold_canonical_ref
            ],
            source=source,
        ),
        "safe_abstention_precision": _metric(
            "safe_abstention_precision",
            safe_correct,
            safe_predictions,
            threshold=1.0,
            operator="=",
            worst=[
                item.signal_id
                for item in abstentions
                if item.safe_abstention_or_review
                and item.grounding_fate != item.expected_grounding_fate
            ],
            source=source,
        ),
        "context_budget_adherence": _metric(
            "context_budget_adherence",
            budget_correct,
            len(receipts),
            threshold=1.0,
            operator="=",
            worst=[
                item.signal_id for item in receipts if not item.context_budget_adhered
            ],
            source=source,
        ),
        "correction_replay_convergence_coverage": _metric(
            "correction_replay_convergence_coverage",
            correction_correct,
            len(corrections),
            threshold=1.0,
            operator="=",
            worst=[
                str(item["signal_id"])
                for item in corrections
                if not item["semantic_convergence"]
            ],
            source=f"{ARTIFACT_NAME}#correction_receipts",
        ),
        "grounding_fate_accuracy": _metric(
            "grounding_fate_accuracy",
            fate_correct,
            len(receipts),
            threshold=1.0,
            operator="=",
            worst=[
                item.signal_id
                for item in receipts
                if item.grounding_fate != item.expected_grounding_fate
            ],
            source=source,
        ),
        "future_context_exclusion": _metric(
            "future_context_exclusion",
            future_excluded,
            len(receipts),
            threshold=1.0,
            operator="=",
            worst=[item.signal_id for item in receipts if item.future_context_selected],
            source=source,
        ),
    }


def _policy_payload() -> dict[str, Any]:
    return {
        "version": EVALUATION_POLICY_VERSION,
        "hard_gates": P3_GATE_IDS,
        "thresholds": {
            "b_cubed_boundary_f1": [">=", 0.90],
            "pairwise_boundary_precision": [">=", 0.92],
            "pairwise_boundary_recall": [">=", 0.85],
            "selected_context_contamination": ["<=", 0.05],
            "sufficient_context_recall": [">=", 0.95],
            "exact_mention_f1": [">=", 0.92],
            "type_accuracy": [">=", 0.95],
            "canonical_link_precision": [">=", 0.98],
            "canonical_link_recall": [">=", 0.90],
            "safe_abstention_precision": ["=", 1.0],
            "context_budget_adherence": ["=", 1.0],
            "correction_replay_convergence_coverage": ["=", 1.0],
            "future_context_exclusion": ["=", 1.0],
        },
    }


def run_p3_perception_grounding(
    *,
    repository_root: Path,
    runtime: P3PerceptionRuntime,
    population: P3Population | None = None,
) -> dict[str, Any]:
    """Execute all 120 cases once and return a validated artifact dictionary."""

    population = population or build_p3_population()
    by_id = {item.signal_id: item for item in population.signals}
    if len(by_id) != 120:
        raise ValueError("P3 population IDs must be unique and total 120")
    manifest = P3SealedManifest(
        population_version=population.version,
        scenario_sha256=population.scenario_digest,
        gold_sha256=population.gold_digest,
        evaluation_policy_version=EVALUATION_POLICY_VERSION,
        evaluation_policy_sha256=_digest(_policy_payload()),
        runtime_source_sha256=_runtime_source_digests(repository_root),
        proof_boundaries=(
            "provider-free deterministic normalized-signal population",
            "no connector or ingestion-listener behavior",
            "no database identity-writer authorization proof",
            "no downstream canonical Model scope application proof",
            "correction proves pure semantic convergence, not durable replay",
            "cross-tenant candidate rejection is not representable in the current pure input contract",
        ),
    )
    receipts: list[P3MemberReceipt] = []
    corrections: list[dict[str, Any]] = []
    for signal in population.signals:
        receipt, correction = _case_receipt(
            signal,
            by_id=by_id,
            runtime=runtime,
        )
        receipts.append(receipt)
        if correction is not None:
            corrections.append(correction)
    receipt_tuple = tuple(receipts)
    correction_tuple = tuple(corrections)
    future_incidents = tuple(
        item.signal_id for item in receipt_tuple if item.future_context_selected
    )
    fate_incidents = tuple(
        item.signal_id
        for item in receipt_tuple
        if not item.mention_fate
        or (
            bool(item.predicted_mention_spans)
            != (item.mention_fate == EntityMentionDetectionFate.DETECTED.value)
        )
    )
    gates = {
        "HG-02": P3Gate(
            gate_id="HG-02",
            status="not_observed",
            observed_count=0,
            eligible_count=5,
            incident_count=0,
            detail=(
                "Five pure adjudication successors were built, but no governed "
                "database identity writer or alias mutation was executed."
            ),
        ),
        "HG-03": P3Gate(
            gate_id="HG-03",
            status="fail" if fate_incidents else "pass",
            observed_count=120,
            eligible_count=120,
            incident_count=len(fate_incidents),
            incident_ids=fate_incidents,
            detail="Every sealed signal has one explicit durable-command mention fate.",
        ),
        "HG-06": P3Gate(
            gate_id="HG-06",
            status="not_observed",
            observed_count=0,
            eligible_count=sum(
                item.admitted_canonical_ref is not None for item in receipt_tuple
            ),
            incident_count=0,
            detail=(
                "Grounding decisions expose decisive identity evidence, but no "
                "downstream canonical Model scope was created in this evaluator."
            ),
        ),
        "HG-14": P3Gate(
            gate_id="HG-14",
            status=("fail" if future_incidents else "not_observed"),
            observed_count=(120 if future_incidents else 0),
            eligible_count=120,
            incident_count=len(future_incidents),
            incident_ids=future_incidents,
            detail=(
                "Future canaries were exercised, but the pure ContextObservationInput "
                "contract carries no tenant ID, so cross-tenant exclusion is not observed."
            ),
        ),
    }
    metrics = _metrics(receipt_tuple, correction_tuple)
    missing = (
        "HG-02: governed database identity application and bypass check",
        "HG-06: downstream canonical Model-scope lineage application",
        "HG-14: cross-tenant context and candidate negative cases",
        "durable correction replay and alias/entity write idempotence",
    )
    population_summary = {
        "version": population.version,
        "signal_count": len(population.signals),
        "family_counts": population.family_counts(),
        "slack_episode_count": len(
            {
                item.episode_id
                for item in population.signals
                if item.family == "slack_interleaved"
            }
        ),
        "split_merge_decision_count": sum(
            item.split_merge_decision for item in population.signals
        ),
        "gold_mention_count": sum(
            bool(item.gold_mention_spans) for item in population.signals
        ),
        "high_consequence_link_count": sum(
            item.high_consequence_link for item in population.signals
        ),
        "safe_abstention_or_review_count": sum(
            item.safe_abstention_or_review for item in population.signals
        ),
        "correction_replay_count": sum(
            item.correction_replay for item in population.signals
        ),
        "scenario_sha256": population.scenario_digest,
        "gold_sha256": population.gold_digest,
    }
    payload: dict[str, Any] = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "execution_status": "complete",
        "generated_at": datetime.now(timezone.utc),
        "sealed_manifest": manifest,
        "population": population_summary,
        "hard_gates": gates,
        "continuous_metrics": metrics,
        "member_receipts": receipt_tuple,
        "correction_receipts": correction_tuple,
        "missing_evidence": missing,
        "proof_boundary": manifest.proof_boundaries,
        "phase_exit_ready": False,
    }
    payload["artifact_content_digest"] = canonical_sha256(
        P3Artifact.model_construct(
            **payload,
            artifact_content_digest="0" * 64,
        ).model_dump(
            mode="json",
            exclude={"generated_at", "artifact_content_digest"},
        )
    )
    artifact = P3Artifact.model_validate(payload)
    return artifact.model_dump(mode="json")


def write_p3_artifact(report: dict[str, Any], path: Path) -> Path:
    """Validate and write one immutable-style JSON artifact."""

    validated = P3Artifact.model_validate(report).model_dump(mode="json")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(validated, indent=2, sort_keys=True) + "\n")
    return path


def write_p3_artifact_schema(path: Path) -> Path:
    """Write the exact machine-readable contract for P3 evidence artifacts."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(P3Artifact.model_json_schema(), indent=2, sort_keys=True) + "\n"
    )
    return path


__all__ = [
    "ARTIFACT_NAME",
    "ARTIFACT_SCHEMA_VERSION",
    "P3Artifact",
    "P3Gate",
    "P3MemberReceipt",
    "P3Metric",
    "P3PerceptionRuntime",
    "P3Population",
    "P3SealedManifest",
    "P3Signal",
    "build_p3_population",
    "run_p3_perception_grounding",
    "write_p3_artifact",
    "write_p3_artifact_schema",
]
