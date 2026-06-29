"""Build provisional substrate candidates from Think context evidence.

This module sits inside the Think path by design. The extraction is cheap and
deterministic, so each reasoning run can surface provisional actors, aliases,
customers, workstreams, systems, vendors, commitments, and discovered patterns
from the exact evidence bundle the LLM is about to read.
"""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from services.domain.substrate_candidates import upsert_substrate_candidate
from services.domain.substrate_promotion import (
    auto_promote_candidate,
    open_candidate_clarification,
    plan_candidate_promotion,
)


_EMAIL_RE = re.compile(
    r"\b([A-Z0-9._%+\-]+)@([A-Z0-9.\-]+\.[A-Z]{2,})\b",
    re.I,
)
_JIRA_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,12}-\d{1,8})\b")
_PR_RE = re.compile(r"\b(?:PR|pull request)\s*#?(\d{1,8})\b", re.I)
_REPO_RE = re.compile(
    r"(?<![@\w.-])([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)(?![@\w.-])"
)
_ORG_SUFFIX_RE = re.compile(
    r"\b([A-Z][A-Za-z0-9&.\- ]{2,80}\s+"
    r"(?:Inc|LLC|Ltd|Corp|Corporation|Company|Bank|Capital|Ventures|Labs))\b"
)
_BOT_RE = re.compile(
    r"\b(bot|noreply|no-reply|automation|ci|buildkite|dependabot|github-actions)\b",
    re.I,
)
_NON_KEY_RE = re.compile(r"[^a-z0-9]+")
_WS_RE = re.compile(r"\s+")
_HEX_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.I)
_NUMBER_RE = re.compile(r"\b\d+\b")
_URL_RE = re.compile(r"https?://[^\s)]+", re.I)

_INTERNAL_EMAIL_DOMAINS_ENV = "FYRALIS_INTERNAL_EMAIL_DOMAINS"
_VENDOR_SOURCE_ROOTS = {
    "ashby",
    "brex",
    "carta",
    "deel",
    "gusto",
    "hibob",
    "mercury",
    "quickbooks",
    "ramp",
}
_SYSTEM_SOURCE_ROOTS = {
    "aws",
    "calendar",
    "discord",
    "drive",
    "figma",
    "fireflies",
    "github",
    "gmail",
    "grafana",
    "jira",
    "linkedin",
    "miro",
    "notion",
    "signal",
    "slack",
    "telegram",
}
_SYSTEM_PHRASES = (
    "checkpoint explorer",
    "strata bridge",
    "bridge relayer",
    "faucet",
    "sequencer",
    "indexer",
    "withdrawal service",
    "signer",
    "terraform",
    "cloudtrail",
    "grafana",
)
_ACTION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("pr_raised", re.compile(r"\b(raised|opened|created)\s+(?:a\s+)?PR\b", re.I)),
    ("pr_merged", re.compile(r"\b(merged|landed)\s+(?:a\s+)?PR\b", re.I)),
    ("review_requested", re.compile(r"\b(review|can someone review|needs review)\b", re.I)),
    ("approval", re.compile(r"\b(approved|lgtm|ship it)\b", re.I)),
    ("blocked", re.compile(r"\b(blocked|stuck|waiting on|on hold)\b", re.I)),
    ("started_work", re.compile(r"\b(started|kicking off|picked up|working on)\b", re.I)),
    ("deploy", re.compile(r"\b(deploy|deployed|deploying|rollout|rolled out)\b", re.I)),
    ("incident", re.compile(r"\b(incident|outage|sev[ -]?[0-3]|page[d]?)\b", re.I)),
    ("payment", re.compile(r"\b(invoice|payment|wire|transaction|card charge)\b", re.I)),
    ("hiring", re.compile(r"\b(candidate|interview|offer|recruiter|hiring)\b", re.I)),
    ("customer_signal", re.compile(r"\b(customer|prospect|renewal|contract|msa|arr)\b", re.I)),
)
_KIND_PRIORITY = {
    "actor": 0,
    "actor_alias": 1,
    "customer": 2,
    "workstream": 3,
    "commitment": 4,
    "system": 5,
    "vendor": 6,
    "pattern": 7,
}


def _normalize_email_domain(domain: str) -> str:
    return domain.strip().strip(".").casefold()


def _configured_internal_email_domains(
    domains: Iterable[str] | None = None,
) -> frozenset[str]:
    values = domains
    if values is None:
        values = os.environ.get(_INTERNAL_EMAIL_DOMAINS_ENV, "").split(",")
    return frozenset(
        normalized
        for value in values
        if (normalized := _normalize_email_domain(str(value)))
    )


def _is_internal_email_domain(domain: str, internal_email_domains: frozenset[str]) -> bool:
    return _normalize_email_domain(domain) in internal_email_domains


@dataclass(frozen=True, slots=True)
class CandidateSpec:
    kind: str
    label: str
    fingerprint: str
    confidence: float
    aliases: tuple[dict[str, Any], ...] = ()
    evidence_observation_ids: tuple[UUID, ...] = ()
    evidence_model_ids: tuple[UUID, ...] = ()
    related_fingerprints: tuple[tuple[str, str], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str = "proposed"

    @property
    def score(self) -> tuple[int, float, int, str]:
        return (
            _KIND_PRIORITY.get(self.kind, 99),
            -self.confidence,
            -len(self.evidence_observation_ids),
            self.fingerprint,
        )


class _SpecAccumulator:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], dict[str, Any]] = {}

    def add(
        self,
        *,
        kind: str,
        label: str,
        fingerprint: str,
        confidence: float,
        observation_id: UUID | None = None,
        aliases: Iterable[dict[str, Any]] = (),
        metadata: dict[str, Any] | None = None,
        status: str = "proposed",
    ) -> tuple[str, str] | None:
        if not label or not fingerprint:
            return None
        key = (kind, fingerprint)
        item = self._items.setdefault(
            key,
            {
                "kind": kind,
                "label": label,
                "fingerprint": fingerprint,
                "confidence": float(confidence),
                "aliases": {},
                "evidence_observation_ids": set(),
                "related_fingerprints": set(),
                "metadata": {},
                "status": status,
            },
        )
        if len(label) > len(item["label"]):
            item["label"] = label
        item["confidence"] = max(float(item["confidence"]), float(confidence))
        if status != "proposed":
            item["status"] = status
        if observation_id is not None:
            item["evidence_observation_ids"].add(observation_id)
        for alias in aliases:
            alias_key = json.dumps(alias, sort_keys=True, default=str)
            item["aliases"][alias_key] = dict(alias)
        if metadata:
            _merge_metadata(item["metadata"], metadata)
        return key

    def relate(
        self,
        left: tuple[str, str] | None,
        right: tuple[str, str] | None,
        *,
        metadata: dict[str, Any],
    ) -> None:
        if left is None or right is None or left == right:
            return
        for source, target in ((left, right), (right, left)):
            item = self._items.get(source)
            if item is None:
                continue
            item["related_fingerprints"].add(target)
            _merge_metadata(
                item["metadata"],
                {
                    "related_candidates": [
                        {
                            "kind": target[0],
                            "fingerprint": target[1],
                            **metadata,
                        }
                    ]
                },
            )

    def specs(self) -> list[CandidateSpec]:
        specs: list[CandidateSpec] = []
        for item in self._items.values():
            specs.append(
                CandidateSpec(
                    kind=item["kind"],
                    label=item["label"],
                    fingerprint=item["fingerprint"],
                    confidence=round(float(item["confidence"]), 4),
                    aliases=tuple(item["aliases"].values()),
                    evidence_observation_ids=tuple(
                        sorted(item["evidence_observation_ids"])
                    ),
                    related_fingerprints=tuple(sorted(item["related_fingerprints"])),
                    metadata=dict(item["metadata"]),
                    status=item["status"],
                )
            )
        return sorted(specs, key=lambda spec: spec.score)


async def build_substrate_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observations: Iterable[Any],
    models: Iterable[Any] = (),
    run_id: UUID | None = None,
    limit: int = 80,
    clarification_limit: int = 5,
    internal_email_domains: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    """Extract, upsert, and return prompt-facing candidate dicts."""

    specs = candidate_specs_from_observations(
        observations,
        limit=limit,
        internal_email_domains=internal_email_domains,
    )
    model_ids = [
        _coerce_uuid(getattr(model, "id", None))
        for model in models
        if _coerce_uuid(getattr(model, "id", None)) is not None
    ][:12]

    candidates: list[dict[str, Any]] = []
    clarification_count = 0
    ordered_specs = specs[: max(1, int(limit))]
    upserted: dict[tuple[str, str], Any] = {}
    for spec in ordered_specs:
        candidate = await upsert_substrate_candidate(
            conn,
            tenant_id=tenant_id,
            kind=spec.kind,
            label=spec.label,
            fingerprint=spec.fingerprint,
            confidence=spec.confidence,
            aliases=spec.aliases,
            evidence_observation_ids=spec.evidence_observation_ids,
            evidence_model_ids=model_ids,
            metadata=spec.metadata,
            status=spec.status,
            created_by_run_id=run_id,
        )
        upserted[(spec.kind, spec.fingerprint)] = candidate

    for spec in ordered_specs:
        related_ids = [
            upserted[key].id
            for key in spec.related_fingerprints
            if key in upserted
        ]
        if not related_ids:
            continue
        candidate = await upsert_substrate_candidate(
            conn,
            tenant_id=tenant_id,
            kind=spec.kind,
            label=spec.label,
            fingerprint=spec.fingerprint,
            confidence=spec.confidence,
            aliases=spec.aliases,
            evidence_observation_ids=spec.evidence_observation_ids,
            evidence_model_ids=model_ids,
            related_candidate_ids=related_ids,
            metadata=spec.metadata,
            status=spec.status,
            created_by_run_id=run_id,
        )
        upserted[(spec.kind, spec.fingerprint)] = candidate

    for spec in ordered_specs:
        candidate = upserted[(spec.kind, spec.fingerprint)]
        plan = plan_candidate_promotion(candidate)
        candidate_dict = candidate.to_dict()
        candidate_dict["promotion_plan"] = plan.to_dict()
        if (
            plan.action == "ask_user"
            and clarification_count < max(0, int(clarification_limit))
            and _should_open_candidate_clarification(candidate)
        ):
            await open_candidate_clarification(conn, candidate=candidate)
            clarification_count += 1
            candidate_dict["status"] = "needs_clarification"
            candidate_dict["clarification_requested"] = True
        else:
            promotion_result = await auto_promote_candidate(conn, candidate=candidate)
            if promotion_result is not None:
                candidate_dict["status"] = "promoted"
                candidate_dict["promotion_ref"] = promotion_result["canonical_ref"]
                candidate_dict["promotion_result"] = {
                    key: (str(value) if isinstance(value, UUID) else value)
                    for key, value in promotion_result.items()
                }
        candidates.append(candidate_dict)
    return candidates


def candidate_specs_from_observations(
    observations: Iterable[Any],
    *,
    limit: int = 80,
    internal_email_domains: Iterable[str] | None = None,
) -> list[CandidateSpec]:
    """Pure deterministic extractor used by Think and unit tests."""

    internal_email_domain_set = _configured_internal_email_domains(
        internal_email_domains
    )
    accumulator = _SpecAccumulator()
    pattern_groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "observation_ids": set(), "actors": set(), "label": ""}
    )

    for observation in observations:
        observation_id = _coerce_uuid(getattr(observation, "id", None))
        source_channel = str(getattr(observation, "source_channel", "") or "")
        source_root = _source_root(source_channel)
        source_actor_ref = getattr(observation, "source_actor_ref", None)
        text = _observation_text(observation)
        entities = _observation_entities(observation)

        _add_source_system_candidate(
            accumulator,
            source_root=source_root,
            source_channel=source_channel,
            observation_id=observation_id,
        )
        actor_fingerprint = _add_actor_candidates(
            accumulator,
            source_channel=source_channel,
            source_actor_ref=source_actor_ref,
            internal_email_domains=internal_email_domain_set,
            observation_id=observation_id,
        )
        customer_keys = _add_entity_candidates(
            accumulator,
            entities=entities,
            source_root=source_root,
            observation_id=observation_id,
        )
        _add_text_candidates(
            accumulator,
            text=text,
            source_root=source_root,
            source_channel=source_channel,
            actor_fingerprint=actor_fingerprint,
            seed_customer_keys=customer_keys,
            internal_email_domains=internal_email_domain_set,
            observation_id=observation_id,
        )
        _track_patterns(
            pattern_groups,
            text=text,
            source_root=source_root,
            actor_fingerprint=actor_fingerprint,
            observation_id=observation_id,
        )

    _add_pattern_candidates(accumulator, pattern_groups)
    return accumulator.specs()[: max(1, int(limit))]


def _should_open_candidate_clarification(candidate: Any) -> bool:
    metadata = getattr(candidate, "metadata", {}) or {}
    if any(
        metadata.get(key)
        for key in (
            "ambiguous",
            "ambiguous_aliases",
            "candidate_conflict",
            "same_label_candidate_ids",
            "possible_matches",
            "merge_candidates",
        )
    ):
        return True
    kind = str(getattr(candidate, "kind", "") or "")
    if kind in {"actor", "actor_alias"}:
        return True
    evidence_count = len(getattr(candidate, "evidence_observation_ids", []) or []) + len(
        getattr(candidate, "evidence_model_ids", []) or []
    )
    return kind == "customer" and evidence_count >= 2


def _add_source_system_candidate(
    accumulator: _SpecAccumulator,
    *,
    source_root: str,
    source_channel: str,
    observation_id: UUID | None,
) -> None:
    if not source_root:
        return
    if source_root in _SYSTEM_SOURCE_ROOTS or source_root in _VENDOR_SOURCE_ROOTS:
        accumulator.add(
            kind="system",
            label=f"{_title(source_root)} source",
            fingerprint=f"system:source:{source_root}",
            confidence=0.65,
            observation_id=observation_id,
            aliases=[
                {
                    "source_channel": source_channel,
                    "source_root": source_root,
                }
            ],
            metadata={"basis": "source_channel", "source_root": source_root},
        )
    if source_root in _VENDOR_SOURCE_ROOTS:
        accumulator.add(
            kind="vendor",
            label=_title(source_root),
            fingerprint=f"vendor:source:{source_root}",
            confidence=0.7,
            observation_id=observation_id,
            aliases=[{"source_channel": source_channel, "source_root": source_root}],
            metadata={"basis": "vendor_source", "source_root": source_root},
        )


def _add_actor_candidates(
    accumulator: _SpecAccumulator,
    *,
    source_channel: str,
    source_actor_ref: Any,
    internal_email_domains: frozenset[str],
    observation_id: UUID | None,
) -> str | None:
    if source_actor_ref in (None, ""):
        return None
    source_actor_ref = str(source_actor_ref)
    source_root = _source_root(source_channel)
    full_ref = (
        source_actor_ref
        if ":" in source_actor_ref
        else f"{source_channel}:{source_actor_ref}"
    )
    identity = _actor_identity(
        source_channel,
        source_actor_ref,
        internal_email_domains=internal_email_domains,
    )
    label = identity["label"]
    actor_fingerprint = f"actor:{identity['key']}"
    alias = {
        "source_channel": source_channel,
        "source_root": source_root,
        "source_actor_ref": source_actor_ref,
        "full_ref": full_ref,
    }
    if identity["is_machine"]:
        accumulator.add(
            kind="system",
            label=label,
            fingerprint=f"system:actor:{identity['key']}",
            confidence=0.72,
            observation_id=observation_id,
            aliases=[alias],
            metadata={
                "basis": "machine_source_actor_ref",
                "identity_key": identity["key"],
            },
        )
        return None

    accumulator.add(
        kind="actor",
        label=label,
        fingerprint=actor_fingerprint,
        confidence=identity["confidence"],
        observation_id=observation_id,
        aliases=[alias],
        metadata={
            "basis": "source_actor_ref",
            "identity_key": identity["key"],
            "source_roots": [source_root],
        },
    )
    accumulator.add(
        kind="actor_alias",
        label=f"{_title(source_root)} alias for {label}",
        fingerprint=f"actor_alias:{_norm_key(full_ref)}",
        confidence=0.82,
        observation_id=observation_id,
        aliases=[alias],
        metadata={
            "basis": "source_actor_ref",
            "proposed_actor_fingerprint": actor_fingerprint,
        },
    )
    return actor_fingerprint


def _add_entity_candidates(
    accumulator: _SpecAccumulator,
    *,
    entities: Iterable[dict[str, Any]],
    source_root: str,
    observation_id: UUID | None,
) -> list[tuple[str, str]]:
    customer_keys: list[tuple[str, str]] = []
    for entity in entities:
        label = _entity_label(entity)
        if not label:
            continue
        entity_kind = _entity_kind(entity, source_root=source_root, label=label)
        if entity_kind is None:
            continue
        confidence = 0.74 if entity_kind in {"system", "workstream"} else 0.62
        key = accumulator.add(
            kind=entity_kind,
            label=label,
            fingerprint=f"{entity_kind}:entity:{_norm_key(label)}",
            confidence=confidence,
            observation_id=observation_id,
            aliases=[{"entity": entity}],
            metadata={"basis": "entities_mentioned", "source_root": source_root},
        )
        if entity_kind == "customer" and key is not None:
            customer_keys.append(key)
    return customer_keys


def _add_text_candidates(
    accumulator: _SpecAccumulator,
    *,
    text: str,
    source_root: str,
    source_channel: str,
    actor_fingerprint: str | None,
    seed_customer_keys: Iterable[tuple[str, str]] = (),
    internal_email_domains: frozenset[str],
    observation_id: UUID | None,
) -> None:
    if not text:
        return
    commitment_keys: list[tuple[str, str]] = []
    customer_keys: list[tuple[str, str]] = list(seed_customer_keys)
    repos = [_clean_repo(match.group(1)) for match in _REPO_RE.finditer(text)]
    repos = [repo for repo in dict.fromkeys(repos) if repo]
    jira_keys = [match.group(1).upper() for match in _JIRA_RE.finditer(text)]
    jira_keys = list(dict.fromkeys(jira_keys))
    pr_numbers = [match.group(1) for match in _PR_RE.finditer(text)]
    pr_numbers = list(dict.fromkeys(pr_numbers))

    for repo in repos[:6]:
        accumulator.add(
            kind="system",
            label=repo,
            fingerprint=f"system:repo:{_norm_key(repo)}",
            confidence=0.78,
            observation_id=observation_id,
            aliases=[{"repo_full_name": repo, "source_channel": source_channel}],
            metadata={"basis": "repo_text", "source_root": source_root},
        )
    for jira_key in jira_keys[:8]:
        accumulator.add(
            kind="workstream",
            label=jira_key,
            fingerprint=f"workstream:jira:{jira_key.lower()}",
            confidence=0.82,
            observation_id=observation_id,
            aliases=[{"issue_key": jira_key, "source_channel": source_channel}],
            metadata={"basis": "jira_issue_key", "source_root": source_root},
        )
        candidate_key = accumulator.add(
            kind="commitment",
            label=f"{jira_key} work item",
            fingerprint=f"commitment:jira:{jira_key.lower()}",
            confidence=0.74,
            observation_id=observation_id,
            aliases=[{"issue_key": jira_key, "source_channel": source_channel}],
            metadata={"basis": "jira_issue_key", "source_root": source_root},
        )
        if candidate_key is not None:
            commitment_keys.append(candidate_key)

    for number in pr_numbers[:8]:
        repo = repos[0] if repos else source_root or "unknown"
        issue = jira_keys[0] if jira_keys else None
        label = f"PR #{number}"
        if issue:
            label = f"{label} for {issue}"
        elif repo and repo != "unknown":
            label = f"{label} in {repo}"
        key = accumulator.add(
            kind="commitment",
            label=label,
            fingerprint=f"commitment:pr:{_norm_key(repo)}:{number}",
            confidence=0.78,
            observation_id=observation_id,
            aliases=[
                {
                    "pull_request": number,
                    "repo": repo,
                    "issue_key": issue,
                    "source_channel": source_channel,
                }
            ],
            metadata={"basis": "pull_request_text", "source_root": source_root},
        )
        if key is not None:
            commitment_keys.append(key)

    action = _first_action(text)
    if action is not None:
        object_key = _context_object_key(
            text,
            repos=repos,
            jira_keys=jira_keys,
            pr_numbers=pr_numbers,
        )
        if object_key:
            actor_part = actor_fingerprint or f"source:{source_root}"
            key = accumulator.add(
                kind="commitment",
                label=_commitment_label(action, object_key),
                fingerprint=(
                    f"commitment:context:{_norm_key(actor_part)}:"
                    f"{_norm_key(action)}:{_norm_key(object_key)}"
                ),
                confidence=0.62,
                observation_id=observation_id,
                aliases=[
                    {
                        "action": action,
                        "object_key": object_key,
                        "source_channel": source_channel,
                    }
                ],
                metadata={
                    "basis": "contextual_action",
                    "action": action,
                    "actor_fingerprint": actor_fingerprint,
                    "source_root": source_root,
                },
            )
            if key is not None:
                commitment_keys.append(key)

    for phrase in _SYSTEM_PHRASES:
        if phrase in text.casefold():
            accumulator.add(
                kind="system",
                label=_title(phrase),
                fingerprint=f"system:phrase:{_norm_key(phrase)}",
                confidence=0.7,
                observation_id=observation_id,
                aliases=[{"phrase": phrase, "source_channel": source_channel}],
                metadata={"basis": "known_system_phrase", "source_root": source_root},
            )

    for local, domain in _EMAIL_RE.findall(text):
        domain_key = domain.casefold()
        if _is_internal_email_domain(domain_key, internal_email_domains):
            continue
        key = accumulator.add(
            kind="customer",
            label=domain_key,
            fingerprint=f"customer:domain:{domain_key}",
            confidence=0.56,
            observation_id=observation_id,
            aliases=[
                {
                    "email": f"{local}@{domain}",
                    "domain": domain_key,
                    "source_channel": source_channel,
                }
            ],
            metadata={"basis": "external_email_domain", "source_root": source_root},
        )
        if key is not None:
            customer_keys.append(key)

    for match in _ORG_SUFFIX_RE.finditer(text):
        label = _clean_label(match.group(1))
        if not label:
            continue
        key = accumulator.add(
            kind="customer",
            label=label,
            fingerprint=f"customer:org:{_norm_key(label)}",
            confidence=0.52,
            observation_id=observation_id,
            aliases=[{"name": label, "source_channel": source_channel}],
            metadata={"basis": "organization_mention", "source_root": source_root},
        )
        if key is not None:
            customer_keys.append(key)

    if customer_keys and commitment_keys:
        relation_metadata = {
            "basis": "same_observation_customer_commitment",
            "source_channel": source_channel,
            "source_root": source_root,
        }
        if observation_id is not None:
            relation_metadata["evidence_observation_id"] = str(observation_id)
        for customer_key in customer_keys:
            for commitment_key in commitment_keys:
                accumulator.relate(
                    customer_key,
                    commitment_key,
                    metadata=relation_metadata,
                )


def _track_patterns(
    pattern_groups: dict[str, dict[str, Any]],
    *,
    text: str,
    source_root: str,
    actor_fingerprint: str | None,
    observation_id: UUID | None,
) -> None:
    if observation_id is None or not text:
        return
    action = _first_action(text)
    object_key = _context_object_key(text, repos=[], jira_keys=[], pr_numbers=[])
    if action is not None and object_key:
        signature = f"action:{source_root}:{action}:{_norm_key(object_key)}"
        group = pattern_groups[signature]
        group["count"] += 1
        group["observation_ids"].add(observation_id)
        if actor_fingerprint:
            group["actors"].add(actor_fingerprint)
        group["label"] = f"Recurring {action.replace('_', ' ')} around {object_key}"

    normalized_text = _normalize_text_signature(text)
    if normalized_text:
        signature = f"text:{source_root}:{normalized_text}"
        group = pattern_groups[signature]
        group["count"] += 1
        group["observation_ids"].add(observation_id)
        if actor_fingerprint:
            group["actors"].add(actor_fingerprint)
        group["label"] = f"Repeated {source_root or 'source'} signal shape"


def _add_pattern_candidates(
    accumulator: _SpecAccumulator,
    pattern_groups: dict[str, dict[str, Any]],
) -> None:
    for signature, group in pattern_groups.items():
        count = int(group["count"])
        if count < 2:
            continue
        observation_ids = sorted(group["observation_ids"])
        actors = sorted(group["actors"])
        confidence = min(0.82, 0.5 + (count * 0.04))
        label = str(group["label"] or "Recurring operational pattern")
        fingerprint = f"pattern:{_norm_key(signature)}"
        for observation_id in observation_ids[:12]:
            accumulator.add(
                kind="pattern",
                label=label,
                fingerprint=fingerprint,
                confidence=confidence,
                observation_id=observation_id,
                metadata={
                    "basis": "contextual_recurrence",
                    "signature": signature,
                    "count_in_context": count,
                    "actor_fingerprints": actors[:12],
                },
            )


def _actor_identity(
    source_channel: str,
    source_actor_ref: str,
    *,
    internal_email_domains: frozenset[str],
) -> dict[str, Any]:
    compact = _strip_ref_channel(source_actor_ref)
    email = _EMAIL_RE.search(compact)
    if email:
        local = _norm_key(email.group(1))
        domain = email.group(2).casefold()
        internal = _is_internal_email_domain(domain, internal_email_domains)
        key = local if internal else f"{local}@{domain}"
        return {
            "key": key,
            "label": _title(email.group(1).replace(".", " ")),
            "confidence": 0.86 if internal else 0.72,
            "is_machine": bool(_BOT_RE.search(compact)),
        }

    cleaned = compact.strip().strip("<>@")
    cleaned = cleaned.removeprefix("urn:li:person:")
    cleaned = cleaned.removeprefix("user:")
    norm = _norm_key(cleaned)
    source_root = _source_root(source_channel)
    opaque = bool(re.fullmatch(r"[A-Z]*\d[A-Z0-9_:-]{4,}", cleaned))
    key = f"{source_root}:{norm}" if opaque else norm
    if not key:
        key = _norm_key(source_actor_ref)
    return {
        "key": key,
        "label": _title(cleaned or source_actor_ref),
        "confidence": 0.68 if opaque else 0.78,
        "is_machine": bool(_BOT_RE.search(source_actor_ref)),
    }


def _entity_label(entity: dict[str, Any]) -> str:
    for key in (
        "name",
        "label",
        "title",
        "identity",
        "repo_full_name",
        "issue_key",
        "key",
        "id",
    ):
        value = entity.get(key)
        if isinstance(value, str) and value.strip():
            return _clean_label(value)
    return ""


def _entity_kind(
    entity: dict[str, Any],
    *,
    source_root: str,
    label: str,
) -> str | None:
    raw_kind = " ".join(
        str(entity.get(key) or "")
        for key in ("type", "kind", "entity_type", "object_type")
    ).casefold()
    if "customer" in raw_kind:
        return "customer"
    if "vendor" in raw_kind:
        return "vendor"
    if "commitment" in raw_kind or "ticket" in raw_kind or _JIRA_RE.search(label):
        return "workstream"
    if (
        "repo" in raw_kind
        or "branch" in raw_kind
        or "aws" in raw_kind
        or "grafana" in raw_kind
        or "dashboard" in raw_kind
        or "service" in raw_kind
        or source_root in _SYSTEM_SOURCE_ROOTS
    ):
        return "system"
    if source_root in _VENDOR_SOURCE_ROOTS:
        return "vendor"
    if _ORG_SUFFIX_RE.search(label):
        return "customer"
    return None


def _first_action(text: str) -> str | None:
    for name, pattern in _ACTION_PATTERNS:
        if pattern.search(text):
            return name
    return None


def _context_object_key(
    text: str,
    *,
    repos: list[str],
    jira_keys: list[str],
    pr_numbers: list[str],
) -> str:
    if jira_keys:
        return jira_keys[0]
    if pr_numbers:
        repo = repos[0] if repos else "unknown"
        return f"{repo} PR {pr_numbers[0]}"
    if repos:
        return repos[0]
    words = [
        word
        for word in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}", text)
        if word.casefold()
        not in {
            "the",
            "and",
            "for",
            "with",
            "this",
            "that",
            "from",
            "have",
            "will",
            "need",
            "needs",
            "can",
            "someone",
            "please",
        }
    ]
    return " ".join(words[:7])


def _commitment_label(action: str, object_key: str) -> str:
    readable = action.replace("_", " ")
    return f"{readable}: {object_key}".strip()


def _observation_text(observation: Any) -> str:
    text = getattr(observation, "content_text", None)
    if isinstance(text, str) and text:
        return text
    content = getattr(observation, "content", None)
    if isinstance(content, dict):
        for key in ("text", "body", "message", "title", "summary"):
            value = content.get(key)
            if isinstance(value, str) and value:
                return value
        return json.dumps(content, sort_keys=True, default=str)
    return ""


def _observation_entities(observation: Any) -> list[dict[str, Any]]:
    raw = getattr(observation, "entities_mentioned", None) or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return [dict(item) for item in raw if isinstance(item, dict)]


def _source_root(source_channel: str) -> str:
    return str(source_channel or "").split(":", 1)[0].casefold()


def _strip_ref_channel(source_actor_ref: str) -> str:
    if ":" not in source_actor_ref:
        return source_actor_ref
    prefix, _, rest = source_actor_ref.partition(":")
    if prefix and rest and len(prefix) <= 32:
        return rest
    return source_actor_ref


def _clean_repo(repo: str) -> str:
    return repo.rstrip(".,;:)").lstrip("([")


def _clean_label(value: str) -> str:
    return _WS_RE.sub(" ", str(value or "").strip(" \t\r\n\"'`[](){}")).strip()


def _title(value: str) -> str:
    cleaned = _clean_label(value.replace("_", " ").replace("-", " "))
    if not cleaned:
        return "Unknown"
    if cleaned.isupper() and len(cleaned) <= 12:
        return cleaned
    return " ".join(part.capitalize() for part in cleaned.split())


def _norm_key(value: str) -> str:
    return _NON_KEY_RE.sub(":", str(value or "").casefold()).strip(":")


def _normalize_text_signature(text: str) -> str:
    value = str(text or "").casefold()
    value = _URL_RE.sub("<url>", value)
    value = _HEX_RE.sub("<hex>", value)
    value = _NUMBER_RE.sub("<num>", value)
    value = _WS_RE.sub(" ", value).strip()
    if len(value) < 16:
        return ""
    return value[:240]


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError):
        return None


def _merge_metadata(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    for key, value in incoming.items():
        if isinstance(value, list):
            existing = target.setdefault(key, [])
            if isinstance(existing, list):
                for item in value:
                    if item not in existing:
                        existing.append(item)
            else:
                target[key] = list(value)
            continue
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_metadata(target[key], value)
            continue
        target[key] = value


__all__ = [
    "CandidateSpec",
    "build_substrate_candidates",
    "candidate_specs_from_observations",
]
