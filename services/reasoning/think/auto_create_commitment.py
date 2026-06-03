"""services/reasoning/think/auto_create_commitment.py — deterministic synthesis
of a `create_commitment` recommendation when the LLM declines to.

The Think LLM (DeepSeek-reasoner with the current prompt) consistently
classifies "I've started X" signals as "purely informational" and
refuses to emit a recommendation, even with strong prompt directives.
This module is a deterministic post-LLM step that detects the trigger
phrase, extracts a candidate title, confirms no existing commitment in
`<acts>` covers the work, and appends a recommendation claim_op to the
diff. The downstream `_maybe_auto_accept` hook on model insert then
materialises the commitment in the ledger without a CEO click.

Idempotent — if the LLM already produced a `create_commitment`
recommendation, this is a no-op.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ActOp, ClaimOp, RawDiff


_BLOCK_PHRASES = [
    r"\bblocked\b",
    r"on hold\b",
    r"\bpaused\b",
    r"\bparked\b",
    r"\bstalled\b",
    r"\bstuck\b",
    r"waiting on\b",
    r"awaiting\b",
    r"can'?t (?:proceed|continue|move forward)",
    r"need(?:s)? (?:to )?(?:approval|signoff|sign-off|greenlight|approve|sign|ack|acknowledge)",
    r"(?:approval|signoff|sign-off|greenlight) (?:from|by|needed|required)",
]
_BLOCK_RE = re.compile(r"(?i)(?:" + "|".join(_BLOCK_PHRASES) + r")")

_DECISION_REVISIT_RE = re.compile(
    r"(?i)\b("
    r"decision (?:is being|was being|is|was|being)?\s*(?:formally\s*)?revisited|"
    r"(?:formally\s*)?revisiting\b|"
    r"\bre-evaluat(?:e|ing|ed)\b|"
    r"\bre-run the option analysis\b|"
    r"\breopened\b"
    r")"
)

_FUTURE_PLAN_RE = re.compile(
    r"(?is)\b("
    r"will\s+(?:ship|deploy|merge|circulate|schedule|review|deliver)|"
    r"(?:ship|deploy|merge|review|circulate|schedule|deliver)\w*"
    r".{0,80}\b(?:tomorrow|today|this afternoon|tonight|next week|by\s+\w+|at\s+\d)|"
    r"(?:scheduled|targeting|eta|due)\b.{0,80}"
    r"\b(?:ship|deploy|merge|review|circulate|deliver|tomorrow|today|"
    r"this afternoon|tonight|next week|by\s+\w+|at\s+\d)"
    r")"
)

_CUSTOMER_RISK_RE = re.compile(
    r"(?is)\b("
    r"churn(?:-risk)?|"
    r"renewal\s+(?:pushback|risk|not a given)|"
    r"evaluating\s+(?:two\s+)?alternatives|"
    r"competitors?|"
    r"consolidate\s+vendors|"
    r"at[-\s]?risk|"
    r"path forward on pricing"
    r")\b"
)


_TRIGGER_PHRASES = [
    r"i['\u2019 ]?ve started",
    r"i['\u2019 ]?m starting",
    r"i started",
    r"i['\u2019 ]?m building",
    r"i['\u2019 ]?m working on",
    r"working on",
    r"kicking off",
    r"picked up",
    r"i['\u2019 ]?ll deliver",
    r"i['\u2019 ]?ll ship",
    r"i['\u2019 ]?ll complete",
    r"i['\u2019 ]?ll finish",
]
_TRIGGER_RE = re.compile(
    r"(?i)\b(?:" + "|".join(_TRIGGER_PHRASES) + r")\b"
)

_DEADLINE_RE = re.compile(
    r"(?i)\b(?:in|within)\s+(?:a|an|one|two|three|four|five|six|seven|"
    r"eight|nine|ten|\d+)\s+(day|days|week|weeks|month|months|"
    r"quarter|quarters)\b"
)
_NUMBER_WORDS = {
    "a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
}


def _extract_title(text: str, match_end: int) -> str | None:
    """Pull a noun phrase out of the signal starting after the trigger
    phrase and ending at the first sentence boundary or conjunction."""
    rest = text[match_end:].lstrip(" .,:;\u2014-")
    parts = re.split(
        r"[.\n;!?]| (?:and|but|because|so|since|while)\b",
        rest,
        maxsplit=1,
    )
    candidate = parts[0].strip()
    candidate = re.sub(r"^(?:the |a |an |new )", "", candidate, flags=re.I)
    candidate = re.sub(r"^(?:work on |work )", "", candidate, flags=re.I)
    candidate = candidate.strip(" .,:;\u2014-")
    if not candidate or len(candidate) < 3:
        return None
    if len(candidate) > 80:
        candidate = candidate[:80].rstrip() + "\u2026"
    return candidate[:1].upper() + candidate[1:]


def _extract_due_date(text: str) -> datetime:
    """Pull a relative deadline like 'in a week' / 'within 30 days' from
    the signal; fall back to 30 days from now."""
    m = _DEADLINE_RE.search(text)
    now = datetime.now(timezone.utc)
    if not m:
        return now + timedelta(days=30)
    qty_raw = m.group(0).split()[1].lower()
    unit = m.group(1).lower()
    qty: int
    if qty_raw.isdigit():
        qty = int(qty_raw)
    else:
        qty = _NUMBER_WORDS.get(qty_raw, 1)
    if unit.startswith("day"):
        delta = timedelta(days=qty)
    elif unit.startswith("week"):
        delta = timedelta(weeks=qty)
    elif unit.startswith("month"):
        delta = timedelta(days=qty * 30)
    elif unit.startswith("quarter"):
        delta = timedelta(days=qty * 90)
    else:
        delta = timedelta(days=30)
    return now + delta


def _prediction_evaluate_at(text: str, seed_time: datetime | None) -> datetime:
    base = seed_time or datetime.now(timezone.utc)
    lower = text.lower()
    if "tomorrow" in lower:
        return base + timedelta(days=1)
    if "next week" in lower:
        return base + timedelta(days=7)
    if any(term in lower for term in ("today", "this afternoon", "tonight")):
        return base + timedelta(days=1)
    return base + timedelta(days=3)


def _has_create_commitment_rec(diff: RawDiff) -> bool:
    for op in diff.claim_ops:
        if op.op != "insert" or op.entry is None:
            continue
        prop = op.entry.get("proposition") or {}
        if prop.get("kind") != "recommendation":
            continue
        pc = prop.get("proposed_change") or {}
        tref = prop.get("target_act_ref") or {}
        if (
            pc.get("operation") == "create"
            and tref.get("type") == "commitment"
        ):
            return True
    return False


def _has_prediction_for_trigger(diff: RawDiff, observation_id: Any) -> bool:
    obs_str = str(observation_id)
    for op in diff.claim_ops:
        if op.op != "insert" or op.entry is None:
            continue
        if str(op.entry.get("born_from_event_id")) != obs_str:
            continue
        prop = op.entry.get("proposition") or {}
        if isinstance(prop, dict) and prop.get("kind") == "prediction":
            return True
    return False


def _first_insert_scope(diff: RawDiff, observation_id: Any) -> tuple[list, list]:
    obs_str = str(observation_id)
    for op in diff.claim_ops:
        if op.op != "insert" or op.entry is None:
            continue
        if str(op.entry.get("born_from_event_id")) != obs_str:
            continue
        return (
            list(op.entry.get("scope_actors") or []),
            list(op.entry.get("scope_entities") or []),
        )
    return [], []


def maybe_inject_future_prediction(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Split explicit future plans out of state-only LLM output.

    DeepSeek often records "PR is open, targeting ship tomorrow" as one
    `state`. For retrieval and deadline resolution, the future clause is a
    different semantic object. This hook appends one low-confidence prediction
    when the trigger contains an explicit plan/date and the LLM did not already
    emit a prediction for that same observation.
    """
    if trigger.kind != "T1" or trigger.observation_id is None:
        return raw_diff
    if _has_prediction_for_trigger(raw_diff, trigger.observation_id):
        return raw_diff

    content = ""
    actor_id = None
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            content = (getattr(obs, "content_text", None) or "").strip()
            actor_id = getattr(obs, "actor_id", None)
            break
    if not content:
        content = (trigger.seed_natural_text or "").strip()
    if not content or not _FUTURE_PLAN_RE.search(content):
        return raw_diff

    scope_actors, scope_entities = _first_insert_scope(
        raw_diff, trigger.observation_id,
    )
    if not scope_actors and actor_id is not None:
        scope_actors = [str(actor_id)]

    evaluate_at = _prediction_evaluate_at(content, trigger.seed_occurred_at)
    valid_from = trigger.seed_occurred_at or datetime.now(timezone.utc)
    expected = re.sub(r"\s+", " ", content).strip()
    if len(expected) > 180:
        expected = expected[:177].rstrip() + "..."

    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(trigger.observation_id),
                "proposition": {
                    "kind": "prediction",
                    "expected": expected,
                    "resolution": (
                        "The stated future plan happens, is delayed, or is "
                        "cancelled."
                    ),
                },
                "natural": f"Future plan to verify: {expected}",
                "confidence": 0.62,
                "scope_actors": scope_actors,
                "scope_entities": scope_entities,
                "scope_temporal": {
                    "valid_from": valid_from.isoformat(),
                    "valid_until": evaluate_at.isoformat(),
                },
                "falsifier": {
                    "kind": "prediction_deadline",
                    "evaluate_at": evaluate_at.isoformat(),
                    "check": (
                        "Look for a later signal confirming, delaying, or "
                        "cancelling the stated plan."
                    ),
                },
            },
        )
    )
    return raw_diff


def _customer_aliases(identity: str) -> list[str]:
    aliases = [identity.strip()]
    folded = identity.casefold().strip()
    for suffix in (" inc", " corp", " llc", " ltd", " incorporated"):
        if folded.endswith(suffix):
            aliases.append(identity[: -len(suffix)].strip())
            break
    return [a for a in aliases if a]


def _customer_ref_from_entity(entity: Any) -> dict[str, str] | None:
    if not isinstance(entity, dict):
        return None
    etype = str(entity.get("type") or "")
    eid = entity.get("id")
    if not eid or etype not in ("customer", "customer_resource"):
        return None
    return {"type": "customer", "id": str(eid)}


def _risk_customer_refs(
    content: str,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(ref: dict[str, str] | None) -> None:
        if ref is None:
            return
        key = (ref["type"], ref["id"])
        if key in seen:
            return
        seen.add(key)
        refs.append(ref)

    for entity in trigger.seed_entity_ids:
        add(_customer_ref_from_entity(entity))

    for obs in bundle.observations:
        if getattr(obs, "id", None) != trigger.observation_id:
            continue
        for entity in getattr(obs, "entities_mentioned", []) or []:
            add(_customer_ref_from_entity(entity))

    content_lc = content.casefold()
    for resource in bundle.resources_summary:
        if getattr(resource, "kind", None) != "relational":
            continue
        identity = getattr(resource, "identity", "") or ""
        if any(alias.casefold() in content_lc for alias in _customer_aliases(identity)):
            add({"type": "customer", "id": str(getattr(resource, "id"))})

    return refs


def _claim_scopes_customer_risk(
    entry: dict[str, Any],
    observation_id: Any,
    customer_ids: set[str],
) -> bool:
    if str(entry.get("born_from_event_id")) != str(observation_id):
        return False
    scoped_customer = False
    for entity in entry.get("scope_entities") or []:
        if not isinstance(entity, dict):
            continue
        if str(entity.get("id")) in customer_ids:
            scoped_customer = True
            break
    if not scoped_customer:
        return False
    prop = entry.get("proposition") or {}
    text = f"{entry.get('natural') or ''} {prop}"
    return bool(_CUSTOMER_RISK_RE.search(text)) or prop.get("kind") == "concern"


def maybe_inject_customer_risk(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Ensure explicit customer churn/renewal-risk signals become memory.

    These signals are production-critical: a customer naming competitors or
    saying renewal is not a given should not disappear because the LLM had a
    no-op pass or failed to attach the resolved customer scope. The hook only
    fires on explicit risk language and a resolved customer id.
    """
    if trigger.kind != "T1" or trigger.observation_id is None:
        return raw_diff

    content = ""
    actor_id = None
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            content = (getattr(obs, "content_text", None) or "").strip()
            actor_id = getattr(obs, "actor_id", None)
            break
    if not content:
        content = (trigger.seed_natural_text or "").strip()
    if actor_id is None and trigger.scope_actors:
        actor_id = trigger.scope_actors[0]
    if not content or not _CUSTOMER_RISK_RE.search(content):
        return raw_diff

    customer_refs = _risk_customer_refs(content, trigger, bundle)
    if not customer_refs:
        return raw_diff
    customer_ids = {ref["id"] for ref in customer_refs}

    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        if _claim_scopes_customer_risk(
            op.entry,
            trigger.observation_id,
            customer_ids,
        ):
            return raw_diff

    scope_actors = [str(actor_id)] if actor_id is not None else []
    risk_text = re.sub(r"\s+", " ", content).strip()
    if len(risk_text) > 200:
        risk_text = risk_text[:197].rstrip() + "..."

    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(trigger.observation_id),
                "proposition": {
                    "kind": "concern",
                    "about": "customer renewal",
                    "nature": (
                        "The customer is showing explicit churn or renewal "
                        "risk signals."
                    ),
                    "raised_by": str(actor_id) if actor_id else "customer",
                },
                "natural": f"Customer renewal risk signal: {risk_text}",
                "confidence": 0.74,
                "scope_actors": scope_actors,
                "scope_entities": customer_refs,
                "scope_temporal": {
                    "valid_from": (
                        trigger.seed_occurred_at or datetime.now(timezone.utc)
                    ).isoformat(),
                    "valid_until": None,
                },
                "falsifier": {
                    "kind": "observation_pattern",
                    "pattern": (
                        "The customer renews without escalation, retracts the "
                        "risk signal, or states they are no longer evaluating "
                        "alternatives."
                    ),
                    "within_window": "P90D",
                },
            },
        )
    )
    return raw_diff


def maybe_inject_create_commitment(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """If the trigger event self-reports new in-flight work and no
    matching commitment exists in `<acts>`, append a `create_commitment`
    recommendation claim_op to the diff. Mutates and returns the diff
    in-place for caller convenience."""
    if trigger.kind != "T1":
        return raw_diff
    if _has_create_commitment_rec(raw_diff):
        return raw_diff
    if trigger.observation_id is None:
        return raw_diff

    # Prefer the triggering observation's content_text from the bundle
    # if available, otherwise fall back to trigger.seed_natural_text
    # (always populated for T1 from the retrieval seed).
    content: str = ""
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            content = (getattr(obs, "content_text", None) or "").strip()
            break
    if not content:
        content = (trigger.seed_natural_text or "").strip()
    if not content:
        return raw_diff

    m = _TRIGGER_RE.search(content)
    if m is None:
        return raw_diff
    title = _extract_title(content, m.end())
    if not title:
        return raw_diff

    commitments = bundle.acts_summary.get("commitments") or []
    title_lc = title.lower()
    title_words = {w for w in re.findall(r"\w+", title_lc) if len(w) >= 4}
    for c in commitments:
        c_title = (getattr(c, "title", None) or "").lower()
        if not c_title:
            continue
        if title_lc in c_title or c_title in title_lc:
            return raw_diff
        c_words = set(re.findall(r"\w+", c_title))
        if title_words and len(title_words & c_words) >= 2:
            return raw_diff

    # Owner: prefer the triggering observation's actor_id; fall back to
    # the first scope_actor on the trigger (T1 ingestion seeds this from
    # the signal author).
    owner_id: Any = None
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            owner_id = getattr(obs, "actor_id", None)
            break
    if owner_id is None and trigger.scope_actors:
        owner_id = trigger.scope_actors[0]
    if owner_id is None or isinstance(owner_id, str):
        return raw_diff

    goals = bundle.acts_summary.get("goals") or []
    goal_id_str: str | None = None
    for g in goals:
        gid = getattr(g, "id", None)
        if gid:
            goal_id_str = str(gid)
            break

    due = _extract_due_date(content)

    payload: dict[str, Any] = {
        "title": title,
        "owner_id": str(owner_id),
        "due_date": due.isoformat(),
    }
    if goal_id_str is not None:
        payload["contributes_to_goal_ids"] = [goal_id_str]
    else:
        payload["is_maintenance"] = True

    proposition = {
        "kind": "recommendation",
        "target_act_ref": {"type": "commitment", "id": None},
        "proposed_change": {
            "operation": "create",
            "payload": payload,
        },
        "qualitative_impact": (
            "Tracks newly self-reported in-flight work in the ledger."
        ),
        "target_actor_id": str(owner_id),
    }

    natural = f'Track "{title}" as a commitment owned by the self-reporter.'
    entry = {
        "born_from_event_id": str(trigger.observation_id),
        "proposition": proposition,
        "natural": natural,
        "confidence": 0.7,
        "scope_actors": [str(owner_id)],
        "scope_entities": (
            [{"type": "goal", "id": goal_id_str}] if goal_id_str else []
        ),
    }

    raw_diff.claim_ops.append(ClaimOp(op="insert", entry=entry))
    return raw_diff


def _commitment_title_match_score(signal_text: str, title: str) -> int:
    """Word-overlap score between signal text and a commitment title.
    Counts shared content words (>=4 chars). Used to pick the best
    target for a deterministic transition_commitment when the LLM
    refuses to emit one."""
    sig_words = {
        w for w in re.findall(r"\w+", signal_text.lower()) if len(w) >= 4
    }
    title_words = {
        w for w in re.findall(r"\w+", (title or "").lower()) if len(w) >= 4
    }
    return len(sig_words & title_words)


def _title_match_score(signal_text: str, title: str) -> int:
    sig_words = {
        w for w in re.findall(r"\w+", signal_text.lower()) if len(w) >= 4
    }
    title_words = {
        w for w in re.findall(r"\w+", (title or "").lower()) if len(w) >= 4
    }
    return len(sig_words & title_words)


def _has_transition_commitment_op(diff: RawDiff, target_id: Any) -> bool:
    target_str = str(target_id)
    for op in diff.act_ops:
        if op.op != "transition_commitment":
            continue
        ent = op.entity or {}
        if str(ent.get("id")) == target_str:
            return True
    return False


def _has_transition_decision_op(diff: RawDiff, target_id: Any) -> bool:
    target_str = str(target_id)
    for op in diff.act_ops:
        if op.op != "transition_decision":
            continue
        ent = op.entity or {}
        if str(ent.get("id")) == target_str:
            return True
    return False


def _decision_scoped_claim_confidence(diff: RawDiff, decision_id: Any) -> float:
    target_str = str(decision_id)
    best = 0.0
    for op in diff.claim_ops:
        if op.op != "insert" or op.entry is None:
            continue
        try:
            conf = float(op.entry.get("confidence") or 0.0)
        except (TypeError, ValueError):
            conf = 0.0
        for ent in op.entry.get("scope_entities") or []:
            if not isinstance(ent, dict):
                continue
            if ent.get("type") == "decision" and str(ent.get("id")) == target_str:
                best = max(best, conf)
    return best


def maybe_inject_decision_revisit(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """Deterministically capture explicit decision-revisit signals.

    Live LLM output frequently treats "I'll mark DEC-007 as revisited" as a
    future bookkeeping note even when the same signal says the review outcome is
    already formal. For the ledger, the meaningful company state is that the
    decision is now revisited, so we add the scoped Model and Act transition.
    """
    if trigger.kind != "T1" or trigger.observation_id is None:
        return raw_diff

    content = ""
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            content = (getattr(obs, "content_text", None) or "").strip()
            break
    if not content:
        content = (trigger.seed_natural_text or "").strip()
    if not content or not _DECISION_REVISIT_RE.search(content):
        return raw_diff

    decisions = bundle.acts_summary.get("decisions") or []
    candidates: list[tuple[int, Any]] = []
    for d in decisions:
        if getattr(d, "state", None) != "active":
            continue
        score = _title_match_score(content, getattr(d, "title", None) or "")
        if score >= 1:
            candidates.append((score, d))
    if not candidates:
        return raw_diff
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0][1]
    decision_id = getattr(best, "id", None)
    if decision_id is None:
        return raw_diff

    if _decision_scoped_claim_confidence(raw_diff, decision_id) < 0.70:
        actor_id = None
        for obs in bundle.observations:
            if getattr(obs, "id", None) == trigger.observation_id:
                actor_id = getattr(obs, "actor_id", None)
                break
        title = getattr(best, "title", "") or "decision"
        raw_diff.claim_ops.append(
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": str(trigger.observation_id),
                    "proposition": {
                        "kind": "concern",
                        "about": title,
                        "nature": (
                            "The decision has been formally revisited and "
                            "requires amended-scope follow-through."
                        ),
                        "raised_by": str(actor_id) if actor_id else "system",
                    },
                    "natural": (
                        f'Decision "{title}" is now formally revisited based '
                        "on the review outcome."
                    ),
                    "confidence": 0.78,
                    "scope_actors": [str(actor_id)] if actor_id else [],
                    "scope_entities": [
                        {"type": "decision", "id": str(decision_id)}
                    ],
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": (
                            "The decision owner states the decision was not "
                            "revisited or the review outcome is rescinded"
                        ),
                        "within_window": "P14D",
                    },
                },
            )
        )

    if not _has_transition_decision_op(raw_diff, decision_id):
        raw_diff.act_ops.append(
            ActOp(
                op="transition_decision",
                confidence_basis=trigger.observation_id,
                entity={"id": str(decision_id), "new_state": "revisited"},
            )
        )
    return raw_diff


def maybe_inject_block_transition(
    raw_diff: RawDiff,
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> RawDiff:
    """If the trigger event reports a known commitment is blocked /
    on hold / awaiting approval, and the LLM didn't already emit a
    transition_commitment, deterministically emit one targeting the
    best-matching commitment whose current state is not already
    'blocked'. Mutates the diff in place."""
    if trigger.kind != "T1":
        return raw_diff

    content = ""
    for obs in bundle.observations:
        if getattr(obs, "id", None) == trigger.observation_id:
            content = (getattr(obs, "content_text", None) or "").strip()
            break
    if not content:
        content = (trigger.seed_natural_text or "").strip()
    if not content:
        return raw_diff
    if not _BLOCK_RE.search(content):
        return raw_diff

    commitments = bundle.acts_summary.get("commitments") or []
    candidates: list[tuple[int, Any]] = []
    for c in commitments:
        title = getattr(c, "title", None) or ""
        state = getattr(c, "state", None)
        if state in ("blocked", "paused"):
            continue
        score = _commitment_title_match_score(content, title)
        if score >= 2:
            candidates.append((score, c))
    if not candidates:
        return raw_diff
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0][1]
    target_id = getattr(best, "id", None)
    if target_id is None:
        return raw_diff
    if _has_transition_commitment_op(raw_diff, target_id):
        return raw_diff

    # The validator requires a confidence_basis Model whose confidence
    # clears `transition_commitment_to_paused` (0.55). Prefer a fresh
    # state Model the LLM emitted with high enough confidence; fall
    # back to retrieved Models above the threshold; if none qualify we
    # synthesise a state Model and use it as the basis ourselves.
    _MIN_BASIS_CONF = 0.55
    basis_id: Any = None
    for op in raw_diff.claim_ops:
        if op.op == "insert" and op.entry is not None:
            entry = op.entry
            conf = entry.get("confidence")
            try:
                conf_f = float(conf) if conf is not None else 0.0
            except (TypeError, ValueError):
                conf_f = 0.0
            if conf_f < _MIN_BASIS_CONF:
                continue
            mid = entry.get("model_id") or entry.get("id")
            if mid:
                basis_id = mid
                break
    if basis_id is None:
        best_conf = -1.0
        for m in bundle.models:
            mid = getattr(m, "id", None)
            mconf_raw = getattr(m, "confidence", None)
            try:
                mconf = float(mconf_raw) if mconf_raw is not None else 0.0
            except (TypeError, ValueError):
                mconf = 0.0
            if mid is None or mconf < _MIN_BASIS_CONF:
                continue
            if mconf > best_conf:
                best_conf = mconf
                basis_id = mid
    if basis_id is None:
        # Synthesise a state Model on the spot so we always have an
        # adequate basis. The ingestion path does not require an LLM
        # for this — we just need a Model row recording the block,
        # which the applier will insert before the act_op runs.
        from uuid import uuid4

        synth_model_id = uuid4()
        synth_entry = {
            "born_from_event_id": str(trigger.observation_id),
            "proposition": {
                "kind": "state",
                "subject": str(target_id),
                "assertion": (
                    f"Commitment '{getattr(best, 'title', '')}' "
                    "is on hold pending external approval"
                ),
            },
            "natural": (
                f"Commitment '{getattr(best, 'title', '')}' is on hold "
                "pending external approval (auto-detected from signal)."
            ),
            "confidence": 0.7,
            "scope_actors": [str(owner_id)] if (owner_id := getattr(best, "owner_id", None)) else [],
            "scope_entities": [
                {"type": "commitment", "id": str(target_id)}
            ],
            "model_id": str(synth_model_id),
        }
        raw_diff.claim_ops.append(
            ClaimOp(op="insert", entry=synth_entry)
        )
        basis_id = synth_model_id

    # Use 'paused' rather than 'blocked' because invariant C8 requires
    # blocked transitions to have an unsatisfied dependency or
    # revisited constraining decision. Social/approval-style blocks
    # don't have those, so 'paused' is the closest legal state.
    raw_diff.act_ops.append(
        ActOp(
            op="transition_commitment",
            confidence_basis=basis_id,
            entity={"id": str(target_id), "new_state": "paused"},
        )
    )

    # Archive any active recommendation Models that target this same
    # commitment — once the commitment is paused, the "CEO should
    # unblock X" cards are stale and just clutter Today.
    target_str = str(target_id)
    archived_rec_ids: set[str] = set()
    for m in bundle.models:
        if getattr(m, "proposition_kind", None) != "recommendation":
            continue
        if getattr(m, "status", None) != "active":
            continue
        prop = getattr(m, "proposition", None) or {}
        if not isinstance(prop, dict):
            continue
        ref = prop.get("target_act_ref") or {}
        if not isinstance(ref, dict):
            continue
        if ref.get("type") != "commitment":
            continue
        if str(ref.get("id") or "") != target_str:
            continue
        mid = getattr(m, "id", None)
        if mid is None:
            continue
        mid_str = str(mid)
        if mid_str in archived_rec_ids:
            continue
        archived_rec_ids.add(mid_str)
        raw_diff.claim_ops.append(
            ClaimOp(
                op="archive",
                model_id=mid,
                reason="situation_resolved",
            )
        )
    return raw_diff


__all__ = [
    "maybe_inject_create_commitment",
    "maybe_inject_block_transition",
    "maybe_inject_decision_revisit",
    "maybe_inject_future_prediction",
    "maybe_inject_customer_risk",
]
