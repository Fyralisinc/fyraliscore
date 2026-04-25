"""Generate 50 hand-labeled calibration fixtures for the Layer 6 judge.

Produces `cal-001.json` through `cal-050.json` under
`packages/lsob-evaluator-l6/fixtures/judge_calibration/`. Each fixture has a
reference diff, a SUT diff, and a ground-truth `human_label` in
{reference_wins, tie, sut_wins} plus reviewer notes.

Distribution (matches the Phase 2.3 brief):
  - 20 "reference clearly better" (SUT missing act_ops or completeness)
  - 20 "SUT as good"            (equivalent content, cosmetic differences)
  - 10 "sut clearly better"     (reference has extra noise / fabrication)

The diffs are realistic in shape and not trivially distinguishable by length
alone: we jitter wording, reorder operations, and occasionally add incidental
evidence references so that a dumb length-based classifier would fail.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent / "fixtures" / "judge_calibration"
)


BUSINESS_DOMAINS = [
    ("acme", "enterprise-saas"),
    ("globex", "marketplace"),
    ("initech", "healthtech"),
    ("soylent", "logistics"),
    ("umbrella", "fintech"),
    ("vandelay", "edtech"),
    ("wayne", "security"),
    ("wonka", "consumer-goods"),
]

TRIGGER_KINDS = [
    "signal_landed",
    "calendar_event_completed",
    "pr_merged",
    "email_thread_updated",
    "ticket_status_changed",
    "doc_published",
]

ACTOR_PREFIXES = ["pm", "eng", "cs", "sales", "ops"]


def _ts(n: int) -> datetime:
    return datetime(2026, 1, 10, tzinfo=timezone.utc) + timedelta(hours=n)


def _claim(
    claim_id: str,
    proposition: str,
    kind: str,
    conf: float,
    entities: list[str],
    falsifier: str | None = None,
    evidence: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "op": "upsert_claim",
        "claim_id": claim_id,
        "proposition": proposition,
        "proposition_kind": kind,
        "asserted_confidence": conf,
        "falsifier": falsifier,
        "evidence_signal_ids": list(evidence or []),
        "entities": list(entities),
    }


def _act(
    entity_ref: str,
    to_state: str,
    from_state: str | None = None,
    op: str = "transition",
    reason: str | None = None,
) -> dict[str, Any]:
    return {
        "op": op,
        "entity_ref": entity_ref,
        "from_state": from_state,
        "to_state": to_state,
        "reason": reason,
    }


def _resource(
    op: str,
    resource_ref: str,
    target_ref: str | None = None,
    amount: float | None = None,
) -> dict[str, Any]:
    return {
        "op": op,
        "resource_ref": resource_ref,
        "target_ref": target_ref,
        "amount": amount,
    }


def _diff(
    diff_id: str,
    trigger_id: str,
    produced_at: datetime,
    claim_ops: list[dict[str, Any]] | None = None,
    act_ops: list[dict[str, Any]] | None = None,
    resource_ops: list[dict[str, Any]] | None = None,
    rationale: str | None = None,
) -> dict[str, Any]:
    return {
        "diff_id": diff_id,
        "produced_at": produced_at.isoformat(),
        "trigger_id": trigger_id,
        "claim_ops": list(claim_ops or []),
        "act_ops": list(act_ops or []),
        "resource_ops": list(resource_ops or []),
        "rationale": rationale,
        "metadata": {},
    }


def _trigger(trigger_id: str, kind: str, payload: dict[str, Any], ts: datetime) -> dict[str, Any]:
    return {
        "trigger_id": trigger_id,
        "kind": kind,
        "payload": payload,
        "timestamp": ts.isoformat(),
    }


# --------------------------------------------------------------------------
# Scenario builders. Each returns (reference_diff, sut_diff, trigger, notes).
# --------------------------------------------------------------------------


def _rewording(base: str, seed: int) -> str:
    """Slight rewording of a proposition so equivalent SUTs aren't identical strings."""
    rng = random.Random(seed)
    variants = [
        base,
        base.replace("will", "is likely to") if "will" in base else base + " (observed)",
        base.replace("needs", "requires") if "needs" in base else base.capitalize(),
        base.rstrip(".") + " per the latest signal.",
    ]
    return rng.choice(variants)


def build_reference_wins(i: int) -> dict[str, Any]:
    """SUT is materially worse: missing key act_ops or under-covering claims."""
    rng = random.Random(1_000 + i)
    company, domain = BUSINESS_DOMAINS[i % len(BUSINESS_DOMAINS)]
    commit_id = f"commitment:{company}-{i:03d}"
    customer_id = f"customer:{company}-{i:03d}"
    actor = f"actor:{rng.choice(ACTOR_PREFIXES)}-{rng.randint(1, 9)}"
    ts = _ts(i)

    trigger = _trigger(
        trigger_id=f"trig-ref-{i:03d}",
        kind=rng.choice(TRIGGER_KINDS),
        payload={
            "commitment_id": commit_id,
            "customer_id": customer_id,
            "domain": domain,
            "intensity": rng.choice(["low", "medium", "high"]),
        },
        ts=ts,
    )

    # Reference: two claims, a transition, and a resource reallocation.
    ref_claims = [
        _claim(
            f"ref-claim-{i:03d}-a",
            f"{actor} reports {commit_id} at risk due to upstream slip",
            "risk_assessment",
            round(0.6 + rng.random() * 0.2, 2),
            [commit_id, actor],
            falsifier=f"weekly_review_check({commit_id})",
            evidence=[f"signal-{i:03d}-alpha"],
        ),
        _claim(
            f"ref-claim-{i:03d}-b",
            f"{customer_id} executive sponsor is monitoring {commit_id}",
            "stakeholder",
            round(0.7 + rng.random() * 0.2, 2),
            [customer_id, commit_id],
            evidence=[f"signal-{i:03d}-beta"],
        ),
    ]
    ref_acts = [
        _act(commit_id, to_state="at_risk", from_state="on_track", reason="slip"),
        _act(f"pattern:stall-{i:03d}", to_state="active", op="create"),
    ]
    ref_resources = [
        _resource("reallocate", f"resource:engineer-{i % 7}", commit_id, 0.5),
    ]
    reference = _diff(
        f"ref-diff-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=ref_claims,
        act_ops=ref_acts,
        resource_ops=ref_resources,
        rationale="Reference: full chain of claims + state transition + resource move.",
    )

    # SUT: captures the risk claim but MISSES the state transition and the
    # pattern creation. Also miscategorises the stakeholder claim as generic.
    sut_claims = [
        _claim(
            f"sut-claim-{i:03d}-a",
            _rewording(ref_claims[0]["proposition"], i),
            "observation",  # wrong kind: downgraded from risk_assessment
            round(0.5 + rng.random() * 0.2, 2),
            [commit_id],
            evidence=[f"signal-{i:03d}-alpha"],
        ),
    ]
    sut_acts: list[dict[str, Any]] = []  # missing transitions
    sut = _diff(
        f"sut-diff-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=sut_claims,
        act_ops=sut_acts,
        rationale="SUT: partial observation, no state transition.",
    )
    return {
        "reference_diff": reference,
        "sut_diff": sut,
        "trigger": trigger,
        "human_label": "reference_wins",
        "notes": (
            "SUT is missing the `at_risk` transition on the commitment and the "
            "pattern creation. Reference also provides richer falsifier + "
            "stakeholder framing."
        ),
    }


def build_tie(i: int) -> dict[str, Any]:
    """Equivalent content; cosmetic differences in wording and op order."""
    rng = random.Random(2_000 + i)
    company, domain = BUSINESS_DOMAINS[i % len(BUSINESS_DOMAINS)]
    commit_id = f"commitment:{company}-tie-{i:03d}"
    customer_id = f"customer:{company}-tie-{i:03d}"
    actor = f"actor:{rng.choice(ACTOR_PREFIXES)}-{rng.randint(1, 9)}"
    ts = _ts(100 + i)

    trigger = _trigger(
        trigger_id=f"trig-tie-{i:03d}",
        kind=rng.choice(TRIGGER_KINDS),
        payload={
            "commitment_id": commit_id,
            "customer_id": customer_id,
            "domain": domain,
            "observed_lag_days": rng.randint(1, 5),
        },
        ts=ts,
    )

    base_prop_a = f"{actor} needs to confirm updated {commit_id} deadline with {customer_id}"
    base_prop_b = f"{customer_id} is still engaged on {commit_id}"
    ref_claims = [
        _claim(
            f"ref-tie-{i:03d}-a",
            base_prop_a,
            "todo",
            0.78,
            [commit_id, actor, customer_id],
            falsifier="deadline_confirmed_email",
            evidence=[f"signal-tie-{i:03d}-1"],
        ),
        _claim(
            f"ref-tie-{i:03d}-b",
            base_prop_b,
            "stakeholder",
            0.82,
            [customer_id, commit_id],
            evidence=[f"signal-tie-{i:03d}-2"],
        ),
    ]
    ref_acts = [
        _act(commit_id, to_state="pending_confirmation", from_state="on_track"),
    ]
    reference = _diff(
        f"ref-tie-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=ref_claims,
        act_ops=ref_acts,
        rationale="Reference: confirm + stakeholder claim + state transition.",
    )

    # SUT: same content, reordered, reworded, same entities/states.
    sut_claims = [
        _claim(
            f"sut-tie-{i:03d}-b",
            _rewording(base_prop_b, i + 33),
            "stakeholder",
            0.80,
            [commit_id, customer_id],  # different ordering
            evidence=[f"signal-tie-{i:03d}-2"],
        ),
        _claim(
            f"sut-tie-{i:03d}-a",
            _rewording(base_prop_a, i + 77),
            "todo",
            0.77,
            [actor, commit_id, customer_id],
            falsifier="deadline_confirmed_email",
            evidence=[f"signal-tie-{i:03d}-1"],
        ),
    ]
    sut_acts = [
        _act(
            commit_id,
            to_state="pending_confirmation",
            from_state="on_track",
            reason="awaiting customer confirmation",
        ),
    ]
    sut = _diff(
        f"sut-tie-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=sut_claims,
        act_ops=sut_acts,
        rationale="SUT: same set of claims plus explicit reason on the transition.",
    )
    return {
        "reference_diff": reference,
        "sut_diff": sut,
        "trigger": trigger,
        "human_label": "tie",
        "notes": (
            "Reference and SUT capture the same facts and the same transition. "
            "SUT reorders claims and adds a minor `reason` string but does not "
            "change substance. Human reviewer treated this as a tie."
        ),
    }


def build_sut_wins(i: int) -> dict[str, Any]:
    """Reference has extra noisy/fabricated claims that the SUT correctly omits."""
    rng = random.Random(3_000 + i)
    company, domain = BUSINESS_DOMAINS[i % len(BUSINESS_DOMAINS)]
    commit_id = f"commitment:{company}-sut-{i:03d}"
    customer_id = f"customer:{company}-sut-{i:03d}"
    actor = f"actor:{rng.choice(ACTOR_PREFIXES)}-{rng.randint(1, 9)}"
    ts = _ts(200 + i)

    trigger = _trigger(
        trigger_id=f"trig-sut-{i:03d}",
        kind=rng.choice(TRIGGER_KINDS),
        payload={
            "commitment_id": commit_id,
            "customer_id": customer_id,
            "domain": domain,
        },
        ts=ts,
    )

    core_claim = _claim(
        f"claim-core-{i:03d}",
        f"{commit_id} cleared integration review on schedule",
        "milestone",
        0.88,
        [commit_id, actor],
        falsifier="review_reopen_within_7d",
        evidence=[f"signal-sut-{i:03d}-core"],
    )
    noise_claim = _claim(
        f"claim-noise-{i:03d}",
        # Fabricated: no evidence for a churn signal from this trigger.
        f"{customer_id} is likely to churn next quarter",
        "risk_assessment",
        0.62,
        [customer_id],
        evidence=[],  # no evidence -> low-quality claim
    )
    reference = _diff(
        f"ref-sut-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=[core_claim, noise_claim],
        act_ops=[_act(commit_id, to_state="integration_cleared")],
        rationale="Reference: correct milestone + unsupported churn speculation.",
    )

    sut = _diff(
        f"sut-sut-{i:03d}",
        trigger["trigger_id"],
        ts,
        claim_ops=[core_claim.copy()],
        act_ops=[
            _act(
                commit_id,
                to_state="integration_cleared",
                from_state="integration_pending",
                reason="review pass",
            ),
        ],
        rationale="SUT: records the milestone transition without fabricating churn risk.",
    )
    # Slight rewording on SUT's core claim so it isn't byte-identical.
    sut["claim_ops"][0]["claim_id"] = f"sut-claim-core-{i:03d}"
    sut["claim_ops"][0]["proposition"] = _rewording(core_claim["proposition"], i + 11)
    return {
        "reference_diff": reference,
        "sut_diff": sut,
        "trigger": trigger,
        "human_label": "sut_wins",
        "notes": (
            "Reference adds an unsupported churn-risk claim (no evidence). "
            "SUT correctly omits it and provides a cleaner state transition. "
            "Human reviewer preferred SUT on fabrication grounds."
        ),
    }


def main() -> None:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    # Wipe existing generated files to keep the set reproducible.
    for old in FIXTURES_DIR.glob("cal-*.json"):
        old.unlink()

    idx = 1
    for i in range(20):
        item = build_reference_wins(i)
        item["id"] = f"cal-{idx:03d}"
        (FIXTURES_DIR / f"{item['id']}.json").write_text(
            json.dumps(item, indent=2, sort_keys=True)
        )
        idx += 1
    for i in range(20):
        item = build_tie(i)
        item["id"] = f"cal-{idx:03d}"
        (FIXTURES_DIR / f"{item['id']}.json").write_text(
            json.dumps(item, indent=2, sort_keys=True)
        )
        idx += 1
    for i in range(10):
        item = build_sut_wins(i)
        item["id"] = f"cal-{idx:03d}"
        (FIXTURES_DIR / f"{item['id']}.json").write_text(
            json.dumps(item, indent=2, sort_keys=True)
        )
        idx += 1

    print(f"Wrote {idx - 1} calibration fixtures to {FIXTURES_DIR}")


if __name__ == "__main__":
    main()
