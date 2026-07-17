"""Frozen small v6 holdout for the active compiled batch-memory contract."""

from __future__ import annotations

from typing import Any

from lib.contracts.kernel import canonical_sha256


_THESES = (
    ("orion", "regulatory evidence and review delays compound filing exposure", (("filing_gap", "disclosure_gap"), ("review_delay", "counsel_queue"), ("control_drift", "attestation_gap"), ("deadline_near", "calendar_pressure"), ("filing_risk", "regulatory_exposure"))),
    ("prairie", "marketplace abuse and response gaps compound trust erosion", (("fraud_spike", "abuse_growth"), ("review_backlog", "moderation_queue"), ("appeal_delay", "remediation_lag"), ("seller_churn", "supply_loss"), ("trust_risk", "marketplace_exposure"))),
    ("quartz", "energy imbalance and recovery limits compound continuity risk", (("load_spike", "demand_surge"), ("reserve_low", "capacity_thin"), ("switch_delay", "dispatch_lag"), ("storage_drift", "battery_loss"), ("continuity_risk", "grid_exposure"))),
    ("rivet", "clinical workflow gaps and queue pressure compound care delays", (("triage_gap", "intake_drift"), ("review_queue", "clinician_backlog"), ("handoff_loss", "referral_gap"), ("followup_delay", "care_lag"), ("care_risk", "clinical_exposure"))),
    ("solace", "localization gaps and accessibility defects compound launch exclusion", (("locale_gap", "translation_drift"), ("a11y_defect", "screenreader_failure"), ("qa_queue", "verification_delay"), ("market_block", "release_hold"), ("inclusion_risk", "launch_exposure"))),
)

MANIFEST_V6: dict[str, Any] = {
    "schema_version": "company-model-hidden-truth-v1",
    "experiment_id": "bounded-active-batch-memory-holdout-v6-untouched",
    "judge_id": "independent-facet-group-judge-v2",
    "hidden_theses": [
        {"thesis_id": subject, "truth": truth,
         "required_groups": [list(group) for group in groups]}
        for subject, truth, groups in _THESES
    ],
}

# One genuine batch per independent thesis. The producer receives only these
# visible facets; the manifest remains judge-only.
BATCHES_V6 = tuple(
    tuple((subject, facet) for group in groups for facet in group)
    for subject, _truth, groups in _THESES
)

MANIFEST_DIGEST_V6 = canonical_sha256(MANIFEST_V6)
CORPUS_DIGEST_V6 = canonical_sha256({"batches": BATCHES_V6})

__all__ = ["BATCHES_V6", "CORPUS_DIGEST_V6", "MANIFEST_DIGEST_V6", "MANIFEST_V6"]
