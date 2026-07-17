"""Frozen untouched cross-batch-required active-memory v7 holdout."""

from __future__ import annotations

from lib.contracts.kernel import canonical_sha256


_FACETS = {
    "tundra": ("permit_gap", "audit_queue", "waiver_delay", "control_drift", "filing_clock", "owner_change", "evidence_gap", "review_hold", "deadline_risk", "compliance_exposure"),
    "uplink": ("packet_loss", "route_flap", "capacity_skew", "retry_growth", "failover_lag", "region_heat", "queue_depth", "recovery_drift", "service_risk", "network_exposure"),
    "velvet": ("forecast_gap", "stockout_rise", "supplier_delay", "allocation_drift", "demand_spike", "margin_pressure", "substitution_loss", "replenish_lag", "revenue_risk", "inventory_exposure"),
    "willow": ("consent_gap", "retention_drift", "delete_queue", "vendor_copy", "purpose_shift", "review_delay", "policy_gap", "access_backlog", "privacy_risk", "data_exposure"),
    "zenith": ("triage_delay", "handoff_gap", "review_queue", "followup_lag", "capacity_loss", "protocol_drift", "referral_hold", "care_backlog", "patient_risk", "clinical_exposure"),
}
MANIFEST_V7 = {
    "schema_version": "company-model-hidden-truth-v1",
    "experiment_id": "active-batch-memory-cross-batch-holdout-v7-untouched",
    "judge_id": "tenant-isolated-collective-facet-judge-v1",
    "hidden_theses": [
        {"thesis_id": subject, "truth": f"distributed evidence establishes {subject} exposure",
         "required_groups": [[facet] for facet in facets]}
        for subject, facets in _FACETS.items()
    ],
}
_PAIRINGS = (("tundra", "uplink"), ("velvet", "willow"), ("zenith", "tundra"),
             ("uplink", "velvet"), ("willow", "zenith"))
_seen = {subject: 0 for subject in _FACETS}
_batches = []
for pair in _PAIRINGS:
    batch = []
    for subject in pair:
        offset = _seen[subject] * 5
        batch.extend((subject, facet) for facet in _FACETS[subject][offset:offset + 5])
        _seen[subject] += 1
    _batches.append(tuple(batch))
BATCHES_V7 = tuple(_batches)
MANIFEST_DIGEST_V7 = canonical_sha256(MANIFEST_V7)
CORPUS_DIGEST_V7 = canonical_sha256({"batches": BATCHES_V7})

__all__ = ["BATCHES_V7", "CORPUS_DIGEST_V7", "MANIFEST_DIGEST_V7", "MANIFEST_V7"]
