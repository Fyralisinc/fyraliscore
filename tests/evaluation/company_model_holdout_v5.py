"""Frozen untouched v5 company-model generalization holdout corpus."""

from __future__ import annotations

from typing import Any

from lib.contracts.kernel import canonical_sha256


MANIFEST_V5: dict[str, Any] = {
    "schema_version": "company-model-hidden-truth-v1",
    "experiment_id": "bounded-company-model-holdout-v5-untouched",
    "judge_id": "independent-facet-group-judge-v2",
    "hidden_theses": [
        {"thesis_id": "helios", "truth": "supplier fragility and qualification drag jointly threaten launch continuity",
         "required_groups": [["single_source", "vendor_concentration"], ["qa_backlog", "certification_wait"], ["buffer_low", "inventory_thin"], ["port_delay", "freight_slip"], ["launch_exposure", "continuity_risk"]]},
        {"thesis_id": "juniper", "truth": "support load and documentation gaps jointly drive expansion churn",
         "required_groups": [["ticket_spike", "support_load"], ["docs_gap", "runbook_missing"], ["admin_friction", "setup_drag"], ["champion_exit", "sponsor_loss"], ["expansion_risk", "churn_pressure"]]},
        {"thesis_id": "kestrel", "truth": "ledger variance and review latency jointly threaten close integrity",
         "required_groups": [["ledger_variance", "recon_gap"], ["review_queue", "approval_latency"], ["cutoff_shift", "period_drift"], ["owner_overlap", "segregation_gap"], ["close_risk", "integrity_pressure"]]},
        {"thesis_id": "lumen", "truth": "latency regression and capacity imbalance jointly degrade regional reliability",
         "required_groups": [["p99_regression", "latency_spike"], ["hot_shard", "capacity_skew"], ["retry_storm", "queue_amplification"], ["failover_slow", "recovery_lag"], ["regional_risk", "reliability_pressure"]]},
        {"thesis_id": "mosaic", "truth": "consent drift and deletion backlog jointly increase privacy exposure",
         "required_groups": [["consent_drift", "purpose_mismatch"], ["delete_backlog", "erasure_delay"], ["vendor_copy", "processor_sprawl"], ["retention_gap", "policy_drift"], ["privacy_risk", "exposure_pressure"]]},
    ],
}

_WAVES = (
    (("single_source", "vendor_concentration"), ("ticket_spike", "support_load"), ("ledger_variance", "recon_gap"), ("p99_regression", "latency_spike"), ("consent_drift", "purpose_mismatch")),
    (("qa_backlog", "certification_wait"), ("docs_gap", "runbook_missing"), ("review_queue", "approval_latency"), ("hot_shard", "capacity_skew"), ("delete_backlog", "erasure_delay")),
    (("buffer_low", "inventory_thin"), ("admin_friction", "setup_drag"), ("cutoff_shift", "period_drift"), ("retry_storm", "queue_amplification"), ("vendor_copy", "processor_sprawl")),
    (("port_delay", "freight_slip"), ("champion_exit", "sponsor_loss"), ("owner_overlap", "segregation_gap"), ("failover_slow", "recovery_lag"), ("retention_gap", "policy_drift")),
    (("launch_exposure", "continuity_risk"), ("expansion_risk", "churn_pressure"), ("close_risk", "integrity_pressure"), ("regional_risk", "reliability_pressure"), ("privacy_risk", "exposure_pressure")),
)
_SUBJECTS = ("helios", "juniper", "kestrel", "lumen", "mosaic")
BATCHES_V5 = tuple(
    tuple((subject, facet) for subject, pair in zip(_SUBJECTS, wave) for facet in pair)
    for wave in _WAVES
)

MANIFEST_DIGEST_V5 = canonical_sha256(MANIFEST_V5)
CORPUS_DIGEST_V5 = canonical_sha256({"batches": BATCHES_V5})

__all__ = ["BATCHES_V5", "CORPUS_DIGEST_V5", "MANIFEST_DIGEST_V5", "MANIFEST_V5"]
