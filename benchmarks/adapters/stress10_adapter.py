"""Built-in ten-case stress benchmark for retrieval bottleneck discovery."""

from __future__ import annotations

from benchmarks.adapters.base import (
    BenchmarkAdapter,
    BenchmarkObservation,
    BenchmarkQuery,
    GoldLabels,
    observed_at,
)


class Stress10Adapter(BenchmarkAdapter):
    """Diverse deterministic end-to-end benchmark slice.

    The cases are synthetic but shaped after the operational bottlenecks that
    matter for Fyralis: dense haystacks, temporal updates, contradictions,
    tenant isolation, dynamic UI state, structured fact gating, and abstention.
    """

    benchmark_name = "stress10"

    def __init__(self) -> None:
        tenant = "bench_stress_tenant"
        other_tenant = "bench_stress_other_tenant"
        self._observations = [
            BenchmarkObservation(
                observation_id="stress_obs_001_dense_target",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 1, 1, 9),
                content=(
                    "Dense haystack memo. "
                    + "status noise " * 250
                    + "The Q4 reliability owner is Priya Nair. "
                    + "status noise " * 250
                ),
                metadata={"stress_axis": "dense_haystack"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_002_temporal_stale",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 1, 4, 9),
                content="Ledger cutoff used to be Friday at 17:00 UTC.",
                metadata={"stress_axis": "temporal_update", "stale": True},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_003_temporal_current",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 4, 9),
                content="Current ledger cutoff moved to Tuesday at 09:00 UTC.",
                metadata={"stress_axis": "temporal_update"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_004_bridge_a",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 7, 9),
                content="Project Aurora depends on service Borealis for invoice event replay.",
                metadata={"stress_axis": "multi_hop_bridge"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_005_bridge_b",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 7, 10),
                content="Service Borealis is owned by Team Kestrel.",
                metadata={"stress_axis": "multi_hop_bridge"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_006_contradiction_low_trust",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 8, 9),
                content="Unverified rumor: Project Helios launch status is green.",
                trust_tier="unverified",
                metadata={"stress_axis": "contradiction", "trust": "low"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_007_contradiction_high_trust",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 8, 11),
                content="Executive review: Project Helios launch status is red.",
                trust_tier="benchmark_gold",
                metadata={"stress_axis": "contradiction", "trust": "high"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_008_dynamic_ui",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 10, 9),
                content="\n".join(
                    [
                        "Before state key UI labels: Filters, Owner, Status.",
                        "After state key UI labels: Filters, Owner, Status, Priority, Escalation.",
                        "Newly visible after action: Priority; Escalation.",
                        "Transition summary: opening Advanced Filters exposed two fields.",
                    ]
                ),
                metadata={"stress_axis": "dynamic_state_transition"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_009_structured_ui",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 10, 10),
                content="Operational memory record: web_agent_state. User opened a caller lookup.",
                metadata={
                    "stress_axis": "structured_ui_gating",
                    "structured_ui_facts": [
                        "autocomplete popup title: Recent selections; field=Caller; options: Asha Rao, Bruno Vale",
                        "table summary row Total: value=512",
                        "field list caller options: Caller, Assignment group, Impact",
                    ],
                },
            ),
            BenchmarkObservation(
                observation_id="stress_obs_010_access_allowed",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 11, 9),
                content="Tenant-local renewal code is CYPRESS-42.",
                metadata={"stress_axis": "tenant_isolation"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_011_access_forbidden",
                source="stress10",
                tenant_id=other_tenant,
                occurred_at=observed_at(2026, 2, 11, 9),
                content="Tenant-local renewal code is MAPLE-99.",
                metadata={"stress_axis": "tenant_isolation"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_012_abstention_decoy",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 12, 9),
                content="Payroll dashboard tracks open approvals but does not show the CFO home address.",
                metadata={"stress_axis": "abstention"},
            ),
            BenchmarkObservation(
                observation_id="stress_obs_013_packet_needle",
                source="stress10",
                tenant_id=tenant,
                occurred_at=observed_at(2026, 2, 13, 9),
                content="\n".join(
                    [
                        "General operating note: " + ("routine planning " * 180),
                        "Escalation runbook detail: the rollback token is ORCHID-17.",
                        "General operating note: " + ("routine planning " * 180),
                    ]
                ),
                metadata={"stress_axis": "packet_needle"},
            ),
        ]
        self._queries = [
            BenchmarkQuery(
                query_id="stress_q_001_dense_haystack",
                tenant_id=tenant,
                query_text="Who owns Q4 reliability?",
                gold_answer="Priya Nair",
                gold_evidence_ids=["stress_obs_001_dense_target"],
                metadata={"stress_axis": "dense_haystack"},
            ),
            BenchmarkQuery(
                query_id="stress_q_002_temporal_update",
                tenant_id=tenant,
                query_text="What is the current ledger cutoff?",
                gold_answer="Tuesday at 09:00 UTC",
                gold_evidence_ids=["stress_obs_003_temporal_current"],
                metadata={"stress_axis": "temporal_update"},
            ),
            BenchmarkQuery(
                query_id="stress_q_003_multi_hop",
                tenant_id=tenant,
                query_text="Which team owns the service that Project Aurora depends on?",
                gold_answer="Team Kestrel",
                gold_evidence_ids=["stress_obs_004_bridge_a", "stress_obs_005_bridge_b"],
                metadata={"stress_axis": "multi_hop_bridge"},
            ),
            BenchmarkQuery(
                query_id="stress_q_004_contradiction",
                tenant_id=tenant,
                query_text="What is Project Helios launch status according to the executive review?",
                gold_answer="red",
                gold_evidence_ids=["stress_obs_007_contradiction_high_trust"],
                metadata={"stress_axis": "contradiction"},
            ),
            BenchmarkQuery(
                query_id="stress_q_005_dynamic_ui",
                tenant_id=tenant,
                query_text="After opening Advanced Filters, which fields became newly visible?",
                query_type="dynamic-environment",
                gold_answer="Priority; Escalation",
                gold_evidence_ids=["stress_obs_008_dynamic_ui"],
                metadata={"stress_axis": "dynamic_state_transition"},
            ),
            BenchmarkQuery(
                query_id="stress_q_006_structured_popup",
                tenant_id=tenant,
                query_text="What title appeared in the caller lookup popup?",
                query_type="dynamic-environment",
                gold_answer="Recent selections",
                gold_evidence_ids=["stress_obs_009_structured_ui"],
                metadata={"stress_axis": "structured_ui_gating"},
            ),
            BenchmarkQuery(
                query_id="stress_q_007_structured_non_leakage",
                tenant_id=tenant,
                query_text="What route did the user open?",
                gold_answer="I don't know",
                gold_evidence_ids=[],
                metadata={"stress_axis": "structured_ui_gating", "expected_abstain": True},
            ),
            BenchmarkQuery(
                query_id="stress_q_008_tenant_isolation",
                tenant_id=tenant,
                query_text="What is this tenant's renewal code?",
                gold_answer="CYPRESS-42",
                gold_evidence_ids=["stress_obs_010_access_allowed"],
                metadata={"stress_axis": "tenant_isolation"},
            ),
            BenchmarkQuery(
                query_id="stress_q_009_abstention",
                tenant_id=tenant,
                query_text="What is the CFO home address?",
                gold_answer="I don't know",
                gold_evidence_ids=[],
                metadata={"stress_axis": "abstention", "expected_abstain": True},
            ),
            BenchmarkQuery(
                query_id="stress_q_010_packet_needle",
                tenant_id=tenant,
                query_text="What is the rollback token in the escalation runbook?",
                gold_answer="ORCHID-17",
                gold_evidence_ids=["stress_obs_013_packet_needle"],
                metadata={"stress_axis": "packet_needle"},
            ),
        ]
        self._gold = {
            query.query_id: GoldLabels(
                answer=query.gold_answer,
                evidence_ids=query.gold_evidence_ids,
                expected_abstain=bool(query.metadata.get("expected_abstain")),
                metadata=query.metadata,
            )
            for query in self._queries
        }

    def iter_observations(self):
        yield from self._observations

    def iter_queries(self):
        yield from self._queries

    def gold(self, query_id: str) -> GoldLabels:
        return self._gold[query_id]
