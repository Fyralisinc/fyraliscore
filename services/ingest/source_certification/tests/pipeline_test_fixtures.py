"""Typed pipeline artifacts shared by source-certification unit tests."""

from __future__ import annotations

from typing import Any

from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_PROBE_SCHEMA_VERSION,
    PIPELINE_REPLAY_SCHEMA_VERSION,
    PIPELINE_TOPOLOGY_SCHEMA_VERSION,
    pipeline_scenario_ids_for_source,
)


def _tenant_pipeline(
    tenant_index: int,
    *,
    observation_count: int = 2,
) -> dict[str, Any]:
    tenant_id = f"00000000-0000-0000-0000-00000000000{tenant_index + 1}"
    observation_hash = ("a" if tenant_index == 0 else "c") * 64
    t1_hash = ("b" if tenant_index == 0 else "d") * 64
    installation_ids = [
        f"10000000-0000-0000-0000-0000000000{tenant_index}{index}" for index in (1, 2)
    ]
    return {
        "tenant_id": tenant_id,
        "raw_topic": {
            "before_replay": {"count": 2},
            "after_replay": {"count": 4},
        },
        "normalized_topic": {
            "before_replay": {"count": 2},
            "after_replay": {"count": 4},
            "observation_identity_count": observation_count,
            "observation_identity_set_sha256": observation_hash,
        },
        "observations": {
            "before_replay": {
                "count": observation_count,
                "identity_set_sha256": observation_hash,
            },
            "after_replay": {
                "count": observation_count,
                "identity_set_sha256": observation_hash,
            },
        },
        "t1_triggers": {
            "before_replay": {
                "t1_count": observation_count,
                "observation_id_set_sha256": t1_hash,
            },
            "after_replay": {
                "t1_count": observation_count,
                "observation_id_set_sha256": t1_hash,
            },
        },
        "installation_attribution": {
            "expected_installation_row_ids": installation_ids,
            "raw_records_by_installation": {
                installation_id: 1 for installation_id in installation_ids
            },
            "normalized_records_by_installation": {
                installation_id: 1 for installation_id in installation_ids
            },
            "exact_installation_row_set": True,
        },
        "replay": {
            "schema_version": PIPELINE_REPLAY_SCHEMA_VERSION,
            "raw_records_before": 2,
            "raw_records_replayed": 2,
            "raw_records_after": 4,
            "raw_topic_record_growth": 2,
            "normalized_records_before": 2,
            "normalized_records_after": 4,
            "normalized_topic_record_growth": 2,
            "observation_count_before": observation_count,
            "observation_count_after": observation_count,
            "observation_count_growth": 0,
            "observation_identity_set_sha256_before": observation_hash,
            "observation_identity_set_sha256_after": observation_hash,
            "t1_count_before": observation_count,
            "t1_count_after": observation_count,
            "t1_count_growth": 0,
            "t1_observation_id_set_sha256_before": t1_hash,
            "t1_observation_id_set_sha256_after": t1_hash,
            "writer_group_drain": {"drained": True},
            "idempotency_proven": True,
        },
    }


def passing_pipeline_probe(source_id: str) -> dict[str, Any]:
    """Return the exact source-capable passing artifact.

    WhatsApp intentionally receives only the live raw-to-T1 boundary.
    Historical sources receive the additional typed 2×2×2 topology.
    """

    scenario_ids = pipeline_scenario_ids_for_source(source_id)
    if source_id == "whatsapp":
        pipeline = _tenant_pipeline(0)
        pipeline.pop("installation_attribution")
    else:
        tenant_pipelines = [_tenant_pipeline(0), _tenant_pipeline(1)]
        tenants: list[dict[str, Any]] = []
        installation_identity: list[dict[str, Any]] = []
        for tenant_index, tenant_pipeline in enumerate(tenant_pipelines):
            installation_ids = tenant_pipeline["installation_attribution"][
                "expected_installation_row_ids"
            ]
            identity_hash = tenant_pipeline["observations"]["before_replay"][
                "identity_set_sha256"
            ]
            tenant_row = {
                "tenant_id": tenant_pipeline["tenant_id"],
                "tenant_slug": f"tenant-{tenant_index}",
                "expected_observation_count": 2,
                "observed_observation_count": 2,
                "installation_keys": [
                    f"tenant-{tenant_index}-installation-1",
                    f"tenant-{tenant_index}-installation-2",
                ],
                "installation_row_ids": installation_ids,
                "trigger_ids": [
                    f"20000000-0000-0000-0000-0000000000" f"{tenant_index}{index}"
                    for index in (1, 2)
                ],
                "onboarding_run_ids": [
                    f"30000000-0000-0000-0000-0000000000" f"{tenant_index}{index}"
                    for index in (1, 2)
                ],
                "normalized_observation_identity_set_sha256": (identity_hash),
                "persisted_observation_identity_set_sha256": (identity_hash),
                "cross_tenant_leak_count": 0,
            }
            tenants.append(tenant_row)
            installation_identity.extend(
                {
                    "source": source_id,
                    "tenant_slug": tenant_row["tenant_slug"],
                    "installation_key": tenant_row["installation_keys"][
                        installation_index
                    ],
                    "tenant_id": tenant_row["tenant_id"],
                    "installation_row_id": tenant_row["installation_row_ids"][
                        installation_index
                    ],
                    "trigger_id": tenant_row["trigger_ids"][installation_index],
                    "onboarding_run_id": tenant_row["onboarding_run_ids"][
                        installation_index
                    ],
                }
                for installation_index in range(2)
            )
        pipeline = {
            "installation_identity": installation_identity,
            "tenant_pipelines": tenant_pipelines,
            "topology": {
                "schema_version": PIPELINE_TOPOLOGY_SCHEMA_VERSION,
                "tenant_count": 2,
                "installations_per_tenant": 2,
                "installation_count": 4,
                "configured_replicas": 2,
                "observed_oauth_replicas": 2,
                "participating_oauth_replicas": 2,
                "oauth_replica_claims": {
                    "oauth-replica-1": 2,
                    "oauth-replica-2": 2,
                },
                "tenants": tenants,
                "exact_installation_identity_proven": True,
                "per_tenant_observation_counts_proven": True,
                "cross_tenant_leak_count": 0,
                "cross_tenant_isolation_proven": True,
                "two_replica_participation_proven": True,
            },
        }
    return {
        "schema_version": PIPELINE_PROBE_SCHEMA_VERSION,
        "state": "passed",
        "source_id": source_id,
        "certified_scenarios": sorted(scenario_ids),
        "pipeline": pipeline,
    }


__all__ = ["passing_pipeline_probe"]
