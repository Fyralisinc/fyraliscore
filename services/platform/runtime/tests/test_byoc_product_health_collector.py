from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

import pytest

from services.platform.runtime.byoc_product_health import (
    canonical_product_health_snapshot_payload,
    signed_product_health_snapshot,
    validate_product_health_snapshot_submission,
)
from services.platform.runtime.byoc_product_health_collector import (
    ByocProductHealthCollectorIdentity,
    collect_product_health_snapshot,
)


DEPLOYMENT_ID = "dep_collector01"
CUSTOMER_ID = "cus_collector01"
AGENT_ID = "agt_collector01"
TENANT_ID = "00000000-0000-4000-8000-000000000001"
COLLECTED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
SIGNING_SECRET = "collector-product-health-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"


class FakeProductHealthDb:
    def __init__(
        self,
        *,
        tables: set[str],
        rows: dict[str, list[dict[str, Any]]] | None = None,
        row: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.tables = tables
        self.rows = rows or {}
        self.row = row or {}
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any]:
        self.fetchrow_calls.append((query, args))
        if "to_regclass" in query:
            return {"exists": args[0] in self.tables}
        return dict(self.row.get(_marker(query), {}))

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        return [dict(item) for item in self.rows.get(_marker(query), [])]


@pytest.mark.asyncio
async def test_collector_builds_metadata_only_product_health_snapshot() -> None:
    db = FakeProductHealthDb(
        tables={
            "observations",
            "ingestion_failures",
            "onboarding_shards",
            "source_onboarding_runs",
            "think_runs",
            "think_trigger_queue",
            "models",
            "model_composition_members",
            "model_belief_addresses",
            "code_embeddings",
        },
        rows={
            "observations_by_source": [
                {
                    "source": "slack",
                    "items_ingested_count": 120,
                    "last_success_at": COLLECTED_AT,
                },
                {
                    "source": "github",
                    "items_ingested_count": 25,
                    "last_success_at": COLLECTED_AT,
                },
                {
                    "source": "https://customer.example/raw",
                    "items_ingested_count": 1,
                    "last_success_at": COLLECTED_AT,
                },
            ],
            "unresolved_ingestion_failures": [
                {
                    "source": "github",
                    "items_failed_count": 2,
                    "auth_failure_count": 1,
                    "latest_failure_at": datetime(2026, 6, 27, 12, 2, tzinfo=UTC),
                }
            ],
            "onboarding_shards_by_source": [
                {
                    "source": "github",
                    "queue_depth_count": 4,
                    "failed_shard_count": 0,
                    "active_shard_count": 1,
                    "observations_seen_count": 30,
                    "last_cursor_advance": COLLECTED_AT,
                }
            ],
            "source_onboarding_runs_by_source": [
                {
                    "source": "slack",
                    "pending_run_count": 0,
                    "active_run_count": 0,
                    "failed_run_count": 0,
                    "latest_completed_at": COLLECTED_AT,
                }
            ],
        },
        row={
            "pipeline_unresolved_failures": {
                "unresolved_count": 2,
                "retry_count": 1,
            },
            "pipeline_discarded_failures": {"dropped_item_count": 1},
            "pipeline_onboarding_queue": {"queue_lag_count": 4},
            "think_runs": {
                "run_count": 12,
                "failed_run_count": 1,
                "latest_run_at": COLLECTED_AT,
            },
            "think_trigger_queue": {"queued_run_count": 3},
            "models": {"model_count": 9},
            "model_composition_members": {
                "model_relation_count": 24,
                "latest_build_at": COLLECTED_AT,
            },
            "model_belief_orphans": {"orphan_model_count": 1},
            "model_belief_latest_update": {"latest_build_at": COLLECTED_AT},
            "observation_vectors": {
                "vector_count": 1000,
                "backlog_count": 3,
                "latest_job_at": COLLECTED_AT,
            },
            "model_vectors": {"vector_count": 9},
            "code_vectors": {"vector_count": 4, "backlog_count": 1},
        },
    )

    payload = await collect_product_health_snapshot(
        db,
        identity=_identity(),
        nonce="nonce-product-health-collector-001",
        collected_at=COLLECTED_AT,
    )
    request = signed_product_health_snapshot(
        payload,
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
    )

    assert validate_product_health_snapshot_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert payload.overall_status == "action_required"
    assert payload.pipeline.dead_letter_count == 2
    assert payload.pipeline.queue_lag_count == 4
    assert payload.think.failed_run_count == 1
    assert payload.models.model_count == 9
    assert payload.models.model_relation_count == 24
    assert payload.models.orphan_model_count == 1
    assert payload.vector_index.vector_count == 1013
    assert payload.vector_index.backlog_count == 4
    assert {source.source for source in payload.sources} == {
        "github",
        "slack",
        "unknown",
    }
    github = next(source for source in payload.sources if source.source == "github")
    assert github.status == "failing"
    assert github.auth_status == "action_required"
    assert github.backfill_status == "running"
    assert {issue.code for issue in payload.issues} >= {
        "source_ingest_failures",
        "source_auth_action_required",
        "pipeline_dead_letters",
        "think_run_failures",
        "model_orphans_detected",
        "vector_backlog_pending",
    }

    rendered = canonical_product_health_snapshot_payload(payload)
    decoded = json.loads(rendered)
    assert decoded["privacy_boundary"]["raw_payloads_included"] is False
    assert "customer.example" not in rendered.decode("utf-8")
    assert b"://" not in rendered
    assert all("content" not in query.lower() for query, _ in db.fetch_calls)
    assert all("error_summary" not in query.lower() for query, _ in db.fetch_calls)
    assert any(args == (TENANT_ID,) for _, args in db.fetch_calls)


@pytest.mark.asyncio
async def test_collector_returns_unknown_snapshot_when_runtime_tables_are_absent() -> None:
    payload = await collect_product_health_snapshot(
        FakeProductHealthDb(tables=set()),
        identity=_identity(tenant_id=None),
        nonce="nonce-product-health-collector-002",
        collected_at=COLLECTED_AT,
    )

    assert payload.overall_status == "unknown"
    assert payload.sources == ()
    assert payload.pipeline.status == "unknown"
    assert payload.think.status == "unknown"
    assert payload.models.status == "unknown"
    assert payload.vector_index.status == "unknown"
    assert payload.issues == ()
    assert payload.stored_scope == "sanitized_product_health_metadata_only"


def _identity(tenant_id: str | None = TENANT_ID) -> ByocProductHealthCollectorIdentity:
    return ByocProductHealthCollectorIdentity(
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        agent_id=AGENT_ID,
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-collector",
        tenant_id=tenant_id,
    )


def _marker(query: str) -> str:
    match = re.search(r"/\* byoc_product_health:([a-z_]+) \*/", query)
    if match is None:
        raise AssertionError(f"missing product-health marker in query: {query}")
    return match.group(1)
