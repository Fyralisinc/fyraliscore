"""Metadata-only BYOC product health contract.

This module owns the aggregate customer-side product-health surface used by
BYOC control-plane reads and the browser-facing control-panel proxy. It accepts
only bounded counters, status codes, timestamps, and low-cardinality product
component metadata; raw customer records, prompts, logs, vectors, model
contents, credentials, URLs, and signed request material are intentionally out
of contract.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.platform.runtime.byoc_control_plane_intake import (
    ByocControlPlaneSignature,
)


_AGENT_ID_RE = re.compile(r"^agt_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_CUSTOMER_ID_RE = re.compile(r"^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_DEPLOYMENT_ID_RE = re.compile(r"^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$")
_SAFE_CODE_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,100}$")
_SNAPSHOT_ID_RE = re.compile(r"^phs_[0-9a-f]{32}$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_FORBIDDEN_VALUE_FRAGMENTS = (
    "://",
    "bearer ",
    "password=",
    "postgresql://",
    "secret=",
    "token=",
)

ProductHealthStoredScope = Literal["sanitized_product_health_metadata_only"]
ProductHealthStatus = Literal["ready", "action_required", "degraded", "unknown"]
ProductHealthSourceStatus = Literal[
    "ready",
    "degraded",
    "failing",
    "disabled",
    "unknown",
]
ProductHealthAuthStatus = Literal[
    "ready",
    "action_required",
    "not_configured",
    "unknown",
]
ProductHealthBackfillStatus = Literal["idle", "running", "blocked", "unknown"]
ProductHealthIssueSeverity = Literal["critical", "warning", "info"]
ProductHealthIssueComponent = Literal[
    "source_ingestion",
    "source_auth",
    "pipeline",
    "think",
    "models",
    "vector_index",
    "runtime",
]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ByocProductHealthViolation(_StrictModel):
    path: str
    code: str
    message: str

    def render(self) -> str:
        return f"{self.path}: {self.code}: {self.message}"


class ByocProductHealthPrivacyBoundary(_StrictModel):
    raw_payloads_included: Literal[False] = False
    raw_prompts_included: Literal[False] = False
    raw_logs_included: Literal[False] = False
    pii_included: Literal[False] = False
    source_records_included: Literal[False] = False
    model_contents_included: Literal[False] = False
    vector_values_included: Literal[False] = False


class ByocProductSourceHealth(_StrictModel):
    source: str
    status: ProductHealthSourceStatus
    auth_status: ProductHealthAuthStatus = "unknown"
    backfill_status: ProductHealthBackfillStatus = "unknown"
    items_ingested_count: int = Field(ge=0)
    items_failed_count: int = Field(ge=0)
    queue_depth_count: int = Field(default=0, ge=0)
    lag_seconds: int | None = Field(default=None, ge=0)
    last_success_at: datetime | None = None

    @field_validator("source")
    @classmethod
    def _source_must_be_safe_code(cls, value: str) -> str:
        return _safe_code(value, "source")


class ByocProductPipelineHealth(_StrictModel):
    status: ProductHealthStatus
    queue_lag_count: int = Field(ge=0)
    dead_letter_count: int = Field(ge=0)
    retry_backlog_count: int = Field(ge=0)
    dropped_item_count: int = Field(ge=0)


class ByocProductThinkHealth(_StrictModel):
    status: ProductHealthStatus
    run_count: int = Field(ge=0)
    failed_run_count: int = Field(ge=0)
    queued_run_count: int = Field(ge=0)
    latest_run_at: datetime | None = None
    breaker_status: Literal["closed", "open", "unknown"] = "unknown"


class ByocProductModelHealth(_StrictModel):
    status: ProductHealthStatus
    model_count: int = Field(ge=0)
    model_build_count: int = Field(ge=0)
    failed_build_count: int = Field(ge=0)
    model_relation_count: int = Field(ge=0)
    orphan_model_count: int = Field(ge=0)
    stale_relation_count: int = Field(ge=0)
    latest_build_at: datetime | None = None
    graph_status: ProductHealthStatus = "unknown"


class ByocProductVectorHealth(_StrictModel):
    status: ProductHealthStatus
    vector_count: int = Field(ge=0)
    backlog_count: int = Field(ge=0)
    failed_job_count: int = Field(ge=0)
    latest_job_at: datetime | None = None
    retrieval_status: ProductHealthStatus = "unknown"


class ByocProductHealthIssue(_StrictModel):
    code: str
    severity: ProductHealthIssueSeverity
    component: ProductHealthIssueComponent
    observed_count: int = Field(default=1, ge=1)
    first_observed_at: datetime | None = None
    latest_observed_at: datetime | None = None

    @field_validator("code")
    @classmethod
    def _code_must_be_safe(cls, value: str) -> str:
        return _safe_code(value, "issue code")


class ByocProductHealthQuery(_StrictModel):
    deployment_id: str
    customer_id: str | None = None

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        return _deployment_id(value)

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        return _customer_id(value)


class ByocProductHealthSnapshotPayload(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health_snapshot.v1"]
    deployment_id: str
    customer_id: str
    agent_id: str
    agent_version: str
    artifact_revision: str
    collected_at: datetime
    nonce: str = Field(min_length=16, max_length=128)
    overall_status: ProductHealthStatus
    sources: tuple[ByocProductSourceHealth, ...] = Field(default=(), max_length=50)
    pipeline: ByocProductPipelineHealth
    think: ByocProductThinkHealth
    models: ByocProductModelHealth
    vector_index: ByocProductVectorHealth
    issues: tuple[ByocProductHealthIssue, ...] = Field(default=(), max_length=50)
    privacy_boundary: ByocProductHealthPrivacyBoundary = Field(
        default_factory=ByocProductHealthPrivacyBoundary
    )
    stored_scope: ProductHealthStoredScope = "sanitized_product_health_metadata_only"

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        return _deployment_id(value)

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        checked = _customer_id(value)
        assert checked is not None
        return checked

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("agent_version", "artifact_revision")
    @classmethod
    def _strings_must_be_bounded(cls, value: str) -> str:
        return _safe_code(value, "product health field")

    @field_validator("nonce")
    @classmethod
    def _nonce_must_be_present(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("nonce must not be empty")
        return value

    @model_validator(mode="after")
    def _issue_status_must_match_critical_issues(
        self,
    ) -> "ByocProductHealthSnapshotPayload":
        if any(issue.severity == "critical" for issue in self.issues):
            if self.overall_status == "ready":
                raise ValueError("critical product-health issues cannot be ready")
        return self


class ByocProductHealthSnapshotRequest(ByocProductHealthSnapshotPayload):
    signature: ByocControlPlaneSignature


class ByocProductHealthReceipt(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health_receipt.v1"]
    status: Literal["accepted"] = "accepted"
    snapshot_id: str
    deployment_id: str
    customer_id: str
    agent_id: str
    snapshot_digest: str
    overall_status: ProductHealthStatus
    source_count: int = Field(ge=0)
    open_issue_count: int = Field(ge=0)
    collected_at: datetime
    accepted_at: datetime
    stored_scope: ProductHealthStoredScope = "sanitized_product_health_metadata_only"

    @field_validator("snapshot_id")
    @classmethod
    def _snapshot_id_shape(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SNAPSHOT_ID_RE.match(value):
            raise ValueError("snapshot_id must look like phs_<32-hex>")
        return value

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        return _deployment_id(value)

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str) -> str:
        checked = _customer_id(value)
        assert checked is not None
        return checked

    @field_validator("agent_id")
    @classmethod
    def _agent_id_shape(cls, value: str) -> str:
        value = value.strip()
        if not _AGENT_ID_RE.match(value):
            raise ValueError("agent_id must look like agt_<stable-id>")
        return value

    @field_validator("snapshot_digest")
    @classmethod
    def _digest_shape(cls, value: str) -> str:
        value = value.strip().lower()
        if not _SHA256_RE.match(value):
            raise ValueError("snapshot_digest must look like sha256:<64-hex>")
        return value


class ByocProductHealth(_StrictModel):
    schema_version: Literal["fyralis.byoc.product_health.v1"]
    deployment_id: str
    customer_id: str | None = None
    generated_at: datetime
    observed: bool
    latest_snapshot_id: str | None = None
    latest_collected_at: datetime | None = None
    overall_status: ProductHealthStatus
    sources: tuple[ByocProductSourceHealth, ...]
    pipeline: ByocProductPipelineHealth
    think: ByocProductThinkHealth
    models: ByocProductModelHealth
    vector_index: ByocProductVectorHealth
    issues: tuple[ByocProductHealthIssue, ...]
    privacy_boundary: ByocProductHealthPrivacyBoundary = Field(
        default_factory=ByocProductHealthPrivacyBoundary
    )
    stored_scope: ProductHealthStoredScope = "sanitized_product_health_metadata_only"

    @field_validator("deployment_id")
    @classmethod
    def _deployment_id_shape(cls, value: str) -> str:
        return _deployment_id(value)

    @field_validator("customer_id")
    @classmethod
    def _customer_id_shape(cls, value: str | None) -> str | None:
        return _customer_id(value)

    @field_validator("latest_snapshot_id")
    @classmethod
    def _snapshot_id_shape(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().lower()
        if not _SNAPSHOT_ID_RE.match(value):
            raise ValueError("latest_snapshot_id must look like phs_<32-hex>")
        return value


class ByocProductHealthIntakeStore(Protocol):
    async def put(
        self,
        request: ByocProductHealthSnapshotRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocProductHealthReceipt:
        """Persist a sanitized product-health snapshot and return its receipt."""
        ...

    async def latest(self, query: ByocProductHealthQuery) -> ByocProductHealth:
        """Return the latest sanitized product-health snapshot for a deployment."""
        ...


class InMemoryByocProductHealthIntakeStore:
    def __init__(self) -> None:
        self._items: dict[str, ByocProductHealth] = {}
        self._receipts: dict[str, ByocProductHealthReceipt] = {}

    async def put(
        self,
        request: ByocProductHealthSnapshotRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocProductHealthReceipt:
        payload = _payload_from_request(request)
        receipt = product_health_receipt(payload, accepted_at=accepted_at)
        health = product_health_from_payload(
            payload,
            latest_snapshot_id=receipt.snapshot_id,
            generated_at=receipt.accepted_at,
        )
        self._items[receipt.snapshot_id] = health
        self._receipts[receipt.snapshot_id] = receipt
        return receipt

    async def latest(self, query: ByocProductHealthQuery) -> ByocProductHealth:
        matches = [
            item
            for item in self._items.values()
            if _health_matches_query(item, query)
        ]
        if not matches:
            return unknown_product_health(query=query)
        return max(
            matches,
            key=lambda item: (
                item.latest_collected_at or datetime.min.replace(tzinfo=UTC),
                item.latest_snapshot_id or "",
            ),
        )


class PostgresByocProductHealthIntakeStore:
    """Postgres-backed sanitized product-health snapshot store.

    The store persists only scalar counters, status codes, timestamps, and safe
    low-cardinality component/source codes. It intentionally does not store raw
    customer records, metric JSON, logs, prompts, vectors, model contents,
    credential material, URLs, signatures, or request bodies.
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def put(
        self,
        request: ByocProductHealthSnapshotRequest,
        *,
        accepted_at: datetime | None = None,
    ) -> ByocProductHealthReceipt:
        payload = _payload_from_request(request)
        receipt = product_health_receipt(payload, accepted_at=accepted_at)
        await self._pool.fetchrow(
            """
            INSERT INTO byoc_product_health_snapshots (
                snapshot_id,
                deployment_id,
                customer_id,
                agent_id,
                agent_version,
                artifact_revision,
                snapshot_digest,
                collected_at,
                accepted_at,
                overall_status,
                pipeline_status,
                queue_lag_count,
                dead_letter_count,
                retry_backlog_count,
                dropped_item_count,
                think_status,
                think_run_count,
                think_failed_run_count,
                think_queued_run_count,
                think_latest_run_at,
                think_breaker_status,
                model_status,
                model_count,
                model_build_count,
                model_failed_build_count,
                model_relation_count,
                orphan_model_count,
                stale_relation_count,
                model_latest_build_at,
                model_graph_status,
                vector_status,
                vector_count,
                vector_backlog_count,
                vector_failed_job_count,
                vector_latest_job_at,
                vector_retrieval_status,
                source_count,
                open_issue_count,
                raw_payloads_included,
                raw_prompts_included,
                raw_logs_included,
                pii_included,
                source_records_included,
                model_contents_included,
                vector_values_included,
                stored_scope
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24,
                $25, $26, $27, $28, $29, $30, $31, $32,
                $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44, $45, $46
            )
            ON CONFLICT (snapshot_id) DO UPDATE
              SET snapshot_id = byoc_product_health_snapshots.snapshot_id
            RETURNING snapshot_id
            """,
            receipt.snapshot_id,
            receipt.deployment_id,
            receipt.customer_id,
            receipt.agent_id,
            payload.agent_version,
            payload.artifact_revision,
            receipt.snapshot_digest,
            receipt.collected_at,
            receipt.accepted_at,
            payload.overall_status,
            payload.pipeline.status,
            payload.pipeline.queue_lag_count,
            payload.pipeline.dead_letter_count,
            payload.pipeline.retry_backlog_count,
            payload.pipeline.dropped_item_count,
            payload.think.status,
            payload.think.run_count,
            payload.think.failed_run_count,
            payload.think.queued_run_count,
            payload.think.latest_run_at,
            payload.think.breaker_status,
            payload.models.status,
            payload.models.model_count,
            payload.models.model_build_count,
            payload.models.failed_build_count,
            payload.models.model_relation_count,
            payload.models.orphan_model_count,
            payload.models.stale_relation_count,
            payload.models.latest_build_at,
            payload.models.graph_status,
            payload.vector_index.status,
            payload.vector_index.vector_count,
            payload.vector_index.backlog_count,
            payload.vector_index.failed_job_count,
            payload.vector_index.latest_job_at,
            payload.vector_index.retrieval_status,
            len(payload.sources),
            len(payload.issues),
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            payload.stored_scope,
        )
        await self._replace_sources(receipt.snapshot_id, payload)
        await self._replace_issues(receipt.snapshot_id, payload)
        return receipt

    async def latest(self, query: ByocProductHealthQuery) -> ByocProductHealth:
        where_clauses = ["deployment_id = $1"]
        args: list[Any] = [query.deployment_id]
        if query.customer_id is not None:
            args.append(query.customer_id)
            where_clauses.append(f"customer_id = ${len(args)}")
        row = await self._pool.fetchrow(
            f"""
            SELECT
                snapshot_id,
                deployment_id,
                customer_id,
                collected_at,
                accepted_at,
                overall_status,
                pipeline_status,
                queue_lag_count,
                dead_letter_count,
                retry_backlog_count,
                dropped_item_count,
                think_status,
                think_run_count,
                think_failed_run_count,
                think_queued_run_count,
                think_latest_run_at,
                think_breaker_status,
                model_status,
                model_count,
                model_build_count,
                model_failed_build_count,
                model_relation_count,
                orphan_model_count,
                stale_relation_count,
                model_latest_build_at,
                model_graph_status,
                vector_status,
                vector_count,
                vector_backlog_count,
                vector_failed_job_count,
                vector_latest_job_at,
                vector_retrieval_status,
                raw_payloads_included,
                raw_prompts_included,
                raw_logs_included,
                pii_included,
                source_records_included,
                model_contents_included,
                vector_values_included,
                stored_scope
            FROM byoc_product_health_snapshots
            WHERE {' AND '.join(where_clauses)}
            ORDER BY collected_at DESC, accepted_at DESC, snapshot_id DESC
            LIMIT 1
            """,
            *args,
        )
        if row is None:
            return unknown_product_health(query=query)
        sources = await self._sources_for(row["snapshot_id"])
        issues = await self._issues_for(row["snapshot_id"])
        return _health_from_row(row, sources=sources, issues=issues)

    async def _replace_sources(
        self,
        snapshot_id: str,
        payload: ByocProductHealthSnapshotPayload,
    ) -> None:
        await self._pool.fetchrow(
            "DELETE FROM byoc_product_health_sources WHERE snapshot_id = $1",
            snapshot_id,
        )
        for source in payload.sources:
            await self._pool.fetchrow(
                """
                INSERT INTO byoc_product_health_sources (
                    snapshot_id,
                    deployment_id,
                    customer_id,
                    source_name,
                    status,
                    auth_status,
                    backfill_status,
                    items_ingested_count,
                    items_failed_count,
                    queue_depth_count,
                    lag_seconds,
                    last_success_at,
                    stored_scope
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10, $11, $12, $13
                )
                ON CONFLICT (snapshot_id, source_name) DO UPDATE SET
                    status = EXCLUDED.status,
                    auth_status = EXCLUDED.auth_status,
                    backfill_status = EXCLUDED.backfill_status,
                    items_ingested_count = EXCLUDED.items_ingested_count,
                    items_failed_count = EXCLUDED.items_failed_count,
                    queue_depth_count = EXCLUDED.queue_depth_count,
                    lag_seconds = EXCLUDED.lag_seconds,
                    last_success_at = EXCLUDED.last_success_at,
                    stored_scope = EXCLUDED.stored_scope
                RETURNING snapshot_id
                """,
                snapshot_id,
                payload.deployment_id,
                payload.customer_id,
                source.source,
                source.status,
                source.auth_status,
                source.backfill_status,
                source.items_ingested_count,
                source.items_failed_count,
                source.queue_depth_count,
                source.lag_seconds,
                source.last_success_at,
                payload.stored_scope,
            )

    async def _replace_issues(
        self,
        snapshot_id: str,
        payload: ByocProductHealthSnapshotPayload,
    ) -> None:
        await self._pool.fetchrow(
            "DELETE FROM byoc_product_health_issues WHERE snapshot_id = $1",
            snapshot_id,
        )
        for issue in payload.issues:
            await self._pool.fetchrow(
                """
                INSERT INTO byoc_product_health_issues (
                    snapshot_id,
                    deployment_id,
                    customer_id,
                    issue_code,
                    severity,
                    component,
                    observed_count,
                    first_observed_at,
                    latest_observed_at,
                    stored_scope
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8,
                    $9, $10
                )
                ON CONFLICT (snapshot_id, issue_code, component) DO UPDATE SET
                    severity = EXCLUDED.severity,
                    observed_count = EXCLUDED.observed_count,
                    first_observed_at = EXCLUDED.first_observed_at,
                    latest_observed_at = EXCLUDED.latest_observed_at,
                    stored_scope = EXCLUDED.stored_scope
                RETURNING snapshot_id
                """,
                snapshot_id,
                payload.deployment_id,
                payload.customer_id,
                issue.code,
                issue.severity,
                issue.component,
                issue.observed_count,
                issue.first_observed_at,
                issue.latest_observed_at,
                payload.stored_scope,
            )

    async def _sources_for(self, snapshot_id: str) -> tuple[ByocProductSourceHealth, ...]:
        rows = await self._pool.fetch(
            """
            SELECT
                source_name,
                status,
                auth_status,
                backfill_status,
                items_ingested_count,
                items_failed_count,
                queue_depth_count,
                lag_seconds,
                last_success_at
            FROM byoc_product_health_sources
            WHERE snapshot_id = $1
            ORDER BY source_name ASC
            """,
            snapshot_id,
        )
        return tuple(
            ByocProductSourceHealth(
                source=row["source_name"],
                status=row["status"],
                auth_status=row["auth_status"],
                backfill_status=row["backfill_status"],
                items_ingested_count=row["items_ingested_count"],
                items_failed_count=row["items_failed_count"],
                queue_depth_count=row["queue_depth_count"],
                lag_seconds=row["lag_seconds"],
                last_success_at=row["last_success_at"],
            )
            for row in rows
        )

    async def _issues_for(self, snapshot_id: str) -> tuple[ByocProductHealthIssue, ...]:
        rows = await self._pool.fetch(
            """
            SELECT
                issue_code,
                severity,
                component,
                observed_count,
                first_observed_at,
                latest_observed_at
            FROM byoc_product_health_issues
            WHERE snapshot_id = $1
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 0
                    WHEN 'warning' THEN 1
                    ELSE 2
                END,
                issue_code ASC
            """,
            snapshot_id,
        )
        return tuple(
            ByocProductHealthIssue(
                code=row["issue_code"],
                severity=row["severity"],
                component=row["component"],
                observed_count=row["observed_count"],
                first_observed_at=row["first_observed_at"],
                latest_observed_at=row["latest_observed_at"],
            )
            for row in rows
        )


def product_health_snapshot_payload(
    *,
    deployment_id: str,
    customer_id: str,
    agent_id: str,
    agent_version: str,
    artifact_revision: str,
    overall_status: ProductHealthStatus,
    pipeline: ByocProductPipelineHealth,
    think: ByocProductThinkHealth,
    models: ByocProductModelHealth,
    vector_index: ByocProductVectorHealth,
    nonce: str,
    collected_at: datetime | None = None,
    sources: tuple[ByocProductSourceHealth, ...] = (),
    issues: tuple[ByocProductHealthIssue, ...] = (),
) -> ByocProductHealthSnapshotPayload:
    return ByocProductHealthSnapshotPayload(
        schema_version="fyralis.byoc.product_health_snapshot.v1",
        deployment_id=deployment_id,
        customer_id=customer_id,
        agent_id=agent_id,
        agent_version=agent_version,
        artifact_revision=artifact_revision,
        collected_at=collected_at or datetime.now(UTC),
        nonce=nonce,
        overall_status=overall_status,
        sources=sources,
        pipeline=pipeline,
        think=think,
        models=models,
        vector_index=vector_index,
        issues=issues,
        privacy_boundary=ByocProductHealthPrivacyBoundary(),
        stored_scope="sanitized_product_health_metadata_only",
    )


def signed_product_health_snapshot(
    payload: ByocProductHealthSnapshotPayload,
    *,
    signing_secret: str,
    key_ref: str,
) -> ByocProductHealthSnapshotRequest:
    if not signing_secret:
        raise ValueError("signing_secret must not be empty")
    signature = ByocControlPlaneSignature(
        key_ref=key_ref,
        value=_hmac_sha256(
            canonical_product_health_snapshot_payload(payload),
            signing_secret,
        ),
    )
    return ByocProductHealthSnapshotRequest(
        **payload.model_dump(),
        signature=signature,
    )


def canonical_product_health_snapshot_payload(
    payload: ByocProductHealthSnapshotPayload,
) -> bytes:
    data = payload.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")


def validate_product_health_snapshot_submission(
    request: ByocProductHealthSnapshotRequest,
    *,
    signing_secret: str,
    expected_key_ref: str | None = None,
) -> list[ByocProductHealthViolation]:
    violations: list[ByocProductHealthViolation] = []
    if not signing_secret:
        return [
            _violation("signature", "missing_signing_secret", "signing secret is empty")
        ]

    if expected_key_ref is not None and request.signature.key_ref != expected_key_ref:
        violations.append(
            _violation(
                "signature.key_ref",
                "signature_key_ref_mismatch",
                "signature key_ref does not match expected intake key",
            )
        )

    payload = _payload_from_request(request)
    expected = _hmac_sha256(
        canonical_product_health_snapshot_payload(payload),
        signing_secret,
    )
    if not hmac.compare_digest(expected, request.signature.value):
        violations.append(
            _violation("signature.value", "invalid_signature", "invalid signature")
        )
    violations.extend(_privacy_boundary_violations(payload))
    return violations


def product_health_receipt(
    payload: ByocProductHealthSnapshotPayload,
    *,
    accepted_at: datetime | None = None,
) -> ByocProductHealthReceipt:
    accepted = accepted_at or datetime.now(UTC)
    digest = digest_product_health_snapshot(payload)
    snapshot_id = "phs_" + digest.removeprefix("sha256:")[:32]
    return ByocProductHealthReceipt(
        schema_version="fyralis.byoc.product_health_receipt.v1",
        status="accepted",
        snapshot_id=snapshot_id,
        deployment_id=payload.deployment_id,
        customer_id=payload.customer_id,
        agent_id=payload.agent_id,
        snapshot_digest=digest,
        overall_status=payload.overall_status,
        source_count=len(payload.sources),
        open_issue_count=len(payload.issues),
        collected_at=payload.collected_at,
        accepted_at=accepted,
        stored_scope="sanitized_product_health_metadata_only",
    )


def digest_product_health_snapshot(payload: ByocProductHealthSnapshotPayload) -> str:
    return "sha256:" + hashlib.sha256(
        canonical_product_health_snapshot_payload(payload)
    ).hexdigest()


def product_health_from_payload(
    payload: ByocProductHealthSnapshotPayload,
    *,
    latest_snapshot_id: str,
    generated_at: datetime | None = None,
) -> ByocProductHealth:
    return ByocProductHealth(
        schema_version="fyralis.byoc.product_health.v1",
        deployment_id=payload.deployment_id,
        customer_id=payload.customer_id,
        generated_at=generated_at or datetime.now(UTC),
        observed=True,
        latest_snapshot_id=latest_snapshot_id,
        latest_collected_at=payload.collected_at,
        overall_status=payload.overall_status,
        sources=payload.sources,
        pipeline=payload.pipeline,
        think=payload.think,
        models=payload.models,
        vector_index=payload.vector_index,
        issues=payload.issues,
        privacy_boundary=payload.privacy_boundary,
        stored_scope="sanitized_product_health_metadata_only",
    )


def unknown_product_health(
    *,
    query: ByocProductHealthQuery,
    generated_at: datetime | None = None,
) -> ByocProductHealth:
    return ByocProductHealth(
        schema_version="fyralis.byoc.product_health.v1",
        deployment_id=query.deployment_id,
        customer_id=query.customer_id,
        generated_at=generated_at or datetime.now(UTC),
        observed=False,
        latest_snapshot_id=None,
        latest_collected_at=None,
        overall_status="unknown",
        sources=(),
        pipeline=ByocProductPipelineHealth(
            status="unknown",
            queue_lag_count=0,
            dead_letter_count=0,
            retry_backlog_count=0,
            dropped_item_count=0,
        ),
        think=ByocProductThinkHealth(
            status="unknown",
            run_count=0,
            failed_run_count=0,
            queued_run_count=0,
            latest_run_at=None,
            breaker_status="unknown",
        ),
        models=ByocProductModelHealth(
            status="unknown",
            model_count=0,
            model_build_count=0,
            failed_build_count=0,
            model_relation_count=0,
            orphan_model_count=0,
            stale_relation_count=0,
            latest_build_at=None,
            graph_status="unknown",
        ),
        vector_index=ByocProductVectorHealth(
            status="unknown",
            vector_count=0,
            backlog_count=0,
            failed_job_count=0,
            latest_job_at=None,
            retrieval_status="unknown",
        ),
        issues=(),
        privacy_boundary=ByocProductHealthPrivacyBoundary(),
        stored_scope="sanitized_product_health_metadata_only",
    )


def model_json_schema_bundle() -> dict[str, Any]:
    return {
        "query": ByocProductHealthQuery.model_json_schema(),
        "snapshot_payload": ByocProductHealthSnapshotPayload.model_json_schema(),
        "snapshot_request": ByocProductHealthSnapshotRequest.model_json_schema(),
        "receipt": ByocProductHealthReceipt.model_json_schema(),
        "product_health": ByocProductHealth.model_json_schema(),
        "stored_scope": "sanitized_product_health_metadata_only",
    }


def _payload_from_request(
    request: ByocProductHealthSnapshotRequest,
) -> ByocProductHealthSnapshotPayload:
    data = request.model_dump(exclude={"signature"})
    return ByocProductHealthSnapshotPayload.model_validate(data)


def _health_from_row(
    row: Any,
    *,
    sources: tuple[ByocProductSourceHealth, ...],
    issues: tuple[ByocProductHealthIssue, ...],
) -> ByocProductHealth:
    return ByocProductHealth(
        schema_version="fyralis.byoc.product_health.v1",
        deployment_id=row["deployment_id"],
        customer_id=row["customer_id"],
        generated_at=datetime.now(UTC),
        observed=True,
        latest_snapshot_id=row["snapshot_id"],
        latest_collected_at=row["collected_at"],
        overall_status=row["overall_status"],
        sources=sources,
        pipeline=ByocProductPipelineHealth(
            status=row["pipeline_status"],
            queue_lag_count=row["queue_lag_count"],
            dead_letter_count=row["dead_letter_count"],
            retry_backlog_count=row["retry_backlog_count"],
            dropped_item_count=row["dropped_item_count"],
        ),
        think=ByocProductThinkHealth(
            status=row["think_status"],
            run_count=row["think_run_count"],
            failed_run_count=row["think_failed_run_count"],
            queued_run_count=row["think_queued_run_count"],
            latest_run_at=row["think_latest_run_at"],
            breaker_status=row["think_breaker_status"],
        ),
        models=ByocProductModelHealth(
            status=row["model_status"],
            model_count=row["model_count"],
            model_build_count=row["model_build_count"],
            failed_build_count=row["model_failed_build_count"],
            model_relation_count=row["model_relation_count"],
            orphan_model_count=row["orphan_model_count"],
            stale_relation_count=row["stale_relation_count"],
            latest_build_at=row["model_latest_build_at"],
            graph_status=row["model_graph_status"],
        ),
        vector_index=ByocProductVectorHealth(
            status=row["vector_status"],
            vector_count=row["vector_count"],
            backlog_count=row["vector_backlog_count"],
            failed_job_count=row["vector_failed_job_count"],
            latest_job_at=row["vector_latest_job_at"],
            retrieval_status=row["vector_retrieval_status"],
        ),
        issues=issues,
        privacy_boundary=ByocProductHealthPrivacyBoundary(
            raw_payloads_included=row["raw_payloads_included"],
            raw_prompts_included=row["raw_prompts_included"],
            raw_logs_included=row["raw_logs_included"],
            pii_included=row["pii_included"],
            source_records_included=row["source_records_included"],
            model_contents_included=row["model_contents_included"],
            vector_values_included=row["vector_values_included"],
        ),
        stored_scope=row["stored_scope"],
    )


def _health_matches_query(
    health: ByocProductHealth,
    query: ByocProductHealthQuery,
) -> bool:
    if health.deployment_id != query.deployment_id:
        return False
    return query.customer_id is None or health.customer_id == query.customer_id


def _deployment_id(value: str) -> str:
    value = value.strip()
    if not _DEPLOYMENT_ID_RE.match(value):
        raise ValueError("deployment_id must look like dep_<stable-id>")
    return value


def _customer_id(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not _CUSTOMER_ID_RE.match(value):
        raise ValueError("customer_id must look like cus_<stable-id>")
    return value


def _safe_code(value: str, label: str) -> str:
    value = value.strip()
    if not value or not _SAFE_CODE_RE.match(value):
        raise ValueError(f"{label} must be a bounded identifier")
    return value


def _privacy_boundary_violations(
    payload: ByocProductHealthSnapshotPayload,
) -> list[ByocProductHealthViolation]:
    violations: list[ByocProductHealthViolation] = []
    _scan_value(payload.model_dump(mode="json"), path="<root>", violations=violations)
    return violations


def _scan_value(
    value: Any,
    *,
    path: str,
    violations: list[ByocProductHealthViolation],
) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            _scan_value(item, path=f"{path}.{key}", violations=violations)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _scan_value(item, path=f"{path}[{index}]", violations=violations)
        return
    if not isinstance(value, str):
        return
    lowered = value.lower()
    if any(fragment in lowered for fragment in _FORBIDDEN_VALUE_FRAGMENTS):
        violations.append(
            _violation(
                path,
                "customer_data_marker_forbidden",
                "snapshot contains a raw data, URL, credential, or token marker",
            )
        )


def _hmac_sha256(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _violation(
    path: str,
    code: str,
    message: str,
) -> ByocProductHealthViolation:
    return ByocProductHealthViolation(path=path, code=code, message=message)


__all__ = [
    "ByocProductHealth",
    "ByocProductHealthIntakeStore",
    "ByocProductHealthIssue",
    "ByocProductHealthPrivacyBoundary",
    "ByocProductHealthQuery",
    "ByocProductHealthReceipt",
    "ByocProductHealthSnapshotPayload",
    "ByocProductHealthSnapshotRequest",
    "ByocProductHealthViolation",
    "ByocProductModelHealth",
    "ByocProductPipelineHealth",
    "ByocProductSourceHealth",
    "ByocProductThinkHealth",
    "ByocProductVectorHealth",
    "InMemoryByocProductHealthIntakeStore",
    "PostgresByocProductHealthIntakeStore",
    "ProductHealthAuthStatus",
    "ProductHealthBackfillStatus",
    "ProductHealthIssueComponent",
    "ProductHealthIssueSeverity",
    "ProductHealthSourceStatus",
    "ProductHealthStatus",
    "ProductHealthStoredScope",
    "canonical_product_health_snapshot_payload",
    "digest_product_health_snapshot",
    "model_json_schema_bundle",
    "product_health_from_payload",
    "product_health_receipt",
    "product_health_snapshot_payload",
    "signed_product_health_snapshot",
    "unknown_product_health",
    "validate_product_health_snapshot_submission",
]
