"""Typed assurance artifact for end-to-end correction propagation."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.correction_propagation import CorrectionPropagationAudit


class _AssuranceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CorrectionRuntimeEvidence(_AssuranceModel):
    expected_dependency_refs: tuple[str, ...] = ()
    discovered_dependency_refs: tuple[str, ...] = ()
    expected_immediate_fence_refs: tuple[str, ...] = ()
    immediate_fence_refs: tuple[str, ...] = ()
    expected_direct_repair_refs: tuple[str, ...] = ()
    direct_repair_refs: tuple[str, ...] = ()
    expected_recursive_repair_refs: tuple[str, ...] = ()
    recursive_repair_refs: tuple[str, ...] = ()
    expected_relation_retirement_refs: tuple[str, ...] = ()
    relation_retirement_refs: tuple[str, ...] = ()
    expected_projection_invalidation_refs: tuple[str, ...] = ()
    projection_invalidation_refs: tuple[str, ...] = ()
    expected_projection_rebuild_refs: tuple[str, ...] = ()
    projection_rebuild_refs: tuple[str, ...] = ()
    residual_unsafe_refs: tuple[str, ...] = ()
    replay_new_work_refs: tuple[str, ...] = ()
    source_before_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_after_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    cross_tenant_change_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def collections_are_unique(self) -> Self:
        for name in (
            "expected_dependency_refs",
            "discovered_dependency_refs",
            "expected_immediate_fence_refs",
            "immediate_fence_refs",
            "expected_direct_repair_refs",
            "direct_repair_refs",
            "expected_recursive_repair_refs",
            "recursive_repair_refs",
            "expected_relation_retirement_refs",
            "relation_retirement_refs",
            "expected_projection_invalidation_refs",
            "projection_invalidation_refs",
            "expected_projection_rebuild_refs",
            "projection_rebuild_refs",
            "residual_unsafe_refs",
            "replay_new_work_refs",
            "cross_tenant_change_refs",
        ):
            values = getattr(self, name)
            if len(values) != len(set(values)):
                raise ValueError(f"{name} must contain unique references")
        return self


class CorrectionAssuranceMetrics(_AssuranceModel):
    expected_dependency_count: int = Field(ge=0)
    discovered_dependency_count: int = Field(ge=0)
    dependency_discovery_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    immediate_fence_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    direct_repair_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    recursive_repair_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    relation_retirement_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    projection_invalidation_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
    )
    projection_rebuild_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    residual_unsafe_debt_count: int = Field(ge=0)
    convergence_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    replay_idempotent: bool
    source_immutable: bool
    tenant_isolated: bool
    converged: bool


class CorrectionAssuranceArtifact(_AssuranceModel):
    schema_version: Literal["correction-assurance-v1"] = "correction-assurance-v1"
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: datetime
    status: Literal["working", "failed", "incomplete"]
    runtime_evidence: CorrectionRuntimeEvidence
    audit: CorrectionPropagationAudit | None = None
    metrics: CorrectionAssuranceMetrics
    incidents: tuple[str, ...]
    proof_gaps: tuple[str, ...]
    component_digests: dict[str, str]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict:
        return {
            **self.model_dump(mode="json"),
            "artifact_digest": self.digest,
        }


def build_correction_assurance(
    *,
    run_id: str,
    system_version: str,
    created_at: datetime,
    runtime_evidence: CorrectionRuntimeEvidence,
    audit: CorrectionPropagationAudit | None = None,
    artifact_refs: tuple[str, ...],
) -> CorrectionAssuranceArtifact:
    expected_dependencies = set(runtime_evidence.expected_dependency_refs)
    discovered_dependencies = set(runtime_evidence.discovered_dependency_refs)
    if audit is not None:
        discovered_dependencies.update(
            dependency.object_ref for dependency in audit.dependencies
        )
    residual_unsafe = set(runtime_evidence.residual_unsafe_refs)
    if audit is not None:
        residual_unsafe.update(
            dependency.object_ref
            for dependency in audit.dependencies
            if dependency.unsafe_readable
        )

    rates = (
        _coverage(
            runtime_evidence.expected_immediate_fence_refs,
            runtime_evidence.immediate_fence_refs,
        ),
        _coverage(
            runtime_evidence.expected_direct_repair_refs,
            runtime_evidence.direct_repair_refs,
        ),
        _coverage(
            runtime_evidence.expected_recursive_repair_refs,
            runtime_evidence.recursive_repair_refs,
        ),
        _coverage(
            runtime_evidence.expected_relation_retirement_refs,
            runtime_evidence.relation_retirement_refs,
        ),
        _coverage(
            runtime_evidence.expected_projection_invalidation_refs,
            runtime_evidence.projection_invalidation_refs,
        ),
        _coverage(
            runtime_evidence.expected_projection_rebuild_refs,
            runtime_evidence.projection_rebuild_refs,
        ),
    )
    measured_rates = tuple(rate for rate in rates if rate is not None)
    source_immutable = (
        runtime_evidence.source_before_digest
        == runtime_evidence.source_after_digest
        and (audit is None or audit.source_immutable is not False)
    )
    tenant_isolated = (
        not runtime_evidence.cross_tenant_change_refs
        and (
            audit is None
            or (
                audit.cross_tenant_reference_count == 0
                and audit.cross_tenant_change_count == 0
            )
        )
    )
    replay_idempotent = not runtime_evidence.replay_new_work_refs
    dependency_discovery_rate = _coverage(
        tuple(expected_dependencies),
        tuple(discovered_dependencies),
    )
    convergence_components = tuple(
        rate
        for rate in (dependency_discovery_rate, *measured_rates)
        if rate is not None
    )
    convergence_ratio = (
        sum(convergence_components) / len(convergence_components)
        if convergence_components
        else None
    )
    complete = (
        bool(expected_dependencies)
        and dependency_discovery_rate == 1.0
        and bool(measured_rates)
        and all(rate == 1.0 for rate in measured_rates)
    )
    converged = (
        complete
        and not residual_unsafe
        and source_immutable
        and tenant_isolated
        and replay_idempotent
        and (audit is None or audit.residual_repair_debt_count == 0)
    )

    incidents: set[str] = set()
    if residual_unsafe:
        incidents.add("residual_unsafe_correction_debt")
    if not replay_idempotent:
        incidents.add("correction_replay_created_new_work")
    if not source_immutable:
        incidents.add("source_observation_mutated")
    if not tenant_isolated:
        incidents.add("tenant_isolation_violation")
    if dependency_discovery_rate not in {None, 1.0}:
        incidents.add("expected_correction_dependency_not_discovered")
    for name, rate in zip(
        (
            "immediate_fence",
            "direct_repair",
            "recursive_repair",
            "relation_retirement",
            "projection_invalidation",
            "projection_rebuild",
        ),
        rates,
        strict=True,
    ):
        if rate not in {None, 1.0}:
            incidents.add(f"incomplete_{name}")

    proof_gaps: set[str] = {
        "This artifact proves only the sealed correction scenario and exact "
        "dependency expectations supplied by its runtime evidence."
    }
    if audit is None:
        proof_gaps.add(
            "No post-correction read-only dependency census was attached."
        )
    else:
        proof_gaps.update(
            uncertainty
            for uncertainty in audit.uncertainty
            if not uncertainty.startswith(
                "This is an audit-only dependency census"
            )
        )
    if not expected_dependencies:
        proof_gaps.add("Expected correction dependencies were not sealed.")
    if not measured_rates:
        proof_gaps.add("No repair obligations were sealed for measurement.")

    metrics = CorrectionAssuranceMetrics(
        expected_dependency_count=len(expected_dependencies),
        discovered_dependency_count=len(discovered_dependencies),
        dependency_discovery_rate=dependency_discovery_rate,
        immediate_fence_rate=rates[0],
        direct_repair_rate=rates[1],
        recursive_repair_rate=rates[2],
        relation_retirement_rate=rates[3],
        projection_invalidation_rate=rates[4],
        projection_rebuild_rate=rates[5],
        residual_unsafe_debt_count=len(residual_unsafe),
        convergence_ratio=convergence_ratio,
        replay_idempotent=replay_idempotent,
        source_immutable=source_immutable,
        tenant_isolated=tenant_isolated,
        converged=converged,
    )
    return CorrectionAssuranceArtifact(
        run_id=run_id,
        system_version=system_version,
        created_at=created_at,
        status=(
            "working"
            if converged
            else "failed"
            if incidents
            else "incomplete"
        ),
        runtime_evidence=runtime_evidence,
        audit=audit,
        metrics=metrics,
        incidents=tuple(sorted(incidents)),
        proof_gaps=tuple(sorted(proof_gaps)),
        component_digests={
            "evidence": canonical_sha256(
                runtime_evidence.model_dump(mode="json")
            ),
            **(
                {
                    "audit": canonical_sha256(
                        audit.model_dump(mode="json")
                    )
                }
                if audit is not None
                else {}
            ),
        },
        artifact_refs=artifact_refs,
    )


def render_correction_assurance_markdown(
    artifact: CorrectionAssuranceArtifact,
) -> str:
    metrics = artifact.metrics
    lines = [
        f"# Correction assurance: {artifact.run_id}",
        "",
        f"- Status: **{artifact.status}**",
        f"- Converged: **{'yes' if metrics.converged else 'no'}**",
        (
            "- Dependency discovery: "
            f"**{metrics.discovered_dependency_count}/"
            f"{metrics.expected_dependency_count}**"
        ),
        f"- Immediate fence rate: **{_fmt(metrics.immediate_fence_rate)}**",
        f"- Direct repair rate: **{_fmt(metrics.direct_repair_rate)}**",
        f"- Recursive repair rate: **{_fmt(metrics.recursive_repair_rate)}**",
        (
            "- Relation retirement rate: "
            f"**{_fmt(metrics.relation_retirement_rate)}**"
        ),
        (
            "- Projection invalidation rate: "
            f"**{_fmt(metrics.projection_invalidation_rate)}**"
        ),
        (
            "- Projection rebuild rate: "
            f"**{_fmt(metrics.projection_rebuild_rate)}**"
        ),
        (
            "- Residual unsafe debt: "
            f"**{metrics.residual_unsafe_debt_count}**"
        ),
        f"- Replay idempotent: **{'yes' if metrics.replay_idempotent else 'no'}**",
        f"- Source immutable: **{'yes' if metrics.source_immutable else 'no'}**",
        f"- Tenant isolated: **{'yes' if metrics.tenant_isolated else 'no'}**",
        "",
        "## Incidents",
        "",
        *(f"- {item}" for item in artifact.incidents or ("none",)),
        "",
        "## Proof boundary",
        "",
        *(f"- {item}" for item in artifact.proof_gaps),
    ]
    return "\n".join(lines).rstrip() + "\n"


def validate_correction_assurance_artifact(
    payload: dict,
) -> CorrectionAssuranceArtifact:
    supplied_digest = str(payload.get("artifact_digest") or "")
    artifact = CorrectionAssuranceArtifact.model_validate(
        {
            key: value
            for key, value in payload.items()
            if key != "artifact_digest"
        }
    )
    if supplied_digest != artifact.digest:
        raise ValueError("correction assurance artifact digest mismatch")
    return artifact


def _coverage(expected: tuple[str, ...], observed: tuple[str, ...]) -> float | None:
    sealed = set(expected)
    if not sealed:
        return None
    return len(sealed.intersection(observed)) / len(sealed)


def _fmt(value: float | None) -> str:
    return "not measured" if value is None else f"{value:.3f}"


__all__ = [
    "CorrectionAssuranceArtifact",
    "CorrectionAssuranceMetrics",
    "CorrectionRuntimeEvidence",
    "build_correction_assurance",
    "render_correction_assurance_markdown",
    "validate_correction_assurance_artifact",
]
