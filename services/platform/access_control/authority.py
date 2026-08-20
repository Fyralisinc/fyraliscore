"""Authority-plane primitives for human-facing reads.

This module is the small public vocabulary behind:

    principal + purpose + object/provenance -> authorized view

It deliberately wraps the existing `can_read` checks instead of replacing them.
The current role/object policy remains the base gate; labels, provenance, and
delegated grants make that gate more precise for derived state and caches.
"""
from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Optional, Sequence
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7

from .checks import AccessDecision, can_read_by_id
from .roles import roles_for_actor


Purpose = Literal[
    "ask",
    "today",
    "model_trace",
    "debug",
    "export",
    "realtime",
    "extension",
    "internal_reasoning",
]

ObjectKind = Literal[
    "observation",
    "commitment",
    "goal",
    "decision",
    "resource",
    "model",
    "projection",
    "cache",
    "evidence",
    "export",
]

CORE_ENTITY_KINDS: frozenset[str] = frozenset(
    ("observation", "commitment", "goal", "decision", "resource", "model")
)

DERIVED_OBJECT_KINDS: frozenset[str] = frozenset(
    ("projection", "cache", "evidence", "export")
)

GrantKind = Literal["object", "label", "scope"]

DEFAULT_POLICY_VERSION = "read-authority-v1"

_READ_TABLES: dict[str, str] = {
    "observation": "observations",
    "commitment": "commitments",
    "goal": "goals",
    "decision": "decisions",
    "resource": "resources",
    "model": "models",
}

_PUBLIC_LABELS: frozenset[str] = frozenset(
    ("public", "classification:public", "internal", "classification:internal")
)

_GRANT_ONLY_LABELS: frozenset[str] = frozenset(
    (
        "restricted",
        "classification:restricted",
        "confidential",
        "classification:confidential",
        "hr",
        "domain:hr",
        "channel:hr",
        "board",
        "executive",
        "executive_only",
    )
)

_LABEL_ROLE_REQUIREMENTS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("financial", "domain:financial", "resource_kind:financial"), ("finance", "leadership")),
    (("legal", "domain:legal", "channel:legal"), ("legal", "leadership")),
    (("ip", "domain:ip", "resource_kind:ip"), ("legal", "leadership")),
    (("regulatory", "domain:regulatory", "resource_kind:regulatory"), ("legal", "leadership")),
    (("infrastructure", "domain:infrastructure", "resource_kind:infrastructure"), ("leadership",)),
)

_RESOURCE_KIND_LABELS: dict[str, tuple[str, ...]] = {
    "financial": ("resource_kind:financial", "domain:financial"),
    "ip": ("resource_kind:ip", "domain:ip"),
    "regulatory": ("resource_kind:regulatory", "domain:regulatory"),
    "infrastructure": ("resource_kind:infrastructure", "domain:infrastructure"),
    "capacity": ("resource_kind:capacity", "domain:capacity"),
    "relational": ("resource_kind:relational", "domain:customer"),
}

_OBSERVATION_CHANNEL_LABELS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (
        ("finance", "ramp", "mercury", "brex", "quickbooks", "stripe", "bank", "ledger"),
        ("domain:financial", "channel:finance"),
    ),
    (
        ("legal", "docusign", "ironclad"),
        ("domain:legal", "channel:legal"),
    ),
    (
        ("aws", "grafana", "sentry", "pagerduty", "incident", "security"),
        ("domain:infrastructure", "channel:incident"),
    ),
)


@dataclass(frozen=True)
class Principal:
    tenant_id: UUID
    actor_id: UUID
    roles: tuple[str, ...] = ()
    active_grant_epoch: int = 0


@dataclass(frozen=True)
class ObjectRef:
    tenant_id: UUID
    object_kind: str
    object_id: UUID


@dataclass(frozen=True)
class AccessLabel:
    tenant_id: UUID
    object_kind: str
    object_id: UUID
    label: str
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProvenanceEdge:
    tenant_id: UUID
    derived: ObjectRef
    source: ObjectRef
    derivation_kind: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AuthorityDecision:
    allowed: bool
    reason: str
    labels_considered: tuple[str, ...] = ()
    provenance_considered: tuple[ObjectRef, ...] = ()
    override_applied: bool = False
    delegation_applied: bool = False
    audit_required: bool = False
    base_reason: Optional[str] = None

    def __bool__(self) -> bool:
        return self.allowed


@dataclass(frozen=True)
class AuthorityFingerprint:
    tenant_id: UUID
    actor_id: UUID
    purpose: str
    role_set_hash: str
    active_grant_epoch: int
    scope_hash: str
    policy_version: str = DEFAULT_POLICY_VERSION

    @property
    def cache_key(self) -> str:
        payload = {
            "tenant_id": str(self.tenant_id),
            "actor_id": str(self.actor_id),
            "purpose": self.purpose,
            "role_set_hash": self.role_set_hash,
            "active_grant_epoch": self.active_grant_epoch,
            "scope_hash": self.scope_hash,
            "policy_version": self.policy_version,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class AuthorityGrantError(CompanyOSError):
    default_code = "authority_grant_error"


async def principal_for_actor(
    actor_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> Principal:
    """Load the compact principal snapshot used by cache fingerprints."""
    roles = await roles_for_actor(actor_id, conn=conn, tenant_id=tenant_id)
    role_names = tuple(sorted({str(r["role"]) for r in roles}))
    return Principal(
        tenant_id=tenant_id,
        actor_id=actor_id,
        roles=role_names,
        active_grant_epoch=await current_grant_epoch(
            conn=conn, tenant_id=tenant_id
        ),
    )


async def current_grant_epoch(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> int:
    epoch = await conn.fetchval(
        """
        SELECT epoch
        FROM access_grant_epochs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return int(epoch or 0)


def authority_fingerprint(
    principal: Principal,
    purpose: Purpose | str,
    *,
    scope: dict[str, Any] | None = None,
    policy_version: str = DEFAULT_POLICY_VERSION,
) -> AuthorityFingerprint:
    return AuthorityFingerprint(
        tenant_id=principal.tenant_id,
        actor_id=principal.actor_id,
        purpose=str(purpose),
        role_set_hash=_hash_payload(sorted(set(principal.roles))),
        active_grant_epoch=principal.active_grant_epoch,
        scope_hash=_hash_payload(scope or {}),
        policy_version=policy_version,
    )


async def authorize_read(
    principal: Principal,
    purpose: Purpose,
    object_ref: ObjectRef,
    *,
    conn: asyncpg.Connection,
) -> AuthorityDecision:
    return await _authorize_read(
        principal,
        purpose,
        object_ref,
        conn=conn,
        seen=frozenset(),
        depth=0,
    )


def authorized_reader(
    principal: Principal,
    purpose: Purpose,
    *,
    conn: asyncpg.Connection,
) -> "AuthorizedReader":
    return AuthorizedReader(principal=principal, purpose=purpose, conn=conn)


async def record_access_label(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    object_kind: str,
    object_id: UUID,
    label: str,
    source: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO object_access_labels (
            tenant_id, object_kind, object_id, label, source, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        ON CONFLICT (tenant_id, object_kind, object_id, label, source)
        DO UPDATE SET metadata = EXCLUDED.metadata
        """,
        tenant_id,
        object_kind,
        object_id,
        label,
        source,
        json.dumps(metadata or {}),
    )


def labels_for_resource_kind(resource_kind: str) -> tuple[str, ...]:
    labels = ["classification:internal"]
    labels.extend(_RESOURCE_KIND_LABELS.get(resource_kind.strip().lower(), ()))
    return tuple(dict.fromkeys(labels))


def labels_for_observation_channel(source_channel: str | None) -> tuple[str, ...]:
    labels = ["classification:internal"]
    if not source_channel:
        return tuple(labels)
    normalized = source_channel.strip().lower()
    family = normalized.split(":", 1)[0]
    for prefixes, mapped_labels in _OBSERVATION_CHANNEL_LABELS:
        if family in prefixes or any(normalized.startswith(f"{prefix}:") for prefix in prefixes):
            labels.extend(mapped_labels)
    return tuple(dict.fromkeys(labels))


async def record_resource_access_labels(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    resource_id: UUID,
    resource_kind: str,
    source: str = "resource_kind",
) -> None:
    for label in labels_for_resource_kind(resource_kind):
        await record_access_label(
            conn=conn,
            tenant_id=tenant_id,
            object_kind="resource",
            object_id=resource_id,
            label=label,
            source=source,
            metadata={"resource_kind": resource_kind},
        )


async def record_observation_access_labels(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID,
    source_channel: str | None,
    source: str = "source_channel",
) -> None:
    for label in labels_for_observation_channel(source_channel):
        await record_access_label(
            conn=conn,
            tenant_id=tenant_id,
            object_kind="observation",
            object_id=observation_id,
            label=label,
            source=source,
            metadata={"source_channel": source_channel},
        )


async def record_provenance_edge(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    derived_kind: str,
    derived_id: UUID,
    source_kind: str,
    source_id: UUID,
    derivation_kind: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO object_provenance_edges (
            tenant_id, derived_kind, derived_id,
            source_kind, source_id, derivation_kind, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        ON CONFLICT (
            tenant_id, derived_kind, derived_id,
            source_kind, source_id, derivation_kind
        )
        DO UPDATE SET metadata = EXCLUDED.metadata
        """,
        tenant_id,
        derived_kind,
        derived_id,
        source_kind,
        source_id,
        derivation_kind,
        json.dumps(metadata or {}),
    )


async def record_derived_access_labels(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    derived_kind: str,
    derived_id: UUID,
    source_refs: Sequence[ObjectRef],
    source: str = "provenance",
) -> None:
    refs = [
        ref
        for ref in source_refs
        if ref.tenant_id == tenant_id
    ]
    if not refs:
        return
    await conn.execute(
        """
        WITH inherited AS (
            SELECT DISTINCT
                src.label,
                src.object_kind AS source_kind,
                src.object_id AS source_id,
                src.source AS source_label_source
            FROM unnest($5::text[], $6::uuid[]) AS ref(object_kind, object_id)
            JOIN object_access_labels src
              ON src.tenant_id = $1
             AND src.object_kind = ref.object_kind
             AND src.object_id = ref.object_id
        ),
        deduped AS (
            SELECT
                label,
                jsonb_build_object(
                    'sources',
                    jsonb_agg(
                        jsonb_build_object(
                            'source_kind', source_kind,
                            'source_id', source_id::text,
                            'source_label_source', source_label_source
                        )
                        ORDER BY source_kind, source_id::text, source_label_source
                    ),
                    'source_count',
                    COUNT(*)
                ) AS metadata
            FROM inherited
            GROUP BY label
        )
        INSERT INTO object_access_labels (
            tenant_id, object_kind, object_id, label, source, metadata
        )
        SELECT
            $1,
            $2,
            $3,
            label,
            $4,
            metadata
        FROM deduped
        ON CONFLICT (tenant_id, object_kind, object_id, label, source)
        DO UPDATE SET metadata = EXCLUDED.metadata
        """,
        tenant_id,
        derived_kind,
        derived_id,
        source,
        [ref.object_kind for ref in refs],
        [ref.object_id for ref in refs],
    )


async def grant_read_authority(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    grantee_actor_id: UUID,
    granted_by_actor_id: UUID,
    purpose: Purpose | str,
    grant_kind: GrantKind,
    reason: str,
    object_ref: ObjectRef | None = None,
    label: str | None = None,
    scope: dict[str, Any] | None = None,
    expires_at: datetime | None = None,
) -> UUID:
    """Create a delegated read grant after checking grantor authority."""
    if grant_kind == "object" and object_ref is None:
        raise ValidationError("object grant requires object_ref")
    if grant_kind == "label" and not label:
        raise ValidationError("label grant requires label")
    if grant_kind == "scope" and not scope:
        raise ValidationError("scope grant requires non-empty scope")

    grantor = await principal_for_actor(
        granted_by_actor_id,
        conn=conn,
        tenant_id=tenant_id,
    )
    if grant_kind == "object":
        assert object_ref is not None
        decision = await authorize_read(grantor, purpose, object_ref, conn=conn)  # type: ignore[arg-type]
        if not decision.allowed or (
            decision.delegation_applied and "admin" not in grantor.roles
        ):
            raise AuthorityGrantError(
                "grantor lacks authority to delegate object read",
                tenant_id=str(tenant_id),
                object_kind=object_ref.object_kind,
                object_id=str(object_ref.object_id),
                reason=decision.reason,
            )
    elif grant_kind == "label":
        assert label is not None
        if not await _principal_can_grant_label(
            grantor,
            purpose,  # type: ignore[arg-type]
            label,
            conn=conn,
        ):
            raise AuthorityGrantError(
                "grantor lacks authority to delegate label read",
                tenant_id=str(tenant_id),
                label=label,
            )
    else:
        if "admin" not in grantor.roles:
            raise AuthorityGrantError(
                "scope grants require admin authority",
                tenant_id=str(tenant_id),
            )

    grant_id = uuid7()
    await conn.execute(
        """
        INSERT INTO read_authority_grants (
            id, tenant_id, grantee_actor_id, granted_by_actor_id,
            purpose, grant_kind, object_kind, object_id, label, scope,
            reason, expires_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12
        )
        """,
        grant_id,
        tenant_id,
        grantee_actor_id,
        granted_by_actor_id,
        str(purpose),
        grant_kind,
        object_ref.object_kind if object_ref is not None else None,
        object_ref.object_id if object_ref is not None else None,
        label,
        json.dumps(scope or {}),
        reason,
        expires_at,
    )
    return grant_id


async def revoke_read_authority(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    grant_id: UUID,
    revoked_by_actor_id: UUID,
    reason: str,
) -> bool:
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, granted_by_actor_id, revoked_at
        FROM read_authority_grants
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        grant_id,
    )
    if row is None:
        return False
    if row["revoked_at"] is not None:
        return False
    revoker = await principal_for_actor(
        revoked_by_actor_id,
        conn=conn,
        tenant_id=tenant_id,
    )
    if row["granted_by_actor_id"] != revoked_by_actor_id and "admin" not in revoker.roles:
        raise AuthorityGrantError(
            "revoker lacks authority over grant",
            tenant_id=str(tenant_id),
            grant_id=str(grant_id),
        )
    tag = await conn.execute(
        """
        UPDATE read_authority_grants
        SET revoked_at = now(),
            revoked_by_actor_id = $3,
            revoked_reason = $4
        WHERE tenant_id = $1
          AND id = $2
          AND revoked_at IS NULL
        """,
        tenant_id,
        grant_id,
        revoked_by_actor_id,
        reason,
    )
    return _row_count(tag) > 0


class AuthorizedReader:
    """Small safe-read facade for product code migrating away from raw SQL."""

    def __init__(
        self,
        *,
        principal: Principal,
        purpose: Purpose,
        conn: asyncpg.Connection,
    ) -> None:
        self._principal = principal
        self._purpose = purpose
        self._conn = conn

    async def authorize(self, object_ref: ObjectRef) -> AuthorityDecision:
        return await authorize_read(
            self._principal,
            self._purpose,
            object_ref,
            conn=self._conn,
        )

    async def get(self, object_ref: ObjectRef) -> dict[str, Any] | None:
        decision = await self.authorize(object_ref)
        if not decision.allowed:
            return None
        table = _READ_TABLES.get(object_ref.object_kind)
        if table is None:
            raise ValidationError(
                "authorized reader does not support object kind",
                object_kind=object_ref.object_kind,
            )
        row = await self._conn.fetchrow(
            f"SELECT * FROM {table} WHERE tenant_id = $1 AND id = $2",
            object_ref.tenant_id,
            object_ref.object_id,
        )
        return dict(row) if row is not None else None

    async def get_model(self, model_id: UUID) -> dict[str, Any] | None:
        return await self.get(_ref(self._principal.tenant_id, "model", model_id))

    async def get_observation(self, observation_id: UUID) -> dict[str, Any] | None:
        return await self.get(
            _ref(self._principal.tenant_id, "observation", observation_id)
        )

    async def get_resource(self, resource_id: UUID) -> dict[str, Any] | None:
        return await self.get(
            _ref(self._principal.tenant_id, "resource", resource_id)
        )

    async def get_commitment(self, commitment_id: UUID) -> dict[str, Any] | None:
        return await self.get(
            _ref(self._principal.tenant_id, "commitment", commitment_id)
        )

    async def get_goal(self, goal_id: UUID) -> dict[str, Any] | None:
        return await self.get(_ref(self._principal.tenant_id, "goal", goal_id))

    async def get_decision(self, decision_id: UUID) -> dict[str, Any] | None:
        return await self.get(
            _ref(self._principal.tenant_id, "decision", decision_id)
        )


async def _authorize_read(
    principal: Principal,
    purpose: Purpose,
    object_ref: ObjectRef,
    *,
    conn: asyncpg.Connection,
    seen: frozenset[tuple[str, UUID]],
    depth: int,
) -> AuthorityDecision:
    if object_ref.tenant_id != principal.tenant_id:
        return AuthorityDecision(False, "tenant_mismatch")

    marker = (object_ref.object_kind, object_ref.object_id)
    if marker in seen:
        return AuthorityDecision(False, "provenance_cycle")
    if depth > 8:
        return AuthorityDecision(False, "provenance_depth_exceeded")

    object_grant = await _has_active_grant(
        principal,
        purpose,
        object_ref,
        labels=(),
        conn=conn,
        grant_kind="object",
    )

    if object_ref.object_kind in CORE_ENTITY_KINDS:
        base = await can_read_by_id(
            principal.actor_id,
            object_ref.object_kind,  # type: ignore[arg-type]
            object_ref.object_id,
            conn=conn,
            tenant_id=principal.tenant_id,
        )
    else:
        base = AccessDecision(False, f"unsupported_object_kind:{object_ref.object_kind}")

    labels = await _labels_for(object_ref, conn=conn)
    label_names = tuple(label.label for label in labels)
    provenance = await _provenance_for(object_ref, conn=conn)
    provenance_refs = tuple(edge.source for edge in provenance)
    if (
        not base.allowed
        and not object_grant
        and object_ref.object_kind in DERIVED_OBJECT_KINDS
        and provenance_refs
    ):
        base = AccessDecision(True, f"derived_authority:{object_ref.object_kind}")

    if not base.allowed and not object_grant:
        return AuthorityDecision(
            False,
            base.reason,
            base_reason=base.reason,
        )

    for label in label_names:
        label_status = await _label_allows(
            principal,
            purpose,
            object_ref,
            label,
            object_grant=object_grant,
            conn=conn,
        )
        if not label_status:
            return AuthorityDecision(
                False,
                f"label_denied:{label}",
                labels_considered=label_names,
                override_applied=base.override_applied,
                audit_required=base.override_applied,
                base_reason=base.reason,
            )

    if provenance_refs and not object_grant:
        next_seen = seen | frozenset((marker,))
        for source_ref in provenance_refs:
            if source_ref.object_kind not in CORE_ENTITY_KINDS:
                return AuthorityDecision(
                    False,
                    f"provenance_unsupported_source_kind:{source_ref.object_kind}",
                    labels_considered=label_names,
                    provenance_considered=provenance_refs,
                    override_applied=base.override_applied,
                    audit_required=base.override_applied,
                    base_reason=base.reason,
                )
            source_decision = await _authorize_read(
                principal,
                purpose,
                source_ref,
                conn=conn,
                seen=next_seen,
                depth=depth + 1,
            )
            if not source_decision.allowed:
                return AuthorityDecision(
                    False,
                    f"provenance_denied:{source_ref.object_kind}",
                    labels_considered=label_names,
                    provenance_considered=provenance_refs,
                    override_applied=base.override_applied,
                    audit_required=base.override_applied,
                    base_reason=base.reason,
                )

    delegation_applied = object_grant
    if not delegation_applied:
        for label in label_names:
            if await _has_active_grant(
                principal,
                purpose,
                object_ref,
                labels=(label,),
                conn=conn,
                grant_kind="label",
            ):
                delegation_applied = True
                break
    return AuthorityDecision(
        True,
        "delegated_read" if delegation_applied and not base.allowed else "authorized",
        labels_considered=label_names,
        provenance_considered=provenance_refs,
        override_applied=base.override_applied,
        delegation_applied=delegation_applied,
        audit_required=base.override_applied or delegation_applied,
        base_reason=base.reason,
    )


async def _label_allows(
    principal: Principal,
    purpose: Purpose,
    object_ref: ObjectRef,
    label: str,
    *,
    object_grant: bool,
    conn: asyncpg.Connection,
) -> bool:
    normalized = _normalize_label(label)
    if normalized in _PUBLIC_LABELS:
        return True
    if object_grant:
        return True
    required_roles = _required_roles_for_label(normalized)
    if required_roles and set(principal.roles).intersection(required_roles):
        return True
    if normalized in _GRANT_ONLY_LABELS or required_roles:
        return await _has_active_grant(
            principal,
            purpose,
            object_ref,
            labels=(label,),
            conn=conn,
            grant_kind="label",
        )
    return True


async def _labels_for(
    object_ref: ObjectRef,
    *,
    conn: asyncpg.Connection,
) -> tuple[AccessLabel, ...]:
    rows = await conn.fetch(
        """
        SELECT tenant_id, object_kind, object_id, label, source, metadata
        FROM object_access_labels
        WHERE tenant_id = $1
          AND object_kind = $2
          AND object_id = $3
        ORDER BY label ASC, source ASC
        """,
        object_ref.tenant_id,
        object_ref.object_kind,
        object_ref.object_id,
    )
    return tuple(
        AccessLabel(
            tenant_id=row["tenant_id"],
            object_kind=row["object_kind"],
            object_id=row["object_id"],
            label=row["label"],
            source=row["source"],
            metadata=dict(row["metadata"] or {}),
        )
        for row in rows
    )


async def _provenance_for(
    object_ref: ObjectRef,
    *,
    conn: asyncpg.Connection,
) -> tuple[ProvenanceEdge, ...]:
    rows = await conn.fetch(
        """
        SELECT tenant_id, derived_kind, derived_id, source_kind, source_id,
               derivation_kind, metadata
        FROM object_provenance_edges
        WHERE tenant_id = $1
          AND derived_kind = $2
          AND derived_id = $3
        ORDER BY source_kind ASC, source_id ASC, derivation_kind ASC
        """,
        object_ref.tenant_id,
        object_ref.object_kind,
        object_ref.object_id,
    )
    return tuple(
        ProvenanceEdge(
            tenant_id=row["tenant_id"],
            derived=_ref(row["tenant_id"], row["derived_kind"], row["derived_id"]),
            source=_ref(row["tenant_id"], row["source_kind"], row["source_id"]),
            derivation_kind=row["derivation_kind"],
            metadata=dict(row["metadata"] or {}),
        )
        for row in rows
    )


async def _has_active_grant(
    principal: Principal,
    purpose: Purpose,
    object_ref: ObjectRef,
    *,
    labels: tuple[str, ...],
    conn: asyncpg.Connection,
    grant_kind: Literal["object", "label"],
) -> bool:
    if grant_kind == "object":
        val = await conn.fetchval(
            """
            SELECT 1
            FROM read_authority_grants
            WHERE tenant_id = $1
              AND grantee_actor_id = $2
              AND revoked_at IS NULL
              AND (expires_at IS NULL OR expires_at > now())
              AND (purpose = $3 OR purpose = '*')
              AND grant_kind = 'object'
              AND object_kind = $4
              AND object_id = $5
            LIMIT 1
            """,
            principal.tenant_id,
            principal.actor_id,
            purpose,
            object_ref.object_kind,
            object_ref.object_id,
        )
        return val is not None

    if not labels:
        return False
    normalized = tuple(_normalize_label(label) for label in labels)
    return await _has_active_label_grant(
        principal,
        purpose,
        normalized,
        conn=conn,
    )


async def _has_active_label_grant(
    principal: Principal,
    purpose: Purpose | str,
    labels: tuple[str, ...],
    *,
    conn: asyncpg.Connection,
) -> bool:
    if not labels:
        return False
    val = await conn.fetchval(
        """
        SELECT 1
        FROM read_authority_grants
        WHERE tenant_id = $1
          AND grantee_actor_id = $2
          AND revoked_at IS NULL
          AND (expires_at IS NULL OR expires_at > now())
          AND (purpose = $3 OR purpose = '*')
          AND grant_kind = 'label'
          AND lower(label) = ANY($4::text[])
        LIMIT 1
        """,
        principal.tenant_id,
        principal.actor_id,
        str(purpose),
        list(labels),
    )
    return val is not None


async def _principal_can_grant_label(
    principal: Principal,
    purpose: Purpose | str,
    label: str,
    *,
    conn: asyncpg.Connection,
) -> bool:
    roles = set(principal.roles)
    if "admin" in roles:
        return True
    normalized = _normalize_label(label)
    required_roles = _required_roles_for_label(normalized)
    if required_roles and roles.intersection(required_roles):
        return True
    return await _has_active_label_grant(
        principal,
        purpose,
        (normalized,),
        conn=conn,
    )


def _row_count(tag: str) -> int:
    try:
        return int(tag.split()[-1])
    except (IndexError, ValueError):
        return 0


def _hash_payload(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _required_roles_for_label(label: str) -> tuple[str, ...]:
    for labels, roles in _LABEL_ROLE_REQUIREMENTS:
        if label in labels:
            return roles
    return ()


def _normalize_label(label: str) -> str:
    return label.strip().lower()


def _ref(tenant_id: UUID, object_kind: str, object_id: UUID) -> ObjectRef:
    return ObjectRef(
        tenant_id=tenant_id,
        object_kind=str(object_kind),
        object_id=object_id if isinstance(object_id, UUID) else UUID(str(object_id)),
    )


__all__ = [
    "AccessLabel",
    "AuthorityFingerprint",
    "AuthorityDecision",
    "AuthorizedReader",
    "ObjectKind",
    "ObjectRef",
    "Principal",
    "ProvenanceEdge",
    "Purpose",
    "authority_fingerprint",
    "authorize_read",
    "authorized_reader",
    "current_grant_epoch",
    "principal_for_actor",
]
