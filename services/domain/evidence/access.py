"""Conservative authorization over immutable source-evidence revisions.

An episode may only be as visible as every evidence revision it contains.
Missing or unknown source ACL state is therefore a denial, not permission.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class EvidenceAccessDecision:
    allowed: bool
    reason: str


def _canonical_audience_entry(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def policy_hash(policy: dict[str, Any]) -> str:
    canonical = json.dumps(
        policy, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def compose_access_policies(
    policies: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Return the intersection policy for a set of evidence revisions.

    ``unknown`` is intentionally contagious. Restricted audiences are
    intersected, while tenant/public inputs add no audience restriction.
    """

    values = list(policies)
    if not values:
        result: dict[str, Any] = {
            "visibility": "unknown",
            "audience": [],
            "source_acl_version": "empty-evidence-set",
        }
        result["policy_hash"] = policy_hash(result)
        return result

    allowed_visibilities = {"public", "tenant", "restricted", "unknown"}
    if any(value.get("visibility") not in allowed_visibilities for value in values):
        visibility = "unknown"
    elif any(value.get("visibility") == "unknown" for value in values):
        visibility = "unknown"
    elif any(value.get("visibility") == "restricted" for value in values):
        visibility = "restricted"
    elif any(value.get("visibility") == "tenant" for value in values):
        visibility = "tenant"
    else:
        visibility = "public"

    restricted_audiences = [
        {
            _canonical_audience_entry(entry): entry
            for entry in value.get("audience", [])
            if isinstance(entry, dict)
        }
        for value in values
        if value.get("visibility") == "restricted"
    ]
    audience: list[dict[str, Any]] = []
    if restricted_audiences:
        common = set(restricted_audiences[0])
        for entries in restricted_audiences[1:]:
            common.intersection_update(entries)
        audience = [restricted_audiences[0][key] for key in sorted(common)]

    versions = sorted(
        {
            str(value.get("source_acl_version", "unknown"))
            for value in values
        }
    )
    result = {
        "visibility": visibility,
        "audience": audience,
        "source_acl_version": "composed:" + ",".join(versions),
        "input_policy_hashes": sorted(policy_hash(value) for value in values),
    }
    result["policy_hash"] = policy_hash(result)
    return result


async def can_actor_read_evidence_set(
    actor_id: UUID,
    *,
    tenant_id: UUID,
    evidence_ids: Iterable[UUID],
    conn: asyncpg.Connection,
) -> EvidenceAccessDecision:
    """Require the actor to satisfy every evidence revision's source ACL."""

    ids = list(dict.fromkeys(evidence_ids))
    if not ids:
        return EvidenceAccessDecision(True, "no_evidence_acl")

    actor_tenant = await conn.fetchval(
        "SELECT tenant_id FROM actors WHERE id = $1", actor_id
    )
    if actor_tenant is None:
        return EvidenceAccessDecision(False, "actor_not_found")
    if actor_tenant != tenant_id:
        return EvidenceAccessDecision(False, "actor_tenant_mismatch")

    rows = await conn.fetch(
        """
        SELECT id, access_policy
          FROM source_evidence
         WHERE tenant_id = $1 AND id = ANY($2::uuid[])
        """,
        tenant_id,
        ids,
    )
    if len(rows) != len(ids):
        return EvidenceAccessDecision(False, "evidence_acl_missing")

    tenant_roles: set[str] | None = None
    for row in rows:
        policy = row["access_policy"]
        if isinstance(policy, str):
            policy = json.loads(policy)
        if not isinstance(policy, dict):
            return EvidenceAccessDecision(False, "evidence_acl_invalid")
        visibility = policy.get("visibility")
        if visibility == "unknown":
            return EvidenceAccessDecision(False, "evidence_acl_unknown")
        if visibility in {"public", "tenant"}:
            continue
        if visibility != "restricted":
            return EvidenceAccessDecision(False, "evidence_acl_invalid")

        actor_ref = {"type": "actor", "id": str(actor_id)}
        audience = policy.get("audience", [])
        if not isinstance(audience, list):
            return EvidenceAccessDecision(False, "evidence_acl_invalid")
        if actor_ref in audience:
            continue

        required_roles = {
            str(entry.get("id"))
            for entry in audience
            if isinstance(entry, dict)
            and entry.get("type") == "role"
            and entry.get("id")
        }
        if required_roles:
            if tenant_roles is None:
                records = await conn.fetch(
                    """
                    SELECT role FROM actor_roles
                     WHERE tenant_id = $1 AND actor_id = $2
                       AND entity_type = 'tenant'
                       AND entity_id IS NULL AND revoked_at IS NULL
                    """,
                    tenant_id,
                    actor_id,
                )
                tenant_roles = {str(record["role"]) for record in records}
            if tenant_roles.intersection(required_roles):
                continue
        return EvidenceAccessDecision(False, "evidence_acl_not_member")

    return EvidenceAccessDecision(True, "evidence_acl_satisfied")


__all__ = [
    "EvidenceAccessDecision",
    "can_actor_read_evidence_set",
    "compose_access_policies",
    "policy_hash",
]
