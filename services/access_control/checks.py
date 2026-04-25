"""
services/access_control/checks.py — core `can_read` check.

Spec refs: ARCHITECTURE-FINAL.md §26. Five layers applied in order:

  Layer 1 — Tenant isolation (absolute).
  Layer 2 — Observation scope: author / mentioned / shared channel /
            manager chain (unless HR channel).
  Layer 3 — Act ownership: Commitment owner/contributor, Goal with
            contributing Commitment, managers of owners, shared-Goal
            team members.
  Layer 4 — Resource-kind: financial → finance/leadership; ip →
            legal/leadership; relational (customer) → account owner +
            leadership; capacity → team + mgr; infrastructure /
            regulatory → scoped.
  Layer 5 — Model visibility: visible_to_subjects OR actor in
            scope_actors. Admin / leadership override. First-person
            override for self-scoped Models.

Return type is `AccessDecision(allowed, reason)` — `reason` is the
rule name that decided the outcome. Callers in Gateway / Realtime log
the reason alongside 403 / dropped-delivery events so operators can
diagnose without re-running the check.

Entity input shape
------------------
`entity` is a dict with at least `{"kind": "<observation|commitment|
goal|decision|resource|model>", "id": <uuid>, "tenant_id": <uuid>}`
plus entity-specific fields the check may need. For convenience,
`can_read_by_id(kind, id, ...)` fetches the missing fields from Postgres
using the public repo APIs (no direct column access).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError

from .hierarchy import is_hr_channel, is_in_manager_chain, is_shared_channel
from .roles import has_role


log = logging.getLogger(__name__)


EntityKind = Literal[
    "observation", "commitment", "goal", "decision", "resource", "model"
]


@dataclass(frozen=True)
class AccessDecision:
    """Structured outcome of `can_read`.

    `allowed=True` paths record the winning rule in `reason`;
    `allowed=False` paths record the last rule considered before the
    default deny. `override_applied=True` means an admin/leadership
    override was used — Gateway writes a row to `access_override_log`.
    """

    allowed: bool
    reason: str
    override_applied: bool = False

    def __bool__(self) -> bool:
        return self.allowed


class AccessCheckError(CompanyOSError):
    default_code = "access_check_error"


# Resource-kind → permitted tenant-wide roles (Layer 4).
_RESOURCE_KIND_ROLES: dict[str, tuple[str, ...]] = {
    "financial": ("finance", "leadership"),
    "ip": ("legal", "leadership"),
    # Customer relational: role-wise only leadership. Account-owner
    # logic requires Resource.metadata inspection, handled inline.
    "relational": ("leadership",),
    # Capacity: team + managers gate — done inline via actor_visible_*.
    # leadership still has a shortcut.
    "capacity": ("leadership",),
    # Infrastructure + regulatory: leadership-only for now; explicit
    # per-entity viewer grants extend this.
    "infrastructure": ("leadership",),
    "regulatory": ("leadership", "legal"),
}


# ---------------------------------------------------------------------
# Public entry point — entity passed as hydrated dict
# ---------------------------------------------------------------------


async def can_read(
    actor_id: UUID,
    entity: dict[str, Any],
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    """Evaluate whether `actor_id` can read `entity` inside `tenant_id`.

    `entity` must carry at minimum `kind` and `tenant_id`. The specific
    fields each entity kind needs are documented below per-layer.

    Tenant isolation (Layer 1) is enforced first and unconditionally
    denies when the entity's tenant differs from the checker's tenant —
    no role / admin override bypasses this layer.
    """
    kind = entity.get("kind")
    entity_tenant_raw = entity.get("tenant_id")
    if kind is None or entity_tenant_raw is None:
        raise ValidationError(
            "entity must carry kind + tenant_id",
            got=list(entity.keys()),
        )
    entity_tenant = (
        entity_tenant_raw
        if isinstance(entity_tenant_raw, UUID)
        else UUID(str(entity_tenant_raw))
    )

    # Layer 1 — absolute tenant isolation.
    if entity_tenant != tenant_id:
        return AccessDecision(False, "tenant_mismatch")

    # Admin / leadership overrides come after Layer 1 but before per-
    # kind rules — the spec's "Admin can override" rule is a single
    # check. HR-channel observations explicitly skip admin override.
    is_hr = kind == "observation" and is_hr_channel(entity.get("source_channel"))
    if not is_hr:
        if await has_role(
            actor_id, "admin", conn=conn, tenant_id=tenant_id,
        ):
            return AccessDecision(True, "admin_override", override_applied=True)
        if await has_role(
            actor_id, "leadership", conn=conn, tenant_id=tenant_id,
        ):
            return AccessDecision(
                True, "leadership_override", override_applied=True
            )

    # Dispatch per kind.
    if kind == "observation":
        return await _check_observation(actor_id, entity, conn, tenant_id)
    if kind == "commitment":
        return await _check_commitment(actor_id, entity, conn, tenant_id)
    if kind == "goal":
        return await _check_goal(actor_id, entity, conn, tenant_id)
    if kind == "decision":
        return await _check_decision(actor_id, entity, conn, tenant_id)
    if kind == "resource":
        return await _check_resource(actor_id, entity, conn, tenant_id)
    if kind == "model":
        return await _check_model(actor_id, entity, conn, tenant_id)
    raise ValidationError(f"unknown entity kind {kind!r}", kind=kind)


# ---------------------------------------------------------------------
# Convenience — load by id and check
# ---------------------------------------------------------------------


async def can_read_by_id(
    actor_id: UUID,
    kind: EntityKind,
    entity_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    """Fetch the entity row and call `can_read`. Returns a denial with
    reason='entity_not_found' when the id doesn't exist in the tenant.
    """
    entity = await _load_entity(kind, entity_id, conn=conn, tenant_id=tenant_id)
    if entity is None:
        return AccessDecision(False, "entity_not_found")
    return await can_read(
        actor_id, entity, conn=conn, tenant_id=tenant_id,
    )


async def _load_entity(
    kind: EntityKind,
    entity_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any] | None:
    """Fetch just the columns each layer needs."""
    if kind == "observation":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, actor_id, source_channel,
                   entities_mentioned, source_actor_ref
            FROM observations
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    elif kind == "commitment":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, owner_id
            FROM commitments
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    elif kind == "goal":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id
            FROM goals
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    elif kind == "decision":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id
            FROM decisions
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    elif kind == "resource":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, kind AS resource_kind, metadata
            FROM resources
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    elif kind == "model":
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, visible_to_subjects, scope_actors,
                   scope_entities
            FROM models
            WHERE id = $1 AND tenant_id = $2
            """,
            entity_id, tenant_id,
        )
    else:
        raise ValidationError(f"unknown entity kind {kind!r}")
    if row is None:
        return None
    out = dict(row)
    out["kind"] = kind
    if kind == "resource":
        out["kind"] = "resource"  # explicit
        # Rename the stored column so the check function is unambiguous.
        out["resource_kind"] = out.get("resource_kind")
    return out


# ---------------------------------------------------------------------
# Layer 2 — Observations
# ---------------------------------------------------------------------


async def _check_observation(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    obs_author = entity.get("actor_id")
    source_channel = entity.get("source_channel") or ""
    mentioned_raw = entity.get("entities_mentioned") or []
    source_actor_ref = entity.get("source_actor_ref")

    # Author.
    if obs_author is not None and UUID(str(obs_author)) == actor_id:
        return AccessDecision(True, "observation_author")

    # Mentioned: `entities_mentioned` is a JSONB list of dicts. An
    # entry like {"type": "actor", "id": "<uuid>"} (or "kind") counts.
    if isinstance(mentioned_raw, (bytes, bytearray)):
        mentioned_raw = json.loads(mentioned_raw.decode())
    elif isinstance(mentioned_raw, str):
        try:
            mentioned_raw = json.loads(mentioned_raw)
        except json.JSONDecodeError:
            mentioned_raw = []
    if isinstance(mentioned_raw, list):
        for ent in mentioned_raw:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type") or ent.get("kind")
            if etype != "actor":
                continue
            raw_id = ent.get("id")
            if raw_id is None:
                continue
            try:
                mid = UUID(str(raw_id))
            except (ValueError, TypeError):
                continue
            if mid == actor_id:
                return AccessDecision(True, "observation_mentioned")

    # Also match by source_actor_ref → identity mapping → actor_id.
    if source_actor_ref:
        mapped = await conn.fetchval(
            """
            SELECT actor_id FROM actor_identity_mappings
            WHERE source_actor_ref = $1
              AND actor_id IN (SELECT id FROM actors WHERE tenant_id = $2)
            LIMIT 1
            """,
            source_actor_ref, tenant_id,
        )
        if mapped == actor_id:
            return AccessDecision(True, "observation_source_actor_ref")

    # Shared channel (HR channels never match).
    if await is_shared_channel(
        source_channel, conn=conn, tenant_id=tenant_id, actor_id=actor_id,
    ):
        return AccessDecision(True, "observation_shared_channel")

    # Manager chain — only for non-HR channels.
    if obs_author is not None and not is_hr_channel(source_channel):
        author_uuid = UUID(str(obs_author))
        if await is_in_manager_chain(
            author_uuid, actor_id, conn=conn, tenant_id=tenant_id,
        ):
            return AccessDecision(True, "observation_manager_chain")

    return AccessDecision(False, "observation_out_of_scope")


# ---------------------------------------------------------------------
# Layer 3 — Acts
# ---------------------------------------------------------------------


async def _check_commitment(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    commitment_id = entity.get("id")
    if commitment_id is None:
        raise ValidationError("commitment entity missing id")
    commitment_id = (
        commitment_id if isinstance(commitment_id, UUID) else UUID(str(commitment_id))
    )
    owner_raw = entity.get("owner_id")
    owner_id = UUID(str(owner_raw)) if owner_raw else None

    # Owner.
    if owner_id is not None and owner_id == actor_id:
        return AccessDecision(True, "commitment_owner")

    # Contributor.
    contrib = await conn.fetchval(
        """
        SELECT 1 FROM commitment_contributors
        WHERE commitment_id = $1 AND actor_id = $2
        LIMIT 1
        """,
        commitment_id, actor_id,
    )
    if contrib:
        return AccessDecision(True, "commitment_contributor")

    # Manager chain: any ancestor of the owner can read.
    if owner_id is not None and await is_in_manager_chain(
        owner_id, actor_id, conn=conn, tenant_id=tenant_id,
    ):
        return AccessDecision(True, "commitment_manager_chain")

    # Shared-goal teammate: actor contributes to some Commitment that
    # also contributes_to a Goal this Commitment contributes_to.
    shared = await conn.fetchval(
        """
        SELECT 1
        FROM contributes_to my_ct
        JOIN commitment_contributors my_cc
          ON my_cc.commitment_id = my_ct.commitment_id
        JOIN contributes_to their_ct
          ON their_ct.goal_id = my_ct.goal_id
        WHERE my_cc.actor_id = $1
          AND their_ct.commitment_id = $2
        LIMIT 1
        """,
        actor_id, commitment_id,
    )
    if shared:
        return AccessDecision(True, "commitment_shared_goal")

    # Entity-scoped role (viewer/contributor/owner) on this commitment.
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_roles
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'commitment'
          AND entity_id = $3
          AND role IN ('viewer', 'contributor', 'owner')
          AND revoked_at IS NULL
        LIMIT 1
        """,
        tenant_id, actor_id, commitment_id,
    )
    if val:
        return AccessDecision(True, "commitment_role_grant")

    return AccessDecision(False, "commitment_out_of_scope")


async def _check_goal(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    goal_id = entity.get("id")
    if goal_id is None:
        raise ValidationError("goal entity missing id")
    goal_id = goal_id if isinstance(goal_id, UUID) else UUID(str(goal_id))

    # An actor sees a Goal when any commitment that contributes_to the
    # Goal is visible to them (owner / contributor / manager chain).
    any_contrib_visible = await conn.fetchval(
        """
        SELECT 1
        FROM contributes_to ct
        JOIN commitments c ON c.id = ct.commitment_id
        WHERE ct.goal_id = $1
          AND (
            c.owner_id = $2
            OR EXISTS (
              SELECT 1 FROM commitment_contributors cc
              WHERE cc.commitment_id = c.id AND cc.actor_id = $2
            )
          )
        LIMIT 1
        """,
        goal_id, actor_id,
    )
    if any_contrib_visible:
        return AccessDecision(True, "goal_contributing_commitment")

    # Manager chain over any contributing owner.
    contrib_owners = await conn.fetch(
        """
        SELECT DISTINCT c.owner_id
        FROM contributes_to ct
        JOIN commitments c ON c.id = ct.commitment_id
        WHERE ct.goal_id = $1 AND c.owner_id IS NOT NULL
        """,
        goal_id,
    )
    for row in contrib_owners:
        oid = row["owner_id"]
        if oid is None:
            continue
        if await is_in_manager_chain(
            oid, actor_id, conn=conn, tenant_id=tenant_id,
        ):
            return AccessDecision(True, "goal_manager_chain")

    # Entity-scoped role (viewer) on this Goal.
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_roles
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'goal'
          AND entity_id = $3
          AND role IN ('viewer', 'contributor', 'owner')
          AND revoked_at IS NULL
        LIMIT 1
        """,
        tenant_id, actor_id, goal_id,
    )
    if val:
        return AccessDecision(True, "goal_role_grant")

    return AccessDecision(False, "goal_out_of_scope")


async def _check_decision(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    decision_id = entity.get("id")
    if decision_id is None:
        raise ValidationError("decision entity missing id")
    decision_id = (
        decision_id if isinstance(decision_id, UUID) else UUID(str(decision_id))
    )

    # Decisions are visible when actor has a role grant on them OR
    # when they constrain a commitment the actor can see (joined live).
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_roles
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'decision'
          AND entity_id = $3
          AND role IN ('viewer', 'contributor', 'owner')
          AND revoked_at IS NULL
        LIMIT 1
        """,
        tenant_id, actor_id, decision_id,
    )
    if val:
        return AccessDecision(True, "decision_role_grant")

    # Visibility via constrained_by → commitment the actor owns/contribs.
    via = await conn.fetchval(
        """
        SELECT 1
        FROM constrained_by cb
        JOIN commitments c ON c.id = cb.commitment_id
        WHERE cb.decision_id = $1
          AND (
            c.owner_id = $2
            OR EXISTS (
              SELECT 1 FROM commitment_contributors cc
              WHERE cc.commitment_id = c.id AND cc.actor_id = $2
            )
          )
        LIMIT 1
        """,
        decision_id, actor_id,
    )
    if via:
        return AccessDecision(True, "decision_via_commitment")

    return AccessDecision(False, "decision_out_of_scope")


# ---------------------------------------------------------------------
# Layer 4 — Resources (kind-aware)
# ---------------------------------------------------------------------


async def _check_resource(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    resource_id = entity.get("id")
    if resource_id is None:
        raise ValidationError("resource entity missing id")
    resource_id = (
        resource_id if isinstance(resource_id, UUID) else UUID(str(resource_id))
    )
    kind = entity.get("resource_kind")
    metadata = entity.get("metadata") or {}
    if isinstance(metadata, (bytes, bytearray)):
        metadata = json.loads(metadata.decode())
    elif isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            metadata = {}
    if not isinstance(metadata, dict):
        metadata = {}

    # Role-based rules per kind.
    allowed_roles = _RESOURCE_KIND_ROLES.get(kind, ())
    for role in allowed_roles:
        if await has_role(
            actor_id, role, conn=conn, tenant_id=tenant_id,
        ):
            return AccessDecision(True, f"resource_role_{role}")

    # Entity-scoped viewer grant on the resource (kind-agnostic).
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_roles
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'resource'
          AND entity_id = $3
          AND role IN ('viewer', 'contributor', 'owner')
          AND revoked_at IS NULL
        LIMIT 1
        """,
        tenant_id, actor_id, resource_id,
    )
    if val:
        return AccessDecision(True, "resource_role_grant")

    if kind == "relational":
        # Customer: account owner listed in metadata.account_owner_id.
        acct_owner_raw = metadata.get("account_owner_id")
        if acct_owner_raw is not None:
            try:
                acct_owner = UUID(str(acct_owner_raw))
            except (ValueError, TypeError):
                acct_owner = None
            if acct_owner == actor_id:
                return AccessDecision(True, "resource_customer_account_owner")
        # Additional account-owner-ids (team).
        aos = metadata.get("account_owners") or []
        if isinstance(aos, list):
            for raw in aos:
                try:
                    aoid = UUID(str(raw))
                except (ValueError, TypeError):
                    continue
                if aoid == actor_id:
                    return AccessDecision(
                        True, "resource_customer_account_owner"
                    )

    if kind == "capacity":
        # Capacity Resource: team members + managers. "Team members"
        # are actors referenced in metadata.team_ids OR who are
        # contributors on any commitment that deploys this resource.
        team_ids = metadata.get("team_ids") or []
        if isinstance(team_ids, list):
            for raw in team_ids:
                try:
                    tid = UUID(str(raw))
                except (ValueError, TypeError):
                    continue
                if tid == actor_id:
                    return AccessDecision(True, "resource_capacity_team")
        # Fallback: any commitment that deploys this resource — actor
        # must own/contribute AND be the commitment owner.
        via = await conn.fetchval(
            """
            SELECT 1
            FROM resource_deployments rd
            JOIN commitments c ON c.id = rd.commitment_id
            WHERE rd.resource_id = $1
              AND rd.released_at IS NULL
              AND (
                c.owner_id = $2
                OR EXISTS (
                  SELECT 1 FROM commitment_contributors cc
                  WHERE cc.commitment_id = c.id AND cc.actor_id = $2
                )
              )
            LIMIT 1
            """,
            resource_id, actor_id,
        )
        if via:
            return AccessDecision(True, "resource_capacity_deployment")

    return AccessDecision(False, f"resource_out_of_scope:{kind}")


# ---------------------------------------------------------------------
# Layer 5 — Models
# ---------------------------------------------------------------------


async def _check_model(
    actor_id: UUID,
    entity: dict[str, Any],
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> AccessDecision:
    visible = entity.get("visible_to_subjects")
    scope_actors = entity.get("scope_actors") or []
    if isinstance(scope_actors, (bytes, bytearray)):
        scope_actors = json.loads(scope_actors.decode())
    # Normalize to list of UUID.
    normalized: list[UUID] = []
    if isinstance(scope_actors, list):
        for raw in scope_actors:
            if isinstance(raw, UUID):
                normalized.append(raw)
            else:
                try:
                    normalized.append(UUID(str(raw)))
                except (ValueError, TypeError):
                    continue

    # Scope entity membership — pattern / external-entity Models.
    scope_entities = entity.get("scope_entities") or []
    if isinstance(scope_entities, (bytes, bytearray)):
        scope_entities = json.loads(scope_entities.decode())
    elif isinstance(scope_entities, str):
        try:
            scope_entities = json.loads(scope_entities)
        except json.JSONDecodeError:
            scope_entities = []

    # Public model.
    if visible:
        return AccessDecision(True, "model_public")

    # Private but actor in scope — first-person access. This is ALSO
    # the "first-person override" path for contestation (§11): a subject
    # can read their own private Model even when visible_to_subjects is
    # False.
    if actor_id in normalized:
        return AccessDecision(
            True, "model_self_scope", override_applied=False,
        )

    # Pattern / external-entity Models: visibility flows through the
    # scope_entities. If any referenced entity is visible to the actor,
    # the Model is too.
    if isinstance(scope_entities, list):
        for ent in scope_entities:
            if not isinstance(ent, dict):
                continue
            etype = ent.get("type") or ent.get("kind")
            raw_id = ent.get("id")
            if raw_id is None:
                continue
            try:
                eid = UUID(str(raw_id))
            except (ValueError, TypeError):
                continue
            if etype == "commitment":
                val = await conn.fetchval(
                    """
                    SELECT 1 FROM actor_visible_commitments
                    WHERE actor_id = $1 AND commitment_id = $2
                      AND tenant_id = $3
                    LIMIT 1
                    """,
                    actor_id, eid, tenant_id,
                )
                if val:
                    return AccessDecision(True, "model_via_commitment_scope")
            elif etype == "goal":
                val = await conn.fetchval(
                    """
                    SELECT 1 FROM actor_visible_goals
                    WHERE actor_id = $1 AND goal_id = $2
                      AND tenant_id = $3
                    LIMIT 1
                    """,
                    actor_id, eid, tenant_id,
                )
                if val:
                    return AccessDecision(True, "model_via_goal_scope")

    return AccessDecision(False, "model_out_of_scope")


__all__ = [
    "AccessCheckError",
    "AccessDecision",
    "EntityKind",
    "can_read",
    "can_read_by_id",
]
