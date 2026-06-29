# Admin And Role Management Guide

Owner: Platform Engineering.
Last reviewed: 2026-06-24.

This guide covers tenant-scoped role management for production operators. It is
for emergency/bootstrap operations and back-office support until the customer
admin UI covers the same workflow.

## Role Model

| Scope | Roles | Entity ID |
| --- | --- | --- |
| Tenant | `admin`, `finance`, `legal`, `leadership` | Must be omitted |
| Entity | `owner`, `contributor`, `viewer` | Required for `goal`, `commitment`, `decision`, or `resource` |

All role operations must include the tenant UUID. Cross-tenant role lookup or
mutation is never allowed.

## CLI

The operator CLI is `scripts/manage_actor_roles.py`. It requires
`DATABASE_URL` or `--dsn`. List, grant, and revoke operations require
`--operator`, and that operator actor must hold tenant-wide `admin` or
`leadership`, except for the explicit first-admin bootstrap flow below.

List roles for an actor:

```bash
python scripts/manage_actor_roles.py list \
  --tenant "$TENANT_ID" \
  --actor "$TARGET_ACTOR_ID"
```

Grant a tenant admin role:

```bash
python scripts/manage_actor_roles.py grant \
  --tenant "$TENANT_ID" \
  --actor "$TARGET_ACTOR_ID" \
  --entity-type tenant \
  --role admin \
  --granted-by "$APPROVER_ACTOR_ID" \
  --operator "$OPERATOR_ACTOR_ID"
```

Grant an entity role:

```bash
python scripts/manage_actor_roles.py grant \
  --tenant "$TENANT_ID" \
  --actor "$TARGET_ACTOR_ID" \
  --entity-type decision \
  --entity-id "$DECISION_ID" \
  --role viewer \
  --granted-by "$APPROVER_ACTOR_ID" \
  --operator "$OPERATOR_ACTOR_ID"
```

Revoke a role:

```bash
python scripts/manage_actor_roles.py revoke \
  --tenant "$TENANT_ID" \
  --actor "$TARGET_ACTOR_ID" \
  --entity-type tenant \
  --role admin \
  --operator "$OPERATOR_ACTOR_ID"
```

## Audit Contract

Grant and revoke commands write `operator_action_log` in the same database
transaction as the role mutation.

Expected audit rows:

| Action | Resource type | Resource ID | Metadata |
| --- | --- | --- | --- |
| `role.grant` | `actor_role` | target actor UUID | role, entity type, optional entity ID, approver |
| `role.revoke` | `actor_role` | target actor UUID | role, entity type, optional entity ID, revoked boolean |

Verify recent role actions:

```sql
SELECT occurred_at, actor_id, action, resource_id, metadata
FROM operator_action_log
WHERE tenant_id = $1
  AND action IN ('role.grant', 'role.revoke')
ORDER BY occurred_at DESC
LIMIT 50;
```

Production operators must pass `--operator`. If omitted, the CLI falls back to
the first-admin bootstrap path only when `--allow-bootstrap` is present and no
tenant-wide `admin` or `leadership` grant exists yet. Normal list, grant, and
revoke operations fail closed without an authorized `--operator`.

## Safety Rules

- Do not grant tenant `admin` without a customer-approved reason or break-glass
  incident.
- Prefer entity-scoped roles over tenant-scoped roles.
- Revoke temporary grants immediately after the support task is complete.
- Do not update `actor_roles` manually unless the CLI is unavailable and an
  incident commander approves the database change.
- Do not delete historical role rows. Revocation preserves audit history by
  setting `revoked_at`.

## Break-Glass Procedure

1. Open an incident or support ticket with tenant, actor, role, reason, and
   expected expiry.
2. Confirm the operator actor exists in the target tenant.
3. Confirm the operator actor has tenant-wide `admin` or `leadership`.
4. Run the grant with `--operator` and `--granted-by`.
5. Verify product behavior and the `operator_action_log` row.
6. Add a revocation reminder to the incident.
7. Revoke the grant and verify the revoke audit row.

## First-Admin Bootstrap

Use this path only while a tenant has no active tenant-wide `admin` or
`leadership` actor. It is intentionally narrow: it can grant only a tenant
`admin` or `leadership` role and writes `operator_bootstrap=true` in the audit
metadata.

```bash
python scripts/manage_actor_roles.py grant \
  --tenant "$TENANT_ID" \
  --actor "$TARGET_ACTOR_ID" \
  --entity-type tenant \
  --role admin \
  --granted-by "$APPROVER_ACTOR_ID" \
  --allow-bootstrap
```

After bootstrap, all future role changes must use `--operator`.

## Remaining Production Gaps

This guide covers role-management operations. The broader checklist still tracks
staging/on-call evidence for every admin surface.
