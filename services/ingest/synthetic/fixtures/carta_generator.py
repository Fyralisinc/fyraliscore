"""Carta cap-table fixture generator (IN-CARTA).

`make_carta(firm_id=..., entities=[...], rows_per_entity=N)` produces a
deterministic per-entity-type fixture shaped to feed `MockCartaClient` (the
in-process `_open_carta_client` seam) and the canonical Provider Lab Carta
adapter. Entities are REAL Issuer v1alpha1 shapes (CONFIRMED — see
integrations/carta/client.py): camelCase field names and protobuf wrapper
objects — `{"value": "<decimal string>"}` (v1alpha1Decimal / dates / datetimes)
and `{"currencyCode": {"value": "USD"}, "amount": {"value": "1.25"}}`
(v1alpha1Money) — so the `carta:object` handler decodes fixtures exactly as it
decodes production payloads.

Entity taxonomy = the four `/v1alpha1/issuers/{id}/{collection}` lists the
fetcher shards on (client.DEFAULT_ENTITIES): `stakeholder` / `shareClass` /
`optionGrant` / `convertibleNote`. ONLY option grants carry
`lastModifiedDatetime` (the one collection with a server-side delta filter,
`lastModifiedDatetimeAfter`); the other kinds have no timestamp — incremental
sync is a full idempotent re-walk.

DEFAULT: 4 entity kinds × 1 row = exactly 4 backfill observations per tenant.
Because the entity_kind is baked into the external_id
(`carta:{firm}:{kind}:{id}:{version}`, version = content digest — see
handlers/carta.py `carta_version`), the four rows stay distinct even if their
`id`s repeat — so multi-entity fixtures never collide (cap-table-shaped, NOT
transaction-shaped).

Determinism: option-grant `lastModifiedDatetime`s are spaced one minute apart,
oldest first, anchored at `base_iso`; ids/amounts are derived from a stable
SHA-256 digest of (firm_id, entity_type, idx). Re-running with the same args
yields byte-identical output. The `seed` kwarg, when set, salts the digest so
distinct tenants get distinct ids/amounts without colliding.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The four cap-table entity kinds the planner shards on — the shard taxonomy
# keys of client.ENTITY_COLLECTIONS (== client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = (
    "stakeholder", "shareClass", "optionGrant", "convertibleNote",
)


def make_carta(
    *,
    firm_id: str = "f6e1d4a0-0000-4000-8000-000000000001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    seed: Any | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 50,
) -> dict[str, Any]:
    """Build a Carta issuer fixture.

    Args:
      firm_id: The Carta **issuer id** (the install scope, stored in
        `carta_installations.firm_id`; stamped into every entity's `issuerId`).
      entities: Entity types to generate; defaults to the four cap-table kinds
        ("stakeholder", "shareClass", "optionGrant", "convertibleNote").
      rows_per_entity: Number of rows generated for EACH entity type. The default
        4 entities × 1 row = exactly 4 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct tenants
        get distinct ids/amounts (the global observations UNIQUE has no tenant_id,
        so per-tenant fixtures must differ — though the entity_kind discriminator
        already keeps same-id rows distinct WITHIN a tenant).
      base_iso: Anchor for the (deterministic, 1-min-spaced) option-grant
        `lastModifiedDatetime` timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock's per-list `pageSize` cap (so callers can drive
        multi-page AIP-158 pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockCartaClient(fixture=...)` and Provider Lab:
        {
          "firm_id": "...",                      # the Carta issuer id
          "page_size": 50,
          "issuer": {"id": "...", "legalName": "..."},
          "entities": {
            "stakeholder":     [ {<v1alpha1 Stakeholder>}, ... ],  # oldest-first
            "shareClass":      [ ... ],
            "optionGrant":     [ ... ],
            "convertibleNote": [ ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(firm_id, entity_type, idx, base, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "firm_id": firm_id,
        "page_size": page_size,
        "issuer": {
            "id": firm_id,
            "legalName": f"Synthetic Issuer {firm_id[:8]}",
        },
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Wrapper builders (v1alpha1Decimal / Date / DateTime / Money)
# ---------------------------------------------------------------------

def _dec(value: Any) -> dict[str, str]:
    """v1alpha1Decimal / date / datetime wrapper: `{"value": "<string>"}`."""
    return {"value": str(value)}


def _money(amount: Any, currency: str = "USD") -> dict[str, Any]:
    """v1alpha1Money: `{"currencyCode": {"value": ...}, "amount": {"value": ...}}`."""
    return {"currencyCode": _dec(currency), "amount": _dec(amount)}


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    firm_id: str, entity_type: str, idx: int, base: datetime, salt: str,
) -> dict[str, Any]:
    digest = _digest(salt, firm_id, entity_type, idx)
    entity_id = str(1000 + idx)
    amount = round(100.0 + (int(digest[:6], 16) % 900_000) / 100.0, 2)
    # ISO lastModifiedDatetime spaced 1 minute apart, oldest first (UTC "Z").
    updated = _iso_z(base + timedelta(minutes=idx))

    if entity_type == "stakeholder":
        return {
            "id": entity_id,
            "issuerId": firm_id,
            "fullName": f"Holder {digest[:6]}",
            "email": f"holder-{digest[:6]}@synthetic.example",
            "employeeId": f"EMP-{entity_id}",
            # Open relationships only — EX_* maps to a "former" state_change
            # in the handler; keep defaults signal-shaped and predictable.
            "relationship": ("EMPLOYEE", "FOUNDER", "INVESTOR", "ADVISOR")[idx % 4],
            "entityType": "INDIVIDUAL",
        }
    if entity_type == "shareClass":
        return {
            "id": entity_id,
            "issuerId": firm_id,
            "name": f"Common {digest[:4].upper()}",
            "prefix": "CS",
            "type": "COMMON",
            "authorizedShareCount": _dec(10_000_000),
            "parValue": _money("0.0001"),
            "seniority": 1,
            "pariPassu": False,
        }
    if entity_type == "optionGrant":
        qty = 500 * (idx + 1)
        return {
            "id": entity_id,
            "issuerId": firm_id,
            "securityLabel": f"OG-{entity_id}",
            "securityId": digest[:16],
            "stakeholderId": str(1 + idx),
            "shareClassId": "1",
            "stockOptionType": "ISO",
            "quantity": _dec(qty),
            "outstandingQuantity": _dec(qty),
            "vestedQuantity": _dec(0),
            # 0 exercised -> the handler classifies "outstanding" (signal).
            "exercisedQuantity": _dec(0),
            "exercisePrice": _money(f"{amount / 100.0:.4f}"),
            "issueDate": _dec(base.date().isoformat()),
            "grantExpirationDate": _dec(
                (base + timedelta(days=3650)).date().isoformat()
            ),
            "lastModifiedDatetime": _dec(updated),
        }
    if entity_type == "convertibleNote":
        return {
            "id": entity_id,
            "issuerId": firm_id,
            "securityLabel": f"CN-{entity_id}",
            "securityId": digest[16:32],
            "stakeholderId": str(1 + idx),
            "cashPaid": _money(f"{amount:.2f}"),
            "priceCap": _money(f"{amount * 100:.2f}"),
            "discountPercentage": _dec("20"),
            "interestRate": _dec("5"),
            "issueDatetime": _dec(_iso_z(base - timedelta(days=1))),
            "maturityDatetime": _dec(_iso_z(base + timedelta(days=730))),
            # No conversion/cancel datetime -> "outstanding" (signal).
        }
    raise ValueError(f"unknown carta entity_type {entity_type!r}")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_iso(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_carta", "DEFAULT_ENTITIES"]
