"""Seed a company's initial identity vocabulary without seeding beliefs.

This is deliberately a narrow authority adapter over ``entity_aliases``. A
founder bootstrap says what a name denotes; it does not assert anything about
how the company behaves. Consequently this module never writes Models,
relations, resources, or observations.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.types import EntityAliasRow
from services.domain.entity_aliases.repo import (
    insert_alias_with_connection,
    normalize_phrase,
)


_REF_PART_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


@dataclass(frozen=True, slots=True)
class FounderIdentityBootstrapEntry:
    """One founder-defined canonical referent and its exact names."""

    canonical_ref: dict[str, Any]
    canonical_name: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FounderIdentityBootstrapResult:
    """Materialized aliases for one idempotent founder manifest."""

    manifest_ref: str
    aliases: tuple[EntityAliasRow, ...]

    @property
    def alias_count(self) -> int:
        return len(self.aliases)


def _canonical_ref(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(
            "canonical_ref must be an object",
            field="canonical_ref",
        )
    ref_type = str(value.get("type") or "").strip()
    ref_id = str(value.get("id") or "").strip()
    version = value.get("version", 1)
    if not ref_type or not _REF_PART_RE.fullmatch(ref_type):
        raise ValidationError(
            "canonical_ref.type must be a lowercase typed identifier",
            field="canonical_ref.type",
            value=ref_type,
        )
    if not ref_id:
        raise ValidationError(
            "canonical_ref.id must be non-empty",
            field="canonical_ref.id",
        )
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise ValidationError(
            "canonical_ref.version must be a positive integer",
            field="canonical_ref.version",
            value=version,
        )
    return {"type": ref_type, "id": ref_id, "version": version}


def _required_text(value: str, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValidationError(f"{field} must be non-empty", field=field)
    return normalized


async def apply_founder_identity_bootstrap(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    manifest_ref: str,
    authority_ref: str,
    asserted_by_ref: str,
    provenance_refs: tuple[str, ...],
    entries: tuple[FounderIdentityBootstrapEntry, ...],
    effective_at: datetime,
) -> FounderIdentityBootstrapResult:
    """Atomically materialize one founder-authoritative exact-name manifest.

    Unknown names are intentionally untouched and therefore continue through
    the normal unresolved/provisional grounding path.
    """

    manifest_ref = _required_text(manifest_ref, field="manifest_ref")
    authority_ref = _required_text(authority_ref, field="authority_ref")
    asserted_by_ref = _required_text(asserted_by_ref, field="asserted_by_ref")
    if effective_at.tzinfo is None:
        raise ValidationError(
            "effective_at must be timezone-aware",
            field="effective_at",
        )
    normalized_provenance = tuple(
        dict.fromkeys(
            _required_text(ref, field="provenance_refs")
            for ref in provenance_refs
        )
    )
    if not normalized_provenance:
        raise ValidationError(
            "provenance_refs must contain at least one reference",
            field="provenance_refs",
        )
    if not entries:
        raise ValidationError("entries must be non-empty", field="entries")

    prepared: list[tuple[str, bool, dict[str, Any]]] = []
    ref_by_normalized_phrase: dict[str, dict[str, Any]] = {}
    seen_exact: set[tuple[str, str]] = set()
    for entry in entries:
        canonical_ref = _canonical_ref(entry.canonical_ref)
        names = ((entry.canonical_name, True),) + tuple(
            (alias, False) for alias in entry.aliases
        )
        for phrase_value, is_canonical in names:
            phrase = _required_text(phrase_value, field="alias")
            normalized = normalize_phrase(phrase)
            prior_ref = ref_by_normalized_phrase.get(normalized)
            if prior_ref is not None and prior_ref != canonical_ref:
                raise ValidationError(
                    "founder manifest maps one exact name to multiple referents",
                    field="entries",
                    phrase=phrase,
                )
            ref_by_normalized_phrase[normalized] = canonical_ref
            exact_key = (phrase, json.dumps(canonical_ref, sort_keys=True))
            if exact_key in seen_exact:
                continue
            seen_exact.add(exact_key)
            prepared.append((phrase, is_canonical, canonical_ref))

    metadata = {
        "founder_bootstrap_contract": {"version": "v1"},
        "identity_basis_class": "source_authoritative",
        "identity_basis_ref": manifest_ref,
        "resolution_scope": "tenant_global_exact",
        "authority_ref": authority_ref,
        "asserted_by_ref": asserted_by_ref,
        "source_provenance_refs": list(normalized_provenance),
        "adjudication_state": "active",
        "canonical_identity_authority": True,
        "behavioral_model_authority": False,
    }

    materialized: list[EntityAliasRow] = []
    async with conn.transaction():
        existing_rows = await conn.fetch(
            """
            SELECT
              regexp_replace(lower(alias_text), '\\s+', ' ', 'g') AS normalized,
              resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id=$1
              AND valid_from <= $2
              AND (valid_until IS NULL OR valid_until > $2)
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
                    = ANY($3::text[])
            """,
            tenant_id,
            effective_at,
            list(ref_by_normalized_phrase),
        )
        for existing in existing_rows:
            stored_ref = existing["resolved_entity_ref"]
            if isinstance(stored_ref, str):
                stored_ref = json.loads(stored_ref)
            expected_ref = ref_by_normalized_phrase[existing["normalized"]]
            if stored_ref != expected_ref:
                raise ValidationError(
                    "an active exact name already maps to a different referent",
                    field="entries",
                    phrase=existing["normalized"],
                )
        for phrase, is_canonical, canonical_ref in prepared:
            row = await insert_alias_with_connection(
                conn,
                phrase=phrase,
                resolved_entity_ref=canonical_ref,
                source="ingestion",
                confidence=1.0,
                tenant_id=tenant_id,
                is_canonical=is_canonical,
                extra_metadata=metadata,
                valid_from=effective_at,
                validity_reason="founder_identity_bootstrap",
            )
            if row.resolved_entity_ref != canonical_ref:
                raise ValidationError(
                    "an active alias already maps to a different referent",
                    field="entries",
                    phrase=phrase,
                )
            row_metadata = row.entity_metadata or {}
            if row_metadata.get("identity_basis_ref") != manifest_ref:
                raise ValidationError(
                    "an active alias already exists outside this founder manifest",
                    field="entries",
                    phrase=phrase,
                )
            materialized.append(row)

    return FounderIdentityBootstrapResult(
        manifest_ref=manifest_ref,
        aliases=tuple(materialized),
    )
