"""services/platform/extensions/marketplace/registry.py — the listing lifecycle (E4).

submit (→ automated gate) → [private: published] / [public: approved → review_and_sign
→ published] → install_listing (signature-verified for public) → lifecycle.install.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from lib.extensions.manifest import ExtensionManifest
from lib.shared.ids import uuid7
from services.platform.extensions.marketplace import signing
from services.platform.extensions.marketplace.review import automated_gate


class MarketplaceError(Exception):
    def __init__(self, code: str, status: int = 400) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


def _manifest_from(row_manifest: Any) -> ExtensionManifest:
    m = row_manifest if isinstance(row_manifest, dict) else json.loads(row_manifest)
    return ExtensionManifest(
        id=m["id"], version=m.get("version", "0.0.0"), publisher=m.get("publisher", "unknown"),
        trust_tier=m.get("trust_tier", "third_party"),
        engines_fyralis_host_api=m.get("engines_fyralis_host_api", ">=1.0,<2.0"),
        contributes=tuple(m.get("contributes", [])),
        activation_events=tuple(m.get("activation_events", [])),
        feature_flag=m.get("feature_flag"), capabilities=m.get("capabilities", {}),
    )


class MarketplaceRepo:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def submit(
        self, manifest: dict[str, Any], *, submitted_by: str, visibility: str = "private",
    ) -> dict[str, Any]:
        """Submit a version. Runs the automated gate; a private listing that passes
        is published immediately, a public one is left 'approved' awaiting
        review_and_sign. A failed gate is recorded 'rejected' with the report."""
        if visibility not in ("private", "public"):
            raise MarketplaceError("invalid_visibility")
        ext_id = (manifest.get("id") or "").strip()
        if not ext_id:
            raise MarketplaceError("manifest_missing_id")
        version = manifest.get("version", "0.0.0")
        gate = automated_gate(manifest, visibility=visibility,
                              callback_url=manifest.get("callback_url"))
        if not gate.passed:
            status = "rejected"
        elif visibility == "private":
            status = "published"
        else:
            status = "approved"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO extension_listings
                  (listing_id, extension_id, version, publisher, trust_tier, visibility,
                   status, manifest, capabilities, gate_report, submitted_by, published_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,
                        CASE WHEN $7='published' THEN now() ELSE NULL END)
                ON CONFLICT (extension_id, version) DO UPDATE SET
                  publisher=EXCLUDED.publisher, trust_tier=EXCLUDED.trust_tier,
                  visibility=EXCLUDED.visibility, status=EXCLUDED.status,
                  manifest=EXCLUDED.manifest, capabilities=EXCLUDED.capabilities,
                  gate_report=EXCLUDED.gate_report, submitted_by=EXCLUDED.submitted_by,
                  submitted_at=now(), signature=NULL, signed_by=NULL,
                  reviewed_by=NULL, reviewed_at=NULL,
                  published_at=CASE WHEN EXCLUDED.status='published' THEN now() ELSE NULL END
                RETURNING listing_id, status
                """,
                uuid7(), ext_id, version, manifest.get("publisher", "unknown"),
                manifest.get("trust_tier", "third_party"), visibility, status,
                json.dumps(manifest), json.dumps(manifest.get("capabilities", {})),
                json.dumps(gate.to_dict()), submitted_by,
            )
        return {"listing_id": row["listing_id"], "extension_id": ext_id, "version": version,
                "status": row["status"], "gate": gate.to_dict()}

    async def review_and_sign(self, *, extension_id: str, version: str, reviewed_by: str) -> dict[str, Any]:
        """Human-approve + sign a public 'approved' listing → published."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT manifest, status, visibility FROM extension_listings "
                "WHERE extension_id=$1 AND version=$2", extension_id, version)
            if row is None:
                raise MarketplaceError("listing_not_found", 404)
            if row["status"] != "approved":
                raise MarketplaceError(f"not_approved (status={row['status']})", 409)
            manifest = row["manifest"] if isinstance(row["manifest"], dict) else json.loads(row["manifest"])
            sig = signing.sign(manifest)
            await conn.execute(
                "UPDATE extension_listings SET status='published', signature=$3, "
                "signed_by=$4, reviewed_by=$4, reviewed_at=now(), published_at=now() "
                "WHERE extension_id=$1 AND version=$2",
                extension_id, version, sig, reviewed_by)
        return {"extension_id": extension_id, "version": version, "status": "published",
                "signature": sig}

    async def reject(self, *, extension_id: str, version: str, reviewed_by: str, notes: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE extension_listings SET status='rejected', reviewed_by=$3, "
                "review_notes=$4, reviewed_at=now() WHERE extension_id=$1 AND version=$2",
                extension_id, version, reviewed_by, notes)

    async def get_published(self, extension_id: str, version: str | None = None) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            if version is not None:
                row = await conn.fetchrow(
                    "SELECT * FROM extension_listings WHERE extension_id=$1 AND version=$2 "
                    "AND status='published'", extension_id, version)
            else:
                row = await conn.fetchrow(
                    "SELECT * FROM extension_listings WHERE extension_id=$1 AND status='published' "
                    "ORDER BY published_at DESC LIMIT 1", extension_id)
        return dict(row) if row is not None else None

    async def list_published(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT extension_id, version, publisher, trust_tier, visibility, published_at "
                "FROM extension_listings WHERE status='published' ORDER BY extension_id, published_at DESC")
        return [dict(r) for r in rows]

    async def install_listing(
        self, *, tenant_id: UUID, extension_id: str, granted_by: str,
        version: str | None = None, requested_capabilities: Any = None,
    ) -> Any:
        """Install a PUBLISHED listing for a tenant (verifies signature for public),
        recording the consent grant via lifecycle.install."""
        listing = await self.get_published(extension_id, version)
        if listing is None:
            raise MarketplaceError("not_published", 404)
        manifest_raw = listing["manifest"]
        manifest_dict = manifest_raw if isinstance(manifest_raw, dict) else json.loads(manifest_raw)
        if listing["visibility"] == "public":
            if not signing.verify(manifest_dict, listing.get("signature")):
                raise MarketplaceError("signature_invalid", 409)
        manifest = _manifest_from(manifest_dict)
        from services.platform.extensions import lifecycle
        return await lifecycle.install(
            self.pool, tenant_id=tenant_id, manifest=manifest,
            requested_capabilities=requested_capabilities, granted_by=granted_by, enable=True)


__all__ = ["MarketplaceRepo", "MarketplaceError"]
