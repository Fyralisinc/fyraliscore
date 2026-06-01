"""lib/shared/secrets/store.py — Fernet-backed secret store.

Backed by `encrypted_secrets` (migration 0040). Every row stores a
Fernet-encrypted blob keyed by a `uuid7()` UUID; `provider_installations
.secret_ref` resolves to that UUID stringified.

Contract: see specs/IN-08-slack-production-integration/contracts/module-secret-store.md.

Constitution alignment:
  §III — every operation hand-rolls `WHERE tenant_id = $...` even
        though RLS would also filter (defense-in-depth).
  §VII — row PKs are `uuid7()` allocated app-side.
  §VIII — failures raise SecretNotFoundError / SecretStoreError
         subclasses of CompanyOSError; no bare exceptions escape.
  §X — MultiFernet rotation seam earns its keep (≥2 backends planned).
"""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.ids import uuid7


if TYPE_CHECKING:
    pass


class FernetSecretStore:
    """Concrete `SecretStore` Protocol impl.

    Two construction shapes:
        # Single key (MVP):
        store = FernetSecretStore(pool, master_kek=<32-byte base64>)

        # Multi-key (rotation):
        store = FernetSecretStore(pool, multi_fernet=MultiFernet([new, old]))

    Exactly one of `master_kek` or `multi_fernet` must be provided.

    Methods raise `SecretNotFoundError` when the ref is unknown for
    the given tenant, and `SecretStoreError` for everything else
    (DB unavailable, Fernet decrypt failure, etc.). `ValueError` for
    obviously-invalid inputs (empty label, malformed ref).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        master_kek: bytes | str | None = None,
        multi_fernet: MultiFernet | None = None,
    ) -> None:
        if (master_kek is None) == (multi_fernet is None):
            raise ValueError(
                "FernetSecretStore: exactly one of master_kek "
                "or multi_fernet must be provided"
            )
        if multi_fernet is not None:
            self._fernet: MultiFernet | Fernet = multi_fernet
        else:
            assert master_kek is not None  # narrow for type checkers
            kek = (
                master_kek.encode("ascii")
                if isinstance(master_kek, str)
                else master_kek
            )
            try:
                self._fernet = Fernet(kek)
            except (ValueError, TypeError) as exc:
                # Malformed Fernet key (wrong length, bad base64, etc.)
                # is a deployment-config bug, not a runtime condition;
                # surface it as a SecretStoreError so the gateway
                # lifespan fails-fast.
                raise SecretStoreError(
                    "Fernet key is malformed", reason="invalid_kek"
                ) from exc
        self._pool = pool

    # -----------------------------------------------------------------
    # put
    # -----------------------------------------------------------------

    async def put(
        self,
        plaintext: bytes | str,
        *,
        label: str,
        tenant_id: UUID,
    ) -> str:
        if not label:
            raise ValueError("label must be non-empty")
        if tenant_id is None:
            raise ValueError("tenant_id must be a UUID")
        plaintext_bytes = (
            plaintext.encode("utf-8")
            if isinstance(plaintext, str)
            else plaintext
        )
        ciphertext = self._fernet.encrypt(plaintext_bytes)
        row_id = uuid7()
        try:
            await self._pool.execute(
                """
                INSERT INTO encrypted_secrets
                    (id, tenant_id, label, ciphertext)
                VALUES ($1, $2, $3, $4)
                """,
                row_id,
                tenant_id,
                label,
                ciphertext,
            )
        except (asyncpg.PostgresError, OSError) as exc:
            raise SecretStoreError(
                "encrypted_secrets insert failed",
                reason="db_error",
            ) from exc
        return str(row_id)

    # -----------------------------------------------------------------
    # get
    # -----------------------------------------------------------------

    async def get(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> bytes:
        try:
            ref_uuid = UUID(ref)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"ref is not a valid UUID: {ref!r}") from exc
        try:
            row = await self._pool.fetchrow(
                """
                SELECT ciphertext
                  FROM encrypted_secrets
                 WHERE id = $1
                   AND tenant_id = $2
                """,
                ref_uuid,
                tenant_id,
            )
        except (asyncpg.PostgresError, OSError) as exc:
            raise SecretStoreError(
                "encrypted_secrets read failed",
                reason="db_error",
            ) from exc
        if row is None:
            # Cross-tenant access and never-existed are deliberately
            # indistinguishable from the caller's perspective. The
            # `unknown_installation` shape upstream relies on this.
            raise SecretNotFoundError(
                "secret ref not found for tenant",
                ref=ref,
            )
        try:
            return self._fernet.decrypt(row["ciphertext"])
        except InvalidToken as exc:
            raise SecretStoreError(
                "ciphertext decryption failed",
                reason="decrypt_failed",
            ) from exc

    # -----------------------------------------------------------------
    # rotate
    # -----------------------------------------------------------------

    async def rotate(
        self,
        ref: str,
        new_plaintext: bytes | str,
        *,
        tenant_id: UUID,
    ) -> None:
        try:
            ref_uuid = UUID(ref)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"ref is not a valid UUID: {ref!r}") from exc
        new_bytes = (
            new_plaintext.encode("utf-8")
            if isinstance(new_plaintext, str)
            else new_plaintext
        )
        if not new_bytes:
            raise ValueError("new_plaintext must be non-empty")
        new_ciphertext = self._fernet.encrypt(new_bytes)
        try:
            row = await self._pool.fetchrow(
                """
                UPDATE encrypted_secrets
                   SET ciphertext = $1,
                       rotated_at = now()
                 WHERE id = $2
                   AND tenant_id = $3
                RETURNING id
                """,
                new_ciphertext,
                ref_uuid,
                tenant_id,
            )
        except (asyncpg.PostgresError, OSError) as exc:
            raise SecretStoreError(
                "encrypted_secrets rotate failed",
                reason="db_error",
            ) from exc
        if row is None:
            raise SecretNotFoundError(
                "secret ref not found for tenant",
                ref=ref,
            )

    # -----------------------------------------------------------------
    # delete
    # -----------------------------------------------------------------

    async def delete(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> None:
        try:
            ref_uuid = UUID(ref)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"ref is not a valid UUID: {ref!r}") from exc
        try:
            await self._pool.execute(
                """
                DELETE FROM encrypted_secrets
                 WHERE id = $1
                   AND tenant_id = $2
                """,
                ref_uuid,
                tenant_id,
            )
        except (asyncpg.PostgresError, OSError) as exc:
            raise SecretStoreError(
                "encrypted_secrets delete failed",
                reason="db_error",
            ) from exc
        # No row returned check: delete is idempotent (tolerant of
        # "already deleted"). Uninstall paths rely on this.


__all__ = ["FernetSecretStore"]
