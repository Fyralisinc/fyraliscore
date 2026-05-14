"""lib.shared.secrets — envelope-encrypted secret store.

Public surface:

    SecretStore        — Protocol (typing only; FernetSecretStore is the
                         concrete MVP impl).
    FernetSecretStore  — Fernet-backed implementation, backed by the
                         encrypted_secrets table (migration 0040).
    build_secret_store — Factory wiring MASTER_KEK into a FernetSecretStore.
    SecretStoreError   — backend / decrypt failures (HTTP 503 upstream).
    SecretNotFoundError — ref unknown for tenant (HTTP 401-shape upstream
                         for webhook signature paths).

Contract: see specs/IN-08-slack-production-integration/contracts/module-secret-store.md.
"""
from __future__ import annotations

import os
import warnings
from collections.abc import Callable
from typing import Protocol, runtime_checkable
from uuid import UUID

import asyncpg
from cryptography.fernet import Fernet

from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.secrets.store import FernetSecretStore


@runtime_checkable
class SecretStore(Protocol):
    """Envelope-encrypted secret store contract.

    Backing table is `encrypted_secrets`. All operations are
    tenant-scoped at the SQL layer; RLS provides defense-in-depth.
    """

    async def put(
        self,
        plaintext: bytes | str,
        *,
        label: str,
        tenant_id: UUID,
    ) -> str:
        """Persist plaintext encrypted-at-rest. Returns an opaque ref
        (stringified UUID) callers persist in their domain rows."""
        ...

    async def get(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> bytes:
        """Resolve ref → plaintext. Raises SecretNotFoundError if
        unknown for this tenant; SecretStoreError on backend failure."""
        ...

    async def rotate(
        self,
        ref: str,
        new_plaintext: bytes | str,
        *,
        tenant_id: UUID,
    ) -> None:
        """Replace ciphertext for ref; ref is stable across rotations."""
        ...

    async def delete(
        self,
        ref: str,
        *,
        tenant_id: UUID,
    ) -> None:
        """Remove the ciphertext row. Idempotent (no error if missing)."""
        ...


def build_secret_store(
    pool: asyncpg.Pool,
    *,
    master_kek_loader: Callable[[], bytes] | None = None,
) -> SecretStore:
    """Construct a production FernetSecretStore.

    Key resolution:

    1. If `master_kek_loader` is provided, call it. The result is the
       URL-safe-base64-encoded 32-byte Fernet key.
    2. Otherwise read the `MASTER_KEK` env var directly.

    In production (`FYRALIS_ENV=prod`):
        Missing/empty key → SecretStoreError (fail-startup).

    In dev (`FYRALIS_ENV` unset or anything other than 'prod'):
        Missing key → generate a one-shot in-memory key, log a
        structured warning, continue. This keeps local dev loops
        from being blocked on operational secret material.
    """
    if master_kek_loader is not None:
        raw = master_kek_loader()
    else:
        env_val = os.environ.get("MASTER_KEK", "")
        raw = env_val.encode("ascii") if env_val else b""

    if not raw:
        # No KEK configured. Behavior split on environment.
        env = os.environ.get("FYRALIS_ENV", "").lower()
        if env == "prod":
            raise SecretStoreError(
                "MASTER_KEK is unset or empty in production environment",
                reason="missing_kek",
            )
        # Dev: generate a one-shot in-memory key. Loud structured
        # warning so it can't sneak through to staging unnoticed.
        warnings.warn(
            "MASTER_KEK unset; generating a one-shot in-memory Fernet "
            "key for dev. Encrypted secrets will not survive process "
            "restart. Set MASTER_KEK from the deployment secret manager "
            "to use a stable key.",
            stacklevel=2,
        )
        raw = Fernet.generate_key()

    return FernetSecretStore(pool, master_kek=raw)


__all__ = [
    "SecretStore",
    "FernetSecretStore",
    "build_secret_store",
    "SecretStoreError",
    "SecretNotFoundError",
]
