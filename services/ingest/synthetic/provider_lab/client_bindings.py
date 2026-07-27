"""Subprocess-safe production-client bindings for Provider Lab.

The validation harness starts Provider Lab in a separate process.  Python
monkeypatches therefore cannot cross the process boundary: production client
builders need concrete, serializable loopback transports instead.

Only protocol boundaries that cannot use a regular endpoint environment
override live here:

* Telegram uses the finite ``TelegramTransport`` surface rather than a fake
  MTProto server.
* AWS still uses the real SigV4 client, with deterministic installation-scoped
  static credentials supplied through the normal secret-store interface.
"""

from __future__ import annotations

import json
from typing import Any

import httpx


class ProviderLabJsonSecretStore:
    """Minimal read-only secret store containing one JSON object."""

    def __init__(self, material: dict[str, Any]) -> None:
        self._value = json.dumps(material, separators=(",", ":"), sort_keys=True)

    async def get(self, _secret_ref: str, *, tenant_id: Any) -> str:
        del tenant_id
        return self._value


class ProviderLabTelegramTransport:
    """HTTP adapter for Provider Lab's deliberately finite Telegram surface."""

    def __init__(
        self,
        *,
        root_url: str,
        session: str,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._root_url = root_url.rstrip("/")
        self._session = session
        self._http = http_client
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def is_user_authorized(self) -> bool:
        return self._connected

    async def _post(
        self,
        operation: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self._http.post(
            f"{self._root_url}/telegram/transport/{operation}",
            headers={"Authorization": f"Session {self._session}"},
            json=body,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("Provider Lab Telegram response must be a JSON object")
        return payload

    async def get_history(
        self,
        *,
        dialog_id: int,
        access_hash: int | None,
        dialog_kind: str,
        offset_id: int,
        min_id: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int | None, bool]:
        payload = await self._post(
            "get_history",
            {
                "dialog_id": dialog_id,
                "access_hash": access_hash,
                "dialog_kind": dialog_kind,
                "offset_id": offset_id,
                "min_id": min_id,
                "limit": limit,
            },
        )
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise TypeError("Provider Lab Telegram history omitted messages")
        return (
            [dict(message) for message in messages if isinstance(message, dict)],
            payload.get("next_offset_id"),
            bool(payload.get("is_last")),
        )

    async def iter_dialogs(self, *, limit: int) -> list[dict[str, Any]]:
        payload = await self._post("iter_dialogs", {"limit": limit})
        dialogs = payload.get("dialogs")
        if not isinstance(dialogs, list):
            raise TypeError("Provider Lab Telegram dialog response omitted dialogs")
        return [dict(dialog) for dialog in dialogs if isinstance(dialog, dict)]

    async def has_history_since(
        self,
        *,
        dialog_id: int,
        access_hash: int | None,
        dialog_kind: str,
        min_id: int,
    ) -> bool:
        payload = await self._post(
            "has_history_since",
            {
                "dialog_id": dialog_id,
                "access_hash": access_hash,
                "dialog_kind": dialog_kind,
                "min_id": min_id,
            },
        )
        return bool(payload.get("has_history"))

    async def me(self) -> dict[str, Any]:
        return await self._post("me", {})

    async def disconnect(self) -> None:
        # The shared httpx client belongs to the fetcher process, not this
        # installation transport.
        self._connected = False


__all__ = [
    "ProviderLabJsonSecretStore",
    "ProviderLabTelegramTransport",
]
