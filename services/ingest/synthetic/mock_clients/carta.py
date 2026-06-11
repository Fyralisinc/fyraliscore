"""MockCartaClient — Carta /v1alpha1 read surface used by IN-CARTA backfill.

In-process replacement for `CartaClient` at the `_open_carta_client`
fetcher seam (services/ingest/ingestion/fetchers/carta.py). Implements ONLY
what the backfill/poll fetcher + reconciler call:

  - list_entity(entity_type, page_size=..., page_token=..., modified_after=...)
        -> (rows, next_page_token | None)
  - list_stakeholders / list_share_classes / list_option_grants /
    list_convertible_notes  (the real client's thin per-collection wrappers;
    the reconciler probes via list_option_grants)
  - list_issuers / get_issuer / probe (connectivity + issuer enumeration)

Pagination mirrors the real client's AIP-158 token semantics exactly so the
fetcher's cursor loop behaves identically:
  - `page_token` is an opaque token THIS mock minted (`"off:<n>"`, an offset
    into the filtered row list); None/"" = first page.
  - The returned next token is None when the page exhausts the collection —
    matching `client._decode_page` ("absent/empty nextPageToken is terminal").

Incremental filter: the fetcher passes `modified_after` (the real
`lastModifiedDatetimeAfter` param) ONLY for `optionGrant` — like the real
client, any other entity_type raises ValueError. The mock honours the bound by
dropping grants whose `lastModifiedDatetime.value` is not STRICTLY greater.
TODO(human): the rendered docs do not state whether the real bound is
inclusive ("on or after") or exclusive; strictly-greater is what the
reconciler's gap-probe termination assumes, and external_id digest dedup makes
either semantics write-safe. Verify on first real (partner-gated) traffic.

Fault injection: `self._check_fault()` runs first on every public method and the
four raisers surface `CartaApiError` with the same stable `code`s the real
client emits, so the fetcher's rate-limit branch (and reconciler error mapping)
see exactly the production exception shape.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.carta.client import (
    ENTITY_COLLECTIONS,
    CartaApiError,
)
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


_DEFAULT_PAGE_SIZE = 50


class MockCartaClient(_MockBase):
    """Stateful in-process replacement for `CartaClient`.

    `fixture` shape (per `make_carta`):
        {
          "firm_id": "<issuer id>",
          "page_size": 50,
          "issuer": {"id": "...", "legalName": "..."},
          "entities": {
            "stakeholder":     [ {<v1alpha1 Stakeholder>}, ... ],
            "shareClass":      [ ... ],
            "optionGrant":     [ ... ],
            "convertibleNote": [ ... ],
          },
        }
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._issuer_id = str(fixture.get("firm_id", ""))
        self._page_cap = int(fixture.get("page_size", _DEFAULT_PAGE_SIZE))

    # ---- Issuer enumeration / probe (oauth wizard + reconciler parity) ----

    async def list_issuers(
        self,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """`GET /v1alpha1/issuers` analogue — the single fixture issuer."""
        self._check_fault()
        issuer = self._issuer()
        return ([issuer] if issuer else []), None

    async def get_issuer(self, issuer_id: str | None = None) -> dict[str, Any]:
        """`GET /v1alpha1/issuers/{id}` analogue (visibility check)."""
        self._check_fault()
        target = issuer_id or self._issuer_id
        issuer = self._issuer()
        if issuer and str(issuer.get("id")) == str(target):
            return issuer
        raise CartaApiError(
            "MockCartaClient: issuer not found or not visible",
            code="carta_api_not_found",
            context={"http_status": 404},
        )

    async def probe(self) -> dict[str, Any]:
        """`GET /v1alpha1/issuers?pageSize=1` analogue (connectivity probe)."""
        self._check_fault()
        issuer = self._issuer()
        return {"issuers": [issuer] if issuer else []}

    # ---- IN-CARTA read surface ----

    async def list_entity(
        self,
        entity_type: str,
        *,
        page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
        modified_after: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One AIP-158 page of `GET /v1alpha1/issuers/{issuer}/{collection}`
        rows + the opaque next page token (None = terminal).

        ValueError parity with the real client: unknown `entity_type`, or
        `modified_after` for any entity_type other than "optionGrant".
        """
        self._check_fault()
        if entity_type not in ENTITY_COLLECTIONS:
            raise ValueError(f"unknown carta entity_type {entity_type!r}")
        if modified_after is not None and entity_type != "optionGrant":
            raise ValueError(
                "lastModifiedDatetimeAfter is only supported for optionGrant",
            )

        rows_all = self._entities_for(entity_type)
        if modified_after is not None:
            rows_all = [
                r for r in rows_all
                if (self._last_modified(r) or "") > modified_after
            ]

        per_page = min(max(1, page_size), self._page_cap)
        offset = self._decode_token(page_token)
        page = rows_all[offset:offset + per_page]
        end = offset + len(page)
        next_token = None if end >= len(rows_all) or not page else f"off:{end}"
        return page, next_token

    async def list_stakeholders(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "stakeholder", page_size=page_size, page_token=page_token,
        )

    async def list_share_classes(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "shareClass", page_size=page_size, page_token=page_token,
        )

    async def list_option_grants(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None, modified_after: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "optionGrant", page_size=page_size, page_token=page_token,
            modified_after=modified_after,
        )

    async def list_convertible_notes(
        self, *, page_size: int = _DEFAULT_PAGE_SIZE,
        page_token: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self.list_entity(
            "convertibleNote", page_size=page_size, page_token=page_token,
        )

    # ---- Helpers ----

    def _issuer(self) -> dict[str, Any]:
        issuer = self._fixture.get("issuer")
        if isinstance(issuer, dict) and issuer.get("id"):
            return issuer
        if self._issuer_id:
            return {
                "id": self._issuer_id,
                "legalName": f"Synthetic Issuer {self._issuer_id[:8]}",
            }
        return {}

    def _entities_for(self, entity_type: str) -> list[dict[str, Any]]:
        ents = self._fixture.get("entities", {})
        rows = ents.get(entity_type, [])
        return list(rows) if isinstance(rows, list) else []

    @staticmethod
    def _decode_token(token: str | None) -> int:
        if not token:
            return 0
        if token.startswith("off:"):
            try:
                return max(0, int(token[4:]))
            except ValueError:
                pass
        raise CartaApiError(
            "MockCartaClient: malformed pageToken",
            code="carta_api_error",
            context={"http_status": 400},
        )

    @staticmethod
    def _last_modified(row: dict[str, Any]) -> str | None:
        wrapper = row.get("lastModifiedDatetime")
        if isinstance(wrapper, dict):
            v = wrapper.get("value")
            return v if isinstance(v, str) else None
        return None

    # ---- Fault raisers (production exception parity) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise CartaApiError(
            "MockCartaClient: rate limit 429 (X2 fault)",
            code="carta_api_rate_limited",
            context={"http_status": 429, "retry_after": "1"},
        )

    def _raise_5xx(self) -> NoReturn:
        raise CartaApiError(
            "MockCartaClient: 503 (X2 fault)",
            code="carta_api_error",
            context={"http_status": 503},
        )

    def _raise_auth_error(self) -> NoReturn:
        raise CartaApiError(
            "MockCartaClient: 401 access token rejected (X2 fault)",
            code="carta_api_unauthorized",
            context={"http_status": 401},
        )

    def _raise_transient(self) -> NoReturn:
        raise CartaApiError(
            "MockCartaClient: transient transport error (X2 fault)",
            code="carta_api_error",
            context={"error_type": "TransportError"},
        )


__all__ = ["MockCartaClient"]
