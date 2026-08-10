"""Reusable OAuth 2.0 capability for REST source connectors."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import json
import os
from typing import Any, Literal
from urllib.parse import parse_qs, urlencode

from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.source_contract.capabilities.installation import (
    AuthorizationRedirect,
    OAuthBeginRequest,
    OAuthCompleteRequest,
    OAuthRefreshRequest,
    OAuthRefreshResult,
    OAuthResult,
    OAuthRevokeRequest,
    OAuthRevocation,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    InvalidConfigurationError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import (
    GovernedHttpRequest,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId


@dataclass(frozen=True)
class OAuthProviderSpec:
    source: str
    authorize_url: str
    token_url: str
    scopes: tuple[str, ...]
    access_slot: str = "oauth_access_token"
    refresh_slot: str | None = "oauth_refresh_token"
    identity_url: str | None = None
    external_id_paths: tuple[str, ...] = (
        "id",
        "sub",
        "email",
        "login",
        "organization.id",
    )
    token_auth: Literal["body", "basic"] = "body"
    client_env_prefix: str | None = None
    redirect_env_prefix: str | None = None
    extra_authorize: tuple[tuple[str, str], ...] = ()
    revoke_url: str | None = None

    @property
    def client_prefix(self) -> str:
        return self.client_env_prefix or self.source.upper()

    @property
    def redirect_prefix(self) -> str:
        return self.redirect_env_prefix or self.source.upper()


def _setting(name: str, *, secret: bool = False) -> str:
    value = load_app_secret_text_from_env(name) if secret else os.environ.get(name, "")
    if not value:
        raise InvalidConfigurationError(
            "OAuth application configuration is incomplete",
            details={"setting": name},
        )
    return value


def _nested(value: Any, path: str) -> Any:
    current = value
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _payload(body: bytes) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            parsed = parse_qs(body.decode())
        except (UnicodeDecodeError, ValueError) as exc:
            raise TransientSourceError("OAuth provider returned malformed data") from exc
        value = {key: items[-1] for key, items in parsed.items() if items}
    if not isinstance(value, dict):
        raise TransientSourceError("OAuth provider returned a non-object response")
    return value


class StandardOAuthCapability:
    def __init__(self, binding: BindingContext, spec: OAuthProviderSpec) -> None:
        self._binding = binding
        self._spec = spec

    def _client_id(self) -> str:
        return _setting(f"{self._spec.client_prefix}_CLIENT_ID")

    def _client_secret(self) -> str:
        return _setting(f"{self._spec.client_prefix}_CLIENT_SECRET", secret=True)

    def _redirect_uri(self) -> str:
        return _setting(f"{self._spec.redirect_prefix}_REDIRECT_URI")

    async def begin(
        self,
        request: OAuthBeginRequest,
        context: OperationContext,
    ) -> AuthorizationRedirect:
        redirect = self._redirect_uri()
        if request.redirect_uri != redirect:
            raise InvalidConfigurationError("OAuth redirect URI does not match")
        scopes = request.requested_scopes or self._spec.scopes
        query = {
            "client_id": self._client_id(),
            "redirect_uri": redirect,
            "response_type": "code",
            "state": request.state,
            "scope": " ".join(scopes),
            **dict(self._spec.extra_authorize),
        }
        return AuthorizationRedirect(
            url=f"{self._spec.authorize_url}?{urlencode(query)}"
        )

    def _token_request(
        self,
        values: dict[str, str],
    ) -> GovernedHttpRequest:
        headers = [("accept", "application/json"), ("content-type", "application/x-www-form-urlencoded")]
        if self._spec.token_auth == "basic":
            raw = f"{self._client_id()}:{self._client_secret()}".encode()
            headers.append(("authorization", f"Basic {base64.b64encode(raw).decode()}"))
        else:
            values.update(
                client_id=self._client_id(),
                client_secret=self._client_secret(),
            )
        return GovernedHttpRequest(
            method="POST",
            url=self._spec.token_url,
            headers=tuple(headers),
            body=urlencode(values).encode(),
        )

    async def _exchange(
        self,
        context: OperationContext,
        values: dict[str, str],
    ) -> dict[str, Any]:
        response = await context.services.http.send(self._token_request(values))
        payload = _payload(response.body)
        if response.status_code >= 400 or payload.get("error"):
            raise AuthenticationRejectedError(
                f"{self._spec.source} rejected the OAuth exchange"
            )
        return payload

    async def _identity(
        self,
        context: OperationContext,
        access_token: str,
    ) -> dict[str, Any]:
        if self._spec.identity_url is None:
            return {}
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="GET",
                url=self._spec.identity_url,
                headers=(
                    ("authorization", f"Bearer {access_token}"),
                    ("accept", "application/json"),
                ),
            )
        )
        if response.status_code >= 400:
            raise AuthenticationRejectedError(
                f"{self._spec.source} rejected the identity lookup"
            )
        return _payload(response.body)

    async def complete(
        self,
        request: OAuthCompleteRequest,
        context: OperationContext,
    ) -> tuple[OAuthResult, tuple[SecretCandidate, ...]]:
        redirect = self._redirect_uri()
        if request.redirect_uri != redirect:
            raise InvalidConfigurationError("OAuth redirect URI does not match")
        token = await self._exchange(
            context,
            {
                "grant_type": "authorization_code",
                "code": request.code,
                "redirect_uri": redirect,
            },
        )
        access = token.get("access_token")
        if not isinstance(access, str) or not access:
            raise AuthenticationRejectedError("OAuth response omitted access_token")
        identity = await self._identity(context, access)
        combined = {
            **identity,
            **token,
            **dict(request.callback_parameters),
        }
        external = next(
            (
                str(value)
                for path in self._spec.external_id_paths
                if (value := _nested(combined, path)) not in (None, "")
            ),
            None,
        )
        if external is None:
            raise AuthenticationRejectedError(
                "OAuth provider response omitted installation identity"
            )
        candidates = [
            SecretCandidate(
                slot=SlotId(self._spec.access_slot),
                value=SecretValue.from_text(access),
            )
        ]
        refresh = token.get("refresh_token")
        if self._spec.refresh_slot and isinstance(refresh, str) and refresh:
            candidates.append(
                SecretCandidate(
                    slot=SlotId(self._spec.refresh_slot),
                    value=SecretValue.from_text(refresh),
                )
            )
        raw_scope = token.get("scope")
        scopes = (
            tuple(part for part in str(raw_scope).replace(",", " ").split() if part)
            if raw_scope
            else self._spec.scopes
        )
        return (
            OAuthResult(
                external_installation_id=external,
                granted_scopes=tuple(sorted(set(scopes))),
                metadata={
                    key: value
                    for key, value in combined.items()
                    if key not in {"access_token", "refresh_token", "id_token"}
                },
            ),
            tuple(candidates),
        )

    async def refresh(
        self,
        request: OAuthRefreshRequest,
        context: OperationContext,
    ) -> tuple[OAuthRefreshResult, tuple[SecretCandidate, ...]]:
        if self._spec.refresh_slot is None:
            return OAuthRefreshResult(
                granted_scopes=request.requested_scopes,
                rotated=False,
                metadata={"token_model": "non_refreshable"},
            ), ()
        refresh = await self._binding.services.secrets.resolve(
            SlotId(self._spec.refresh_slot)
        )
        payload = await self._exchange(
            context,
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh.reveal_text(),
            },
        )
        access = payload.get("access_token")
        if not isinstance(access, str) or not access:
            raise AuthenticationRejectedError("OAuth refresh omitted access_token")
        candidates = (
            SecretCandidate(
                slot=SlotId(self._spec.access_slot),
                value=SecretValue.from_text(access),
            ),
        )
        return OAuthRefreshResult(
            granted_scopes=request.requested_scopes or self._spec.scopes,
            rotated=True,
        ), candidates

    async def revoke(
        self,
        request: OAuthRevokeRequest,
        context: OperationContext,
    ) -> OAuthRevocation:
        if not request.revoke_remote or self._spec.revoke_url is None:
            return OAuthRevocation(
                complete=True,
                remote_revoked=False,
                reason_code="local_revocation",
            )
        access = await self._binding.services.secrets.resolve(
            SlotId(self._spec.access_slot)
        )
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="POST",
                url=self._spec.revoke_url,
                headers=(("authorization", f"Bearer {access.reveal_text()}"),),
            )
        )
        complete = response.status_code < 400
        return OAuthRevocation(
            complete=complete,
            remote_revoked=complete,
            reason_code="complete" if complete else "provider_rejected",
        )


__all__ = ["OAuthProviderSpec", "StandardOAuthCapability"]
