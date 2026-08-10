"""First-class OAuth facets for the native Slack and Notion connectors."""

from __future__ import annotations

import base64
import json
import os
from urllib.parse import urlencode

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
)
from services.ingest.source_contract.host_services import (
    GovernedHttpRequest,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId


SLACK_BOT_SCOPES = (
    "channels:read",
    "channels:history",
    "groups:read",
    "groups:history",
    "users:read",
    "team:read",
)
SLACK_USER_SCOPES = ("im:read", "im:history", "mpim:read", "mpim:history")


def _require_env(name: str, *, secret: bool = False) -> str:
    value = load_app_secret_text_from_env(name) if secret else os.environ.get(name, "")
    if not value:
        raise InvalidConfigurationError(
            "OAuth application configuration is incomplete",
            details={"setting": name},
        )
    return value


def _require_redirect(actual: str, configured: str) -> None:
    if actual != configured:
        raise InvalidConfigurationError(
            "OAuth callback URI does not match connector configuration"
        )


class SlackOAuthCapability:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def begin(
        self, request: OAuthBeginRequest, context: OperationContext
    ) -> AuthorizationRedirect:
        client_id = _require_env("SLACK_CLIENT_ID")
        redirect_uri = _require_env("SLACK_REDIRECT_URI")
        _require_redirect(request.redirect_uri, redirect_uri)
        scopes = request.requested_scopes or SLACK_BOT_SCOPES
        query = urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "state": request.state,
                "scope": ",".join(scopes),
                "user_scope": ",".join(SLACK_USER_SCOPES),
            }
        )
        return AuthorizationRedirect(
            url=f"https://slack.com/oauth/v2/authorize?{query}"
        )

    async def complete(
        self, request: OAuthCompleteRequest, context: OperationContext
    ) -> tuple[OAuthResult, tuple[SecretCandidate, ...]]:
        redirect_uri = _require_env("SLACK_REDIRECT_URI")
        _require_redirect(request.redirect_uri, redirect_uri)
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="POST",
                url="https://slack.com/api/oauth.v2.access",
                headers=(("content-type", "application/x-www-form-urlencoded"),),
                body=urlencode(
                    {
                        "code": request.code,
                        "client_id": _require_env("SLACK_CLIENT_ID"),
                        "client_secret": _require_env(
                            "SLACK_CLIENT_SECRET", secret=True
                        ),
                        "redirect_uri": redirect_uri,
                    }
                ).encode(),
            )
        )
        payload = json.loads(response.body)
        if response.status_code >= 400 or payload.get("ok") is not True:
            raise AuthenticationRejectedError("Slack rejected the OAuth exchange")
        team = payload.get("team") or {}
        team_id = team.get("id")
        access_token = payload.get("access_token")
        if not isinstance(team_id, str) or not isinstance(access_token, str):
            raise AuthenticationRejectedError("Slack OAuth response was incomplete")
        candidates = [
            SecretCandidate(
                slot=SlotId("oauth_access_token"),
                value=SecretValue.from_text(access_token),
            ),
            SecretCandidate(
                slot=SlotId("webhook_signing_secret"),
                value=SecretValue.from_text(
                    _require_env("SLACK_SIGNING_SECRET", secret=True)
                ),
            ),
        ]
        authed_user = payload.get("authed_user")
        user_id = None
        user_scopes = None
        if isinstance(authed_user, dict):
            user_token = authed_user.get("access_token")
            user_id = authed_user.get("id")
            user_scopes = authed_user.get("scope")
            if (
                isinstance(user_token, str)
                and user_token
                and isinstance(user_id, str)
                and user_id
            ):
                candidates.append(
                    SecretCandidate(
                        slot=SlotId("oauth_user_access_token"),
                        value=SecretValue.from_text(user_token),
                    )
                )
            else:
                user_id = None
                user_scopes = None
        scopes = tuple(
            sorted(
                (
                    set(str(payload.get("scope") or "").split(","))
                    | set(str(user_scopes or "").split(","))
                )
                - {""}
            )
        )
        return (
            OAuthResult(
                external_installation_id=team_id,
                granted_scopes=scopes,
                metadata={
                    "team_name": team.get("name"),
                    "authed_user_id": user_id,
                    "granted_user_scopes": user_scopes,
                },
            ),
            tuple(candidates),
        )

    async def refresh(
        self, request: OAuthRefreshRequest, context: OperationContext
    ) -> tuple[OAuthRefreshResult, tuple[SecretCandidate, ...]]:
        return OAuthRefreshResult(
            granted_scopes=request.requested_scopes,
            rotated=False,
            metadata={"token_model": "long_lived"},
        ), ()

    async def revoke(
        self, request: OAuthRevokeRequest, context: OperationContext
    ) -> OAuthRevocation:
        if not request.revoke_remote:
            return OAuthRevocation(complete=True, remote_revoked=False)
        token = await self._binding.services.secrets.resolve(
            SlotId("oauth_access_token")
        )
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="POST",
                url="https://slack.com/api/auth.revoke",
                headers=(("authorization", f"Bearer {token.reveal_text()}"),),
            )
        )
        payload = json.loads(response.body)
        complete = response.status_code < 400 and payload.get("ok") is True
        return OAuthRevocation(
            complete=complete,
            remote_revoked=complete,
            reason_code="complete" if complete else "provider_rejected",
        )


class NotionOAuthCapability:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def begin(
        self, request: OAuthBeginRequest, context: OperationContext
    ) -> AuthorizationRedirect:
        redirect_uri = _require_env("NOTION_REDIRECT_URI")
        _require_redirect(request.redirect_uri, redirect_uri)
        query = urlencode(
            {
                "client_id": _require_env("NOTION_CLIENT_ID"),
                "response_type": "code",
                "owner": "user",
                "redirect_uri": redirect_uri,
                "state": request.state,
            }
        )
        return AuthorizationRedirect(
            url=f"https://api.notion.com/v1/oauth/authorize?{query}"
        )

    async def complete(
        self, request: OAuthCompleteRequest, context: OperationContext
    ) -> tuple[OAuthResult, tuple[SecretCandidate, ...]]:
        redirect_uri = _require_env("NOTION_REDIRECT_URI")
        _require_redirect(request.redirect_uri, redirect_uri)
        raw_credentials = (
            f"{_require_env('NOTION_CLIENT_ID')}:"
            f"{_require_env('NOTION_CLIENT_SECRET', secret=True)}"
        ).encode()
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="POST",
                url="https://api.notion.com/v1/oauth/token",
                headers=(
                    (
                        "authorization",
                        f"Basic {base64.b64encode(raw_credentials).decode()}",
                    ),
                    ("content-type", "application/json"),
                ),
                body=json.dumps(
                    {
                        "grant_type": "authorization_code",
                        "code": request.code,
                        "redirect_uri": redirect_uri,
                    }
                ).encode(),
            )
        )
        payload = json.loads(response.body)
        workspace_id = payload.get("workspace_id")
        access_token = payload.get("access_token")
        if (
            response.status_code >= 400
            or not isinstance(workspace_id, str)
            or not isinstance(access_token, str)
        ):
            raise AuthenticationRejectedError("Notion rejected the OAuth exchange")
        return (
            OAuthResult(
                external_installation_id=workspace_id,
                metadata={
                    "workspace_name": payload.get("workspace_name"),
                    "bot_id": payload.get("bot_id"),
                },
            ),
            (
                SecretCandidate(
                    slot=SlotId("oauth_access_token"),
                    value=SecretValue.from_text(access_token),
                ),
            ),
        )

    async def refresh(
        self, request: OAuthRefreshRequest, context: OperationContext
    ) -> tuple[OAuthRefreshResult, tuple[SecretCandidate, ...]]:
        return OAuthRefreshResult(
            granted_scopes=request.requested_scopes,
            rotated=False,
            metadata={"token_model": "long_lived"},
        ), ()

    async def revoke(
        self, request: OAuthRevokeRequest, context: OperationContext
    ) -> OAuthRevocation:
        return OAuthRevocation(
            complete=True,
            remote_revoked=False,
            reason_code="local_revocation_required",
        )


__all__ = [
    "NotionOAuthCapability",
    "SLACK_BOT_SCOPES",
    "SLACK_USER_SCOPES",
    "SlackOAuthCapability",
]
